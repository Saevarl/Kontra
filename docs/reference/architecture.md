# Architecture Reference

Internal design for contributors. For usage, see [Getting Started](../getting-started.md).

## Design Principles

1. **Measurement, not decision**: Kontra returns violation counts; consumers interpret them
2. **Speed over ceremony**: Metadata first, scan only when necessary
3. **Semantic honesty**: Different execution paths have different guarantees (documented, not hidden)
4. **Automation-ready**: Built for developers first, with pipelines and agents in mind

## Core Concept

**Kontra is a measurement engine, not a decision engine.**

A **rule** measures a property of a dataset and returns a violation count. Kontra does not decide what constitutes "failure"—that belongs to consumers (CLI, CI pipelines, agents, dashboards).

| Concept | Engine responsibility | Consumer responsibility |
|---------|----------------------|------------------------|
| Violation count | Measure it | Decide if acceptable |
| Severity | Attach as metadata | Interpret (block, warn, ignore) |
| Exit codes | Not applicable | CLI maps blocking → exit 1 |

## Execution Model

Kontra has two fast paths:

| Path | What happens | When used |
|------|--------------|-----------|
| **Preplan** | Proves rules from metadata (no scan) | When metadata is available and sufficient |
| **Pushdown** | Runs batched SQL in the engine | When rules can't be proven from metadata |

If pushdown is unavailable or disabled, Kontra falls back to local execution (Polars).

### Execution Flow

```
Contract YAML → Parse (Pydantic) → Build Rules (Factory) → Compile Plan
    ↓
Preplan: Attempt metadata resolution (Parquet stats, pg_stats)
    ↓
Pushdown: Batch remaining rules into SQL (DuckDB/Postgres/SQL Server)
    ↓
Fallback: Execute residual rules in Polars
    ↓
Merge results (deterministic order) → Report
```

### Preplan (Metadata Resolution)

Zero-scan validation using file/database metadata.

**Parquet:** Reads row-group statistics from file footer:
- `null_count`: Proves `not_null` rules
- `min`/`max`: Proves `range` rules
- `num_rows`: Proves `min_rows`/`max_rows` rules

**PostgreSQL:** Reads `pg_stats` catalog (requires `ANALYZE`).

**SQL Server:** Reads `sys.dm_db_stats_histogram` (more limited).

**Execution source:** `metadata`

### Pushdown (SQL Execution)

Batches rules into SQL queries. Two strategies based on tally mode:

**tally=False (fail-fast):** EXISTS checks, stops at first violation
```sql
SELECT
  EXISTS(SELECT 1 FROM data WHERE "user_id" IS NULL) AS "COL:user_id:not_null",
  EXISTS(SELECT 1 FROM data WHERE "status" NOT IN ('active','inactive')) AS "COL:status:allowed_values"
```

**tally=True (exact counts):** Aggregates, counts all violations
```sql
SELECT
  SUM(CASE WHEN "user_id" IS NULL THEN 1 ELSE 0 END) AS "COL:user_id:not_null",
  COUNT(*) - COUNT(DISTINCT "email") AS "COL:email:unique"
FROM data;
```

**SQL engine by data source:**

| Data Source | Engine |
|-------------|--------|
| Local Parquet/CSV | DuckDB |
| S3 Parquet | DuckDB |
| Azure ADLS (`abfss://`) | DuckDB |
| `postgres://` | PostgreSQL |
| `mssql://` | SQL Server |

DuckDB is a core dependency—it powers local file execution.

**Execution source:** `sql`

### Polars Fallback

In-memory validation for rules that can't be handled by metadata or SQL:
- Complex regex patterns
- Custom validation logic
- When pushdown is disabled

Uses column projection to load only needed columns.

**Execution source:** `polars`

---

## Source Structure

```
src/kontra/
├── __init__.py           # Public Python API
├── api/
│   ├── results.py        # ValidationResult, RuleResult, Diff
│   └── rules.py          # rules.not_null(), rules.unique(), etc.
├── cli/
│   └── main.py           # validate, profile, init commands
├── config/
│   ├── loader.py         # Contract loading (file, S3)
│   └── models.py         # Pydantic models
├── connectors/
│   ├── handle.py         # DatasetHandle (unified data source)
│   ├── postgres.py       # PostgreSQL connection
│   └── sqlserver.py      # SQL Server connection
├── engine/
│   ├── engine.py         # ValidationEngine orchestrator
│   ├── sql_utils.py      # Shared SQL generation
│   ├── executors/        # SQL pushdown
│   │   ├── duckdb_sql.py
│   │   ├── postgres_sql.py
│   │   └── sqlserver_sql.py
│   ├── materializers/    # Data loading with projection
│   │   ├── duckdb.py
│   │   ├── postgres.py
│   │   └── sqlserver.py
│   └── backends/
│       └── polars_backend.py
├── preplan/              # Metadata resolution
│   ├── planner.py        # Parquet row-group analysis
│   ├── postgres.py       # pg_stats analysis
│   └── sqlserver.py      # sys.columns analysis
├── rule_defs/            # Rule definitions
│   ├── base.py           # BaseRule abstract class
│   ├── factory.py        # Rule instantiation
│   ├── registry.py       # Rule registration
│   ├── execution_plan.py # CompiledPlan
│   └── builtin/          # 18 built-in rules
├── scout/                # Dataset profiling
│   ├── profiler.py       # ScoutProfiler
│   ├── suggest.py        # Rule suggestion
│   └── backends/         # DuckDB, PostgreSQL, SQL Server
├── state/                # Validation history
│   └── backends/         # local, s3, postgres
├── reporters/
│   ├── rich_reporter.py
│   └── json_reporter.py
└── errors.py             # Error types
```

---

## Key Components

### DatasetHandle

Unified abstraction for all data sources:

```python
handle = DatasetHandle.from_uri("postgres://user:pass@host/db/schema.table")
handle = DatasetHandle.from_uri("s3://bucket/data.parquet")
handle = DatasetHandle.from_uri("data/local.csv")

handle.scheme    # "postgres", "s3", "file"
handle.uri       # Original URI
handle.db_params # Database connection params
handle.fs_opts   # S3/cloud credentials
```

### CompiledPlan

Rules compiled into execution plan:

```python
plan = RuleExecutionPlan(rules)
compiled = plan.compile()

compiled.predicates      # Vectorizable Polars expressions
compiled.fallback_rules  # Rules requiring full DataFrame
compiled.required_cols   # Columns needed (for projection)
compiled.sql_specs       # SQL pushdown specifications
```

### SQL Utilities

Dialect-aware SQL generation:

```python
from kontra.engine.sql_utils import (
    esc_ident,           # "name" vs [name]
    agg_not_null,        # SUM(CASE WHEN col IS NULL...)
    agg_unique,          # COUNT(*) - COUNT(DISTINCT col)
    exists_not_null,     # EXISTS(SELECT 1 WHERE col IS NULL)
)

# Dialect examples
agg_not_null("user_id", "rule_1", dialect="postgres")
# → SUM(CASE WHEN "user_id" IS NULL THEN 1 ELSE 0 END) AS "rule_1"

agg_not_null("user_id", "rule_1", dialect="mssql")
# → SUM(CASE WHEN [user_id] IS NULL THEN 1 ELSE 0 END) AS [rule_1]
```

---

## Guarantees

### What Kontra Guarantees

- **Path agreement**: If preplan says "pass", pushdown and Polars will agree
- **Deterministic results**: Same input → same output (except `freshness` rule)
- **Stable rule IDs**: Derived consistently from name + column

### What Kontra Does Not Guarantee

- **Exact counts from preplan**: Returns "≥1 violation", not exact count
- **Metadata availability**: Parquet stats depend on writer; pg_stats depends on ANALYZE
- **Identical SQL behavior**: DuckDB/PostgreSQL/SQL Server may differ on edge cases

---

## Adding a New Rule

1. Create rule class in `src/kontra/rule_defs/builtin/`:

```python
from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
import polars as pl

@register_rule("positive")
class PositiveRule(BaseRule):
    def validate(self, df):
        column = self.params["column"]
        mask = df[column].is_null() | (df[column] <= 0)
        return self._failures(df, mask, f"{column} must be positive")

    def compile_predicate(self):
        column = self.params["column"]
        return Predicate(
            rule_id=self.rule_id,
            expr=pl.col(column).is_null() | (pl.col(column) <= 0),
            message=f"{column} must be positive",
            columns={column},
        )

    def to_sql_spec(self):
        return {
            "kind": "positive",
            "rule_id": self.rule_id,
            "column": self.params["column"],
            "tally": self.params.get("tally", False),
        }
```

2. Add SQL support in `sql_utils.py` and executors.

3. Add helper in `api/rules.py`:

```python
def positive(column: str, **kwargs) -> Dict[str, Any]:
    return {"name": "positive", "params": {"column": column}, **kwargs}
```

---

## Adding a New Data Source

1. Create connector in `src/kontra/connectors/`
2. Extend `DatasetHandle.from_uri()` in `handle.py`
3. Create materializer in `src/kontra/engine/materializers/`
4. Create executor in `src/kontra/engine/executors/`
5. Optionally add preplan in `src/kontra/preplan/`

---

## Dependencies

| Source | Extra | Notes |
|--------|-------|-------|
| Parquet/CSV | (built-in) | DuckDB always available |
| S3 | `kontra[s3]` | Requires s3fs |
| Azure ADLS | (built-in) | DuckDB azure extension |
| PostgreSQL | `kontra[postgres]` | Requires psycopg |
| SQL Server | `kontra[sqlserver]` | Requires pymssql |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KONTRA_VERBOSE` | Verbose error output |
| `KONTRA_IO_DEBUG` | I/O metrics in stats |
| `PGHOST`, `PGPORT`, etc. | PostgreSQL connection |
| `AWS_ACCESS_KEY_ID` | S3 credentials |
| `AWS_ENDPOINT_URL` | MinIO/custom S3 endpoint |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure storage account |
| `AZURE_STORAGE_ACCESS_KEY` | Azure account key |

---

## Tests

```bash
pytest                    # Full suite
pytest -q                 # Quick run
pytest -m slow            # Large datasets (1M+ rows)
pytest -m integration     # End-to-end tests
pytest -m pushdown        # SQL pushdown tests
```
