# Architecture Reference

Internal design for contributors. For usage docs, see [Getting Started](../getting-started.md).

## Design Principles

1. **Measurement, not decision**: Kontra returns violation counts; consumers interpret them
2. **Semantic honesty**: Different execution tiers have different guarantees (documented, not hidden)
3. **Speed over ceremony**: Metadata-first, scan only when necessary
4. **Agentic-first**: Built for LLM integration from the ground up
5. **Progressive disclosure**: Simple surface, infinite depth

## Core Concepts

**Kontra is a measurement engine, not a decision engine.**

A **rule** is a deterministic function that measures a property of a dataset and returns a violation count.

When executed, a rule produces:

- A **violation count** (how many rows/values violate the property)
- **Execution metadata** (which tier resolved it, timing)
- **Classification hints** (severity tag)

Kontra does not decide what constitutes "failure" or trigger alerts. Those decisions belong to the consuming system—whether that's the CLI, a CI pipeline, an agent, or a dashboard.

This separation matters:

| Concept | Engine responsibility | Consumer responsibility |
|---------|----------------------|------------------------|
| Violation count | Measure it | Decide if it's acceptable |
| Severity | Attach as metadata | Interpret (block pipeline, warn, ignore) |
| Exit codes | Not applicable | CLI maps blocking failures → exit 1 |
| Thresholds | Not applicable | Consumer defines acceptable violation % |

The Kontra CLI is one consumer of the engine. Other consumers may interpret results differently.

## Execution Flow

```
Contract YAML → Parse (Pydantic) → Build Rules (Factory) → Compile Plan
    ↓
Three independent execution paths (can run in parallel):
├─ Preplan: Metadata analysis (min/max/null_count per row-group)
├─ SQL Pushdown: Database executes eligible rules as aggregate queries
└─ Residual: Polars executes remaining rules locally
    ↓
Merge results (deterministic order: preplan → SQL → Polars) → Report
```

## Three-Tier Execution

Kontra uses a hybrid execution model with three semantically distinct tiers:

| Tier | What It Returns | Guarantees | Limitations |
|------|-----------------|------------|-------------|
| **Metadata (Preplan)** | Binary: 0 or ≥1 | Instant, no data scan | No exact counts; depends on metadata quality |
| **SQL Pushdown** | Varies by rule | Database does the work | `not_null` uses EXISTS (returns 1); others exact |
| **Polars** | Exact count | Full Python regex, precise | Requires loading data |

**All tiers agree on whether violations exist.** A rule that passes in preplan will pass in SQL and Polars. But:

- **Preplan returns `failed_count: 1` for any failure**—not an exact count. It means "≥1 violation exists".
- **SQL `not_null` uses EXISTS** for speed—returns 1 on failure, not exact count. Other SQL rules return exact counts.
- **Preplan depends on metadata quality.** Parquet writers vary; pg_stats may be stale.
- **SQL dialects differ.** DuckDB, PostgreSQL, and SQL Server may behave differently for edge cases.

For exact violation counts, use `--preplan off --pushdown off`.

### Tier 1: Metadata Preplan

**Zero-scan validation using file/database metadata.**

For Parquet files, Kontra reads row-group statistics from the file footer without scanning data:
- `null_count`: Prove not_null rules pass/fail instantly
- `min`/`max`: Prove range rules pass/fail
- `row_count`: Prove min_rows/max_rows rules

For PostgreSQL, Kontra uses `pg_stats` catalog:
- `null_frac`: Null percentage per column
- `n_distinct`: Unique value count/ratio

For SQL Server, Kontra uses `sys.columns` metadata:
- `is_nullable`: Column nullability constraints

**Execution source**: `metadata`

### Tier 2: SQL Pushdown

**Push validation to the database engine.**

Instead of pulling data into Python, Kontra generates SQL queries in two phases:

**Phase 1: EXISTS for `not_null`** (fast, early termination)
```sql
SELECT
  EXISTS(SELECT 1 FROM schema.table WHERE user_id IS NULL) AS "not_null_user_id"
```

**Phase 2: Aggregates for other rules** (batched into one query)
```sql
SELECT
  COUNT(*) - COUNT(DISTINCT email) AS "unique_email",
  SUM(CASE WHEN status NOT IN ('active','inactive') THEN 1 ELSE 0 END) AS "allowed_values_status"
FROM schema.table;
```

**Which SQL dialect?**

| Data Source | SQL Engine |
|-------------|------------|
| Local Parquet/CSV | DuckDB |
| S3 Parquet | DuckDB |
| Azure ADLS (`abfss://`) | DuckDB |
| `postgres://` URI | PostgreSQL |
| `mssql://` URI | SQL Server |

DuckDB is a core dependency, installed automatically. It powers local file execution and is used even when you don't explicitly connect to a database.

**Execution source**: `sql`

### Tier 3: Polars Execution

**In-memory validation using Polars.**

Rules that can't be handled by metadata or SQL are executed in Polars:
- Full vectorized operations
- Complex regex patterns
- Custom SQL checks

Kontra uses column projection to load only the columns needed for remaining rules.

**Execution source**: `polars`

## Source Structure

```
src/kontra/
├── __init__.py       # Public Python API (validate, profile, etc.)
├── api/              # Python API types and helpers
│   ├── results.py    # ValidationResult, RuleResult, Diff, Suggestions
│   └── rules.py      # rules.not_null(), rules.unique(), etc.
├── cli/              # Typer CLI entry points
│   └── main.py       # validate, profile, init commands
├── config/           # Contract YAML parsing
│   ├── loader.py     # Load from file, S3
│   └── models.py     # Pydantic models
├── connectors/       # Dataset I/O
│   ├── handle.py     # DatasetHandle (unified data source)
│   ├── postgres.py   # PostgreSQL connection
│   └── sqlserver.py  # SQL Server connection
├── engine/           # Validation orchestration
│   ├── engine.py     # Main ValidationEngine
│   ├── sql_utils.py  # Shared SQL utilities
│   ├── executors/    # SQL pushdown executors
│   │   ├── duckdb_sql.py
│   │   ├── postgres_sql.py
│   │   └── sqlserver_sql.py
│   ├── materializers/  # Data loading
│   │   ├── duckdb.py
│   │   ├── postgres.py
│   │   └── sqlserver.py
│   └── backends/     # Polars execution
│       └── polars_backend.py
├── preplan/          # Metadata-only optimization
│   ├── planner.py    # Parquet row-group analysis
│   ├── postgres.py   # pg_stats analysis
│   └── sqlserver.py  # sys.columns analysis
├── rules/            # Rule definitions
│   ├── base.py       # BaseRule abstract class
│   ├── factory.py    # Rule instantiation
│   ├── registry.py   # Rule registration
│   ├── execution_plan.py  # CompiledPlan
│   └── builtin/      # 18 built-in rules
├── scout/            # Dataset profiling
│   ├── profiler.py   # ScoutProfiler
│   ├── suggest.py    # Rule inference
│   ├── patterns.py   # Pattern detection
│   └── backends/     # DuckDB, PostgreSQL, SQL Server
├── reporters/        # Output formatting
│   ├── rich_reporter.py
│   └── json_reporter.py
└── errors.py         # Error types with suggestions
```

## Key Components

### DatasetHandle

Unified abstraction for all data sources:

```python
handle = DatasetHandle.from_uri("postgres://user:pass@host/db/schema.table")
handle = DatasetHandle.from_uri("s3://bucket/data.parquet")
handle = DatasetHandle.from_uri("data/local.csv")

# Properties
handle.scheme    # "postgres", "s3", "file", etc.
handle.uri       # Original URI
handle.db_params # Database-specific params (PostgresConnectionParams, etc.)
handle.fs_opts   # S3 credentials, options
```

### RuleExecutionPlan

Compiles rules into an execution plan:

```python
plan = RuleExecutionPlan(rules)
compiled = plan.compile()

# CompiledPlan contains:
compiled.predicates      # Vectorizable Polars expressions
compiled.fallback_rules  # Rules requiring full DataFrame
compiled.required_cols   # Columns needed for projection
compiled.sql_rules       # SQL pushdown specs
```

### SQL Utilities

Shared SQL generation across all database executors:

```python
from kontra.engine.sql_utils import (
    esc_ident,       # Escape identifiers ("name" vs [name])
    agg_not_null,    # Generate not_null aggregate
    agg_unique,      # Generate unique aggregate
    agg_allowed_values,
    results_from_row,  # Parse SQL results
)

# Dialect-aware generation
agg_not_null("user_id", "rule_1", dialect="postgres")
# → SUM(CASE WHEN "user_id" IS NULL THEN 1 ELSE 0 END) AS "rule_1"

agg_not_null("user_id", "rule_1", dialect="sqlserver")
# → SUM(CASE WHEN [user_id] IS NULL THEN 1 ELSE 0 END) AS [rule_1]
```

### Preplan

Metadata analysis without data scan:

```python
from kontra.preplan.planner import preplan_single_parquet

preplan = preplan_single_parquet(
    path="data.parquet",
    required_columns=["user_id", "email"],
    predicates=static_predicates,
)

# PrePlan contains:
preplan.rule_decisions    # {"rule_1": "pass_meta", "rule_2": "unknown"}
preplan.manifest_row_groups  # [0, 1, 3, 5] - row groups to scan
preplan.manifest_columns     # Columns in surviving row groups
```

## Determinism

Kontra guarantees deterministic execution:

1. **Result order**: preplan → SQL → Polars (always)
2. **Rule ID derivation**: Stable based on name + column
3. **No random sampling**: Consistent across runs

**Exception**: The `freshness` rule is time-dependent. Its result depends on when you run it, not just the data content.

## Limitations & Guarantees

Kontra makes explicit what it does and does not guarantee:

### What Kontra Guarantees

- **Tier agreement on pass/fail**: If preplan says "pass", SQL and Polars will agree
- **Deterministic results**: Same input → same output (except `freshness`)
- **Stable rule IDs**: Rule identifiers are derived consistently

### What Kontra Does Not Guarantee

- **Exact counts from preplan**: Metadata returns "≥1 violation", not exact counts
- **Metadata availability**: Parquet row-group stats depend on the writer; pg_stats depends on ANALYZE
- **Identical SQL behavior**: DuckDB, PostgreSQL, and SQL Server may differ on edge cases (collation, regex)
- **Rule suggestion quality**: Auto-generated rules from `profile()` are heuristic, may overfit to sample data

### Data Source Dependencies

| Source | Dependency | Notes |
|--------|------------|-------|
| Parquet | DuckDB (built-in) | Always available |
| CSV | DuckDB (built-in) | Always available |
| Azure ADLS | DuckDB (built-in) | Uses DuckDB's azure extension |
| PostgreSQL | `kontra[postgres]` | Requires psycopg |
| SQL Server | `kontra[sqlserver]` | Requires pymssql |
| S3 | `kontra[s3]` | Requires s3fs |

## Adding a New Rule

1. Create rule class in `src/kontra/rules/builtin/`:

```python
from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate

@register_rule("my_rule")
class MyRule(BaseRule):
    def validate(self, df):
        column = self.params["column"]
        mask = df[column] < 0
        return self._failures(df, mask, f"{column} has negative values")

    def compile_predicate(self):
        # Optional: vectorized Polars expression
        column = self.params["column"]
        return Predicate(
            rule_id=self.rule_id,
            expr=pl.col(column) < 0,
            message=f"{column} has negative values",
            columns={column},
        )

    def to_sql_spec(self):
        # Optional: SQL pushdown spec
        return {
            "kind": "my_rule",
            "rule_id": self.rule_id,
            "column": self.params["column"],
        }
```

2. Import in `engine.py`:
```python
import kontra.rule_defs.builtin.my_rule  # noqa: F401
```

3. Add SQL support in executors (optional).

## Adding a New Data Source

1. Create connector in `src/kontra/connectors/`:

```python
@dataclass
class NewDBConnectionParams:
    host: str
    port: int
    ...

def resolve_connection_params(uri: str) -> NewDBConnectionParams:
    ...

def get_connection(params: NewDBConnectionParams):
    ...
```

2. Extend `DatasetHandle.from_uri()` in `handle.py`.

3. Create materializer in `src/kontra/engine/materializers/`.

4. Create executor in `src/kontra/engine/executors/`.

5. Optionally add preplan in `src/kontra/preplan/`.

## Performance Tips

### For Large Datasets

```bash
# Use preplan + pushdown (default, fastest for most cases)
kontra validate contract.yml

# Metadata-only mode (no data scanning, only if all rules resolvable from metadata)
kontra validate contract.yml --preplan on --pushdown off

# Use column projection (default on, reduces I/O)
kontra validate contract.yml --projection on
```

### For CSVs

```bash
# Stage CSV to Parquet (faster for multiple rules)
kontra validate contract.yml --csv-mode parquet

# Use DuckDB directly (simpler)
kontra validate contract.yml --csv-mode duckdb
```

### Debugging Performance

```bash
kontra validate contract.yml --stats summary
```

Output includes:
- `phases_ms`: Time per phase (preplan, pushdown, data_load, execute)
- `projection`: Columns loaded vs available
- `preplan`: Rules resolved via metadata

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KONTRA_VERBOSE` | Enable verbose error output |
| `KONTRA_IO_DEBUG` | Show I/O metrics in stats |
| `PGHOST`, `PGPORT`, etc. | PostgreSQL connection |
| `AWS_ACCESS_KEY_ID` | S3 credentials |
| `AWS_ENDPOINT_URL` | MinIO/custom S3 endpoint |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure storage account |
| `AZURE_STORAGE_ACCESS_KEY` | Azure account key |
| `AZURE_STORAGE_SAS_TOKEN` | Azure SAS token (alternative) |

## Tests

```bash
# Full test suite
pytest

# Quick run
pytest -q

# Single file
pytest tests/test_determinism.py

# Markers
pytest -m slow          # Large datasets (1M+ rows)
pytest -m integration   # End-to-end tests
pytest -m pushdown      # SQL pushdown tests
```
