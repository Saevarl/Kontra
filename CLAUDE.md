# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kontra is a developer-first data quality **measurement engine**. It measures dataset properties against declarative rules, returning violation counts and metadata. Consumers (CLI, CI, agents) interpret results—Kontra measures, it doesn't decide.

Supports Parquet, CSV, PostgreSQL, SQL Server, local files, and S3. Uses a hybrid execution model: metadata preplan → SQL pushdown → Polars.

## Key Commands

```bash
# Initialize project
kontra init

# Run validation
kontra validate <contract.yml>
kontra validate <contract.yml> --data prod_db.users
kontra validate <contract.yml> -o json

# Profile data
kontra scout <data_source>
kontra scout <data_source> --suggest-rules

# View configuration
kontra config show
kontra config show --env production

# Compare validation runs
kontra diff

# Run tests
pytest                      # Full test suite
pytest -q                   # Quiet output (default)
pytest tests/test_file.py   # Single file
pytest -k "test_name"       # Single test by name
pytest -m slow              # Large dataset tests (1M+ rows)
pytest -m integration       # End-to-end engine tests
pytest -m pushdown          # Pushdown toggle tests
pytest -m projection        # Column projection tests
```

## Architecture

### Execution Flow

```
Contract YAML → Parse (Pydantic) → Build Rules (Factory) → Compile Plan
    ↓
Three independent execution paths:
├─ Preplan: Parquet metadata analysis (min/max/null_count per row-group)
├─ SQL Pushdown: DuckDB/PostgreSQL/SQL Server executes eligible rules
└─ Residual: Polars executes remaining rules locally
    ↓
Merge results (deterministic order: preplan → SQL → Polars) → Report
```

### Source Structure

```
src/kontra/
├── cli/                   # Typer CLI entry point
├── api/                   # Python API (public interface)
│   ├── results.py         # ValidationResult, RuleResult, Diff, Suggestions
│   └── rules.py           # rules.not_null(), rules.unique(), etc.
├── config/                # Configuration and contract parsing
│   ├── models.py          # Contract Pydantic models
│   ├── loader.py          # Contract loading
│   └── settings.py        # Project config system (.kontra/config.yml)
├── connectors/            # Dataset URI handling
│   ├── handle.py          # URI parsing and named datasource resolution
│   ├── postgres.py        # PostgreSQL connection
│   └── sqlserver.py       # SQL Server connection
├── engine/
│   ├── backends/          # Polars & DuckDB adapters
│   ├── executors/         # SQL pushdown executors
│   │   ├── duckdb_sql.py
│   │   ├── postgres_sql.py
│   │   └── sqlserver_sql.py
│   ├── materializers/     # Data loading with projection
│   │   ├── duckdb.py
│   │   ├── postgres.py
│   │   └── sqlserver.py
│   └── engine.py          # Main orchestrator
├── preplan/               # Metadata-only optimization
│   ├── planner.py         # Parquet metadata analysis
│   ├── postgres.py        # PostgreSQL stats queries
│   └── sqlserver.py       # SQL Server stats queries
├── reporters/             # JSON & Rich output
├── rules/
│   ├── builtin/           # 12 built-in rules
│   ├── base.py            # BaseRule abstract class
│   ├── factory.py         # Rule instantiation
│   └── execution_plan.py  # CompiledPlan
├── scout/                 # Data profiling
│   ├── backends/          # DB-specific profilers
│   ├── reporters/         # Profile output (JSON, Rich, Markdown)
│   ├── profiler.py        # Main ScoutProfiler class
│   └── suggest.py         # Rule suggestion from profile
├── state/                 # Validation state tracking
│   └── backends/          # local, s3, postgres backends
├── actions/               # Post-validation actions (future)
└── errors.py              # Error types
```

### Key Components

**Rules**: Each rule implements `validate(df) → {rule_id, passed, failed_count, message}`. Optional methods: `compile_predicate()` for vectorized Polars expressions, `required_columns()` for projection.

**Rule ID derivation** (in `factory.py`): explicit ID → use as-is; column param exists → `COL:{column}:{name}`; else → `DATASET:{name}`

**Built-in Rules** (12):
- Column: `not_null`, `unique`, `allowed_values`, `range`, `regex`, `dtype`
- Cross-column: `compare`, `conditional_not_null`
- Dataset: `min_rows`, `max_rows`, `freshness`, `custom_sql_check`

**Materializers**: Load data with column projection. DuckDBMaterializer handles Parquet/CSV/S3, PostgresMaterializer and SQLServerMaterializer for databases.

**Execution Plan**: `CompiledPlan` holds predicates (vectorizable), fallback_rules (non-vectorizable), sql_rules (for executor), required_cols.

**Scout**: Data profiler with presets (lite, standard, deep, llm). Can suggest validation rules from profile.

**State**: Tracks validation history for `kontra diff`. Backends: local file, S3, PostgreSQL.

**Config**: Project-level `.kontra/config.yml` with named datasources, environments, and defaults.

### CLI Toggles (Independent)

- `--preplan on|off|auto` - metadata preflight
- `--pushdown on|off|auto` - SQL execution
- `--projection on|off` - column pruning
- `--stats none|summary|profile` - timing/profiling output
- `--env <name>` - use named environment from config

### Exit Codes

- `0` SUCCESS
- `1` VALIDATION_FAILED (data quality issue)
- `2` CONFIG_ERROR (contract/data not found)
- `3` RUNTIME_ERROR (unexpected failure)

## Testing

- Fixtures in `tests/conftest.py` (session-scoped synthetic data)
- Use `@pytest.mark.slow` for tests with 1M+ rows
- Use `@pytest.mark.integration` for end-to-end tests
- Determinism tests verify identical inputs produce identical outputs
- Tier equivalence tests verify all execution paths agree on violation existence
- CLI tests use `typer.testing.CliRunner`
- 430+ tests

### Test Files

- `tests/test_cli.py` - CLI command tests
- `tests/test_integration.py` - End-to-end ValidationEngine tests
- `tests/test_python_api.py` - Python API tests (validate, scout, rules helpers)
- `tests/test_reporters.py` - JSON/Rich reporter tests
- `tests/test_config_settings.py` - Configuration system tests
- `tests/test_scout.py` - ScoutProfiler tests
- `tests/test_state.py` - State management tests
- `tests/test_tier_equivalence.py` - Verify all tiers agree on violation existence
- `tests/test_rules_*.py` - Individual rule tests

## Key Patterns

1. **Measurement, not decision**: Kontra returns violation counts. Severity is metadata. Consumers interpret what counts as "failure".

2. **Tier equivalence**: All execution tiers (preplan, SQL, Polars) agree on whether violations exist. Preplan reports `failed_count: 1` as lower bound; SQL/Polars return exact counts.

3. **Deterministic execution**: Result order is fixed (preplan → SQL → Polars). Rule IDs are stable.

4. **Graceful fallback**: SQL pushdown failure silently continues with Polars execution.

5. **CSV handling modes** (`--csv-mode`): `auto` tries DuckDB, falls back to staging CSV→Parquet; `duckdb` uses DuckDB only; `parquet` forces staging.

6. **Environment variables**: `KONTRA_VERBOSE` for detailed errors, `KONTRA_IO_DEBUG` for I/O metrics. AWS credentials via standard env vars. PostgreSQL via `PGHOST`, `PGUSER`, etc.

7. **Named Datasources**: Define datasources in `.kontra/config.yml`, reference as `prod_db.users` in contracts or CLI.

## Common Pitfalls (Lessons Learned)

These patterns caused bugs in the past. Avoid them:

1. **NaN vs NULL**: Polars treats NaN and NULL differently. `is_null()` does NOT catch NaN. For float columns that might have NaN, use `include_nan=True` parameter on `not_null` rule, or explicitly check with `is_nan()`.

2. **Fingerprint consistency**: Use `fingerprint_contract(contract_obj)` (semantic, based on name+rules) not `fingerprint_contract_file(path)` (file hash). The engine uses semantic fingerprints, so lookups must too. All history/state functions must resolve contract names to fingerprints first.

3. **Test all code paths**: Features like `stats='profile'` were broken because no test exercised that path. Every CLI flag and parameter combination needs a test.

4. **Validate inputs early**: Bad regex patterns should fail at rule construction, not during batch execution where errors are harder to trace. Add validation in `__init__` for complex parameters.

5. **`.to_llm()` on all public types**: Any type that might be returned to integrations (MCP, agents) needs a `to_llm()` method. Check: `ValidationResult`, `DatasetProfile`, `Diff`, `RuleResult`.

6. **Service vs CLI patterns**: CLI discovers config from cwd. Services need explicit config injection via `kontra.set_config(path)`. Don't assume cwd-based discovery works everywhere.

7. **Document edge cases**: If a rule has non-obvious behavior (like `allowed_values` treating NULL as failure even if NULL is in the list), document it explicitly in docstrings and rules.md.

8. **Differentiate pass/fail messages**: Rule messages should say "Passed" when passing, not the failure description. The vectorized execution path (`execution_plan.py`) must check `passed` before setting the message.

9. **API completeness**: If a parameter exists in dict format (like `id` for rules), the helper functions should support it too. Users shouldn't need to fall back to dict format for common use cases.

## Data Sources

| Source | URI Format | Example |
|--------|-----------|---------|
| Local Parquet | `./path/file.parquet` | `data/users.parquet` |
| Local CSV | `./path/file.csv` | `data/events.csv` |
| S3 | `s3://bucket/key` | `s3://datalake/users.parquet` |
| PostgreSQL | `postgres://user:pass@host:port/db/schema.table` | `postgres:///public.users` |
| SQL Server | `mssql://user:pass@host:port/db/schema.table` | `mssql:///dbo.orders` |
| Named | `datasource.table` | `prod_db.users` |

## Documentation

Structure by audience:

```
README.md                      # Evaluator pitch only
docs/
├── getting-started.md         # New users (happy path)
├── python-api.md              # Library users
├── reference/
│   ├── rules.md               # All rules (flat, exhaustive)
│   ├── config.md              # Configuration reference
│   └── architecture.md        # Contributors
└── advanced/
    ├── state-and-diff.md      # History, diff, state backends
    ├── agents-and-llms.md     # to_llm(), MCP, services
    └── performance.md         # Execution model, preplan, pushdown
```

Key rule: **No document may explain a caveat before demonstrating a successful use.**

## Documentation Standards

### Core Rule

**No document may explain a caveat before demonstrating a successful use.**

### Audience Separation

| Audience | Doc | What they need |
|----------|-----|----------------|
| Evaluator | README.md | "What is this? Should I care?" |
| New user | getting-started.md | Happy path to success |
| Python dev | python-api.md | Library usage |
| Advanced | advanced/* | State, agents, execution model |
| Contributor | reference/* | Exhaustive reference |

Don't collapse audiences. An evaluator should never see execution tiers. A new user should never encounter preplan semantics.

### What Belongs Where

| Topic | Where it goes |
|-------|---------------|
| Execution tiers, preplan semantics | `advanced/performance.md` |
| State backends, diff mechanics | `advanced/state-and-diff.md` |
| to_llm(), MCP, health checks | `advanced/agents-and-llms.md` |
| NULL semantics, rule edge cases | `reference/rules.md` |
| Exhaustive option tables | `reference/config.md` |

### Technical Honesty

These facts must be documented (in advanced/reference docs):

1. Preplan returns `failed_count: 1` for any failure—not exact counts
2. Scout suggestions are heuristic starting points, not ground truth
3. NULL semantics vary by rule
4. `freshness` is time-dependent (not deterministic)
5. SQL dialects (DuckDB, PostgreSQL, SQL Server) may differ on edge cases
6. DuckDB is a core dependency

### Documentation Checklist

When adding a new feature:

- [ ] Happy path example in appropriate doc
- [ ] Caveats in advanced/ or reference/ (not in getting-started)
- [ ] Python API helpers added if applicable
- [ ] Rule count updated if adding rules

## Roadmap

See `docs/ROADMAP.md` for planned features, future ideas, and explicit non-goals.
