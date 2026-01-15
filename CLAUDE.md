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
│   ├── builtin/           # 10 built-in rules
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

**Built-in Rules** (10):
- Column: `not_null`, `unique`, `allowed_values`, `range`, `regex`, `dtype`
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

- `docs/quickstart.md` - Getting started guide
- `docs/python-api.md` - Python library API reference
- `docs/config.md` - Configuration system
- `docs/rules.md` - Rule reference
- `docs/architecture.md` - Technical architecture
- `docs/KONTRA_REFERENCE.md` - Complete reference guide
