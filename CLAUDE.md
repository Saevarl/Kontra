# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kontra is a developer-first data quality validation engine. It validates datasets (Parquet, CSV) against contracts defined in YAML, supporting local files and S3/cloud storage. The engine uses a hybrid execution model combining metadata-only analysis, SQL pushdown, and Polars-based execution.

## Key Commands

```bash
# Run the CLI
kontra validate <contract.yml>

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
├─ SQL Pushdown: DuckDB executes eligible rules as aggregate queries
└─ Residual: Polars executes remaining rules locally
    ↓
Merge results (deterministic order: preplan → SQL → Polars) → Report
```

### Source Structure

```
src/kontra/
├── cli/           # Typer CLI entry point
├── config/        # Contract YAML parsing (Pydantic models)
├── connectors/    # Dataset I/O (local, S3)
├── engine/
│   ├── backends/      # Polars & DuckDB adapters
│   ├── executors/     # SQL pushdown (DuckDB)
│   ├── materializers/ # Data loading with projection
│   └── engine.py      # Main orchestrator
├── preplan/       # Metadata-only optimization
├── reporters/     # JSON & Rich output
└── rules/
    ├── builtin/       # 8 built-in rules
    ├── base.py        # BaseRule abstract class
    ├── factory.py     # Rule instantiation
    └── execution_plan.py
```

### Key Components

**Rules**: Each rule implements `validate(df) → {rule_id, passed, failed_count, message}`. Optional methods: `compile_predicate()` for vectorized Polars expressions, `required_columns()` for projection.

**Rule ID derivation** (in `factory.py`): explicit ID → use as-is; column param exists → `COL:{column}:{name}`; else → `DATASET:{name}`

**Materializers**: Load data with column projection. DuckDBMaterializer handles Parquet/CSV, supports S3 via httpfs.

**Execution Plan**: `CompiledPlan` holds predicates (vectorizable), fallback_rules (non-vectorizable), sql_rules (for executor), required_cols.

### CLI Toggles (Independent)

- `--preplan on|off|auto` - metadata preflight
- `--pushdown on|off|auto` - SQL execution
- `--projection on|off` - column pruning
- `--stats none|summary|profile` - timing/profiling output

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

## Key Patterns

1. **Deterministic execution**: Result order is fixed (preplan → SQL → Polars). Rule IDs are stable.

2. **Graceful fallback**: SQL pushdown failure silently continues with Polars execution.

3. **CSV handling modes** (`--csv-mode`): `auto` tries DuckDB, falls back to staging CSV→Parquet; `duckdb` uses DuckDB only; `parquet` forces staging.

4. **Environment variables**: `KONTRA_VERBOSE` for detailed errors, `KONTRA_IO_DEBUG` for I/O metrics. AWS credentials via standard env vars.
