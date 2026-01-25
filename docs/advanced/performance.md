# Execution Model & Performance

How Kontra executes validation rules.

## Three-Tier Execution

Kontra uses three execution tiers, each with different characteristics:

| Tier | Speed | What It Returns | When Used |
|------|-------|-----------------|-----------|
| **Metadata (Preplan)** | Instant | Binary: 0 or ≥1 | Parquet stats, pg_stats |
| **SQL Pushdown** | Fast | Varies by rule* | DuckDB, PostgreSQL, SQL Server |
| **Polars** | Varies | Exact count | Fallback |

*`not_null` uses EXISTS (returns 1 on failure for speed). Other rules return exact counts.

### How Tiers Are Selected

1. **Preplan** tries to resolve rules from metadata (Parquet row-group stats, pg_stats)
2. **SQL Pushdown** generates SQL for remaining rules (EXISTS for `not_null`, aggregates for others)
3. **Polars** handles anything SQL can't do

All three agree on whether violations exist. The difference is precision and speed.

## Important: Preplan Returns Binary

When preplan resolves a rule, it returns:
- `failed_count: 0` if no violations
- `failed_count: 1` if ≥1 violations

This is a **lower bound**, not an exact count. If you need exact counts, disable preplan:

```bash
kontra validate contract.yml --preplan off
```

```python
result = kontra.validate(df, rules=[...], preplan="off")
```

## SQL Dialect Mapping

| Data Source | SQL Engine |
|-------------|------------|
| Local Parquet | DuckDB |
| Local CSV | DuckDB |
| S3 Parquet | DuckDB |
| Azure ADLS (`abfss://`) | DuckDB |
| `postgres://` | PostgreSQL |
| `mssql://` | SQL Server |

DuckDB is a core dependency, installed automatically.

## CLI Controls

```bash
# Force preplan on (fastest, binary results only)
kontra validate contract.yml --preplan on

# Disable preplan (exact counts)
kontra validate contract.yml --preplan off

# Force SQL pushdown
kontra validate contract.yml --pushdown on

# Disable SQL pushdown (Polars only)
kontra validate contract.yml --pushdown off

# Disable column projection
kontra validate contract.yml --projection off

# Show execution stats
kontra validate contract.yml --stats summary
```

## Python API Controls

```python
result = kontra.validate(
    df,
    rules=[...],
    preplan="auto",      # "on" | "off" | "auto"
    pushdown="auto",     # "on" | "off" | "auto"
    projection=True,     # column pruning
)
```

**Note:** When passing a DataFrame, only Polars executes (no preplan, no SQL pushdown). For tiered execution, pass a file path or database URI.

## Debugging Execution

```bash
# Show which tier resolved each rule
kontra validate contract.yml --stats summary

# Show preplan decisions
kontra validate contract.yml --explain-preplan

# Show SQL plan
kontra validate contract.yml --show-plan
```

Example output:
```
Stats  rows=1,000,000  cols=12  duration=234 ms  engine=hybrid
Preplan: analyze=12 ms
SQL pushdown: compile=5 ms, execute=45 ms
Projection [on]: 4/12 (req/avail) (pruned)

Rules:
  [metadata] COL:user_id:not_null    ← resolved from Parquet stats
  [sql]      COL:email:unique        ← SQL pushdown
  [sql]      COL:status:allowed_values
  [polars]   COL:email:regex         ← Polars fallback
```

## Metadata Limitations

### Parquet

Preplan reads row-group statistics from the file footer:
- `null_count`: not_null rules
- `min`/`max`: range rules
- `num_rows`: min_rows/max_rows rules

Limitations:
- Not all writers record statistics
- String min/max may be truncated
- NaN is not detected (only NULL)
- If any row-group is missing stats, preplan returns "unknown"

### PostgreSQL

Preplan queries `pg_stats` catalog:
- `null_frac`: null percentage
- `n_distinct`: unique count/ratio

Limitations:
- Stats are populated by `ANALYZE`
- If table changed since last `ANALYZE`, stats may be stale
- Stats are based on sampling, not full data

### SQL Server

Preplan queries `sys.columns`:
- `is_nullable`: column constraints

More limited than PostgreSQL.

## Performance Tips

### Large Parquet Files

Default settings are usually optimal:
```bash
kontra validate contract.yml  # preplan=auto, pushdown=auto, projection=on
```

### Large Database Tables

Enable pushdown to avoid loading data:
```bash
kontra validate contract.yml --pushdown on
```

### Many Rules

SQL pushdown uses two phases:

**Phase 1: EXISTS for `not_null`** (fast, early termination)
```sql
SELECT
  EXISTS(SELECT 1 FROM table WHERE user_id IS NULL) AS "not_null_user_id"
```

**Phase 2: Aggregates for other rules** (one query)
```sql
SELECT
  COUNT(*) - COUNT(DISTINCT email) AS "unique_email",
  SUM(CASE WHEN status NOT IN (...) THEN 1 ELSE 0 END) AS "allowed_values_status"
FROM table;
```

### CSV Files

Options:
```bash
# Use DuckDB directly (simpler)
kontra validate contract.yml --csv-mode duckdb

# Stage to Parquet first (faster for many rules)
kontra validate contract.yml --csv-mode parquet
```

## Profiling

```bash
kontra validate contract.yml --stats profile
```

Outputs detailed timing:
- `preplan_ms`: metadata analysis
- `compile_ms`: SQL generation
- `execute_ms`: SQL execution
- `data_load_ms`: data loading (if needed)
- `polars_ms`: Polars execution

## Profile Presets

`kontra.profile()` uses three presets with different speed/detail tradeoffs:

| Preset | What It Does | Best For |
|--------|--------------|----------|
| **scout** | Metadata only (no table scan) | Quick recon, large tables |
| **scan** | Strategic queries + sampling | General profiling (default) |
| **interrogate** | Full table scan | Exact values, percentiles |

### How Presets Work

**scout** reads only metadata (no data scan):
- PostgreSQL: `pg_stats` (null_frac, n_distinct, most_common_vals)
- Parquet: Row-group statistics (null_count, min/max)
- SQL Server: `sys.dm_db_stats_histogram`

**scan** uses strategic queries:
- Metadata for null/distinct counts
- `TABLESAMPLE SYSTEM` for numeric stats (1% of blocks)
- Batched `GROUP BY` for low-cardinality columns
- Skips expensive operations (median, percentiles)

**interrogate** does a full table scan for exact values including median and percentiles.

### Choosing a Preset

```python
# Quick recon (metadata only)
profile = kontra.profile("data.parquet", preset="scout")

# Systematic pass (default)
profile = kontra.profile("data.parquet", preset="scan")

# Deep investigation (everything + percentiles)
profile = kontra.profile("data.parquet", preset="interrogate")
```

### Scan vs Interrogate

| Metric | scan | interrogate |
|--------|------|-------------|
| null_count | ✅ metadata | ✅ exact |
| distinct_count | ✅ estimated | ✅ exact |
| min/max | ✅ sampled | ✅ exact |
| mean/std | ✅ sampled | ✅ exact |
| median | ❌ | ✅ |
| percentiles | ❌ | ✅ |
| top_values | ✅ low/med cardinality | ✅ all |

`scan` gives ~80% of the information without a full table scan. Numeric stats come from block sampling (typically within 1-2% of exact).
