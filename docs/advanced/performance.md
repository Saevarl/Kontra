# Execution Model & Performance

How Kontra executes validation rules.

## Three-Tier Execution

Kontra uses three execution tiers, each with different characteristics:

| Tier | Speed | What It Returns | When Used |
|------|-------|-----------------|-----------|
| **Metadata (Preplan)** | Instant | Binary: 0 or ≥1 | Parquet stats, pg_stats |
| **SQL Pushdown** | Fast | Exact count | DuckDB, PostgreSQL, SQL Server |
| **Polars** | Varies | Exact count | Fallback |

### How Tiers Are Selected

1. **Preplan** tries to resolve rules from metadata (Parquet row-group stats, pg_stats)
2. **SQL Pushdown** generates a single aggregate query for remaining rules
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

SQL pushdown combines all rules into one query:
```sql
SELECT
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS "not_null_user_id",
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

## Scout Presets & Performance

Scout uses three presets with different speed/detail tradeoffs:

| Preset | PostgreSQL | SQL Server | What You Get |
|--------|------------|------------|--------------|
| **lite** | ~37ms | ~27ms | Schema, null counts, distinct counts (metadata only) |
| **standard** | ~68ms | ~40ms | Lite + numeric stats, top values (strategic queries) |
| **deep** | ~4300ms | ~570ms | Standard + median, percentiles (full scan) |

*Benchmarks on 100K rows*

### How Presets Work

**Lite** reads only metadata:
- PostgreSQL: `pg_stats` (null_frac, n_distinct, most_common_vals)
- Parquet: Row-group statistics (null_count, min/max)
- SQL Server: `sys.dm_db_stats_histogram` + sampled null query

**Standard** uses strategic queries:
- Metadata for null/distinct counts
- `TABLESAMPLE SYSTEM` for numeric stats (1% of blocks, not rows)
- Batched `GROUP BY` for low-cardinality columns only
- Skips expensive operations (median, percentiles)

**Deep** does a full table scan for exact values.

### Choosing a Preset

```python
# Quick schema exploration
profile = kontra.scout("data.parquet", preset="lite")

# Balanced detail (default)
profile = kontra.scout("data.parquet", preset="standard")

# Full analysis for reports
profile = kontra.scout("data.parquet", preset="deep")

# LLM-optimized (schema + key stats)
profile = kontra.scout("data.parquet", preset="llm")
```

### Standard vs Deep Trade-offs

Standard gives you ~80% of the information at ~1% of the cost:

| Metric | Standard | Deep |
|--------|----------|------|
| null_count | ✅ metadata | ✅ exact |
| distinct_count | ✅ estimated | ✅ exact |
| min/max | ✅ sampled | ✅ exact |
| mean/std | ✅ sampled | ✅ exact |
| median | ❌ | ✅ exact |
| percentiles | ❌ | ✅ exact |
| top_values | ✅ low/med cardinality | ✅ all |

Standard's numeric stats come from block sampling, so values may vary slightly from exact (typically within 1-2%).
