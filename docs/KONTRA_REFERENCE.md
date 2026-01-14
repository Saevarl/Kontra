# Kontra Reference Guide

> Developer-first Data Quality Validation Engine

## Table of Contents

1. [Supported Data Sources](#supported-data-sources)
2. [Validate Command](#validate-command)
3. [Scout Command](#scout-command)
4. [Validation Rules](#validation-rules)
5. [Execution Architecture](#execution-architecture)
6. [PostgreSQL Deep Dive](#postgresql-deep-dive)
7. [SQL Server Deep Dive](#sql-server-deep-dive)
8. [Scout for LLM Context Compression](#scout-for-llm-context-compression)
9. [Performance Benchmarks](#performance-benchmarks)
10. [Future Rules & Enhancements](#future-rules--enhancements)

---

## Supported Data Sources

| Source Type | URI Format | Validate | Scout | SQL Pushdown |
|-------------|------------|----------|-------|--------------|
| Local Parquet | `./data.parquet` or `/path/to/file.parquet` | ✅ | ✅ | DuckDB |
| Local CSV | `./data.csv` | ✅ | ✅ | DuckDB |
| S3 Parquet | `s3://bucket/key.parquet` | ✅ | ✅ | DuckDB (httpfs) |
| S3 CSV | `s3://bucket/key.csv` | ✅ | ✅ | DuckDB → staged Parquet |
| HTTP(S) | `https://example.com/data.parquet` | ✅ | ✅ | DuckDB (httpfs) |
| PostgreSQL | `postgres://user:pass@host:port/db/schema.table` | ✅ | ✅ | Native PostgreSQL |
| SQL Server | `mssql://user:pass@host:port/db/schema.table` | ✅ | ✅ | Native SQL Server |

### Environment Variables

**S3/MinIO:**
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000  # For MinIO
```

**PostgreSQL (libpq standard):**
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=kontra
export PGPASSWORD=secret
export PGDATABASE=mydb
# Or use DATABASE_URL (Heroku/Railway style):
export DATABASE_URL=postgres://user:pass@host:5432/database
```

**SQL Server:**
```bash
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_USER=sa
export MSSQL_PASSWORD=secret
export MSSQL_DATABASE=mydb
# Or use SQLSERVER_URL:
export SQLSERVER_URL=mssql://user:pass@host:1433/database
```

---

## Validate Command

```bash
kontra validate <contract.yml> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `contract` | Path or URI to contract YAML (local or `s3://...`) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--data` | (from contract) | Override dataset path/URI |
| `--output-format`, `-o` | `rich` | Output format: `rich` (terminal) or `json` (CI/CD) |
| `--stats` | `none` | Attach statistics: `none`, `summary`, or `profile` |
| `--preplan` | `auto` | Metadata preflight: `on`, `off`, `auto` |
| `--pushdown` | `auto` | SQL pushdown: `on`, `off`, `auto` |
| `--projection` | `on` | Column pruning: `on` or `off` |
| `--csv-mode` | `auto` | CSV handling: `auto`, `duckdb`, `parquet` |
| `--show-plan` | `false` | Print generated SQL for debugging |
| `--explain-preplan` | `false` | Print preplan manifest decisions |
| `--verbose`, `-v` | `false` | Show detailed error messages |

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | SUCCESS - all rules passed |
| `1` | VALIDATION_FAILED - one or more rules failed |
| `2` | CONFIG_ERROR - contract or data not found |
| `3` | RUNTIME_ERROR - unexpected failure |

### Example Contract

```yaml
dataset: "postgres://kontra:secret@localhost:5432/mydb/public.users"

rules:
  - name: not_null
    params: { column: email }

  - name: unique
    params: { column: user_id }

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: min_rows
    id: at_least_1000_rows  # Explicit ID for duplicate rules
    params: { threshold: 1000 }
```

---

## Scout Command

```bash
kontra scout <source> [OPTIONS]
```

Scout profiles datasets **without requiring a contract**. It's designed for:
- **LLM context compression** - compact dataset summaries for AI assistants
- **Data exploration** - understand a dataset before writing contracts
- **Rule generation** - auto-suggest validation rules based on data

### Arguments

| Argument | Description |
|----------|-------------|
| `source` | Path or URI to dataset (local, `s3://...`, `postgres://...`) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--output-format`, `-o` | `rich` | Output: `rich`, `json`, or `markdown` |
| `--preset`, `-p` | `standard` | Profiling depth: `lite`, `standard`, `deep` |
| `--list-values-threshold`, `-l` | (from preset) | List all values if distinct ≤ threshold |
| `--top-n`, `-t` | (from preset) | Top N frequent values per column |
| `--sample`, `-s` | (all rows) | Sample N rows for profiling |
| `--include-patterns` | `false` | Detect patterns (email, phone, uuid, etc.) |
| `--columns`, `-c` | (all) | Comma-separated columns to profile |
| `--suggest-rules` | `false` | Generate YAML rules from profile |
| `--verbose`, `-v` | `false` | Enable verbose output |

### Preset Comparison

| Feature | `lite` | `standard` | `deep` |
|---------|--------|------------|--------|
| **Speed** | Fastest | Balanced | Slowest |
| **Row count** | ✅ | ✅ | ✅ |
| **Null/distinct counts** | ✅ | ✅ | ✅ |
| **Numeric stats** (min/max/mean/median/std) | ❌ | ✅ | ✅ |
| **String stats** (min/max/avg length) | ❌ | ✅ | ✅ |
| **Temporal stats** (date range) | ❌ | ✅ | ✅ |
| **Top values** | ❌ | Top 5 | Top 10 |
| **Percentiles** (p25/p50/p75/p99) | ❌ | ❌ | ✅ |
| **Low-cardinality threshold** | 5 | 10 | 20 |

**Use Cases:**
- `lite`: Quick schema overview, CI pipelines, LLM summaries
- `standard`: Interactive exploration, most profiling tasks
- `deep`: Thorough analysis, data quality reports

---

## Validation Rules

### Built-in Rules (9 total)

| Rule | Description | Column-level | Dataset-level | SQL Pushdown |
|------|-------------|--------------|---------------|--------------|
| `not_null` | Fails if column contains NULL | ✅ | ❌ | DuckDB ✅, PostgreSQL ✅, SQL Server ✅ |
| `unique` | Fails if column has duplicates | ✅ | ❌ | PostgreSQL ✅, SQL Server ✅ |
| `allowed_values` | Fails if values not in list | ✅ | ❌ | PostgreSQL ✅, SQL Server ✅ |
| `min_rows` | Fails if row count < threshold | ❌ | ✅ | DuckDB ✅, PostgreSQL ✅, SQL Server ✅ |
| `max_rows` | Fails if row count > threshold | ❌ | ✅ | DuckDB ✅, PostgreSQL ✅, SQL Server ✅ |
| `freshness` | Fails if data is too old | ✅ | ❌ | PostgreSQL ✅, SQL Server ✅ |
| `dtype` | Fails if column type mismatch | ✅ | ❌ | ❌ (Polars) |
| `regex` | Fails if values don't match pattern | ✅ | ❌ | ❌ (Polars) |
| `custom_sql_check` | Custom SQL returning pass/fail | ❌ | ✅ | ❌ (Polars) |

### Rule Parameters

```yaml
# not_null - column cannot contain NULL
- name: not_null
  params: { column: email }

# unique - column values must be unique
- name: unique
  params: { column: user_id }

# allowed_values - column values must be in list
- name: allowed_values
  params:
    column: status
    values: [active, inactive, pending, deleted]

# min_rows - dataset must have at least N rows
- name: min_rows
  params: { threshold: 1000 }

# max_rows - dataset must have at most N rows
- name: max_rows
  params: { threshold: 1000000 }

# freshness - data must be recent (max_age: "24h", "7d", "30m", etc.)
- name: freshness
  params:
    column: updated_at
    max_age: "24h"

# dtype - column must have specific type (Polars type names)
- name: dtype
  params:
    column: age
    type: int64  # int64, float64, utf8, bool, date, datetime

# regex - column values must match pattern
- name: regex
  params:
    column: email
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

### SQL Pushdown by Data Source

| Rule | Files (DuckDB) | PostgreSQL | SQL Server |
|------|----------------|------------|------------|
| `not_null` | ✅ EXISTS (early exit) | ✅ EXISTS (early exit) | ✅ EXISTS (early exit) |
| `unique` | ❌ (Polars) | ✅ `COUNT(*) - COUNT(DISTINCT)` | ✅ Same |
| `allowed_values` | ❌ (Polars) | ✅ `SUM(CASE WHEN NOT IN...)` | ✅ Same |
| `min_rows` | ✅ `GREATEST(0, N - COUNT(*))` | ✅ Same | ✅ CASE expression |
| `max_rows` | ✅ `GREATEST(0, COUNT(*) - N)` | ✅ Same | ✅ CASE expression |
| `freshness` | ❌ (Polars) | ✅ `MAX(col) >= NOW() - interval` | ✅ `MAX(col) >= DATEADD(...)` |
| `dtype` | ❌ | ❌ | ❌ |
| `regex` | ❌ | ❌ | ❌ |

---

## Execution Architecture

### What Happens When You Run `kontra validate`

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. LOAD CONTRACT                                                        │
│    Parse YAML → Build RuleSpec list → Instantiate Rule objects         │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. COMPILE PLAN                                                         │
│    • Compute required columns (from rule params)                        │
│    • Identify SQL-capable rules (not_null, min_rows, max_rows, etc.)   │
│    • Build vectorized predicates for Polars execution                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. PREPLAN (Parquet only, --preplan=on|auto)                           │
│    • Read Parquet footer metadata WITHOUT scanning data                 │
│    • Extract: row counts, min/max values, null counts per row-group     │
│    • Prove rules PASS/FAIL from metadata alone (zero I/O!)              │
│    • Build scan manifest: which row-groups need actual data             │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. SQL PUSHDOWN (--pushdown=on|auto)                                    │
│    • Pick executor: DuckDB (files) or PostgreSQL (database)             │
│    • Compile rules to single aggregate query                            │
│    • Execute: SELECT agg1, agg2, ... FROM data                          │
│    • Return: {rule_id: failed_count} for each pushed-down rule          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 5. MATERIALIZE (remaining rules only)                                   │
│    • Pick materializer: DuckDB (remote), Polars (local), PostgreSQL     │
│    • Column projection: load only required columns                      │
│    • Convert to Polars DataFrame                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 6. POLARS EXECUTION (residual rules)                                    │
│    • dtype, regex, unique (for files), custom checks                    │
│    • Vectorized predicates for performance                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 7. MERGE & REPORT                                                       │
│    • Deterministic order: preplan → SQL → Polars                        │
│    • Generate summary: passed/failed counts, messages                   │
│    • Output: rich terminal or JSON                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Preplan (Metadata-Only Optimization)

**Parquet files** have rich metadata in the file footer:
- Row count (instant, no scan)
- Per-column: min value, max value, null count (per row-group)

Kontra can use this to **prove rules without reading data**:

| Rule | Preplan Logic |
|------|---------------|
| `not_null` | If `null_count = 0` for column → PASS |
| `min_rows` | If `total_rows >= threshold` → PASS |
| `max_rows` | If `total_rows <= threshold` → PASS |

**PostgreSQL** has `pg_stats` (populated by `ANALYZE`):
- `reltuples` - estimated row count
- `null_frac` - fraction of nulls per column (0.0 = no nulls)
- `n_distinct` - distinct values (-1 = all unique)

| Rule | pg_stats Logic |
|------|---------------|
| `not_null` | If `null_frac = 0` → PASS (metadata) |
| `unique` | If `n_distinct = -1` → PASS (metadata) |

---

## PostgreSQL Deep Dive

### Connection Methods

```bash
# Full URI (explicit)
kontra scout postgres://user:pass@localhost:5432/mydb/public.users

# Short form with environment variables
export PGHOST=localhost PGPORT=5432 PGUSER=kontra PGPASSWORD=secret PGDATABASE=mydb
kontra scout postgres:///public.users

# DATABASE_URL (Heroku/Railway)
export DATABASE_URL=postgres://user:pass@host:5432/database
kontra scout postgres:///public.users
```

### URI Format

```
postgres://[user[:password]@]host[:port]/database/schema.table
         │      │           │     │      │        │
         │      │           │     │      │        └── Table name
         │      │           │     │      └── Schema (default: public)
         │      │           │     └── Database name
         │      │           └── Port (default: 5432)
         │      └── Password (or from PGPASSWORD)
         └── Username (or from PGUSER)
```

### PostgreSQL-Specific Execution

**Materializer:** Loads data via `psycopg3` with column projection
```python
# Only requested columns are loaded
SELECT user_id, email, status FROM public.users
```

**SQL Executor:** 5 rules pushed down
```sql
-- Single aggregate query for all rules
SELECT
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS "COL:email:not_null",
  COUNT(*) - COUNT(DISTINCT user_id) AS "COL:user_id:unique",
  GREATEST(0, 1000 - COUNT(*)) AS "min_rows_1000",
  GREATEST(0, COUNT(*) - 10000) AS "max_rows_10000",
  SUM(CASE WHEN status NOT IN ('active','inactive') THEN 1 ELSE 0 END) AS "COL:status:allowed_values"
FROM public.users;
```

**pg_stats Preplan:**
```sql
-- Fetch statistics without scanning data
SELECT attname, null_frac, n_distinct
FROM pg_stats
WHERE schemaname = 'public' AND tablename = 'users';
```

### Scout with PostgreSQL

```bash
# Quick schema overview
kontra scout postgres://...//public.users --preset lite

# Full profiling with pattern detection
kontra scout postgres://...//public.users --preset standard --include-patterns

# Generate contract rules
kontra scout postgres://...//public.users --suggest-rules > contract.yml
```

**PostgreSQL Scout Backend:**
- Uses `information_schema.columns` for schema
- Uses `pg_class.reltuples` for row count estimate (instant)
- Uses `pg_total_relation_size()` for size estimate
- Aggregations via standard SQL with dialect adjustments:
  - `MEDIAN()` → `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)`
  - Sampling: `TABLESAMPLE BERNOULLI(...)` or `ORDER BY random() LIMIT N`

---

## SQL Server Deep Dive

### Connection Methods

```bash
# Full URI (explicit)
kontra scout 'mssql://sa:Password123!@localhost:1433/mydb/dbo.users'

# Short form with environment variables
export MSSQL_HOST=localhost MSSQL_PORT=1433 MSSQL_USER=sa MSSQL_PASSWORD=secret MSSQL_DATABASE=mydb
kontra scout mssql:///dbo.users

# SQLSERVER_URL
export SQLSERVER_URL=mssql://user:pass@host:1433/database
kontra scout mssql:///dbo.users
```

### URI Format

```
mssql://[user[:password]@]host[:port]/database/schema.table
       │      │           │     │      │        │
       │      │           │     │      │        └── Table name
       │      │           │     │      └── Schema (default: dbo)
       │      │           │     └── Database name
       │      │           └── Port (default: 1433)
       │      └── Password (or from MSSQL_PASSWORD)
       └── Username (or from MSSQL_USER)
```

### SQL Server-Specific Execution

**Materializer:** Loads data via `pymssql` with column projection
```sql
-- Only requested columns are loaded
SELECT [user_id], [email], [status] FROM [dbo].[users]
```

**SQL Executor:** Two-phase execution for optimal performance
```sql
-- Phase 1: EXISTS checks (early termination for not_null)
SELECT
  (SELECT CASE WHEN EXISTS (SELECT 1 FROM [dbo].[users] WHERE [email] IS NULL)
   THEN 1 ELSE 0 END) AS [COL:email:not_null]

-- Phase 2: Aggregate query for remaining rules
SELECT
  COUNT(*) - COUNT(DISTINCT [user_id]) AS [COL:user_id:unique],
  CASE WHEN COUNT(*) >= 1000 THEN 0 ELSE 1000 - COUNT(*) END AS [min_rows_1000],
  SUM(CASE WHEN [status] NOT IN ('active','inactive') THEN 1 ELSE 0 END) AS [COL:status:allowed_values]
FROM [dbo].[users];
```

**Metadata Preplan:**
```sql
-- Uses constraint metadata (NOT NULL, UNIQUE indexes)
SELECT c.name, c.is_nullable, c.is_identity
FROM sys.columns c
JOIN sys.objects o ON c.object_id = o.object_id
WHERE o.name = 'users';
```

### PostgreSQL vs SQL Server Metadata

| Feature | PostgreSQL | SQL Server |
|---------|------------|------------|
| Statistical metadata (`null_frac`, `n_distinct`) | ✅ Rich (from `ANALYZE`) | ❌ Limited |
| Preplan from stats alone | ✅ Yes | ❌ Only from constraints |
| NOT NULL detection | ✅ From stats or constraints | ✅ From constraints only |
| Unique detection | ✅ From `n_distinct = -1` or index | ✅ From unique index only |
| Median/percentiles | ✅ `PERCENTILE_CONT` | ❌ Requires workaround |

**Implication:** PostgreSQL can prove rules pass from statistics even without constraints, while SQL Server requires explicit constraints (NOT NULL, UNIQUE index).

### Scout with SQL Server

```bash
# Quick schema overview
kontra scout 'mssql://sa:pass@host:1433/mydb/dbo.users' --preset lite

# Full profiling
kontra scout 'mssql://sa:pass@host:1433/mydb/dbo.users' --preset standard

# Generate contract rules
kontra scout 'mssql://sa:pass@host:1433/mydb/dbo.users' --suggest-rules > contract.yml
```

**SQL Server Scout Backend:**
- Uses `information_schema.columns` for schema
- Uses `sys.dm_db_partition_stats` for row count estimate (instant)
- Uses page counts for size estimate
- Dialect adjustments:
  - No `MEDIAN()` - skipped for SQL Server
  - `LENGTH()` → `LEN()`
  - `STDDEV()` → `STDEV()`
  - Sampling: `TABLESAMPLE (N ROWS)`

---

## Scout for LLM Context Compression

Scout's **primary design goal** is providing compact dataset context for LLMs.

### JSON Output for LLMs

```bash
kontra scout data.parquet --output-format json --preset lite
```

```json
{
  "source_uri": "data.parquet",
  "source_format": "parquet",
  "row_count": 1000000,
  "column_count": 15,
  "columns": [
    {
      "name": "user_id",
      "dtype": "int",
      "null_rate": 0.0,
      "distinct_count": 1000000,
      "uniqueness_ratio": 1.0,
      "is_low_cardinality": false,
      "semantic_type": "identifier"
    },
    {
      "name": "status",
      "dtype": "string",
      "null_rate": 0.0,
      "distinct_count": 4,
      "is_low_cardinality": true,
      "values": ["active", "inactive", "pending", "deleted"],
      "semantic_type": "category"
    }
  ]
}
```

### Markdown Output (for Claude/ChatGPT)

```bash
kontra scout data.parquet --output-format markdown --preset lite
```

```markdown
# Dataset Profile: data.parquet

**Rows:** 1,000,000 | **Columns:** 15 | **Format:** parquet

## Columns

| Column | Type | Nulls | Distinct | Semantic |
|--------|------|-------|----------|----------|
| user_id | int | 0% | 1,000,000 | identifier |
| status | string | 0% | 4 | category |
| email | string | 2% | 980,000 | - |
...

## Low-Cardinality Values

- **status**: active, inactive, pending, deleted
- **country**: US, UK, DE, FR, JP
```

### Best Practices for LLM Context

1. **Use `--preset lite`** for minimal token usage
2. **Use `--json`** for structured context in prompts
3. **Use `--columns`** to focus on relevant columns
4. **Use `--sample`** for very large datasets

```bash
# Minimal context for LLM
kontra scout s3://bucket/huge_dataset.parquet \
  --preset lite \
  --output-format json \
  --sample 10000 \
  --columns "user_id,email,status,created_at"
```

---

## Future Rules & Enhancements

### Missing Rules (High Value)

| Rule | Description | Viable For | Implementation Complexity |
|------|-------------|------------|---------------------------|
| **`range`** | Value within min/max bounds | All sources | Low - SQL pushdown possible |
| **`string_length`** | String length within bounds | All sources | Low - SQL pushdown possible |
| **`foreign_key`** | Values exist in another table/column | PostgreSQL, multi-file | Medium - requires JOIN |
| **`date_range`** | Date/timestamp within bounds | All sources | Low - SQL pushdown possible |
| **`freshness`** | Max age of most recent timestamp | All sources | Low - MAX() query |
| **`completeness`** | % of non-null values >= threshold | All sources | Low - already have null stats |
| **`referential_integrity`** | Cross-table FK validation | PostgreSQL | Medium - JOIN required |
| **`incremental`** | New data only increases | All sources | Medium - requires state |

### PostgreSQL-Only Opportunities

| Rule | Description | Why PostgreSQL-Only |
|------|-------------|---------------------|
| **`check_constraint`** | Validate existing CHECK constraints | Uses `pg_constraint` |
| **`index_coverage`** | Columns have proper indexes | Uses `pg_index` |
| **`bloat_check`** | Table bloat within threshold | Uses `pgstattuple` |
| **`sequence_gaps`** | Detect gaps in sequences | Sequential scan needed |
| **`trigger_validation`** | Validate trigger-enforced rules | Uses `pg_trigger` |

### Scout Enhancements

| Feature | Description | Status |
|---------|-------------|--------|
| **Correlation detection** | Detect correlated columns | Not implemented |
| **Anomaly detection** | Flag statistical outliers | Not implemented |
| **Trend analysis** | Detect data drift over time | Not implemented |
| **Schema evolution** | Compare profiles over time | Not implemented |
| **Data quality score** | Single 0-100 score | Not implemented |

### File Format Support

| Format | Current | Future |
|--------|---------|--------|
| Parquet | ✅ Full support | - |
| CSV | ✅ Full support | - |
| JSON/JSONL | ❌ | Possible via DuckDB |
| Delta Lake | ❌ | Possible via delta-rs |
| Iceberg | ❌ | Complex |
| Avro | ❌ | Possible via DuckDB |
| ORC | ❌ | Possible via DuckDB |

### Database Support

| Database | Current | Future |
|----------|---------|--------|
| PostgreSQL | ✅ Full support | - |
| MySQL/MariaDB | ❌ | Similar to PostgreSQL |
| SQLite | ❌ | Easy via DuckDB |
| Snowflake | ❌ | High value, medium complexity |
| BigQuery | ❌ | High value, needs SDK |
| Redshift | ❌ | Similar to PostgreSQL |
| DuckDB (database) | ❌ | Trivial extension |

---

## Quick Reference

```bash
# Validate Parquet file
kontra validate contract.yml --data data.parquet

# Validate PostgreSQL table
kontra validate contract.yml --data postgres://user:pass@host:5432/db/public.table

# Profile for LLM context
kontra scout data.parquet -o json --preset lite

# Generate contract from data
kontra scout data.parquet --suggest-rules > contract.yml

# Debug execution
kontra validate contract.yml --verbose --show-plan --explain-preplan --stats summary
```
