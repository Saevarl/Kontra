# Kontra

**Developer-first Data Quality Engine**

Kontra validates datasets against declarative contracts. It combines metadata analysis, SQL pushdown, and Polars execution for performance. Built for developers who care about data quality without ceremony.

```bash
# Initialize project
kontra init

# Define datasources in .kontra/config.yml, then:
kontra scout users                              # Profile a table
kontra scout users --suggest-rules              # Generate contract
kontra validate contracts/users.yml             # Validate

# Or use local files directly
kontra scout data.parquet
kontra validate contract.yml --data data.parquet
```

## Features

- **Declarative contracts**: YAML-based validation rules
- **10 built-in rules**: not_null, unique, range, allowed_values, regex, freshness, dtype, min_rows, max_rows, custom_sql_check
- **Multi-source**: Parquet, CSV, PostgreSQL, SQL Server, S3
- **Smart execution**: Metadata-first, SQL pushdown, column projection
- **State tracking**: Compare validation runs over time with `kontra diff`
- **Scout profiler**: Explore data and auto-generate contracts
- **Named datasources**: Centralize credentials in config, keep contracts portable

## Installation

```bash
pip install kontra
```

With database support:
```bash
pip install kontra[postgres]    # PostgreSQL
pip install kontra[sqlserver]   # SQL Server
pip install kontra[s3]          # S3/MinIO
pip install kontra[all]         # Everything
```

## Quick Start

### 1. Initialize Project

```bash
kontra init
```

Creates:
- `.kontra/config.yml` - Configuration with defaults and datasources
- `contracts/` - Directory for contract files

### 2. Profile Your Data

```bash
kontra scout data.parquet
```

Output:
```
Dataset: data.parquet
Rows: 1,000,000 | Columns: 12

Columns:
  user_id     int64    100% non-null, 100% unique (identifier)
  email       string   98% non-null, pattern: email
  status      string   100% non-null, 4 values: active, inactive, pending, deleted
  age         int64    99.9% non-null, range: [18, 99]
```

### 3. Generate Contract

```bash
kontra scout data.parquet --suggest-rules > contracts/users.yml
```

Or write manually:

```yaml
# contracts/users.yml
name: users_quality
dataset: data/users.parquet

rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking

  - name: unique
    params: { column: email }
    severity: blocking

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]
    severity: warning

  - name: range
    params:
      column: age
      min: 0
      max: 150
```

### 4. Validate

```bash
kontra validate contracts/users.yml
```

Output:
```
PASSED - users.parquet (4 of 4 rules)

  [metadata] COL:user_id:not_null
  [sql]      COL:email:unique
  [sql]      COL:status:allowed_values
  [metadata] COL:age:range
```

## Commands

| Command | Description |
|---------|-------------|
| `kontra init` | Initialize Kontra project |
| `kontra validate <contract>` | Run validation |
| `kontra scout <source>` | Profile dataset |
| `kontra diff [contract]` | Compare validation runs |
| `kontra scout-diff [source]` | Compare profile changes |
| `kontra config show` | Show effective configuration |

## Configuration

`.kontra/config.yml`:

```yaml
version: "1"

defaults:
  preplan: "auto"        # Metadata preflight (on|off|auto)
  pushdown: "auto"       # SQL pushdown (on|off|auto)
  projection: "on"       # Column pruning (on|off)
  output_format: "rich"  # Output (rich|json)
  state_backend: "local" # State storage

datasources:
  prod_db:
    type: postgres
    host: ${PGHOST}
    port: 5432
    user: ${PGUSER}
    password: ${PGPASSWORD}
    database: ${PGDATABASE}
    tables:
      users: public.users
      orders: public.orders

  local_data:
    type: files
    base_path: ./data
    tables:
      events: events.parquet

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}
    pushdown: "on"
```

Then use simple names everywhere:
```bash
kontra scout users                    # Profile prod_db.users
kontra scout orders                   # Profile prod_db.orders
kontra scout events                   # Profile local_data.events
kontra validate contract.yml --data users
```

## Data Sources

**Named datasources (recommended):**
```bash
kontra scout users                    # Looks up "users" in config
kontra scout prod_db.orders           # Explicit: datasource.table
```

**Direct paths/URIs:**
| Source | Format | Example |
|--------|--------|---------|
| Local file | Path | `data/users.parquet` |
| S3 | `s3://bucket/key` | `s3://data-lake/users.parquet` |
| PostgreSQL | `postgres://...` | `postgres://user:pass@host/db/schema.table` |
| SQL Server | `mssql://...` | `mssql://user:pass@host/db/dbo.table` |

## Rules

| Rule | Description | Supports Preplan | Supports SQL |
|------|-------------|------------------|--------------|
| `not_null` | No NULL values | Yes | Yes |
| `unique` | No duplicates | - | Yes |
| `allowed_values` | Values in set | Yes | Yes |
| `range` | Min/max bounds | Yes | Yes |
| `dtype` | Type check | Schema | - |
| `freshness` | Data recency | - | Yes |
| `min_rows` | Minimum rows | Yes | Yes |
| `max_rows` | Maximum rows | Yes | Yes |
| `regex` | Pattern match | - | Yes |
| `custom_sql_check` | Custom SQL | - | DuckDB |

## Severity Levels

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking   # Fails validation (exit code 1)

  - name: allowed_values
    params:
      column: status
      values: [active, inactive]
    severity: warning    # Warns but passes (exit code 0)

  - name: range
    params: { column: optional_score, min: 0 }
    severity: info       # Informational only
```

## State & Diff

Track validation history and detect regressions:

```bash
# Run validation (automatically saves state)
kontra validate contract.yml

# Compare to previous run
kontra diff

# Compare to 7 days ago
kontra diff --since 7d

# Output formats
kontra diff -o json      # CI/CD integration
kontra diff -o llm       # Token-optimized for LLMs
```

Output:
```
Diff: users_contract
Comparing: 2024-01-10 14:30 -> 2024-01-12 09:15
==================================================

Overall: PASSED -> FAILED

New Blocking Failures (1)
  - COL:email:not_null (523 violations) [null_values]

Warning Regressions (1)
  - COL:status:allowed_values: 10 -> 45 (+35) [novel_category]

Resolved (1)
  - COL:age:range
```

## Performance

Kontra optimizes validation through:

1. **Preplan**: Uses Parquet row-group statistics to prove rules without scanning
2. **SQL Pushdown**: Executes rules in database (DuckDB/PostgreSQL/SQL Server)
3. **Projection**: Loads only columns needed by rules

```bash
# Show what happens
kontra validate contract.yml --stats summary --explain-preplan
```

Output:
```
Stats  rows=1,000,000  cols=12  duration=234 ms  engine=hybrid
Preplan: analyze=12 ms
SQL pushdown: compile=5 ms, execute=45 ms
Projection [on]: 4/12 (req/avail) (pruned)
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All blocking rules passed |
| 1 | Validation failed (data quality issue) |
| 2 | Configuration error (contract/file not found) |
| 3 | Runtime error (unexpected failure) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KONTRA_VERBOSE` | Detailed error output |
| `KONTRA_ENV` | Default environment profile |
| `PGHOST`, `PGUSER`, etc. | PostgreSQL connection |
| `AWS_ACCESS_KEY_ID` | S3 credentials |
| `AWS_ENDPOINT_URL` | MinIO/custom S3 |

## Documentation

- [Quick Start Guide](docs/quickstart.md)
- [Rule Reference](docs/rules.md)
- [Architecture Guide](docs/architecture.md)
- [Configuration Guide](docs/config.md)

## License

MIT
