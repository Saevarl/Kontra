# Kontra Quick Start

Get from zero to validated in under 5 minutes.

## Installation

```bash
pip install kontra
```

For database support:
```bash
pip install kontra[postgres]    # PostgreSQL
pip install kontra[sqlserver]   # SQL Server
pip install kontra[s3]          # S3/MinIO
pip install kontra[all]         # Everything
```

## Initialize Project

```bash
kontra init
```

This creates:
- `.kontra/config.yml` - Project configuration
- `contracts/` - Directory for contract files

## Your First Validation

### Option 1: Auto-Generate Contract (Recommended)

Let Kontra analyze your data and generate a contract:

```bash
# Profile your data
kontra scout data.parquet

# Generate contract from profile
kontra scout data.parquet --suggest-rules > contracts/data.yml

# Run validation
kontra validate contracts/data.yml
```

### Option 2: Write Contract Manually

Create a contract YAML file:

```yaml
# contracts/users.yml
name: users_quality
dataset: data/users.parquet

rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: user_id }

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: min_rows
    params: { threshold: 1000 }
```

Run validation:

```bash
kontra validate contracts/users.yml
```

Output:
```
PASSED - users.parquet (4 of 4 rules)

  [metadata] COL:user_id:not_null
  [sql]      COL:user_id:unique
  [sql]      COL:status:allowed_values
  [metadata] DATASET:min_rows
```

## Validation Options

### Output Formats

```bash
# Rich console output (default)
kontra validate contract.yml -o rich

# JSON for CI/CD
kontra validate contract.yml -o json
```

### Statistics

```bash
kontra validate contract.yml --stats summary
```

### Dry Run (Validate Contract Only)

```bash
kontra validate contract.yml --dry-run
```

Output:
```
Dry run validation
========================================
  OK Contract syntax valid: contracts/users.yml
  OK Dataset URI parseable: data/users.parquet
  OK All 4 rules recognized
  OK All 4 rules valid

Ready to validate (4 checks passed)

Run without --dry-run to execute:
  kontra validate contracts/users.yml
```

## Scout: Profile Without a Contract

Explore your data before writing rules:

```bash
# Quick overview
kontra scout data.parquet --preset lite

# Full analysis
kontra scout data.parquet --preset standard

# Comprehensive with patterns
kontra scout data.parquet --preset deep --include-patterns

# Token-optimized for LLMs
kontra scout data.parquet --preset llm
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
  created_at  datetime 100% non-null
```

## Database Sources

### Named Datasources (Recommended)

Define your datasources once in `.kontra/config.yml`:

```yaml
# .kontra/config.yml
datasources:
  prod_db:
    type: postgres
    host: ${PGHOST}
    user: ${PGUSER}
    password: ${PGPASSWORD}
    database: ${PGDATABASE}
    tables:
      users: public.users
      orders: public.orders

  warehouse:
    type: sqlserver
    host: ${MSSQL_HOST}
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
    database: analytics
    tables:
      events: dbo.events
```

Then use simple names:

```bash
# Profile tables
kontra scout users                    # Resolves to prod_db.users
kontra scout orders
kontra scout warehouse.events

# Validate
kontra validate contract.yml --data users
```

### Direct URIs (Alternative)

**PostgreSQL:**
```bash
export PGHOST=localhost PGUSER=kontra PGPASSWORD=secret PGDATABASE=mydb
kontra scout postgres:///public.users
```

**SQL Server:**
```bash
kontra scout mssql://user:pass@localhost/mydb/dbo.users
```

**S3/MinIO:**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_ENDPOINT_URL=http://localhost:9000  # For MinIO

kontra scout s3://bucket/data.parquet
```

## Execution Modes

Kontra optimizes validation through three independent execution tiers:

```bash
# Metadata-only (fastest, when possible)
kontra validate contract.yml --preplan on

# SQL pushdown (push validation to database)
kontra validate contract.yml --pushdown on

# Column projection (load only needed columns)
kontra validate contract.yml --projection on

# All enabled (default)
kontra validate contract.yml --preplan auto --pushdown auto --projection on
```

## Debug Mode

```bash
# Show SQL plan
kontra validate contract.yml --show-plan

# Show preplan decisions
kontra validate contract.yml --explain-preplan

# Verbose errors
kontra validate contract.yml --verbose
```

## State & Diff

Track validation over time:

```bash
# Run validation (saves state automatically)
kontra validate contract.yml

# Compare to previous run
kontra diff

# Compare to 7 days ago
kontra diff --since 7d

# Compare to specific date
kontra diff --run 2024-01-15
```

## Configuration

Create project config:
```bash
kontra init
```

View effective config:
```bash
kontra config show
kontra config show --env production
```

See [Configuration Guide](config.md) for details.

## Exit Codes

| Code | Meaning | Use in CI/CD |
|------|---------|--------------|
| 0 | All blocking rules passed | Success |
| 1 | Validation failed | Data quality issue |
| 2 | Configuration error | Contract/file not found |
| 3 | Runtime error | Unexpected failure |

## Next Steps

- [Rule Reference](rules.md) - All 10 built-in rules
- [Configuration Guide](config.md) - Datasources, environments, settings
- [Architecture Guide](architecture.md) - How Kontra works
