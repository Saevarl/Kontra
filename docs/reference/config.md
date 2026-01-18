# Configuration Reference

Project-level configuration for defaults, datasources, and environments.

## Quick Start

Initialize a project with config:

```bash
kontra init
```

This creates `.kontra/config.yml` with documented defaults.

## Config File Location

Kontra looks for `.kontra/config.yml` in the current working directory only. It does **not** search parent directories.

```bash
# View config file path
kontra config path

# View effective configuration
kontra config show

# For services/agents, set path explicitly
kontra.set_config("/path/to/config.yml")
```

If running from a subdirectory, either `cd` to the project root or use `kontra.set_config()` in Python.

## Configuration Precedence

Settings are resolved in this order (highest to lowest):

1. **CLI flags** (explicit user intent)
2. **Environment variables** (`KONTRA_ENV`, etc.)
3. **Environment profile** (`--env production`)
4. **Config file defaults**
5. **Hardcoded defaults**

## Full Config Example

```yaml
# .kontra/config.yml
version: "1"

# ─────────────────────────────────────────────────────────────
# Default Settings
# ─────────────────────────────────────────────────────────────

defaults:
  # Execution controls
  preplan: "auto"        # on | off | auto - Parquet metadata preflight
  pushdown: "auto"       # on | off | auto - SQL pushdown to database
  projection: "on"       # on | off - Column pruning at source

  # Output
  output_format: "rich"  # rich | json - CLI output format
  stats: "none"          # none | summary | profile - Statistics detail

  # State management
  state_backend: "local" # local | s3://bucket/prefix | postgres://...

  # CSV handling
  csv_mode: "auto"       # auto | duckdb | parquet

# ─────────────────────────────────────────────────────────────
# Profile Settings
# ─────────────────────────────────────────────────────────────

profile:
  preset: "scan"               # scout | scan | interrogate
  save_profile: false          # Auto-save profiles for diffing
  list_values_threshold: 10    # List all values if distinct <= N
  top_n: 5                     # Show top N frequent values
  include_patterns: false      # Detect patterns (email, uuid, etc.)

# ─────────────────────────────────────────────────────────────
# Datasources
# ─────────────────────────────────────────────────────────────

datasources:
  # PostgreSQL database
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
      products: inventory.products

  # Local files
  local_data:
    type: files
    base_path: ./data
    tables:
      events: events.parquet
      metrics: metrics.csv

  # S3 data lake
  data_lake:
    type: s3
    bucket: ${S3_BUCKET}
    prefix: warehouse/
    tables:
      transactions: transactions.parquet
      logs: logs/

# ─────────────────────────────────────────────────────────────
# Environments
# ─────────────────────────────────────────────────────────────

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}
    preplan: "on"
    pushdown: "on"
    output_format: "json"

  staging:
    state_backend: s3://${S3_BUCKET}/kontra-state/
    stats: "summary"

  local:
    state_backend: "local"
    stats: "profile"
```

## Environment Variable Substitution

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
datasources:
  prod_db:
    host: ${PGHOST}           # Resolves from env
    password: ${PGPASSWORD}   # Secrets stay in env
```

Missing variables resolve to empty string.

## Datasources

### PostgreSQL

```yaml
datasources:
  prod_db:
    type: postgres
    host: ${PGHOST}
    port: 5432
    user: ${PGUSER}
    password: ${PGPASSWORD}
    database: ${PGDATABASE}
    tables:
      users: public.users      # alias: schema.table
      orders: public.orders
```

Usage:
```bash
kontra validate contract.yml --data prod_db.users
kontra profile prod_db.orders
```

Resolves to:
```
postgres://user:pass@host:5432/database/public.users
```

### Local Files

```yaml
datasources:
  local_data:
    type: files
    base_path: ./data
    tables:
      users: users.parquet
      orders: orders/orders.csv
```

Usage:
```bash
kontra profile local_data.users
```

Resolves to: `data/users.parquet`

### S3

```yaml
datasources:
  data_lake:
    type: s3
    bucket: my-bucket
    prefix: warehouse/
    tables:
      events: events.parquet
```

Usage:
```bash
kontra profile data_lake.events
```

Resolves to: `s3://my-bucket/warehouse/events.parquet`

## Environments

Define named profiles for different contexts:

```yaml
environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}
    preplan: "on"
    pushdown: "on"
    output_format: "json"

  development:
    state_backend: "local"
    stats: "profile"
```

Activate with `--env`:

```bash
kontra validate contract.yml --env production
```

Or set default via environment variable:

```bash
export KONTRA_ENV=production
kontra validate contract.yml
```

## Settings Reference

### Execution Controls

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `preplan` | on, off, auto | auto | Metadata preflight |
| `pushdown` | on, off, auto | auto | SQL pushdown |
| `projection` | on, off | on | Column pruning |

See [Performance](../advanced/performance.md) for execution details.

### Output

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `output_format` | rich, json | rich | CLI output format |
| `stats` | none, summary, profile | none | Statistics detail |

### State

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `state_backend` | local, s3://..., postgres://... | local | State storage |

### CSV Handling

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `csv_mode` | auto, duckdb, parquet | auto | CSV processing strategy |

### Scout

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `preset` | lite, standard, deep, llm | standard | Profiling depth |
| `save_profile` | true, false | false | Auto-save profiles |
| `list_values_threshold` | integer | 10 | List all if distinct <= N |
| `top_n` | integer | 5 | Top N frequent values |
| `include_patterns` | true, false | false | Detect patterns |

## CLI Commands

```bash
# Initialize project with config
kontra init

# View effective configuration
kontra config show

# View config with environment overlay
kontra config show --env production

# View config file location
kontra config path

# Output as JSON
kontra config show -o json
```

## Benefits of Named Datasources

1. **Credentials stay in config** - gitignore `.kontra/` or use env vars
2. **Contracts are portable** - share contracts without credentials
3. **Central registry** - one place for all data sources
4. **Self-documenting** - `prod_db.users` is clearer than a URI

## Migration from Direct URIs

Before (in contract):
```yaml
datasource: postgres://user:pass@host:5432/mydb/public.users
```

After (in config):
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
```

```yaml
# contract.yml
datasource: prod_db.users
```

Direct URIs still work for backward compatibility:
```bash
kontra validate contract.yml --data postgres://...
```
