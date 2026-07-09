# Configuration Reference

Project-level configuration for defaults, datasources, and environments.

---

## Initialize

```bash
kontra init
```

Creates `.kontra/config.yml` with documented defaults. See [Project Setup](../advanced/state-and-diff.md) for details.

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

## Configuration Precedence

Settings are resolved in this order (highest to lowest):

1. **CLI flags** (explicit user intent)
2. **Environment variables** (`KONTRA_ENV`, etc.)
3. **Environment profile** (`--env production`)
4. **Config file defaults**
5. **Hardcoded defaults**

---

## Full Config Example

```yaml
# .kontra/config.yml
version: "1"

# ─────────────────────────────────────────────────────────────
# Default Settings
# ─────────────────────────────────────────────────────────────

defaults:
  # Execution controls
  preplan: "on"          # on | off
  pushdown: "on"         # on | off
  projection: "on"       # on | off

  # Output
  output_format: "rich"  # rich | json
  stats: "none"          # none | summary | profile

  # State management
  state_backend: "local" # local | s3://... | postgres://... | mssql://...

  # CSV handling
  csv_mode: "auto"       # auto | duckdb | parquet

# ─────────────────────────────────────────────────────────────
# Profile Settings
# ─────────────────────────────────────────────────────────────

profile:
  preset: "scan"              # scout | scan | interrogate
  save_profile: false         # Auto-save profiles for diffing
  # list_values_threshold: 10 # List all values if distinct <= N
  # top_n: 5                  # Show top N frequent values
  # include_patterns: false   # Detect patterns (email, uuid, etc.)

# ─────────────────────────────────────────────────────────────
# Datasources
# ─────────────────────────────────────────────────────────────

datasources:
  # PostgreSQL
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

  # SQL Server
  warehouse:
    type: mssql
    host: ${MSSQL_HOST}
    port: 1433
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
    database: ${MSSQL_DATABASE}
    tables:
      sales: dbo.sales
      inventory: dbo.inventory

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

---

## Environment Variable Substitution

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
datasources:
  prod_db:
    host: ${PGHOST}           # Resolves from env
    password: ${PGPASSWORD}   # Secrets stay in env
```

Missing variables resolve to empty string.

---

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
      users: public.users
      orders: public.orders
```

Usage:
```bash
kontra validate contract.yml --data prod_db.users
kontra profile prod_db.orders
```

### SQL Server

```yaml
datasources:
  warehouse:
    type: mssql
    host: ${MSSQL_HOST}
    port: 1433
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
    database: ${MSSQL_DATABASE}
    tables:
      sales: dbo.sales
```

Usage:
```bash
kontra profile warehouse.sales
```

#### Entra ID (Azure AD) authentication

On Azure compute (VMs, App Service, Container Apps, AKS, Azure ML) Kontra can
authenticate to **Azure SQL Managed Instance** and **Azure SQL Database** with
Entra ID instead of a password. The Microsoft ODBC driver acquires the token, so
no secrets live in your config.

Managed Instance with the environment's default credential (recommended):

```yaml
datasources:
  mi:
    type: mssql
    host: mymi.abcd1234.database.windows.net  # MI private endpoint (port 1433)
    port: 1433
    database: sales
    auth: entra_default        # DefaultAzureCredential: env SP -> managed identity -> az cli
    tables:
      orders: dbo.orders
```

```bash
kontra validate contract.yml --data mi.orders
```

Azure SQL Database with a managed identity:

```yaml
datasources:
  prod:
    type: mssql
    host: myserver.database.windows.net
    database: appdb
    auth: entra_mi             # system-assigned managed identity
    tables:
      users: dbo.users
```

User-assigned managed identity — set `client_id` to the identity's client id:

```yaml
    auth: entra_mi
    client_id: 11111111-2222-3333-4444-555555555555
```

Service principal (app registration):

```yaml
    auth: entra_service_principal
    client_id: ${AZURE_CLIENT_ID}
    client_secret: ${AZURE_CLIENT_SECRET}
    tenant_id: ${AZURE_TENANT_ID}
```

Equivalent direct-URI forms (the query string carries the auth mode):

```bash
# Managed Instance, public endpoint on port 3342
kontra profile "mssql://mymi.abcd1234.database.windows.net:3342/sales/dbo.orders?auth=entra_default"

# User-assigned managed identity
kontra profile "mssql://myserver.database.windows.net/appdb/dbo.users?auth=entra_mi&client_id=<id>"
```

Auth modes and their resolution:

| `auth` value | ODBC `Authentication` | Notes |
|--------------|-----------------------|-------|
| `sql` (default) | — | Username/password via pymssql. Unchanged. |
| `entra_default` | `ActiveDirectoryDefault` | Env service principal → managed identity → az cli. Recommended. |
| `entra_mi` | `ActiveDirectoryMsi` | Managed identity. Add `client_id` for user-assigned. |
| `entra_service_principal` | `ActiveDirectoryServicePrincipal` | Uses `client_id`/`client_secret`. |
| `entra_interactive` | `ActiveDirectoryInteractive` | Browser login, for dev workstations. |
| `entra_password` | `ActiveDirectoryPassword` | Entra username (UPN) + password via the normal user/password fields. Not usable with MFA-required accounts. |

The auth mode is resolved with priority: URI query string
(`?auth=…&client_id=…`) > datasource config > env vars (`MSSQL_AUTH`,
`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`) > default (`sql`).

**Requirements and notes:**

- Install the extra: `pip install kontra[sqlserver-entra]` (adds `pyodbc` and
  `azure-identity`).
- Install a Microsoft ODBC driver on the host — **msodbcsql18** (or 17). Kontra
  picks the newest `ODBC Driver NN for SQL Server` it finds.
- **Platform note (token modes):** on Linux and macOS the ODBC driver acquires
  the token itself via the `Authentication=ActiveDirectory*` keywords, so
  `azure-identity` is not strictly needed. Windows' msodbcsql18 does not support
  those keywords for the token modes (`entra_default`, `entra_mi`,
  `entra_service_principal`), so on Windows Kontra acquires the token with
  `azure-identity` and passes it to the driver via pyodbc `attrs_before`. This is
  transparent — the same `auth:` values work everywhere. If you cannot install
  `azure-identity` on Windows, `entra_password` works on all platforms without
  it.
- All Entra modes emit `Encrypt=yes` (mandatory for Managed Instance and Azure SQL).
- The identity must be mapped to a database user with the needed permissions.
- For `entra_service_principal`, the tenant is the directory of the SQL resource.
  msodbcsql18 has no dedicated tenant connection keyword, so `tenant_id` /
  `AZURE_TENANT_ID` is accepted for completeness but not injected into the
  connection string.

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

Requires `pip install kontra[s3]` and AWS credentials.

### Azure ADLS Gen2

Azure ADLS is supported via direct URIs. Named datasources are not yet available.

```bash
# Direct URI
kontra profile "abfss://container@account.dfs.core.windows.net/data/users.parquet"
```

```python
result = kontra.validate(
    "abfss://container@account.dfs.core.windows.net/data/users.parquet",
    rules=[...]
)
```

Requires environment variables:
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCESS_KEY` or `AZURE_STORAGE_SAS_TOKEN`

Account keys are validated as base64 up front — a malformed or truncated key
fails immediately with a clear error instead of an opaque HTTP failure at
query time.

**Containers:** on Linux, Kontra sets DuckDB's Azure transport to `curl`
automatically, which avoids CA-bundle lookup failures common in slim Docker
images. Override with `storage_options={"transport": "default"}` or the
`KONTRA_AZURE_TRANSPORT` environment variable (`curl` or `default`).

### ClickHouse

ClickHouse is a columnar OLAP store; Kontra pushes validation aggregates down to
it (countIf, uniqExact, native `match()` regex) so almost nothing is transferred,
and resolves `not_null`/row counts from `system.columns`/`system.parts` metadata
without scanning data — the same "use the source's metadata" strategy Kontra
applies to Parquet row groups.

```yaml
datasources:
  events:
    type: clickhouse
    host: ${CH_HOST}
    port: 8123          # HTTP interface (8443 for TLS with secure: true)
    user: ${CH_USER}
    password: ${CH_PASSWORD}
    database: analytics
    tables:
      pageviews: pageviews   # ClickHouse has no schema layer: just <table>
```

```bash
kontra validate contract.yml --data events.pageviews
kontra profile "clickhouse://user:pass@host:8123/analytics/pageviews"
```

Requires `pip install kontra[clickhouse]` (clickhouse-connect). Direct URIs use
`clickhouse://user:pass@host:8123/database/table` (or `clickhouses://` for TLS).

**Performance notes:**

- A non-`Nullable(T)` column cannot contain NULL, so `not_null` on it is proven
  from the schema with zero rows read.
- Row counts, `min_rows`/`max_rows` come from `system.parts` (exact, no scan).
- Every other rule (including regex, via `match()`) executes as a native
  ClickHouse aggregate; the Polars tier rarely runs.

---

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

---

## Settings Reference

### Execution Controls

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `preplan` | on, off | on | Metadata preflight (Parquet stats, pg_stats) |
| `pushdown` | on, off | on | SQL execution in database engine |
| `projection` | on, off | on | Column pruning at source |

See [Performance](../advanced/performance.md) for execution details.

### Output

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `output_format` | rich, json | rich | CLI output format |
| `stats` | none, summary, profile | none | Execution statistics detail |

### State

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `state_backend` | local, s3://..., postgres://..., mssql://... | local | Validation history storage |

See [State & History](../advanced/state-and-diff.md) for backend details.

### CSV Handling

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `csv_mode` | auto, duckdb, parquet | auto | CSV processing strategy |

- `auto`: Try DuckDB, fall back to staging as Parquet
- `duckdb`: Use DuckDB only (fails if DuckDB can't parse)
- `parquet`: Always stage CSV as Parquet first

### Profile

| Setting | Values | Default | Description |
|---------|--------|---------|-------------|
| `preset` | scout, scan, interrogate | scan | Profiling depth |
| `save_profile` | true, false | false | Auto-save profiles to state |
| `list_values_threshold` | integer | - | List all values if distinct <= N |
| `top_n` | integer | - | Show top N frequent values |
| `include_patterns` | true, false | false | Detect patterns (email, uuid) |

---

## CLI Commands

```bash
# Initialize project
kontra init

# View effective configuration
kontra config show

# View with environment overlay
kontra config show --env production

# View config file path
kontra config path

# Output as JSON
kontra config show -o json
```

---

## Benefits of Named Datasources

1. **Credentials stay in config** - gitignore `.kontra/` or use env vars
2. **Contracts are portable** - share contracts without credentials
3. **Central registry** - one place for all data sources
4. **Self-documenting** - `prod_db.users` is clearer than a URI

## Direct URIs Still Work

For quick validation or one-off use:

```bash
kontra validate contract.yml --data postgres://user:pass@host/db/public.users
kontra profile s3://bucket/data.parquet
```

Named datasources and direct URIs can be mixed freely.
