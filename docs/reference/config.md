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

### Trino

Trino is a distributed SQL query engine. Tables live in a catalog (Iceberg,
Hive, PostgreSQL or any other Trino connector), so a table reference has three
parts: `catalog/schema.table`.

```yaml
datasources:
  lake:
    type: trino
    host: ${TRINO_HOST}
    port: 8080
    user: ${TRINO_USER}
    catalog: iceberg
    tables:
      orders: sales.orders   # <schema>.<table> within the catalog
```

```bash
kontra validate contract.yml --data lake.orders
kontra profile "trino://user@host:8080/iceberg/sales.orders"
```

Requires `pip install kontra[trino]` (the `trino` client). Direct URIs use
`trino://user@host:8080/catalog/schema.table`; `catalog/schema/table` also
works. A password enables HTTP basic authentication, which Trino only accepts
over HTTPS: use `trinos://user:pass@host:443/catalog/schema.table`, or set
`secure: true` and `password: ${TRINO_PASSWORD}` on the datasource.

You can also pass your own `trino.dbapi` connection:

```python
conn = trino.dbapi.connect(host="localhost", port=8080, user="kontra")
kontra.validate(conn, table="iceberg.sales.orders", rules=[rules.not_null("id")])
```

Kontra never changes your connection: it sets no session property or isolation
level, and never commits or rolls back. A validation through a URI or a named
datasource opens one connection for all its queries and closes it at the end,
also when the validation fails. Connections Kontra opens use a UTC session time
zone. `freshness` on a naive `timestamp` or `date` column compares with the
current time in UTC on any connection, as the Polars tier does.

**One table state per validation.** Every query of a validation (column types,
metadata, pushed-down rules, custom SQL and the Polars-tier fetch) describes the
same state of an Iceberg table, even when a writer commits in between:

- On Kontra's own connection, the validation runs in one Trino transaction. A
  check reads the table's newest metadata file at the start and after the last
  query. If a commit landed, the validation runs once more in a new
  transaction. If the table changes again, it raises `kontra.errors.DataError`
  instead of returning a result that no single table state explains.
- On your connection with an isolation level, the validation runs inside your
  transaction, which Kontra leaves open. A detected change raises at once,
  because a rerun would read the same transaction.
- On your autocommit connection, Kontra can't open a transaction. If the current
  snapshot is the table's latest commit and reads with the current columns, the
  table's queries are pinned to it with `FOR VERSION AS OF`. Otherwise it runs with the same check, one rerun, and then
  `DataError`. Custom SQL can name the table past the pin, and the file
  metadata can't be pinned, so validations that use either keep the check.
- Views, materialized views and tables in other catalogs have no state Kontra
  can hold, so Kontra doesn't hold them to one.

**Performance notes:**

- Some rules are settled from metadata with zero rows read:
    - `not_null` on a column declared `NOT NULL` in `information_schema.columns`
    - `dtype`, from the declared type, through the same type map the Polars tier
      loads with (`integer` is `Int32`, as in Parquet)
    - on an Iceberg table, from the data files' statistics in `$files`:
      `not_null`, `conditional_not_null`, `range` on integer, decimal and date
      columns with integer or date bounds, `min_rows` / `max_rows`, and
      `allowed_values` on an identity-partition column. Per-file counts are
      exact; bounds are only bounds, so a rule passes only when every file
      proves it. Delete files leave the statistics counting deleted rows, so
      with delete files present only a PASS is taken from them.

  Trino's table statistics (`SHOW STATS`) are estimates and never decide a rule.
- The other rules run in one aggregate over the table, in fail-fast mode too: on
  Trino a passing `EXISTS` probe reads the whole table, so one shared scan is
  cheaper than a probe per rule. Fail-fast still reports a violation as at
  least 1. `unique` joins that scan on tables with fewer than 1M rows; on larger
  tables, or when the size is unknown, each `unique` column gets its own
  `GROUP BY ... HAVING count(*) > 1` query, which needs little memory. Those
  queries and custom SQL run beside the scan, up to four at a time. Inside your
  transaction, custom SQL runs after the scan, one query at a time, because a
  failed query aborts the transaction.
- A rule is pushed to Trino only when Trino's answer matches the Polars tier for
  the column's type. Where Trino's semantics differ (NaN compares as false,
  `CHAR(n)` drops its padding, regex is Java), the SQL writes Polars' meaning
  out: `range` on `real`/`double` counts NaN as out of range, `compare`
  between two of them uses Polars' NaN order, string rules on `CHAR(n)` read the padded value, and `unique` treats NaN as
  one value. These rules run in Polars instead, and report
  `execution_source: polars`:
    - value lists on `real`/`double` columns, and `range` bounds that don't
      convert to the same float on both sides (NaN, infinities, integers beyond
      2^53)
    - `unique` and `compare` on `CHAR(n)` columns, and value lists on them with
      non-string values
    - value lists whose literal type doesn't match the column
    - `range` on any column but an integer, decimal or float column, and float
      literals against integer or decimal columns
    - `compare` between columns of different types, other than integer with
      decimal
    - every rule but `not_null` on other types (`uuid`, `time`, arrays, maps,
      rows), and on timestamps finer than microseconds (`timestamp(7)` and
      up), which load at microsecond precision
    - regex outside the subset that Rust and Java read the same way: shorthand
      classes (`\w`, `\d`, `\s`, `\b`), inline flags, lookaround,
      backreferences, nested classes, and a `-` inside a class anywhere other
      than at either end or between two plain characters
- The Polars-tier fetch decodes Trino's values column by column, in chunks of
  100,000 rows. A `timestamp with time zone` column holding several zones loads
  as UTC instants.
- `kontra.profile()` computes every statistic in Trino. The row count is an
  exact `COUNT(*)`. `scout` runs one aggregate with exact null counts and
  `approx_distinct` distinct counts, which are labelled estimated. `scan` and
  `interrogate` count distinct values exactly. Exact percentiles hold a
  column's values in memory, so use `sample` on very large tables. With
  `sample`, an Iceberg table with at least 100 data files is sampled with
  `TABLESAMPLE SYSTEM`, which reads only the files it picks. If that sample has
  fewer rows than requested, the profile uses the first rows instead, as it
  does for every other table.

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
