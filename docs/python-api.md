# Python API

Use Kontra as a library in your Python code.

## Install

```bash
pip install kontra
```

## Basic Usage

```python
import kontra
from kontra import rules
import polars as pl

df = pl.read_parquet("data.parquet")

result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.range("age", min=0, max=150),
])

if result.passed:
    print("All rules passed!")
else:
    for rule in result.blocking_failures:
        print(f"FAILED: {rule.rule_id} - {rule.message}")
```

## Common Patterns

### Validate with a Contract File

```python
result = kontra.validate(df, "contracts/users.yml")
```

### Mix Contract and Inline Rules

```python
result = kontra.validate(df, "contracts/base.yml", rules=[
    rules.freshness("updated_at", max_age="24h"),
])
```

### Profile Data

```python
profile = kontra.scout(df)
print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")

for col in profile.columns:
    print(f"  {col.name}: {col.dtype}, {col.null_rate:.0%} null")
```

### Generate Rules from Profile

```python
profile = kontra.scout(df, preset="deep")
suggestions = kontra.suggest_rules(profile)

# Use directly
result = kontra.validate(df, rules=suggestions.to_dict())

# Or save as contract
suggestions.save("contracts/generated.yml")
```

### Validate Database Tables

```python
# Named datasource (from .kontra/config.yml)
result = kontra.validate("prod_db.users", "contracts/users.yml")

# Named datasource with inline rules
result = kontra.validate("prod_db.orders", rules=[
    rules.not_null("order_id"),
    rules.range("quantity", min=1),
    rules.allowed_values("status", ["pending", "shipped", "delivered"]),
])

# Direct URI
result = kontra.validate(
    "postgres://user:pass@host/db/public.users",
    rules=[rules.not_null("id")]
)
```

Named datasources are defined in `.kontra/config.yml`:

```yaml
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: myapp
    tables:
      users: public.users
      orders: public.orders
```

### BYOC (Bring Your Own Connection)

Use your own database connection with Kontra. This gives you full control over connection management (pooling, auth, lifecycle) while Kontra still performs SQL pushdown and preplan optimizations.

```python
import psycopg
import kontra
from kontra import rules

# Create your connection (you manage its lifecycle)
conn = psycopg.connect(
    host="localhost",
    dbname="myapp",
    user="app_user",
)

# Validate using your connection
result = kontra.validate(
    conn,                           # Your connection object
    table="public.users",           # Required: table to validate
    rules=[
        rules.not_null("user_id"),
        rules.unique("email"),
    ],
)

# Important: YOU close the connection when done
conn.close()
```

**Supported connection types:**
- `psycopg` / `psycopg2` / `psycopg3` (PostgreSQL)
- `pg8000` (PostgreSQL)
- `pyodbc` (SQL Server, PostgreSQL via ODBC)
- `pymssql` (SQL Server)
- SQLAlchemy engines and connections

**Table reference formats:**
- `"users"` - uses default schema (public for PostgreSQL, dbo for SQL Server)
- `"public.users"` - schema.table
- `"mydb.dbo.orders"` - database.schema.table

**Benefits:**
- Use your existing connection pool
- Control authentication and credentials
- Integrate with your connection management patterns
- Rules execute as SQL on your connection (pushdown)

### Validate JSON Data

Validate lists of dicts (e.g., API responses) or single records:

```python
# List of dicts (flat tabular JSON)
api_data = [
    {"id": 1, "email": "alice@example.com", "status": "active"},
    {"id": 2, "email": "bob@example.com", "status": "pending"},
]

result = kontra.validate(api_data, rules=[
    rules.not_null("email"),
    rules.allowed_values("status", ["active", "pending", "inactive"]),
])

# Single dict (single record validation)
record = {"id": 1, "email": "test@example.com"}
result = kontra.validate(record, rules=[
    rules.not_null("email"),
    rules.regex("email", r".*@.*"),
])
```

This is useful for:
- Validating API response data before processing
- Single-record validation in web applications
- Testing data transformations

**Note**: Data must be flat (tabular). Nested JSON like `{"user": {"email": "..."}}` should be flattened before validation.

## Available Rules

```python
from kontra import rules

# Column checks
rules.not_null("column")
rules.unique("column")
rules.dtype("column", "int64")
rules.range("column", min=0, max=100)
rules.allowed_values("column", ["a", "b", "c"])
rules.regex("column", r"^[A-Z]{2}\d{4}$")

# Cross-column checks
rules.compare("end_date", "start_date", ">=")
rules.conditional_not_null("shipping_date", when="status == 'shipped'")

# Dataset checks
rules.min_rows(1000)
rules.max_rows(1000000)
rules.freshness("updated_at", max_age="24h")

# All rules accept optional parameters
rules.not_null("email", severity="warning")  # "blocking" | "warning" | "info"
rules.range("score", min=0, max=100, id="score_range")  # custom rule ID
```

## Working with Results

```python
result = kontra.validate(df, rules=[...])

# Status
result.passed          # bool
result.total_rules     # int
result.failed_count    # int

# Iterate rules
for rule in result.rules:
    print(f"{rule.rule_id}: {'PASS' if rule.passed else 'FAIL'}")

# Filter failures
for rule in result.blocking_failures:
    print(f"{rule.rule_id}: {rule.failed_count} violations")

for rule in result.warnings:
    print(f"Warning: {rule.rule_id}")

# Serialize
result.to_dict()       # dict
result.to_json()       # JSON string
```

---

## Going Deeper

### Validation Options

```python
result = kontra.validate(
    df,
    "contract.yml",
    preplan="auto",      # "on" | "off" | "auto"
    pushdown="auto",     # "on" | "off" | "auto"
    projection=True,     # column pruning
    env="production",    # environment from config
    save=True,           # save to history (default: True, use False to disable)
)
```

### Execution Model

When you pass a DataFrame, Kontra validates in-memory using Polars.

When you pass a file path or database URI, Kontra uses a tiered execution model:
1. **Metadata preplan**: Check Parquet statistics or pg_stats (instant)
2. **SQL pushdown**: Run rules as SQL aggregates (DuckDB, PostgreSQL, SQL Server)
3. **Polars fallback**: Load data for remaining rules

For full details, see [Execution Model](advanced/performance.md).

### Dry Run

Validate contract/rules syntax without executing against data:

```python
check = kontra.validate(df, "contract.yml", dry_run=True)
check.valid          # bool - is contract syntax valid?
check.rules_count    # int - number of rules that would run
check.columns_needed # list - columns the contract requires
check.contract_name  # str - name from contract (if any)
check.errors         # list - any parse/validation errors
```

Use `dry_run=True` to:
- Validate contract files before deploying to production
- Check which columns a contract needs without loading data
- Catch syntax errors early in CI/CD pipelines

```python
# Example: Pre-validate contracts
check = kontra.validate(None, "new_contract.yml", dry_run=True)
if not check.valid:
    print(f"Contract errors: {check.errors}")
    sys.exit(1)
print(f"Contract OK: {check.rules_count} rules need columns {check.columns_needed}")
```

Note: `save=False` skips state persistence but still executes validation. Use `dry_run=True` to skip execution entirely.

### Compare Runs Over Time

```python
# Compare latest to previous run
diff = kontra.diff("my_contract")

if diff.regressed:
    print("Quality regressed!")
    for rule in diff.new_failures:
        print(f"  NEW: {rule.rule_id}")
```

For full diff capabilities, see [State & Diff](advanced/state-and-diff.md).

### LLM-Optimized Output

```python
result.to_llm()   # token-efficient string
profile.to_llm()  # token-efficient string
diff.to_llm()     # token-efficient string
```

For agent integration, see [Agents & Services](advanced/agents-and-llms.md).

---

## Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `kontra.validate(data, contract, **opts)` | Validate data |
| `kontra.scout(data, **opts)` | Profile data |
| `kontra.suggest_rules(profile)` | Generate rules from profile |
| `kontra.diff(contract, **opts)` | Compare validation runs |

### Result Types

| Type | Key Properties |
|------|----------------|
| `ValidationResult` | `passed`, `rules`, `blocking_failures`, `warnings` |
| `RuleResult` | `rule_id`, `passed`, `failed_count`, `severity`, `message` |
| `DryRunResult` | `valid`, `rules_count`, `columns_needed`, `errors` |
| `Profile` | `row_count`, `column_count`, `columns` |
| `ColumnProfile` | `name`, `dtype`, `null_rate`, `unique_count` |
| `Diff` | `has_changes`, `regressed`, `new_failures`, `resolved` |

### Error Handling

```python
from kontra.errors import (
    KontraError,           # base class
    ContractNotFoundError,
    ContractParseError,
    DataNotFoundError,
    ConnectionError,
)

try:
    result = kontra.validate(df, "contract.yml")
except ContractNotFoundError as e:
    print(f"Contract not found: {e}")
except KontraError as e:
    print(f"Kontra error: {e}")
```
