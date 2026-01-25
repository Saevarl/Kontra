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

result = kontra.validate("data.parquet", rules=[
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

Pass file paths directly—no need to load data yourself. Kontra handles Parquet, CSV, and database connections.

### With DataFrames

Already have data loaded? Pass it directly:

```python
import polars as pl

df = pl.read_parquet("data.parquet")
result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
])
```

Works with Polars and pandas DataFrames.

## Common Patterns

### Validate with a Contract File

```python
result = kontra.validate("data.parquet", "contracts/users.yml")
```

### Mix Contract and Inline Rules

```python
result = kontra.validate("data.parquet", "contracts/base.yml", rules=[
    rules.freshness("updated_at", max_age="24h"),
])
```

### Profile Data

```python
profile = kontra.profile("data.parquet")
print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")

for col in profile.columns:
    print(f"  {col.name}: {col.dtype}, {col.null_rate:.0%} null")
```

Presets control profiling depth:
- `"scout"` - Quick recon (metadata only, no table scan)
- `"scan"` - Full stats via metadata + strategic queries [default]
- `"interrogate"` - Deep investigation (full table scan + percentiles)

### Draft Rules from Profile

```python
profile = kontra.profile("data.parquet", preset="interrogate")
suggestions = kontra.draft(profile)

# Use directly
result = kontra.validate("data.parquet", rules=suggestions.to_dict())

# Or save as contract
suggestions.save("contracts/generated.yml")
```

### Validate Database Tables

```python
import psycopg
import kontra
from kontra import rules

# Pass your own connection
conn = psycopg.connect(host="localhost", dbname="myapp")
result = kontra.validate(
    conn,
    table="public.users",
    rules=[rules.not_null("user_id")],
)
conn.close()

# Or use a direct URI
result = kontra.validate(
    "postgres://user:pass@host/db/public.users",
    rules=[rules.not_null("id")]
)
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

### Named Datasources (Alternative)

Instead of passing connections or URIs, you can define reusable datasources in `.kontra/config.yml`:

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

Then reference by name:

```python
result = kontra.validate("prod_db.users", "contracts/users.yml")

result = kontra.validate("prod_db.orders", rules=[
    rules.not_null("order_id"),
    rules.range("quantity", min=1),
])
```

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

### Debug Failed Rules

When validation fails, each rule includes sample failing rows for debugging:

```python
result = kontra.validate("data.parquet", rules=[
    rules.not_null("email"),
    rules.allowed_values("status", ["active", "inactive"]),
])

if not result.passed:
    for rule in result.blocking_failures:
        print(f"{rule.rule_id}: {rule.failed_count} failures")
        for row in rule.samples or []:
            print(f"  Row {row['_row_index']}: {row}")
```

By default, no samples are collected (`sample=0`) for performance. Enable with `sample` and `sample_budget` parameters:

```python
result = kontra.validate(..., sample=5)  # Collect up to 5 samples per rule
result = kontra.validate(..., sample=10, sample_budget=100)  # More samples
```

For token efficiency (e.g., when working with LLMs), limit which columns appear in samples:

```python
# Only include specific columns in samples
result = kontra.validate(..., sample_columns=["id", "email", "status"])

# Only include columns relevant to each rule (+ _row_index)
result = kontra.validate(..., sample_columns="relevant")
```

For more samples than cached, use `sample_failures()`:

```python
samples = result.sample_failures("COL:email:not_null", n=20)
```

**Note**: For database connections (BYOC), keep the connection open until done with `sample_failures()`.

### Pipeline Validation Decorator

Validate data returned from functions with the `@kontra.validate_decorator`:

```python
import kontra
from kontra import rules

@kontra.validate_decorator(
    rules=[rules.not_null("id"), rules.unique("email")],
    on_fail="raise",  # "raise" | "warn" | "return_result"
)
def load_users():
    return pl.read_parquet("users.parquet")

# Call as normal - validation happens automatically
users = load_users()  # Raises ValidationError if validation fails
```

**`on_fail` options:**

| Option | Behavior |
|--------|----------|
| `"raise"` | Raise `ValidationError` on blocking failures—pipeline stops (default) |
| `"warn"` | Emit warning to stderr, return data—pipeline continues |
| `"return_result"` | Return `(data, ValidationResult)` tuple—caller decides |
| `callable` | Custom callback `(result, data) -> data`—you decide |

```python
# Custom callback: Kontra measures, you decide
def notify_and_continue(result, data):
    if not result.passed:
        slack.post(f"Validation failed: {result.failed_count} violations")
    return data

@kontra.validate_decorator(rules=[...], on_fail=notify_and_continue)
def fetch_data():
    ...

# Lambda for quick transformations
@kontra.validate_decorator(
    rules=[...],
    on_fail=lambda result, data: data.drop_nulls() if not result.passed else data
)
def get_orders():
    ...

# Get validation result alongside data
@kontra.validate_decorator(rules=[...], on_fail="return_result")
def load_users():
    ...

data, result = load_users()
if not result.passed:
    print(f"Validation issues: {result.failed_count}")
```

Works with contract files too:

```python
@kontra.validate_decorator(contract="contracts/users.yml", on_fail="raise")
def load_users():
    return pl.read_parquet("users.parquet")
```

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
rules.conditional_range("discount", when="tier == 'premium'", min=10, max=50)

# Dataset checks
rules.min_rows(1000)
rules.max_rows(1000000)
rules.freshness("updated_at", max_age="24h")
rules.custom_sql_check("SELECT * FROM {table} WHERE balance < 0")

# All rules accept optional parameters
rules.not_null("email", severity="warning")  # "blocking" | "warning" | "info"
rules.range("score", min=0, max=100, id="score_range")  # custom rule ID
```

### Rule Context in Contracts

Add consumer-defined context to rules in contract files. Kontra stores this data but doesn't use it for validation—consumers/agents access it for routing, explanations, or fix hints:

```yaml
rules:
  - name: not_null
    params:
      column: email
    context:
      owner: data_eng_team
      fix_hint: Ensure email is provided for all users
      tags: ["daily_check", "critical"]
```

Access in code:

```python
result = kontra.validate("data.parquet", "contract.yml")

for rule in result.blocking_failures:
    msg = rule.message
    if rule.context and rule.context.get("fix_hint"):
        msg += f" → {rule.context['fix_hint']}"
    print(msg)
```

## Working with Results

```python
result = kontra.validate("data.parquet", rules=[...])

# Status
result.passed          # bool
result.total_rows      # int - row count of validated dataset
result.total_rules     # int
result.failed_count    # int

# Per-rule violation rates (LLM-friendly)
for rule in result.rules:
    if rule.violation_rate:  # None if passed or no failures
        print(f"{rule.rule_id}: {rule.violation_rate:.2%} of rows failed")

# Iterate rules
for rule in result.rules:
    print(f"{rule.rule_id}: {'PASS' if rule.passed else 'FAIL'}")

# Filter failures
for rule in result.blocking_failures:
    print(f"{rule.rule_id}: {rule.failed_count} violations")

for rule in result.warnings:
    print(f"Warning: {rule.rule_id}")

# Access rule details for programmatic use
for rule in result.rules:
    print(f"{rule.rule_id}:")
    print(f"  message: {rule.message}")  # Human-readable description
    print(f"  details: {rule.details}")  # Structured failure data
    print(f"  context: {rule.context}")  # Consumer-defined metadata

# Serialize
result.to_dict()       # dict
result.to_json()       # JSON string
```

---

## Going Deeper

### Validation Options

```python
result = kontra.validate(
    "data.parquet",
    "contract.yml",
    preplan="auto",      # "on" | "off" | "auto"
    pushdown="auto",     # "on" | "off" | "auto"
    projection=True,     # column pruning
    csv_mode="auto",     # "auto" | "duckdb" | "parquet" (for CSV files)
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
check = kontra.validate(None, "contract.yml", dry_run=True)
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

### Annotations

Annotations provide "memory without authority"—agents and humans can record context about validation runs without affecting Kontra's behavior:

```python
# Annotate the latest run for a contract
kontra.annotate(
    "users_contract.yml",
    actor_type="agent",
    actor_id="repair-agent-v2",
    annotation_type="resolution",
    summary="Fixed null emails by backfilling from user_profiles",
)

# Annotate a specific rule
kontra.annotate(
    "users_contract.yml",
    rule_id="COL:email:not_null",
    actor_type="human",
    actor_id="alice@example.com",
    annotation_type="false_positive",
    summary="Service accounts are expected to have null emails",
)

# Annotate with structured payload
kontra.annotate(
    "users_contract.yml",
    actor_type="agent",
    actor_id="analysis-agent",
    annotation_type="root_cause",
    summary="Upstream data source failed validation",
    payload={
        "upstream_source": "crm_export",
        "failure_time": "2024-01-15T08:30:00Z",
        "affected_rows": 1523,
    },
)
```

**Invariant**: Kontra never reads annotations during validation or diff. They're purely for consumer use.

Load runs with annotations:

```python
result = kontra.get_run_with_annotations("users_contract.yml")

# Run-level annotations
for ann in result.annotations or []:
    print(f"[{ann['annotation_type']}] {ann['summary']}")

# Rule-level annotations
for rule in result.rules:
    for ann in rule.annotations or []:
        print(f"  {rule.rule_id}: [{ann['annotation_type']}] {ann['summary']}")
```

Common annotation types (suggested, not enforced):
- `"resolution"`: I fixed this
- `"root_cause"`: This failed because...
- `"false_positive"`: This isn't actually a problem
- `"acknowledged"`: I saw this, will address later
- `"suppressed"`: Intentionally ignoring this
- `"note"`: General comment

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
| `kontra.profile(data, **opts)` | Profile data (presets: scout, scan, interrogate) |
| `kontra.draft(profile)` | Draft rules from profile |
| `kontra.diff(contract, **opts)` | Compare validation runs |
| `kontra.list_rules()` | List all available rule types |
| `@kontra.validate_decorator(...)` | Decorator for pipeline validation |

### Transformation Probes

| Function | Description |
|----------|-------------|
| `kontra.compare(before, after, key)` | Measure transformation effects between datasets |
| `kontra.profile_relationship(left, right, on)` | Measure JOIN structure between datasets |

See [Transformation Probes](probes.md) for full documentation.

### History Functions

| Function | Description |
|----------|-------------|
| `kontra.get_history(contract, since=None, limit=None, failed_only=False)` | Get validation history with filtering |
| `kontra.list_runs(contract)` | List past validation runs |
| `kontra.get_run(contract, run_id=None)` | Get specific run (default: latest) |
| `kontra.has_runs(contract)` | Check if history exists for contract |

### Annotation Functions

| Function | Description |
|----------|-------------|
| `kontra.annotate(contract, ...)` | Add annotation to a run or rule |
| `kontra.get_annotations(contract, rule_id=, ...)` | Query annotations across runs |
| `kontra.get_run_with_annotations(contract)` | Get run with annotations loaded |

### Result Types

| Type | Key Properties |
|------|----------------|
| `ValidationResult` | `passed`, `total_rows`, `data`, `rules`, `blocking_failures`, `warnings`, `annotations` (opt-in), `sample_failures()`, `to_dict()`, `to_json()`, `to_llm()` |
| `FailureSamples` | `count`, `rule_id`, `to_dict()`, `to_json()`, `to_llm()` (iterable) |
| `RuleResult` | `rule_id`, `name`, `passed`, `failed_count`, `violation_rate`, `severity`, `message`, `column`, `details`, `context`, `annotations` (opt-in), `samples`, `samples_source`, `samples_reason` |
| `DryRunResult` | `valid`, `rules_count`, `columns_needed`, `errors` |
| `DatasetProfile` | `row_count`, `column_count`, `columns` |
| `ColumnProfile` | `name`, `dtype`, `null_rate`, `unique_count` |
| `Diff` | `has_changes`, `regressed`, `new_failures`, `resolved` |
| `Suggestions` | `filter(min_confidence)`, `to_dict()`, `to_yaml()`, `save(path)` |
| `CompareResult` | `row_delta`, `duplicated_after`, `changed_rows`, `columns_modified`, `samples_*` |
| `RelationshipProfile` | `left_key_multiplicity_max`, `right_key_multiplicity_max`, `*_keys_with_match`, `samples_*` |

All result types support `to_dict()`, `to_json()`, and `to_llm()` for serialization.

### Error Handling

```python
from kontra.errors import (
    KontraError,           # base class
    ContractNotFoundError,
    ContractParseError,
    DataNotFoundError,
    ConnectionError,
    DuplicateRuleIdError,
)
from kontra import ValidationError  # raised by @validate_decorator

try:
    result = kontra.validate("data.parquet", "contract.yml")
except ContractNotFoundError as e:
    print(f"Contract not found: {e}")
except DuplicateRuleIdError as e:
    # Multiple rules with same auto-generated ID
    print(f"Duplicate rule ID: {e.rule_id}")
    print(f"Add explicit 'id' field to distinguish rules")
except KontraError as e:
    print(f"Kontra error: {e}")

# ValidationError from decorator
try:
    users = load_users()  # decorated function
except ValidationError as e:
    print(f"Validation failed: {e}")
    print(f"Failed rules: {len(e.result.blocking_failures)}")
```
