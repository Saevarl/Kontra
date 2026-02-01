# Python API

Validate files, databases, and DataFrames. Profile data. Draft contracts. Track quality over time.

## Basic Usage

```python
import kontra
from kontra import rules

result = kontra.validate("users.parquet", rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.range("age", min=0, max=120),
])

if result.passed:
    print("All rules passed!")
else:
    for rule in result.blocking_failures:
        print(f"{rule.rule_id}: {rule.message}")
```

### DataFrames

```python
import polars as pl

df = pl.read_parquet("users.parquet")
result = kontra.validate(df, rules=[...])
```

Works with Polars and pandas DataFrames.

### Contracts

```python
result = kontra.validate("users.parquet", "contracts/users.yml")

# Mix contract and inline rules
result = kontra.validate("users.parquet", "contracts/base.yml", rules=[
    rules.freshness("updated_at", max_age="24h"),
])
```

### Databases

```python
# URI
result = kontra.validate(
    "postgres://user:pass@localhost:5432/myapp/public.users",
    rules=[rules.not_null("user_id")]
)

# Bring your own connection
import psycopg
conn = psycopg.connect(host="localhost", dbname="myapp")
result = kontra.validate(conn, table="public.users", rules=[...])
```

Works with common PostgreSQL and SQL Server drivers, plus SQLAlchemy engines.

### Cloud Storage

```python
# S3 with environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
result = kontra.validate("s3://bucket/data.parquet", rules=[...])

# S3 with explicit credentials
result = kontra.validate(
    "s3://bucket/data.parquet",
    storage_options={
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "us-east-1",
    },
    rules=[...]
)

# MinIO / S3-compatible
result = kontra.validate(
    "s3://bucket/data.parquet",
    storage_options={
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "aws_region": "us-east-1",
        "endpoint_url": "http://localhost:9000",
    },
    rules=[...]
)

# Azure ADLS Gen2 (uses AZURE_STORAGE_* env vars)
result = kontra.validate(
    "abfss://container@account.dfs.core.windows.net/data.parquet",
    rules=[...]
)
```

The `storage_options` parameter also works with `profile()`.

### Dicts and Lists

Two formats are supported:

```python
# List of dicts (row-oriented)
data = [
    {"id": 1, "email": "alice@example.com", "status": "active"},
    {"id": 2, "email": "bob@example.com", "status": "pending"},
]

# Dict of lists (columnar)
data = {
    "id": [1, 2],
    "email": ["alice@example.com", "bob@example.com"],
    "status": ["active", "pending"],
}

result = kontra.validate(data, rules=[
    rules.not_null("email"),
    rules.allowed_values("status", ["active", "pending", "inactive"]),
])
```

Both formats produce identical results. Single-row dicts like `{"id": 1, "email": "a@b.com"}` are also supported.

---

## Rule Helpers

```python
from kontra import rules

# Common rules
rules.not_null("user_id")
rules.unique("email")
rules.range("age", min=0, max=120)
rules.allowed_values("status", ["active", "pending"])
rules.regex("email", r".*@.*")

# Cross-column
rules.compare("end_date", "start_date", ">=")
rules.conditional_not_null("shipping_date", when="status == 'shipped'")

# Dataset-level
rules.min_rows(1000)
rules.freshness("updated_at", max_age="24h")
```

All rules accept optional parameters:

```python
rules.not_null("email", severity="warning")  # blocking | warning | info
rules.not_null("email", tally=True)          # exact counts
rules.not_null("email", id="custom_id")      # custom rule ID
```

See [Rules Reference](reference/rules.md) for all 18 rules and parameters.

---

## Working with Results

```python
result = kontra.validate("users.parquet", rules=[...])

# Status
result.passed          # bool
result.total_rows      # int
result.total_rules     # int
result.failed_count    # int - number of rules that failed
result.quality_score   # float 0.0-1.0, or None if weights not configured

# Iterate rules
for rule in result.rules:
    print(f"{rule.rule_id}: {'PASS' if rule.passed else 'FAIL'}")
    print(f"  source: {rule.source}")  # "metadata", "sql", or "polars"

# Filter by severity
result.blocking_failures   # failed rules with severity=blocking
result.warnings            # failed rules with severity=warning

# Violation rates
for rule in result.rules:
    if rule.violation_rate:
        print(f"{rule.rule_id}: {rule.violation_rate:.2%} of rows failed")

# Serialize
result.to_dict()       # dict
result.to_json()       # JSON string
result.to_llm()        # token-efficient string
```

### RuleResult Properties

```python
rule.rule_id          # e.g., "COL:email:not_null"
rule.name             # e.g., "not_null"
rule.passed           # bool
rule.failed_count     # int - violating rows (exact or ≥1 depending on tally)
rule.violation_rate   # float or None
rule.severity         # "blocking", "warning", or "info"
rule.severity_weight  # float or None (if weights configured)
rule.source           # "metadata", "sql", or "polars"
rule.message          # human-readable description
rule.column           # column name if applicable
rule.context          # consumer-defined metadata from contract
rule.samples          # list of failing rows or None
```

---

## Profiling

```python
profile = kontra.profile("users.parquet")

print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")

for col in profile.columns:
    print(f"  {col.name}: {col.dtype}, {col.null_rate:.0%} null")

profile.to_llm()  # token-efficient summary
```

### Presets

| Preset | What it does | When to use |
|--------|--------------|-------------|
| `scout` | Metadata only, zero data access | Quick recon, schema exploration |
| `scan` | Metadata + strategic queries | Default. Rich stats without full scan |
| `interrogate` | Full table scan | Deep analysis, percentiles, exact distributions |

```python
kontra.profile("data.parquet", preset="scout")       # metadata only
kontra.profile("data.parquet", preset="scan")        # default
kontra.profile("data.parquet", preset="interrogate") # full scan
```

`scan` is the sweet spot: it extracts null rates, distinct counts, min/max, and top values using targeted aggregations instead of scanning every row. See [Performance](advanced/performance.md) for how this works.

### Draft Rules from Profile

```python
profile = kontra.profile("data.parquet", preset="interrogate")
suggestions = kontra.draft(profile)

# Use directly
result = kontra.validate("data.parquet", rules=suggestions.to_dict())

# Filter by confidence
suggestions.filter(min_confidence=0.8)

# Save as contract
suggestions.save("contracts/generated.yml")
```

---

## Sampling

By default, no samples are collected. Enable with `sample`:

```python
result = kontra.validate("users.parquet", rules=[...], sample=5)

for rule in result.blocking_failures:
    print(f"{rule.rule_id}: {rule.failed_count} failures")
    for row in rule.samples or []:
        print(f"  {row}")
```

### Lazy Sampling

Fetch more samples after validation:

```python
samples = result.sample_failures("COL:email:not_null", n=20)
```

**Note:** For BYOC (bring your own connection), keep the connection open until done with `sample_failures()`.

### Sample Columns

Limit columns in samples for token efficiency:

```python
result = kontra.validate(..., sample=5, sample_columns=["id", "email", "status"])
result = kontra.validate(..., sample=5, sample_columns="relevant")  # rule columns only
```

### Tally and Sampling

In fail-fast mode (`tally=False`), Kontra stops at the first violation, so you get at most 1 sample per rule. Use `sample_failures()` for more, or set `tally=True` for a full scan.

---

## Validation Options

```python
result = kontra.validate(
    "data.parquet",
    "contract.yml",

    # Execution control
    preplan="auto",      # "on" | "off" | "auto"
    pushdown="auto",     # "on" | "off" | "auto"
    tally=False,         # exact counts vs fail-fast
    projection=True,     # column pruning

    # Sampling
    sample=5,            # samples per rule
    sample_budget=50,    # total samples across all rules
    sample_columns=None, # None | list | "relevant"

    # Environment
    env="production",    # environment from config
    csv_mode="auto",     # "auto" | "duckdb" | "parquet"

    # History
    save=True,           # save to history
)
```

### Dry Run

Validate contract syntax without executing:

```python
check = kontra.validate(None, "contract.yml", dry_run=True)

check.valid          # bool
check.rules_count    # int
check.columns_needed # list
check.errors         # list
```

---

## Decorator

Validate data returned from functions:

```python
@kontra.validate_decorator(
    rules=[rules.not_null("id"), rules.unique("email")],
    on_fail="raise",
)
def load_users():
    return pl.read_parquet("users.parquet")

users = load_users()  # Raises ValidationError if fails
```

### on_fail Options

| Option | Behavior |
|--------|----------|
| `"raise"` | Raise `ValidationError` (default) |
| `"warn"` | Emit warning, return data |
| `"return_result"` | Return `(data, ValidationResult)` tuple |
| `callable` | Custom callback `(result, data) -> data` |

```python
# Custom callback
@kontra.validate_decorator(
    rules=[...],
    on_fail=lambda result, data: data.drop_nulls() if not result.passed else data
)
def get_orders():
    ...

# Get result alongside data
@kontra.validate_decorator(rules=[...], on_fail="return_result")
def load_users():
    ...

data, result = load_users()
```

Works with contracts:

```python
@kontra.validate_decorator(contract="contracts/users.yml")
def load_users():
    return pl.read_parquet("users.parquet")
```

---

## History and Diff

```python
# Compare latest to previous run
diff = kontra.diff("my_contract")

if diff.regressed:
    print("Quality regressed!")
    for rule in diff.new_failures:
        print(f"  NEW: {rule.rule_id}")

diff.to_llm()  # token-efficient summary
```

### History Functions

```python
kontra.get_history(contract, since=None, limit=None, failed_only=False)
kontra.list_runs(contract)
kontra.get_run(contract, run_id=None)  # default: latest
kontra.has_runs(contract)
```

See [State & Diff](advanced/state-and-diff.md) for full details.

---

## Annotations

Record context about validation runs. Kontra stores annotations but never reads them during validation.

```python
kontra.annotate(
    "users_contract.yml",
    actor_type="agent",
    actor_id="repair-agent-v2",
    annotation_type="resolution",
    summary="Fixed null emails by backfilling from user_profiles",
)

# Annotate specific rule
kontra.annotate(
    "users_contract.yml",
    rule_id="COL:email:not_null",
    actor_type="human",
    actor_id="alice@example.com",
    annotation_type="false_positive",
    summary="Service accounts are expected to have null emails",
)
```

Load runs with annotations:

```python
result = kontra.get_run_with_annotations("users_contract.yml")

for ann in result.annotations or []:
    print(f"[{ann['annotation_type']}] {ann['summary']}")
```

Common types: `resolution`, `root_cause`, `false_positive`, `acknowledged`, `suppressed`, `note`

---

## Output Examples

### ValidationResult

```python
print(result.to_llm())
```

Passing:
```
VALIDATION: users_contract PASSED (50,000 rows)
PASSED: 4 rules
```

Failing:
```
VALIDATION: users_contract FAILED (5 rows)
BLOCKING: COL:age:range (1), COL:email:not_null (2), COL:status:allowed_values (1)
PASSED: 0 rules
```

### RuleResult

```python
for rule in result.rules:
    print(rule.to_llm())
```

```
COL:age:range: FAIL (1 failures)[20.0%]
COL:email:not_null: FAIL (2 failures)[40.0%]
COL:status:allowed_values: FAIL (1 failures)[20.0%]
```

### DatasetProfile

```python
print(profile)
```

```
DatasetProfile(users.parquet)
  Preset: scan
  Rows: 50,000 | Columns: 5
  Columns:
    - user_id: int, 50,000 distinct, [identifier]
    - email: string, 2% null, 49,000 distinct
    - status: string, 3 distinct, [category]
    - age: int, 78 distinct, [measure]
    - created_at: datetime, [timestamp]
```

```python
print(profile.to_llm())
```

```
PROFILE: users.parquet
rows=50,000 cols=5

COLUMNS:
  user_id (int) [identifier] distinct=50,000 range=[1.0, 50000.0]
  email (string) nulls=1,000 (2.0%) distinct=49,000
  status (string) [category] distinct=3 top='pending'(16,667)
  age (int) [measure] distinct=78 range=[18.0, 95.0]
  created_at (datetime) [timestamp]
```

### JSON Output

```python
result.to_dict()
```

```json
{
  "passed": false,
  "dataset": "users_contract",
  "total_rows": 50000,
  "total_rules": 4,
  "passed_count": 2,
  "failed_count": 2,    // number of rules that failed
  "warning_count": 0,
  "rules": [...]
}
```

```python
rule.to_dict()
```

```json
{
  "rule_id": "COL:email:not_null",
  "name": "not_null",
  "passed": false,
  "failed_count": 1000,  // violating rows
  "message": "1000 null values found in email",
  "severity": "blocking",
  "source": "sql",
  "violation_rate": 0.02,
  "column": "email"
}
```

All `to_llm()` outputs are designed for token efficiency. See [Agents & Services](advanced/agents-and-llms.md) for integration patterns.

---

## Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `kontra.validate(data, contract, **opts)` | Validate data |
| `kontra.profile(data, preset, **opts)` | Profile data |
| `kontra.draft(profile)` | Draft rules from profile |
| `kontra.diff(contract, **opts)` | Compare validation runs |
| `kontra.list_rules()` | List available rule types |
| `@kontra.validate_decorator(...)` | Pipeline validation decorator |

### Transformation Probes

| Function | Description |
|----------|-------------|
| `kontra.compare(before, after, key)` | Measure transformation effects |
| `kontra.profile_relationship(left, right, on)` | Measure JOIN structure |

See [Transformation Probes](reference/probes.md) for details.

### Result Types

| Type | Key Properties |
|------|----------------|
| `ValidationResult` | `passed`, `total_rows`, `quality_score`, `rules`, `blocking_failures`, `warnings`, `sample_failures()`, `to_dict()`, `to_llm()` |
| `RuleResult` | `rule_id`, `passed`, `failed_count`, `violation_rate`, `severity`, `severity_weight`, `source`, `message`, `context`, `samples` |
| `DatasetProfile` | `row_count`, `column_count`, `columns`, `to_llm()` |
| `ColumnProfile` | `name`, `dtype`, `null_rate`, `unique_count` |
| `Diff` | `has_changes`, `regressed`, `new_failures`, `resolved`, `to_llm()` |
| `Suggestions` | `filter()`, `to_dict()`, `to_yaml()`, `save()` |
| `DryRunResult` | `valid`, `rules_count`, `columns_needed`, `errors` |

### Errors

```python
from kontra.errors import (
    KontraError,           # base class
    ContractNotFoundError,
    ContractParseError,
    InvalidDataError,      # invalid data type or format
    ConnectionError,
    DuplicateRuleIdError,
)
from kontra import ValidationError  # from @validate_decorator
```

Note: Missing files raise `RuntimeError` with a descriptive message. Use a try/except block to handle file access errors.
