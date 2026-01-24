# Agents & LLM Integration

Kontra is a data quality measurement engine designed for programmatic use by LLM agents and services.

---

## Core Functions

### Validate Data

```python
import kontra
from kontra import rules

result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
])

result.passed        # bool
result.failed_count  # int
result.total_rows    # int

for rule in result.blocking_failures:
    print(f"{rule.rule_id}: {rule.failed_count} violations")
```

### Profile Data

```python
profile = kontra.profile(df)
print(f"Rows: {profile.row_count}")
for col in profile.columns:
    print(f"{col.name}: {col.dtype}, {col.null_rate:.0%} null")
```

---

## Transformation Probes

### Compare (Before/After)

Measures what changed between two datasets:

```python
result = kontra.compare(before_df, after_df, key="user_id")
print(result.to_llm())
```

Output includes:
- `row_stats.delta` - change in row count
- `row_stats.ratio` - ratio of after/before rows
- `key_stats.duplicated_after` - keys appearing >1x in after
- `key_stats.dropped` - keys lost in transformation
- `change_stats.changed_rows` - rows where values differ
- `samples.duplicated_keys` - example duplicated keys

### Profile Relationship (JOIN Structure)

Measures JOIN viability before writing transformation:

```python
profile = kontra.profile_relationship(left_df, right_df, on="customer_id")
print(profile.to_llm())
```

Output includes:
- `key_stats.left/right.unique_keys` - distinct keys per side
- `key_stats.right.duplicate_keys` - keys appearing >1x in right
- `cardinality.right_key_multiplicity.max` - max rows per key in right
- `coverage.left_keys_without_match` - left keys not in right
- `samples.right_keys_with_multiple_rows` - example duplicated keys

---

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
```

---

## Token-Optimized Output

All result types have a `.to_llm()` method that returns a compact, token-efficient string:

```python
# Validation result
result = kontra.validate("data.parquet", rules=[...])
print(result.to_llm())
# VALIDATION: my_contract PASSED
# PASSED: 5 rules

# With failures
# VALIDATION: my_contract FAILED
# BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
# WARNING: COL:age:range (3 out of bounds)
# PASSED: 13 rules

# Profile
profile = kontra.profile("data.parquet")
print(profile.to_llm())
# DATASET: users.parquet (50K rows, 8 cols)
# COLS: user_id(int64,100%,unique), email(str,98%), status(str,100%,3vals), ...

# Diff
diff = kontra.diff("my_contract")
print(diff.to_llm())
# DIFF: my_contract 2024-01-10 -> 2024-01-12
# REGRESSION: COL:email:not_null (0 -> 523 nulls)
# RESOLVED: COL:age:range
```

### Output Methods

| Method | Description |
|--------|-------------|
| `.to_dict()` | Nested dictionary |
| `.to_json()` | JSON string |
| `.to_llm()` | Compact string for LLM context |

---

## Service Integration

### Health Check

```python
health = kontra.health()

# {
#     "version": "0.x.x",
#     "status": "ok",
#     "config_found": True,
#     "config_path": "/app/.kontra/config.yml",
#     "rule_count": 13,
#     "rules": ["not_null", "unique", "range", ...]
# }

if health["status"] == "ok":
    print(f"Kontra {health['version']} ready")
```

### Config Path Injection

Services that don't run from a project directory need explicit config:

```python
# Set config path for service use
kontra.set_config("/etc/kontra/config.yml")

# All subsequent calls use this config
result = kontra.validate("prod_db.users", rules=[...])

# Check current setting
path = kontra.get_config_path()

# Reset to auto-discovery
kontra.set_config(None)
```

### Datasource Resolution

```python
# Resolve datasource name to URI
uri = kontra.resolve("users")           # searches all datasources
uri = kontra.resolve("prod_db.users")   # explicit datasource

# List available datasources
sources = kontra.list_datasources()
# {
#     "prod_db": ["users", "orders", "products"],
#     "local_data": ["events", "metrics"],
# }
```

### Rule Discovery

```python
rules_list = kontra.list_rules()

for rule in rules_list:
    print(f"{rule['name']} ({rule['scope']})")
    print(f"  {rule['description']}")
    print(f"  Params: {rule['params']}")

# not_null (column)
#   Fails where column contains NULL values
#   Params: {'column': 'required', 'include_nan': 'optional'}
```

---

## Suggested Rules

When an agent needs to generate validation rules from data:

```python
profile = kontra.profile("data.parquet", preset="interrogate")
suggestions = kontra.draft(profile)

# Filter by confidence
high_confidence = suggestions.filter(min_confidence=0.9)

# Get as dict for validation
rules = high_confidence.to_dict()
result = kontra.validate("data.parquet", rules=rules)
```

**Note:** Suggested rules are heuristic. They reflect observed patterns in the data, not ground truth. Agents should present them as starting points, not authoritative contracts.

---

## Error Handling

```python
from kontra.errors import (
    KontraError,           # base class
    ContractNotFoundError,
    DataNotFoundError,
    ConnectionError,
)

try:
    result = kontra.validate("data.parquet", "contract.yml")
except ContractNotFoundError as e:
    return {"error": "contract_not_found", "message": str(e)}
except DataNotFoundError as e:
    return {"error": "data_not_found", "message": str(e)}
except KontraError as e:
    return {"error": "kontra_error", "message": str(e)}
```

---

## Workflow Examples

### Transformation Pipeline

1. **Profile relationship** before writing JOIN:
```python
profile = kontra.profile_relationship(orders, customers, on="customer_id")
# Check: profile.right_key_multiplicity_max > 1 means duplicates
```

2. **Write transformation** based on profile insights

3. **Compare** to measure transformation effects:
```python
result = kontra.compare(orders, joined_result, key="order_id")
# Check: result.duplicated_after > 0 means key duplication
```

4. **Validate** final output:
```python
result = kontra.validate(final_df, rules=[
    rules.unique("order_id"),
    rules.not_null("customer_name"),
])
```

### Agent-Callable Function

```python
def validate_data(data_source: str, contract: str) -> dict:
    """Agent-callable validation function."""

    result = kontra.validate(data_source, contract=contract)

    response = {
        "passed": result.passed,
        "total_rows": result.total_rows,
        "summary": result.to_llm(),
    }

    if result.blocking_failures:
        failure = result.blocking_failures[0]
        response["status"] = "blocked"
        response["worst_rule"] = {
            "id": failure.rule_id,
            "message": failure.message,
            "failed_count": failure.failed_count,
            "owner": failure.context.get("owner") if failure.context else None,
        }
    elif result.warnings:
        response["status"] = "warnings"
    else:
        response["status"] = "passed"

    return response
```

### Contracts with Severity and Context

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking
    context:
      owner: data_platform
      fix_hint: User ID is required

  - name: range
    params: { column: age, min: 0 }
    severity: warning
```

See [Rule Context](../python-api.md#rule-context-in-contracts) for details.
