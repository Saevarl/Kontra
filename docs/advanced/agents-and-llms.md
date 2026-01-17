# Agents & LLM Integration

Kontra is designed for programmatic use by LLM agents and services.

## Token-Optimized Output

All result types have a `.to_llm()` method that returns a compact, token-efficient string:

```python
import kontra

# Validation result
result = kontra.validate("data.parquet", rules=[...])
print(result.to_llm())
# VALIDATION: my_contract PASSED
# PASSED: 5 rules

# With failures
# VALIDATION: my_contract FAILED
# BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
# WARNING: COL:age:range (3 out of bounds)
# PASSED: 12 rules

# Profile
profile = kontra.scout("data.parquet")
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

## Service Health Check

```python
import kontra

health = kontra.health()

# {
#     "version": "0.x.x",
#     "status": "ok",
#     "config_found": True,
#     "config_path": "/app/.kontra/config.yml",
#     "rule_count": 12,
#     "rules": ["not_null", "unique", "range", ...]
# }

if health["status"] == "ok":
    print(f"Kontra {health['version']} ready")
```

## Rule Discovery

List available rules with descriptions:

```python
rules = kontra.list_rules()

for rule in rules:
    print(f"{rule['name']} ({rule['scope']})")
    print(f"  {rule['description']}")
    print(f"  Params: {rule['params']}")

# not_null (column)
#   Fails where column contains NULL values
#   Params: {'column': 'required', 'include_nan': 'optional'}
# range (column)
#   Fails where column values are outside [min, max] range
#   Params: {'column': 'required', 'min': 'optional', 'max': 'optional'}
```

## Config Path Injection

Services that don't run from a project directory need explicit config:

```python
import kontra

# Set config path for service use
kontra.set_config("/etc/kontra/config.yml")

# All subsequent calls use this config
result = kontra.validate("prod_db.users", rules=[...])
profile = kontra.scout("prod_db.orders")

# Check current setting
path = kontra.get_config_path()

# Reset to auto-discovery
kontra.set_config(None)
```

## Datasource Resolution

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

## Suggested Rules for Agents

When an agent needs to generate validation rules from data:

```python
profile = kontra.scout("data.parquet", preset="deep")
suggestions = kontra.suggest_rules(profile)

# Filter by confidence
high_confidence = suggestions.filter(min_confidence=0.9)

# Get as dict for validation
rules = high_confidence.to_dict()
result = kontra.validate("data.parquet", rules=rules)
```

**Note:** Suggested rules are heuristic. They reflect observed patterns in the data, not ground truth. Agents should present them as starting points, not authoritative contracts.

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
    # Contract file not found
    return {"error": "contract_not_found", "message": str(e)}
except DataNotFoundError as e:
    # Data source not found
    return {"error": "data_not_found", "message": str(e)}
except KontraError as e:
    # Other Kontra errors
    return {"error": "kontra_error", "message": str(e)}
```

## Example: Agent Workflow

```python
import kontra

def validate_user_data(data_source: str) -> str:
    """Agent-callable function to validate user data."""

    # Profile first
    profile = kontra.scout(data_source, preset="llm")

    # Check for obvious issues
    if profile.row_count == 0:
        return "EMPTY: Dataset has no rows"

    # Validate with standard rules
    result = kontra.validate(data_source, rules=[
        {"name": "not_null", "params": {"column": "user_id"}},
        {"name": "unique", "params": {"column": "user_id"}},
        {"name": "not_null", "params": {"column": "email"}},
    ])

    # Return token-optimized output
    return result.to_llm()
```
