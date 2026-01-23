# Kontra Agent Reference

Kontra is a data quality measurement engine. It measures dataset properties and returns structured results.

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

## Output Methods

All result types support:

| Method | Description |
|--------|-------------|
| `.to_dict()` | Nested dictionary |
| `.to_json()` | JSON string |
| `.to_llm()` | JSON string for LLM context |

---

## Workflow Example

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
# Check: result.passed == True
```
