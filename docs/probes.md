# Transformation Probes

Kontra provides two probes for measuring transformation effects:

- `compare()` - Measure differences between before/after datasets
- `profile_relationship()` - Measure JOIN structure between two datasets

Probes return structured measurements. They do not interpret results or suggest fixes.

---

## `compare(before, after, key)`

Measures what changed between two datasets.

```python
import kontra

result = kontra.compare(
    before=raw_df,
    after=transformed_df,
    key="order_id",  # or ["col1", "col2"] for composite
)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `before` | DataFrame or path | Dataset before transformation |
| `after` | DataFrame or path | Dataset after transformation |
| `key` | str or list[str] | Column(s) identifying rows |
| `sample_limit` | int | Max samples per category (default: 5) |

### Output Schema

```python
result.to_dict()  # Returns:
{
  "meta": {
    "before_rows": 1000,
    "after_rows": 1200,
    "key": ["order_id"],
    "execution_tier": "polars"
  },
  "row_stats": {
    "delta": 200,      # after_rows - before_rows
    "ratio": 1.2       # after_rows / before_rows
  },
  "key_stats": {
    "unique_before": 1000,
    "unique_after": 1000,
    "preserved": 1000,     # keys in both
    "dropped": 0,          # keys in before only
    "added": 0,            # keys in after only
    "duplicated_after": 50 # keys appearing >1x in after
  },
  "change_stats": {
    "unchanged_rows": 800,
    "changed_rows": 200
  },
  "column_stats": {
    "added": ["new_col"],
    "removed": [],
    "modified": ["amount"],
    "modified_fraction": {"amount": 0.15},
    "nullability_delta": {
      "amount": {"before": 0.0, "after": 0.12}
    }
  },
  "samples": {
    "duplicated_keys": ["A123", "B456"],
    "dropped_keys": [],
    "changed_rows": [
      {"key": "A123", "before": {"amount": 100}, "after": {"amount": 200}}
    ]
  }
}
```

### Key Fields

| Field | Meaning |
|-------|---------|
| `row_stats.delta` | Change in row count |
| `row_stats.ratio` | Ratio of after/before rows |
| `key_stats.preserved` | Keys present in both datasets |
| `key_stats.dropped` | Keys lost in transformation |
| `key_stats.added` | New keys in after |
| `key_stats.duplicated_after` | Count of keys appearing more than once in after |
| `change_stats.changed_rows` | Rows where non-key columns differ |
| `column_stats.modified_fraction` | Per-column: fraction of rows where value changed |

### Usage

```python
result = kontra.compare(before, after, key="user_id")

# Access structured output
print(result.to_llm())  # JSON for LLM context

# Access specific fields
print(result.row_delta)
print(result.duplicated_after)
print(result.samples_duplicated_keys)
```

---

## `profile_relationship(left, right, on)`

Measures the structural relationship between two datasets on a join key.

```python
import kontra

profile = kontra.profile_relationship(
    left=orders,
    right=customers,
    on="customer_id",  # or ["col1", "col2"] for composite
)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `left` | DataFrame or path | Left dataset |
| `right` | DataFrame or path | Right dataset |
| `on` | str or list[str] | Column(s) to join on |
| `sample_limit` | int | Max samples per category (default: 5) |

### Output Schema

```python
profile.to_dict()  # Returns:
{
  "meta": {
    "on": ["customer_id"],
    "left_rows": 10000,
    "right_rows": 500,
    "execution_tier": "polars"
  },
  "key_stats": {
    "left": {
      "null_rate": 0.0,
      "unique_keys": 10000,
      "duplicate_keys": 0,
      "rows": 10000
    },
    "right": {
      "null_rate": 0.02,
      "unique_keys": 450,
      "duplicate_keys": 50,
      "rows": 500
    }
  },
  "cardinality": {
    "left_key_multiplicity": {"min": 1, "max": 1},
    "right_key_multiplicity": {"min": 1, "max": 3}
  },
  "coverage": {
    "left_keys_with_match": 9800,
    "left_keys_without_match": 200,
    "right_keys_with_match": 450,
    "right_keys_without_match": 0
  },
  "samples": {
    "left_keys_without_match": ["C991", "C882"],
    "right_keys_without_match": [],
    "right_keys_with_multiple_rows": ["C123", "C456"]
  }
}
```

### Key Fields

| Field | Meaning |
|-------|---------|
| `key_stats.left.unique_keys` | Distinct key values in left |
| `key_stats.right.duplicate_keys` | Keys appearing >1x in right |
| `cardinality.left_key_multiplicity.max` | Maximum rows per key in left |
| `cardinality.right_key_multiplicity.max` | Maximum rows per key in right |
| `coverage.left_keys_with_match` | Left keys that exist in right |
| `coverage.left_keys_without_match` | Left keys not in right |

### Usage

```python
profile = kontra.profile_relationship(orders, customers, on="customer_id")

# Access structured output
print(profile.to_llm())  # JSON for LLM context

# Access specific fields
print(profile.right_key_multiplicity_max)
print(profile.left_keys_without_match)
print(profile.samples_right_duplicates)
```

---

## Output Methods

Both probes support:

| Method | Description |
|--------|-------------|
| `.to_dict()` | Nested dictionary matching schema above |
| `.to_json()` | JSON string |
| `.to_llm()` | JSON string (same as to_json, for LLM context) |

---

## Notes

- Probes measure structure. They do not interpret correctness.
- `duplicated_after` counts keys (not rows) appearing more than once.
- `modified_fraction` is computed only for preserved keys.
- NULL handling: NULLs in join keys are excluded from unique counts.
- Samples are bounded and explanatory only. They do not affect counts.
