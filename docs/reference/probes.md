# Transformation Probes

> **Experimental.** DataFrame input only. API may change.

Two probes for measuring transformation effects:

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

### Property Access

```python
result = kontra.compare(before, after, key="user_id")

# Direct attributes
result.before_rows           # 1000
result.after_rows            # 1200
result.row_delta             # 200
result.row_ratio             # 1.2
result.preserved             # 1000
result.dropped               # 0
result.added                 # 0
result.duplicated_after      # 50
result.changed_rows          # 200
result.unchanged_rows        # 800
result.columns_added         # ["new_col"]
result.columns_removed       # []
result.columns_modified      # ["amount"]
result.modified_fraction     # {"amount": 0.15}

# Samples
result.samples_duplicated_keys   # ["A123", "B456"]
result.samples_dropped_keys      # []
result.samples_changed_rows      # [{"key": ..., "before": ..., "after": ...}]

# Output formats
result.to_dict()   # Nested dict
result.to_json()   # JSON string
result.to_llm()    # Compact text for LLM context
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

### Property Access

```python
profile = kontra.profile_relationship(orders, customers, on="customer_id")

# Direct attributes
profile.left_rows              # 10000
profile.right_rows             # 500
profile.left_unique_keys       # 10000
profile.right_unique_keys      # 450
profile.left_duplicate_keys    # 0
profile.right_duplicate_keys   # 50
profile.left_null_rate         # 0.0
profile.right_null_rate        # 0.02

# Cardinality
profile.left_key_multiplicity_min    # 1
profile.left_key_multiplicity_max    # 1
profile.right_key_multiplicity_min   # 1
profile.right_key_multiplicity_max   # 3

# Coverage
profile.left_keys_with_match      # 9800
profile.left_keys_without_match   # 200
profile.right_keys_with_match     # 450
profile.right_keys_without_match  # 0

# Samples
profile.samples_left_unmatched     # ["C991", "C882"]
profile.samples_right_unmatched    # []
profile.samples_right_duplicates   # ["C123", "C456"]

# Output formats
profile.to_dict()   # Nested dict
profile.to_json()   # JSON string
profile.to_llm()    # Compact text for LLM context
```

---

## Notes

- Probes measure structure. They do not interpret correctness.
- `duplicated_after` counts keys (not rows) appearing more than once.
- `modified_fraction` is computed only for preserved keys.
- NULL handling: NULLs in join keys are excluded from unique counts.
- Samples are bounded and explanatory only. They do not affect counts.
