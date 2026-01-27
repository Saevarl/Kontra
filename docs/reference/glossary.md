# Glossary

Common terms used in Kontra documentation and output.

## Severity Levels

| Term | Description |
|------|-------------|
| **blocking** | Rule failure causes overall validation to fail. Use for critical data quality requirements. |
| **warning** | Rule failure is logged but doesn't fail validation. Use for non-critical issues worth tracking. |
| **info** | Informational rule. Failures are recorded but have no impact on pass/fail status. |

## Execution Modes

| Term | Description |
|------|-------------|
| **preplan** | Metadata-only analysis that can prove violations exist without scanning data. Uses Parquet row-group stats or database statistics. Returns `failed_count: 1` as lower bound when violations detected. |
| **pushdown** | Execute validation rules directly in the database (DuckDB, PostgreSQL, SQL Server). Avoids transferring data to memory. |
| **tally** | Count all violations exactly (`tally=true`) vs early-stop when first violation found (`tally=false`). Early-stop is faster but returns approximate counts. |
| **projection** | Only load columns needed for validation, not the entire dataset. Reduces memory usage and speeds up execution. |

## Profile Presets

| Preset | Speed | What's Computed |
|--------|-------|-----------------|
| **scout** | Fastest | Null counts, semantic types. No distinct counts or statistics. |
| **scan** | Medium | Full statistics: nulls, distinct counts, min/max/mean, top values. |
| **interrogate** | Slowest | Everything in scan + percentiles (p25, p50, p75, p99). |

## Semantic Types

Inferred column roles shown in profile output:

| Type | Description | Example |
|------|-------------|---------|
| **identifier** | Likely primary key - unique, non-null, high cardinality | `user_id`, `order_id` |
| **category** | Low-cardinality string - suitable for grouping | `status`, `country` |
| **measure** | Numeric column suitable for aggregation | `amount`, `score` |
| **timestamp** | Date or datetime column | `created_at`, `event_date` |

## Cardinality

Number of distinct values in a column:

| Level | Threshold | Description |
|-------|-----------|-------------|
| **low** | ≤20 distinct | Good for categorical analysis, values can be listed |
| **medium** | 21-99 distinct | Moderate cardinality |
| **high** | 100-999 distinct | High cardinality |
| **unique** | All values unique | Likely identifier column |

## Output Formats

| Term | Description |
|------|-------------|
| **to_llm()** | Token-optimized format for LLM agents. 85-92% smaller than JSON. |
| **to_dict()** | Python dictionary format. Full data, includes all fields. |
| **to_json()** | JSON string format. Same as to_dict() but serialized. |

### Validation JSON Schema

```json
{
  "passed": true,
  "total_rows": 50000,
  "total_rules": 5,
  "failed_count": 0,
  "rules": [
    {
      "rule_id": "COL:email:not_null",
      "passed": true,
      "failed_count": 0,
      "failed_count_exact": true,
      "severity": "blocking",
      "message": "Passed: email has no null values",
      "samples": []
    },
    {
      "rule_id": "COL:age:range",
      "passed": false,
      "failed_count": 3,
      "failed_count_exact": true,
      "severity": "blocking",
      "message": "Failed: 3 failures in age",
      "context": {"owner": "data_team"},
      "samples": [
        {"_row_index": 42, "id": 42, "age": -5}
      ]
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `passed` | bool | Overall validation result (all blocking rules passed) |
| `total_rows` | int | Row count of validated dataset |
| `total_rules` | int | Number of rules executed |
| `failed_count` | int | Total violations across all blocking rules |
| `rules[].rule_id` | string | Unique rule identifier |
| `rules[].passed` | bool | Whether this rule passed |
| `rules[].failed_count` | int | Number of violations (0 if passed) |
| `rules[].failed_count_exact` | bool | `true` if count is exact, `false` if lower bound (tally=false) |
| `rules[].severity` | string | `"blocking"`, `"warning"`, or `"info"` |
| `rules[].message` | string | Human-readable result description |
| `rules[].context` | object | Optional consumer-defined metadata from contract |
| `rules[].samples` | array | Sample failing rows (if `sample > 0`) |

### Profile JSON Schema

```json
{
  "source_uri": "data.parquet",
  "row_count": 50000,
  "column_count": 8,
  "preset": "scan",
  "columns": [
    {
      "name": "user_id",
      "dtype": "int64",
      "null_count": 0,
      "null_rate": 0.0,
      "distinct_count": 50000,
      "semantic_type": "identifier",
      "cardinality": "unique"
    },
    {
      "name": "age",
      "dtype": "int64",
      "null_count": 0,
      "null_rate": 0.0,
      "distinct_count": 78,
      "semantic_type": "measure",
      "numeric": {
        "min": 18,
        "max": 95,
        "mean": 42.3,
        "percentiles": {"p25": 28, "p50": 41, "p75": 56, "p99": 89}
      }
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `source_uri` | string | Data source path or description |
| `row_count` | int | Total rows in dataset |
| `column_count` | int | Number of columns |
| `preset` | string | Profile preset used (`scout`, `scan`, `interrogate`) |
| `columns[].name` | string | Column name |
| `columns[].dtype` | string | Data type (e.g., `int64`, `string`, `datetime`) |
| `columns[].null_count` | int | Number of null values |
| `columns[].null_rate` | float | Fraction of nulls (0.0 to 1.0) |
| `columns[].distinct_count` | int | Unique values (omitted in `scout` preset) |
| `columns[].semantic_type` | string | Inferred role: `identifier`, `category`, `measure`, `timestamp` |
| `columns[].numeric` | object | Statistics for numeric columns (min, max, mean, percentiles) |
| `columns[].top_values` | array | Most frequent values for categorical columns |

## Rule IDs

Format: `SCOPE:column:rule_name`

| Scope | Example | Description |
|-------|---------|-------------|
| **COL** | `COL:email:not_null` | Column-level rule |
| **DATASET** | `DATASET:min_rows` | Dataset-level rule |

## Sample Output Format

When viewing failure samples:

```
[0] row=83: id=83, email=None, status=active
 │    │
 │    └── Original row number (0-indexed)
 └── Sample index (0 to N-1)
```

For unique rule violations:
```
[0] row=5, dupes=3: user_id=123
                │
                └── Number of duplicate occurrences
```

## Exit Codes

| Code | Meaning |
|------|---------|
| **0** | Validation passed (all rules passed) |
| **1** | Validation failed (one or more blocking rules failed) |
| **2** | Configuration error (contract/data not found, invalid YAML) |
| **3** | Runtime error (unexpected failure, connection issues) |

## Contract vs Inline Rules

| Approach | Description |
|----------|-------------|
| **Contract** | YAML file defining rules. Version controllable, reusable. |
| **Inline rules** | Rules defined directly in Python code. Quick validation. |

```python
# Contract approach
result = kontra.validate("contract.yml", data="data.parquet")

# Inline approach
result = kontra.validate(df, rules=[
    rules.not_null("id"),
    rules.unique("email"),
])
```

## State and History

| Term | Description |
|------|-------------|
| **state** | Stored validation results for tracking history. Backends: local file, S3, PostgreSQL. |
| **fingerprint** | Hash of contract content (name + rules). Used to track changes across renames. |
| **diff** | Comparison between two validation runs. Shows new failures, resolved issues, regressions. |
