# Rules Reference

Kontra provides 18 built-in validation rules.

## Quick Reference

| Rule | Description | Parameters |
|------|-------------|------------|
| `not_null` | No NULL values | `column`, `include_nan` |
| `unique` | No duplicates | `column` |
| `allowed_values` | Values in set | `column`, `values` |
| `disallowed_values` | Values NOT in set | `column`, `values` |
| `range` | Min/max bounds | `column`, `min`, `max` |
| `length` | String length bounds | `column`, `min`, `max` |
| `regex` | Pattern match | `column`, `pattern` |
| `contains` | Contains substring | `column`, `substring` |
| `starts_with` | Starts with prefix | `column`, `prefix` |
| `ends_with` | Ends with suffix | `column`, `suffix` |
| `dtype` | Type check | `column`, `type` |
| `compare` | Cross-column comparison | `left`, `right`, `op` |
| `conditional_not_null` | Conditional not-null | `column`, `when` |
| `conditional_range` | Conditional range check | `column`, `when`, `min`, `max` |
| `min_rows` | Minimum rows | `threshold` |
| `max_rows` | Maximum rows | `threshold` |
| `freshness` | Data recency | `column`, `max_age` |
| `custom_sql_check` | Custom SQL | `sql` |

---

## Column Rules

### not_null

No NULL values in column.

```python
rules.not_null("user_id")
rules.not_null("price", include_nan=True)  # Also catch NaN
```

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `column` | string | Yes | |
| `include_nan` | boolean | No | false |

**NaN handling:** `include_nan=True` works reliably with DataFrames. For file-based validation, neither preplan nor DuckDB distinguishes NaN from NULL. To detect NaN in files, validate a DataFrame directly or use `preplan="off", pushdown="off"` to force Polars execution.

---

### unique

No duplicate values. NULLs are ignored (SQL semantics).

```python
rules.unique("email")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |

**Count semantics:** `failed_count` = total rows - distinct values. For `[a, a, b, c, c, c]`, `failed_count` = 3 (6 total - 3 distinct).

---

### allowed_values

Values must be in allowed set.

```python
rules.allowed_values("status", ["active", "inactive", "pending"])
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `values` | list | Yes |

---

### disallowed_values

Values must NOT be in set. Inverse of `allowed_values`.

```python
rules.disallowed_values("status", ["deleted", "banned", "spam"])
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `values` | list | Yes |

**Use case:** Block known-bad values while allowing everything else.

---

### range

Values must be within bounds.

```python
rules.range("age", min=0, max=150)
rules.range("price", min=0)  # No upper bound
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `min` | number | No* |
| `max` | number | No* |

*At least one of `min` or `max` required.

---

### length

String length must be within bounds.

```python
rules.length("username", min=3, max=50)
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `min` | integer | No* |
| `max` | integer | No* |

*At least one of `min` or `max` required.

---

### regex

Values must match pattern.

```python
rules.regex("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `pattern` | string | Yes |

**SQL Server:** Limited regex support (PATINDEX only). Falls back to Polars for correct results.

---

### contains

Values must contain substring. Uses efficient LIKE patterns.

```python
rules.contains("email", "@")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `substring` | string | Yes |

For complex patterns, use `regex` instead.

---

### starts_with

Values must start with prefix. Uses efficient LIKE patterns.

```python
rules.starts_with("url", "https://")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `prefix` | string | Yes |

---

### ends_with

Values must end with suffix. Uses efficient LIKE patterns.

```python
rules.ends_with("filename", ".csv")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `suffix` | string | Yes |

---

### dtype

Column must have expected type. Schema check only.

```python
rules.dtype("user_id", "int64")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `type` | string | Yes |

**Supported types:** `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `utf8`, `bool`, `date`, `datetime`

---

## Cross-Column Rules

### compare

Compare two columns using a comparison operator.

```python
rules.compare("end_date", "start_date", ">=")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `left` | string | Yes |
| `right` | string | Yes |
| `op` | string | Yes |

**Operators:** `>`, `>=`, `<`, `<=`, `==`, `!=`

**NULL handling:** Rows where either column is NULL are counted as failures. You cannot meaningfully compare NULL values.

---

### conditional_not_null

Column must not be NULL when condition is met.

```python
rules.conditional_not_null("shipping_date", when="status == 'shipped'")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `when` | string | Yes |

**Condition format:** `column_name operator value`
- Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Values: `'string'`, `123`, `true`, `false`, `null`

---

### conditional_range

Column must be within range when condition is met.

```python
rules.conditional_range("discount_percent", when="customer_type == 'premium'", min=10, max=50)
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `when` | string | Yes |
| `min` | number | No* |
| `max` | number | No* |

*At least one of `min` or `max` required.

**Behavior:**
- Only checks rows where `when` condition is TRUE
- NULL in column when condition is TRUE = violation
- NULL in condition column = condition is FALSE (no check)

---

## Dataset Rules

### min_rows

Dataset must have at least N rows.

```python
rules.min_rows(1000)
```

| Parameter | Type | Required |
|-----------|------|----------|
| `threshold` | integer | Yes |

---

### max_rows

Dataset must have at most N rows.

```python
rules.max_rows(1000000)
```

| Parameter | Type | Required |
|-----------|------|----------|
| `threshold` | integer | Yes |

---

### freshness

Most recent timestamp must be within max_age of now.

```python
rules.freshness("updated_at", max_age="24h")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `max_age` | string | Yes |

**Formats:** `Xs` (seconds), `Xm` (minutes), `Xh` (hours), `Xd` (days), `XhYm` (e.g., "1h30m")

**Note:** Results depend on when you run the check, not just data content.

---

### custom_sql_check

Custom SQL for cases not covered by built-in rules. Write a query that selects violation rows.

```python
rules.custom_sql_check("SELECT * FROM {table} WHERE balance < 0 AND account_type = 'savings'")
```

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |

Use `{table}` placeholder. Kontra transforms your query to `COUNT(*)` for efficiency.

**Cross-table queries:**

```python
rules.custom_sql_check("""
    SELECT * FROM {table}
    WHERE category_id NOT IN (SELECT id FROM valid_categories)
""")
```

**Safety:** Only SELECT statements are allowed. Queries are validated to reject INSERT, UPDATE, DELETE, DROP, system catalog access, and dangerous functions.

---

## NULL Semantics

| Rule | NULL Behavior |
|------|---------------|
| `not_null` | NULL = violation |
| `unique` | NULLs ignored |
| `allowed_values` | NULL = violation |
| `disallowed_values` | NULL = pass |
| `range` | NULL = violation |
| `length` | NULL = violation |
| `regex` | NULL = violation |
| `contains` | NULL = violation |
| `starts_with` | NULL = violation |
| `ends_with` | NULL = violation |
| `compare` | NULL in either column = violation |
| `conditional_not_null` | NULL in condition column = condition FALSE |
| `conditional_range` | NULL in column = violation; NULL in condition = condition FALSE |
| `freshness` | NULLs excluded from MAX |
| `dtype`, `min_rows`, `max_rows` | N/A |
| `custom_sql_check` | User-defined |

**NaN vs NULL:** In Polars, NaN and NULL are distinct. Use `include_nan=True` on `not_null` to catch both.

---

## Execution Support

Rules resolve through preplan (metadata) or SQL pushdown when possible, falling back to Polars otherwise.

### Column Rules

| Rule | Preplan | SQL Pushdown | Tally |
|------|:-------:|:------------:|:-----:|
| `not_null` | ✓ | ✓ | ✓ |
| `unique` | | ✓ | ✓ |
| `allowed_values` | | ✓ | ✓ |
| `disallowed_values` | | ✓ | ✓ |
| `range` | ✓ | ✓ | ✓ |
| `length` | | ✓ | ✓ |
| `regex` | | ✓* | ✓ |
| `contains` | | ✓ | ✓ |
| `starts_with` | | ✓ | ✓ |
| `ends_with` | | ✓ | ✓ |
| `dtype` | schema | | |

*SQL Server has limited regex support; falls back to Polars.

### Cross-Column Rules

| Rule | Preplan | SQL Pushdown | Tally |
|------|:-------:|:------------:|:-----:|
| `compare` | | ✓ | ✓ |
| `conditional_not_null` | | ✓ | ✓ |
| `conditional_range` | | ✓ | ✓ |

### Dataset Rules

| Rule | Preplan | SQL Pushdown | Tally |
|------|:-------:|:------------:|:-----:|
| `min_rows` | ✓ | ✓ | |
| `max_rows` | ✓ | ✓ | |
| `freshness` | | ✓ | |
| `custom_sql_check` | | ✓ | |

Dataset rules return exact counts or are binary by nature, so tally doesn't apply.

---

## Edge Cases

### CSV Files

**Empty strings vs NULL:** CSV parsing differs between engines:

| Raw CSV | Polars | DuckDB |
|---------|--------|--------|
| `""` | Empty string | NULL |
| `` (trailing) | NULL | NULL |

For consistent behavior, load CSV with Polars first: `kontra.validate(pl.read_csv("file.csv"), ...)`

### Large Integers

Very large integers (e.g., 10^100) cause `OverflowError` because they exceed Polars integer types. Use string columns for arbitrary-precision numbers.

### SQL Server Regex

SQL Server doesn't support true regex. The `regex` rule falls back to Polars for correct results.
