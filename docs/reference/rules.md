# Rules Reference

Kontra provides 12 built-in validation rules.

## Most Contracts Use These

Start here. These four rules cover 80% of use cases:

| Rule | What It Checks | Example |
|------|---------------|---------|
| `not_null` | No NULL values | `{ column: user_id }` |
| `unique` | No duplicates | `{ column: email }` |
| `allowed_values` | Values in set | `{ column: status, values: [a, b, c] }` |
| `range` | Min/max bounds | `{ column: age, min: 0, max: 150 }` |

```yaml
rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: email }

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: range
    params:
      column: age
      min: 0
      max: 150
```

---

## Quick Reference

| Rule | Description | Parameters |
|------|-------------|------------|
| `not_null` | No NULL values | `column`, `include_nan` |
| `unique` | No duplicates | `column` |
| `allowed_values` | Values in set | `column`, `values` |
| `range` | Min/max bounds | `column`, `min`, `max` |
| `regex` | Pattern match | `column`, `pattern` |
| `dtype` | Type check | `column`, `type` |
| `min_rows` | Minimum rows | `threshold` |
| `max_rows` | Maximum rows | `threshold` |
| `freshness` | Data recency | `column`, `max_age` |
| `compare` | Cross-column comparison | `left`, `right`, `op` |
| `conditional_not_null` | Conditional not-null | `column`, `when` |
| `custom_sql_check` | Custom SQL | `sql` |

---

## Full Reference

### not_null

No NULL values in column.

```yaml
- name: not_null
  params:
    column: user_id

# Also catch NaN values
- name: not_null
  params:
    column: price
    include_nan: true
```

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `column` | string | Yes | |
| `include_nan` | boolean | No | false |

---

### unique

No duplicate values in column. NULLs are ignored (SQL semantics).

```yaml
- name: unique
  params:
    column: email
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |

---

### allowed_values

Values must be in allowed set. NULL = violation.

```yaml
- name: allowed_values
  params:
    column: status
    values: [active, inactive, pending]
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `values` | list | Yes |

---

### range

Values must be within bounds. NULL = violation.

```yaml
- name: range
  params:
    column: age
    min: 0
    max: 150
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `min` | number | No* |
| `max` | number | No* |

*At least one of `min` or `max` required.

---

### regex

Values must match pattern. NULL = violation.

```yaml
- name: regex
  params:
    column: email
    pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `pattern` | string | Yes |

SQL Server has limited regex support (PATINDEX only).

---

### dtype

Column must have expected type. Schema check only.

```yaml
- name: dtype
  params:
    column: user_id
    type: int64
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `type` | string | Yes |

Supported types: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `utf8`, `bool`, `date`, `datetime`

---

### min_rows

Dataset must have at least N rows.

```yaml
- name: min_rows
  params:
    threshold: 1000
```

| Parameter | Type | Required |
|-----------|------|----------|
| `threshold` | integer | Yes |

---

### max_rows

Dataset must have at most N rows.

```yaml
- name: max_rows
  params:
    threshold: 1000000
```

| Parameter | Type | Required |
|-----------|------|----------|
| `threshold` | integer | Yes |

---

### freshness

Most recent timestamp must be within max_age of now.

```yaml
- name: freshness
  params:
    column: updated_at
    max_age: "24h"
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `max_age` | string | Yes |

Formats: `Xs` (seconds), `Xm` (minutes), `Xh` (hours), `Xd` (days), `XhYm` (e.g., "1h30m")

**Note:** Results depend on when you run the check, not just data content.

---

### compare

Compare two columns. NULL in either = violation.

```yaml
- name: compare
  params:
    left: end_date
    right: start_date
    op: ">="
```

| Parameter | Type | Required |
|-----------|------|----------|
| `left` | string | Yes |
| `right` | string | Yes |
| `op` | string | Yes |

Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`

---

### conditional_not_null

Column must not be NULL when condition is met.

```yaml
- name: conditional_not_null
  params:
    column: shipping_date
    when: "status == 'shipped'"
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `when` | string | Yes |

Condition format: `column_name operator value`
- Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Values: `'string'`, `123`, `true`, `false`, `null`

**Multiple conditions on same column:** If you have multiple `conditional_not_null` rules for the same column with different conditions, add an explicit `id` to avoid rule ID collisions:

```yaml
- name: conditional_not_null
  id: shipped_needs_date
  params: { column: shipping_date, when: "status == 'shipped'" }

- name: conditional_not_null
  id: delivered_needs_date
  params: { column: shipping_date, when: "status == 'delivered'" }
```

---

### custom_sql_check

Escape hatch for custom SQL. Returns violation count.

```yaml
- name: custom_sql_check
  params:
    sql: |
      SELECT COUNT(*)
      FROM {table}
      WHERE balance < 0 AND account_type = 'savings'
```

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |

Use `{table}` placeholder. Query must return single integer.

**Limitation:** This rule executes via DuckDB on local files (Parquet, CSV). It does **not** push SQL to remote databases (PostgreSQL, SQL Server). For database sources, use the other built-in rules or query the database directly.

---

## Severity

All rules accept an optional `severity` parameter:

```yaml
- name: not_null
  params: { column: user_id }
  severity: blocking   # default

- name: allowed_values
  params: { column: status, values: [a, b] }
  severity: warning    # reported but exit code 0

- name: range
  params: { column: score, min: 0 }
  severity: info       # informational only
```

---

## NULL Semantics

| Rule | NULL Behavior |
|------|---------------|
| `not_null` | NULL = violation |
| `unique` | NULLs ignored |
| `allowed_values` | NULL = violation |
| `range` | NULL = violation |
| `regex` | NULL = violation |
| `compare` | NULL = violation |
| `conditional_not_null` | NULL in condition → condition is FALSE |
| `dtype`, `min_rows`, `max_rows` | N/A |
| `freshness` | NULLs excluded from MAX |
| `custom_sql_check` | User-defined |

**NaN vs NULL:** In Polars, NaN and NULL are distinct. Use `include_nan: true` on `not_null` to catch both.

---

## Execution Tiers

| Rule | Preplan | SQL | Notes |
|------|---------|-----|-------|
| `not_null` | ✓ | ✓ | |
| `unique` | | ✓ | |
| `allowed_values` | | ✓ | |
| `range` | ✓ | ✓ | |
| `regex` | | ✓ | SQL Server limited |
| `dtype` | Schema | | |
| `min_rows` | ✓ | ✓ | |
| `max_rows` | ✓ | ✓ | |
| `freshness` | | ✓ | |
| `compare` | | ✓ | |
| `conditional_not_null` | | ✓ | |
| `custom_sql_check` | | DuckDB | |

Preplan returns binary (0 or ≥1), not exact counts. Use `--preplan off` for exact counts.

---

## Adding Custom Rules

```python
from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("my_rule")
class MyRule(BaseRule):
    def validate(self, df):
        column = self.params["column"]
        mask = df[column] < 0
        return self._failures(df, mask, f"{column} has negative values")
```
