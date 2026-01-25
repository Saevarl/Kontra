# Rules Reference

Kontra provides 18 built-in validation rules.

## Most Contracts Use These

Start here. These rules cover 80% of use cases:

| Rule | What It Checks | Example |
|------|---------------|---------|
| `not_null` | No NULL values | `{ column: user_id }` |
| `unique` | No duplicates | `{ column: email }` |
| `dtype` | Expected type | `{ column: age, type: int64 }` |
| `allowed_values` | Values in set | `{ column: status, values: [a, b, c] }` |
| `range` | Min/max bounds | `{ column: age, min: 0, max: 150 }` |

```yaml
rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: email }

  - name: dtype
    params: { column: age, type: int64 }

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
| `disallowed_values` | Values NOT in set | `column`, `values` |
| `range` | Min/max bounds | `column`, `min`, `max` |
| `length` | String length bounds | `column`, `min`, `max` |
| `regex` | Pattern match | `column`, `pattern` |
| `contains` | Contains substring | `column`, `substring` |
| `starts_with` | Starts with prefix | `column`, `prefix` |
| `ends_with` | Ends with suffix | `column`, `suffix` |
| `dtype` | Type check | `column`, `type` |
| `min_rows` | Minimum rows | `threshold` |
| `max_rows` | Maximum rows | `threshold` |
| `freshness` | Data recency | `column`, `max_age` |
| `compare` | Cross-column comparison | `left`, `right`, `op` |
| `conditional_not_null` | Conditional not-null | `column`, `when` |
| `conditional_range` | Conditional range check | `column`, `when`, `min`, `max` |
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

**Count semantics**: `failed_count` = total rows - distinct values (i.e., "extra" rows that would need to be removed to make values unique). For `[a, a, b, c, c, c]`, `failed_count` = 3 (6 total - 3 distinct).

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

### disallowed_values

Values must NOT be in disallowed set. Inverse of `allowed_values`. NULL = pass (NULL is not in any list).

```yaml
- name: disallowed_values
  params:
    column: status
    values: [deleted, banned, spam]
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `values` | list | Yes |

**Use case**: Block specific known-bad values while allowing everything else.

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

### length

String length must be within bounds. NULL = violation.

```yaml
- name: length
  params:
    column: username
    min: 3
    max: 50
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `min` | integer | No* |
| `max` | integer | No* |

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

### contains

Values must contain substring. NULL = violation. Uses efficient LIKE patterns (faster than regex).

```yaml
- name: contains
  params:
    column: email
    substring: "@"
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `substring` | string | Yes |

For complex patterns, use `regex` instead.

---

### starts_with

Values must start with prefix. NULL = violation. Uses efficient LIKE patterns.

```yaml
- name: starts_with
  params:
    column: url
    prefix: "https://"
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `prefix` | string | Yes |

---

### ends_with

Values must end with suffix. NULL = violation. Uses efficient LIKE patterns.

```yaml
- name: ends_with
  params:
    column: filename
    suffix: ".csv"
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `suffix` | string | Yes |

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

Compare two columns using a comparison operator.

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

**NULL handling**: Rows where *either* column is NULL are counted as failures. This is intentional—you cannot meaningfully compare NULL values. If you need to allow NULLs, combine with a conditional rule or filter NULLs upstream.

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

### conditional_range

Column must be within range when condition is met. NULL in column when condition is true = violation.

```yaml
- name: conditional_range
  params:
    column: discount_percent
    when: "customer_type == 'premium'"
    min: 10
    max: 50
```

| Parameter | Type | Required |
|-----------|------|----------|
| `column` | string | Yes |
| `when` | string | Yes |
| `min` | number | No* |
| `max` | number | No* |

*At least one of `min` or `max` required.

Condition format: `column_name operator value`
- Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Values: `'string'`, `123`, `true`, `false`, `null`

**Behavior:**
- Only checks rows where `when` condition is TRUE
- Fails if column is NULL when condition is TRUE
- Fails if column is outside `[min, max]` when condition is TRUE
- NULL in condition column → condition is FALSE (no check)

**Example use cases:**
- Premium customers must get 10-50% discount
- Orders over $100 must have shipping fee between $0-$10
- Active users must have session duration 1-3600 seconds

---

### custom_sql_check

Escape hatch for custom SQL. Write a query that selects "violation rows" and Kontra counts them.

```yaml
- name: custom_sql_check
  params:
    sql: |
      SELECT * FROM {table}
      WHERE balance < 0 AND account_type = 'savings'
```

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |

Use `{table}` placeholder. Kontra transforms your query to `COUNT(*)` for efficiency.

**SQL pushdown:** When the data source is PostgreSQL or SQL Server, the SQL is validated using sqlglot to ensure it's safe (SELECT-only, no dangerous functions). If safe, it executes directly on the database.

**Cross-table queries:** You can reference other tables in your SQL:

```yaml
- name: custom_sql_check
  params:
    sql: |
      SELECT * FROM {table}
      WHERE category_id NOT IN (SELECT id FROM valid_categories)
```

**Safety:** Only SELECT statements are allowed. Queries are validated to reject:
- INSERT, UPDATE, DELETE, DROP, CREATE, ALTER
- Dangerous functions like `pg_sleep`, `xp_cmdshell`, `dblink`
- Multiple statements (SQL injection prevention)
- System catalog access (`pg_*`, `sys.*`, `information_schema.*`)

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
| `disallowed_values` | NULL = pass (NULL is not in any list) |
| `range` | NULL = violation |
| `length` | NULL = violation |
| `regex` | NULL = violation |
| `contains` | NULL = violation |
| `starts_with` | NULL = violation |
| `ends_with` | NULL = violation |
| `compare` | NULL = violation |
| `conditional_not_null` | NULL in condition → condition is FALSE |
| `conditional_range` | NULL in column = violation (if condition TRUE); NULL in condition → condition is FALSE |
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
| `disallowed_values` | | ✓ | |
| `range` | ✓ | ✓ | |
| `length` | | ✓ | SQL Server uses LEN() |
| `regex` | | ✓ | SQL Server limited |
| `contains` | | ✓ | Uses LIKE (fast) |
| `starts_with` | | ✓ | Uses LIKE (fast) |
| `ends_with` | | ✓ | Uses LIKE (fast) |
| `dtype` | Schema | | |
| `min_rows` | ✓ | ✓ | |
| `max_rows` | ✓ | ✓ | |
| `freshness` | | ✓ | |
| `compare` | | ✓ | |
| `conditional_not_null` | | ✓ | |
| `conditional_range` | | ✓ | |
| `custom_sql_check` | | ✓ | |

Preplan returns binary (0 or ≥1), not exact counts. Use `--preplan off` for exact counts.

---

## Adding Custom Rules

### Basic Custom Rule (Polars Only)

```python
from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("my_rule")
class MyRule(BaseRule):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = self._get_required_param("column", str)

    def validate(self, df):
        mask = df[self.column] < 0
        return self._failures(df, mask, f"{self.column} has negative values")
```

This rule works but requires data to be loaded into Polars.

### Where to Put Custom Rules

Custom rules must be **imported before** `kontra.validate()` is called. The `@register_rule` decorator registers the rule when the module is imported.

```python
# my_rules.py
from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("positive")
class PositiveRule(BaseRule):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = self._get_required_param("column", str)

    def validate(self, df):
        mask = df[self.column].is_null() | (df[self.column] <= 0)
        return self._failures(df, mask, f"{self.column} must be positive")
```

```python
# main.py
import my_rules  # Must import to register the rule
import kontra

result = kontra.validate("data.parquet", rules=[
    {"name": "positive", "params": {"column": "amount"}}
])
```

### Parameter Validation

Use built-in helpers for parameter validation:

```python
def __init__(self, name, params):
    super().__init__(name, params)

    # Required parameter - raises ValueError if missing or wrong type
    self.column = self._get_required_param("column", str)

    # Optional parameter with default
    self.threshold = params.get("threshold", 0)

    # Manual validation
    if self.threshold < 0:
        raise ValueError(f"threshold must be >= 0, got {self.threshold}")
```

### Available Helper Methods

| Method | Purpose |
|--------|---------|
| `_get_required_param(name, type)` | Get required param, raises if missing/wrong type |
| `_failures(df, mask, message)` | Create failure result from boolean mask |
| `_check_columns(df, columns)` | Check columns exist, returns error dict if not |
| `self.params` | Dict of all parameters passed to the rule |
| `self.rule_id` | Auto-generated ID like `COL:amount:positive` |

### Custom Rule with SQL Pushdown

Add `to_sql_agg()` to enable SQL pushdown without modifying executors:

```python
import polars as pl
from kontra.rules.base import BaseRule
from kontra.rules.predicates import Predicate
from kontra.rules.registry import register_rule

@register_rule("positive")
class PositiveRule(BaseRule):
    """Values must be > 0. NULL = violation."""

    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = params["column"]

    def validate(self, df):
        """Fallback: Polars execution."""
        mask = df[self.column].is_null() | (df[self.column] <= 0)
        return self._failures(df, mask, f"{self.column} non-positive")

    def compile_predicate(self):
        """Optional: Vectorized Polars (faster than validate())."""
        return Predicate(
            rule_id=self.rule_id,
            expr=pl.col(self.column).is_null() | (pl.col(self.column) <= 0),
            columns={self.column},
            message=f"{self.column} non-positive",
        )

    def to_sql_agg(self, dialect="duckdb"):
        """SQL pushdown: no data loading needed."""
        col = f'"{self.column}"'
        return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'

    def required_columns(self):
        """For projection optimization."""
        return {self.column}
```

| Method | Purpose | Execution Path |
|--------|---------|----------------|
| `validate(df)` | **Required**. Fallback | Polars (data loaded) |
| `compile_predicate()` | Vectorized Polars + sampling | Polars (faster) |
| `to_sql_agg(dialect)` | SQL pushdown | DuckDB/PostgreSQL/SQL Server |
| `required_columns()` | Projection | Load fewer columns |

> **Sample Collection**: `compile_predicate()` is required for `sample_failures()` to work.
> Without it, rule results will have `samples=None` and `samples_reason="rule_unsupported"`.
> The predicate's `expr` is used to filter the DataFrame for failing rows.

### Dialect-Specific SQL

`to_sql_agg(dialect)` is called once per dialect (`"duckdb"`, `"postgres"`, `"mssql"`). Handle differences:

```python
def to_sql_agg(self, dialect="duckdb"):
    # SQL Server uses [brackets], others use "double quotes"
    if dialect == "mssql":
        col = f'[{self.column}]'
    else:
        col = f'"{self.column}"'

    return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'
```

Return `None` to skip SQL pushdown for a dialect (falls back to Polars):

```python
def to_sql_agg(self, dialect="duckdb"):
    if dialect == "mssql":
        return None  # SQL Server not supported, use Polars

    col = f'"{self.column}"'
    return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'
```

### Using a Custom Rule

After defining your rule, use it like any built-in rule:

```python
import kontra

# Validate a DataFrame
result = kontra.validate(df, rules=[
    {"name": "positive", "params": {"column": "amount"}},
])

# Or in YAML contracts:
# rules:
#   - name: positive
#     params:
#       column: amount

# Check if SQL pushdown was used
print(result.rules[0].source)  # "sql" for parquet/database, "polars" for DataFrames
```

---

## Data Format Edge Cases

### CSV Files

**Empty strings vs NULL**: CSV parsing differs between engines:

| Row | Raw CSV | Polars (DataFrame) | DuckDB (file path) |
|-----|---------|--------------------|--------------------|
| 2 | `""` | Empty string | NULL |
| 3 | `` (trailing) | NULL | NULL |

```csv
id,name
1,Alice
2,""     # Polars: empty string, DuckDB: NULL
3,       # Both: NULL
```

**Impact**: `kontra.profile(df)` vs `kontra.profile("file.csv")` may report different `null_rate` for columns with empty strings. This is inherent CSV ambiguity - quoted empty (`""`) has no universal interpretation.

**Recommendation**: For consistent behavior, load CSV with Polars first: `kontra.profile(pl.read_csv("file.csv"))`

**First row is always header**: CSV files are assumed to have a header row. If your CSV has no header, the first data row becomes column names.

### Large Values

**Integer overflow**: Very large integers (e.g., 10^100) cause `OverflowError` because they exceed Polars integer types. Use string columns for arbitrary-precision numbers.

### SQL Server

**Regex falls back to Polars**: SQL Server doesn't support true regex (PATINDEX uses LIKE wildcards). The `regex` rule automatically falls back to Polars execution for correct results.
