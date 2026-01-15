# Kontra Rule Reference

Kontra provides 10 built-in validation rules for common data quality checks. Each rule can be executed via three tiers: metadata preplan, SQL pushdown, or Polars execution.

**How rules work:** Each rule measures violations and returns a count. When we say a rule "fails if X", we mean the rule reports violations when X is true. Whether violations constitute "failure" depends on how the consumer (CLI, CI, agent) interprets severity. See [Architecture Guide](architecture.md#core-concepts) for details.

## Rule Support Matrix

| Rule | Description | DuckDB | PostgreSQL | SQL Server | Parquet Preplan |
|------|-------------|--------|------------|------------|-----------------|
| `not_null` | No NULL values | SQL | SQL | SQL | Metadata |
| `unique` | No duplicate values | - | SQL | SQL | - |
| `min_rows` | Minimum row count | SQL | SQL | SQL | Metadata |
| `max_rows` | Maximum row count | SQL | SQL | SQL | Metadata |
| `allowed_values` | Values in set | - | SQL | SQL | - |
| `freshness` | Data recency | SQL | SQL | SQL | - |
| `range` | Min/max bounds | SQL | SQL | SQL | Metadata |
| `regex` | Pattern matching | SQL | SQL | SQL* | - |
| `dtype` | Type checking | - | - | - | Schema |
| `custom_sql_check` | User SQL | Polars | - | - | - |

*SQL Server regex uses PATINDEX with limited pattern support.

**Execution tier notes:**
- **SQL/Polars**: Return exact violation counts
- **Metadata (Preplan)**: Can only prove pass (0 violations) or fail (≥1 violation). Reports `failed_count: 1` for any failure. Use `--preplan off` for exact counts.

---

## not_null

Fails if the specified column contains any NULL values.

```yaml
- name: not_null
  params:
    column: user_id
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |

### Execution

- **Parquet preplan**: Uses row-group null counts (instant)
- **SQL pushdown**: `SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)`
- **Polars**: `df[col].is_null().sum()`

---

## unique

Fails if the specified column contains duplicate values.

```yaml
- name: unique
  params:
    column: email
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |

### Execution

- **SQL pushdown**: `COUNT(*) - COUNT(DISTINCT col)`
- **Polars**: Counts duplicates via groupby

### Notes

NULL values are ignored when checking uniqueness (SQL semantics).

---

## min_rows

Fails if the dataset has fewer than the specified number of rows.

```yaml
- name: min_rows
  params:
    threshold: 1000
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `threshold` | integer | Yes | Minimum row count |

### Execution

- **Parquet preplan**: Uses Parquet footer row count (instant)
- **SQL pushdown**: `GREATEST(0, threshold - COUNT(*))`
- **Polars**: `df.height`

---

## max_rows

Fails if the dataset has more than the specified number of rows.

```yaml
- name: max_rows
  params:
    threshold: 1000000
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `threshold` | integer | Yes | Maximum row count |

### Execution

- **Parquet preplan**: Uses Parquet footer row count (instant)
- **SQL pushdown**: `GREATEST(0, COUNT(*) - threshold)`
- **Polars**: `df.height`

---

## allowed_values

Fails if the column contains values not in the allowed set.

```yaml
- name: allowed_values
  params:
    column: status
    values: ["active", "inactive", "pending"]
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `values` | list | Yes | Allowed values |

### Execution

- **SQL pushdown**: `SUM(CASE WHEN col NOT IN (...) THEN 1 ELSE 0 END)`
- **Polars**: `~df[col].is_in(values)`

### Notes

NULL values are treated as failures.

---

## freshness

Fails if the maximum timestamp in the column is older than the specified age.

```yaml
- name: freshness
  params:
    column: updated_at
    max_age: "24h"
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Timestamp column |
| `max_age` | string | Yes | Maximum age (e.g., "24h", "7d", "1h30m") |

### Supported Age Formats

- `Xs` - X seconds
- `Xm` - X minutes
- `Xh` - X hours
- `Xd` - X days
- `XhYm` - X hours and Y minutes (e.g., "1h30m")

### Execution

- **SQL pushdown**: `CASE WHEN MAX(col) >= NOW() - interval THEN 0 ELSE 1 END`
- **Polars**: Compares max timestamp to current time

---

## range

Fails if values in the column are outside the specified range.

```yaml
- name: range
  params:
    column: age
    min: 0
    max: 150
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `min` | number | No* | Minimum value (inclusive) |
| `max` | number | No* | Maximum value (inclusive) |

*At least one of `min` or `max` must be provided.

### Execution

- **Parquet preplan**: Uses row-group min/max stats
- **SQL pushdown**: `SUM(CASE WHEN col < min OR col > max THEN 1 ELSE 0 END)`
- **Polars**: `(col < min) | (col > max)`

### Notes

NULL values are treated as failures.

---

## regex

Fails if values don't match the specified regular expression pattern.

```yaml
- name: regex
  params:
    column: email
    pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `pattern` | string | Yes | Regular expression |

### Execution

- **DuckDB**: `regexp_matches(col, pattern)`
- **PostgreSQL**: `col ~ pattern`
- **SQL Server**: `PATINDEX('%pattern%', col)` (limited support)
- **Polars**: Full Python regex support

### SQL Server Limitations

SQL Server's PATINDEX only supports:
- `%` - any characters (like `.*`)
- `_` - single character (like `.`)
- `[abc]` - character class
- `[a-z]` - range
- `[^abc]` - negated class

Does NOT support: `^`, `$`, `+`, `*`, `?`, `\d`, `\w`, grouping, alternation.

---

## dtype

Checks that a column has the expected data type.

```yaml
- name: dtype
  params:
    column: user_id
    type: int64
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `type` | string | Yes | Expected Polars dtype |

### Supported Types

- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `utf8` (string)
- `bool`
- `date`, `datetime`

### Execution

- **Schema check**: Compares column dtype to expected (no data scan)

---

## custom_sql_check

Escape hatch for custom validation logic. Unlike declarative rules, this directly executes your SQL and reports the returned count as violations.

```yaml
- name: custom_sql_check
  params:
    sql: |
      SELECT COUNT(*)
      FROM {table}
      WHERE balance < 0 AND account_type = 'savings'
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | Yes | SQL query returning failure count |

### Execution

- **DuckDB via Polars**: Query executed against in-memory data
- **Not supported**: PostgreSQL, SQL Server (use native rules instead)

### Notes

- Use `{table}` placeholder for the data source
- Query must return a single integer (0 = pass, >0 = fail with count)
- Useful for complex cross-column or aggregate validations

---

## Example Contract

```yaml
name: users_quality_contract
description: Quality checks for users table
datasource: postgres://user:pass@localhost/db/public.users

rules:
  # Structural checks
  - name: min_rows
    params: { threshold: 1000 }

  - name: max_rows
    params: { threshold: 10000000 }

  # Column checks
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: user_id }

  - name: not_null
    params: { column: email }

  - name: regex
    params:
      column: email
      pattern: '@.*[.]'

  - name: allowed_values
    params:
      column: status
      values: ["active", "inactive", "pending", "deleted"]

  - name: range
    params:
      column: age
      min: 0
      max: 150

  - name: freshness
    params:
      column: updated_at
      max_age: "24h"
```

---

## Adding Custom Rules

Custom rules can be registered using the `@register_rule` decorator:

```python
from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("my_custom_rule")
class MyCustomRule(BaseRule):
    def validate(self, df):
        column = self.params["column"]
        # Your validation logic here
        mask = df[column] < 0  # Example: fail on negative values
        return self._failures(df, mask, f"{column} has negative values")
```

See `src/kontra/rules/builtin/` for implementation examples.
