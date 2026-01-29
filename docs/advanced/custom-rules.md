# Custom Rules

Extend Kontra with your own validation rules.

## Basic Rule

```python
from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule

@register_rule("positive")
class PositiveRule(BaseRule):
    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = self._get_required_param("column", str)

    def validate(self, df):
        mask = df[self.column].is_null() | (df[self.column] <= 0)
        return self._failures(df, mask, f"{self.column} must be positive")
```

This rule runs in Polars after data is loaded.

## Registration

Custom rules must be imported before `kontra.validate()` is called:

```python
# my_rules.py
from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule

@register_rule("positive")
class PositiveRule(BaseRule):
    # ...
```

```python
# main.py
import my_rules  # Import registers the rule
import kontra

result = kontra.validate("data.parquet", rules=[
    {"name": "positive", "params": {"column": "amount"}}
])
```

Use in YAML contracts:

```yaml
rules:
  - name: positive
    params: { column: amount }
```

---

## Helper Methods

| Method | Purpose |
|--------|---------|
| `_get_required_param(name, type)` | Get required param, raises if missing/wrong type |
| `_failures(df, mask, message)` | Create failure result from boolean mask |
| `_check_columns(df, columns)` | Check columns exist, returns error dict if not |
| `self.params` | Dict of all parameters |
| `self.rule_id` | Auto-generated ID (e.g., `COL:amount:positive`) |

```python
def __init__(self, name, params):
    super().__init__(name, params)
    self.column = self._get_required_param("column", str)
    self.threshold = params.get("threshold", 0)  # Optional with default

    if self.threshold < 0:
        raise ValueError(f"threshold must be >= 0, got {self.threshold}")
```

---

## Adding SQL Pushdown

Implement optional methods to enable preplan and SQL pushdown:

```python
import polars as pl
from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.predicates import Predicate
from kontra.rule_defs.registry import register_rule

@register_rule("positive")
class PositiveRule(BaseRule):
    """Values must be > 0. NULL = violation."""

    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = params["column"]

    def validate(self, df):
        """Required. Fallback execution in Polars."""
        mask = df[self.column].is_null() | (df[self.column] <= 0)
        return self._failures(df, mask, f"{self.column} non-positive")

    def compile_predicate(self):
        """Optional. Vectorized Polars, enables sample collection."""
        return Predicate(
            rule_id=self.rule_id,
            expr=pl.col(self.column).is_null() | (pl.col(self.column) <= 0),
            columns={self.column},
            message=f"{self.column} non-positive",
        )

    def to_sql_agg(self, dialect="duckdb"):
        """Optional. SQL pushdown for exact counts."""
        col = f'"{self.column}"'
        return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'

    def to_sql_exists(self, dialect="duckdb"):
        """Optional. SQL pushdown for fail-fast (tally=False)."""
        col = f'"{self.column}"'
        return f'{col} IS NULL OR {col} <= 0'

    def required_columns(self):
        """Optional. Enables column projection."""
        return {self.column}
```

| Method | Purpose | When Used |
|--------|---------|-----------|
| `validate(df)` | **Required.** Polars fallback | Always available |
| `compile_predicate()` | Vectorized Polars + sampling | Polars execution |
| `to_sql_agg(dialect)` | SQL COUNT expression | `tally=True` |
| `to_sql_exists(dialect)` | SQL WHERE condition | `tally=False` |
| `required_columns()` | Column projection | Load optimization |

**Note:** `compile_predicate()` is required for `sample_failures()` to work. Without it, samples will be `None`.

---

## Dialect-Specific SQL

Handle SQL dialect differences in `to_sql_agg()` and `to_sql_exists()`:

```python
def to_sql_agg(self, dialect="duckdb"):
    # SQL Server uses [brackets], others use "double quotes"
    if dialect == "mssql":
        col = f'[{self.column}]'
    else:
        col = f'"{self.column}"'

    return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'
```

Return `None` to skip pushdown for a dialect (falls back to Polars):

```python
def to_sql_agg(self, dialect="duckdb"):
    if dialect == "mssql":
        return None  # Not supported, use Polars

    col = f'"{self.column}"'
    return f'SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)'
```

Dialects: `"duckdb"`, `"postgres"`, `"mssql"`

---

## Verify Execution Path

Check which execution path was used:

```python
result = kontra.validate("data.parquet", rules=[
    {"name": "positive", "params": {"column": "amount"}}
])

print(result.rules[0].source)  # "sql", "metadata", or "polars"
```
