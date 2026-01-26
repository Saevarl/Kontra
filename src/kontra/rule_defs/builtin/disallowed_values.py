# src/kontra/rules/builtin/disallowed_values.py
"""
Disallowed values rule - Column must NOT contain any of the specified values.

Inverse of allowed_values: fails if value IS in the list.

Usage:
    - name: disallowed_values
      params:
        column: status
        values: ["deleted", "banned", "spam"]

Fails when:
    - The column value IS in the disallowed values list

Passes when:
    - The column value is NOT in the disallowed values list
    - The column value is NULL (NULL is not in any list)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


@register_rule("disallowed_values")
class DisallowedValuesRule(BaseRule):
    """
    Fails where column value IS in the disallowed set.

    params:
      - column: str (required) - Column to check
      - values: list (required) - Values that are NOT allowed

    NULL handling:
      - NULL values are NOT failures (NULL is not in any list)
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        if "values" not in self.params:
            raise ValueError(
                f"Rule '{self.name}' requires parameter 'values' but it was not provided"
            )
        self._values: Sequence[Any] = self.params["values"]

    def required_columns(self) -> Set[str]:
        return {self._column}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check column exists before accessing
        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        # Failure = value IS in the disallowed list (not including NULL)
        # is_in returns NULL for NULL values, we want NULL -> not a failure
        mask = df[self._column].is_in(list(self._values)).fill_null(False)

        res = super()._failures(df, mask, f"{self._column} contains disallowed values")
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.NOVEL_CATEGORY)
            res["details"] = self._explain_failure(df, mask)

        return res

    def _explain_failure(self, df: pl.DataFrame, mask: pl.Series) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        # Find which disallowed values were found and their counts
        found_values = (
            df.filter(mask)
            .group_by(self._column)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )

        found_list: List[Dict[str, Any]] = []
        for row in found_values.iter_rows(named=True):
            found_list.append({
                "value": row[self._column],
                "count": row["count"],
            })

        return {
            "disallowed": [str(v) for v in self._values],
            "found_values": found_list,
        }

    def compile_predicate(self) -> Optional[Predicate]:
        # Failure = value IS in the disallowed list
        expr = pl.col(self._column).is_in(self._values).fill_null(False)
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{self._column} contains disallowed values",
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        return {
            "kind": "disallowed_values",
            "rule_id": self.rule_id,
            "column": self._column,
            "values": list(self._values),
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """Generate SQL filter for sampling failing rows."""
        col = f'"{self._column}"'

        # Build IN list (exclude None values)
        quoted_values = []
        for v in self._values:
            if v is None:
                continue
            elif isinstance(v, str):
                escaped = v.replace("'", "''")
                quoted_values.append(f"'{escaped}'")
            elif isinstance(v, bool):
                quoted_values.append("TRUE" if v else "FALSE")
            else:
                quoted_values.append(str(v))

        if not quoted_values:
            return None  # No values to check

        in_list = ", ".join(quoted_values)
        # Failure = value IS in the disallowed list (not null)
        return f"{col} IN ({in_list})"
