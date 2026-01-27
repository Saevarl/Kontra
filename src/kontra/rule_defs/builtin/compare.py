# src/kontra/rules/builtin/compare.py
"""
Compare rule - Compares two columns using a comparison operator.

Usage:
    - name: compare
      params:
        left: end_date
        right: start_date
        op: ">="

Fails when:
    - Either column is NULL (can't compare NULL values)
    - The comparison left op right is FALSE
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Set

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


# Map operator strings to Polars comparison methods
POLARS_OP_MAP = {
    ">": pl.Expr.__gt__,
    ">=": pl.Expr.__ge__,
    "<": pl.Expr.__lt__,
    "<=": pl.Expr.__le__,
    "==": pl.Expr.__eq__,
    "!=": pl.Expr.__ne__,
}

# Map for human-readable operator descriptions
OP_DESCRIPTIONS = {
    ">": "greater than",
    ">=": "greater than or equal to",
    "<": "less than",
    "<=": "less than or equal to",
    "==": "equal to",
    "!=": "not equal to",
}

SUPPORTED_OPS = set(POLARS_OP_MAP.keys())


@register_rule("compare")
class CompareRule(BaseRule):
    """
    Fails where left column does not satisfy the comparison with right column.

    params:
      - left: str (required) - Left column name
      - right: str (required) - Right column name
      - op: str (required) - Comparison operator: >, >=, <, <=, ==, !=

    NULL handling:
      Rows where either column is NULL are considered failures.
      You can't meaningfully compare NULL values.
    """

    rule_scope = "cross_column"

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        # Validate parameters at construction time
        self._left = self._get_required_param("left", str)
        self._right = self._get_required_param("right", str)
        self._op = self._get_required_param("op", str)

        if self._op not in SUPPORTED_OPS:
            raise ValueError(
                f"Rule 'compare' unsupported operator '{self._op}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_OPS))}"
            )

    def required_columns(self) -> Set[str]:
        return {self._left, self._right}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check columns exist before accessing
        col_check = self._check_columns(df, {self._left, self._right})
        if col_check is not None:
            return col_check

        left_col = pl.col(self._left)
        right_col = pl.col(self._right)

        # Get the comparison function
        compare_fn = POLARS_OP_MAP[self._op]

        # Build mask expression: True = failure
        # Failures are: NULL in either column OR comparison is FALSE
        comparison_expr = compare_fn(left_col, right_col)
        mask_expr = (
            left_col.is_null()
            | right_col.is_null()
            | ~comparison_expr
        )

        # Evaluate the expression to get a Series
        mask = df.select(mask_expr.alias("_mask"))["_mask"]

        op_desc = OP_DESCRIPTIONS[self._op]
        message = f"{self._left} is not {op_desc} {self._right}"

        res = super()._failures(df, mask, message)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.COMPARISON_FAILED)
            res["details"] = self._explain_failure(df, res["failed_count"])

        return res

    def _explain_failure(
        self, df: pl.DataFrame, failed_count: int
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        total_rows = df.height
        failure_rate = failed_count / total_rows if total_rows > 0 else 0

        # Count NULLs in each column
        left_nulls = df[self._left].is_null().sum()
        right_nulls = df[self._right].is_null().sum()

        details: Dict[str, Any] = {
            "failed_count": failed_count,
            "failure_rate": round(failure_rate, 4),
            "total_rows": total_rows,
            "left_column": self._left,
            "right_column": self._right,
            "operator": self._op,
            "left_null_count": int(left_nulls),
            "right_null_count": int(right_nulls),
        }

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        left_col = pl.col(self._left)
        right_col = pl.col(self._right)

        compare_fn = POLARS_OP_MAP[self._op]
        comparison_expr = compare_fn(left_col, right_col)

        # Violation mask: NULL in either column OR comparison is FALSE
        expr = (
            left_col.is_null()
            | right_col.is_null()
            | ~comparison_expr
        )

        op_desc = OP_DESCRIPTIONS[self._op]
        message = f"{self._left} is not {op_desc} {self._right}"

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=message,
            columns={self._left, self._right},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Return SQL spec for SQL pushdown executors."""
        return {
            "kind": "compare",
            "rule_id": self.rule_id,
            "left": self._left,
            "right": self._right,
            "op": self._op,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        left = f'"{self._left}"'
        right = f'"{self._right}"'

        # Map Python operators to SQL
        sql_op = self._op
        if sql_op == "==":
            sql_op = "="
        elif sql_op == "!=":
            sql_op = "<>"

        # Failures: NULL in either column OR comparison is FALSE
        return f"{left} IS NULL OR {right} IS NULL OR NOT ({left} {sql_op} {right})"
