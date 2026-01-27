# src/kontra/rules/builtin/conditional_not_null.py
"""
Conditional not-null rule - Column must not be NULL when a condition is met.

Usage:
    - name: conditional_not_null
      params:
        column: shipping_date
        when: "status == 'shipped'"

Fails when:
    - The `when` condition is TRUE AND the `column` is NULL

Passes when:
    - The `when` condition is FALSE (regardless of column value)
    - The `when` condition is TRUE AND the `column` is NOT NULL
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.rule_defs.condition_parser import parse_condition, ConditionParseError
from kontra.state.types import FailureMode


# Map operators to Polars comparison methods
POLARS_OP_MAP = {
    "==": pl.Expr.__eq__,
    "!=": pl.Expr.__ne__,
    ">": pl.Expr.__gt__,
    ">=": pl.Expr.__ge__,
    "<": pl.Expr.__lt__,
    "<=": pl.Expr.__le__,
}


@register_rule("conditional_not_null")
class ConditionalNotNullRule(BaseRule):
    """
    Fails where column is NULL when a condition is met.

    params:
      - column: str (required) - Column that must not be null
      - when: str (required) - Condition expression (e.g., "status == 'shipped'")

    Condition syntax:
      column_name operator value

      Supported operators: ==, !=, >, >=, <, <=
      Supported values: 'string', 123, 123.45, true, false, null

    Examples:
      - status == 'shipped'
      - amount > 0
      - is_active == true
    """

    rule_scope = "cross_column"

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        # Validate parameters at construction time
        self._column = self._get_required_param("column", str)
        self._when_expr = self._get_required_param("when", str)

        # Parse the when expression at init time to fail early
        try:
            self._when_column, self._when_op, self._when_value = parse_condition(self._when_expr)
        except ConditionParseError as e:
            raise ValueError(f"Rule 'conditional_not_null' invalid 'when' expression: {e}") from e

    def required_columns(self) -> Set[str]:
        return {self._column, self._when_column}

    def _build_condition_expr(self) -> pl.Expr:
        """Build the Polars expression for the when condition."""
        when_col = pl.col(self._when_column)
        compare_fn = POLARS_OP_MAP[self._when_op]

        # Handle NULL value in condition
        if self._when_value is None:
            if self._when_op == "==":
                return when_col.is_null()
            elif self._when_op == "!=":
                return when_col.is_not_null()
            else:
                # Other operators with NULL don't make sense; treat as always false
                return pl.lit(False)

        # Build comparison expression
        return compare_fn(when_col, self._when_value)

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check columns exist before accessing
        col_check = self._check_columns(df, {self._column, self._when_column})
        if col_check is not None:
            return col_check

        # Build condition expression
        condition_expr = self._build_condition_expr()

        # Mask: True = failure
        # Failure = condition is TRUE AND column is NULL
        mask_expr = condition_expr & pl.col(self._column).is_null()

        # Evaluate the expression to get a Series
        mask = df.select(mask_expr.alias("_mask"))["_mask"]

        message = f"{self._column} is null when {self._when_expr}"

        res = super()._failures(df, mask, message)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.CONDITIONAL_NULL)
            res["details"] = self._explain_failure(df, mask, res["failed_count"])

        return res

    def _explain_failure(
        self, df: pl.DataFrame, mask: pl.Series, failed_count: int
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        total_rows = df.height
        failure_rate = failed_count / total_rows if total_rows > 0 else 0

        # Count rows matching the condition
        condition_expr = self._build_condition_expr()
        condition_matches = df.select(condition_expr.sum())[0, 0]

        details: Dict[str, Any] = {
            "failed_count": failed_count,
            "failure_rate": round(failure_rate, 4),
            "total_rows": total_rows,
            "column": self._column,
            "when_condition": self._when_expr,
            "rows_matching_condition": int(condition_matches) if condition_matches else 0,
        }

        # Sample failing row positions (first 5)
        if failed_count > 0 and failed_count <= 1000:
            positions: List[int] = []
            for i, val in enumerate(mask):
                if val:
                    positions.append(i)
                    if len(positions) >= 5:
                        break
            if positions:
                details["sample_positions"] = positions

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        # Build condition expression
        condition_expr = self._build_condition_expr()

        # Mask: condition is TRUE AND column is NULL
        expr = condition_expr & pl.col(self._column).is_null()

        message = f"{self._column} is null when {self._when_expr}"

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=message,
            columns={self._column, self._when_column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Return SQL spec for SQL pushdown executors."""
        return {
            "kind": "conditional_not_null",
            "rule_id": self.rule_id,
            "column": self._column,
            "when_column": self._when_column,
            "when_op": self._when_op,
            "when_value": self._when_value,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        col = f'"{self._column}"'
        when_col = f'"{self._when_column}"'

        # Map operators
        sql_op = self._when_op
        if sql_op == "==":
            sql_op = "="
        elif sql_op == "!=":
            sql_op = "<>"

        # Format the value
        if self._when_value is None:
            # Special handling for NULL comparison
            if sql_op == "=":
                condition = f"{when_col} IS NULL"
            elif sql_op == "<>":
                condition = f"{when_col} IS NOT NULL"
            else:
                return None  # Can't compare with NULL using < > etc.
        elif isinstance(self._when_value, str):
            escaped = self._when_value.replace("'", "''")
            condition = f"{when_col} {sql_op} '{escaped}'"
        elif isinstance(self._when_value, bool):
            val = "TRUE" if self._when_value else "FALSE"
            condition = f"{when_col} {sql_op} {val}"
        else:
            condition = f"{when_col} {sql_op} {self._when_value}"

        # Failure = condition is TRUE AND column is NULL
        return f"({condition}) AND {col} IS NULL"
