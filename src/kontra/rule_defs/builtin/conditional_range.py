# src/kontra/rules/builtin/conditional_range.py
"""
Conditional range rule - Column must be within range when a condition is met.

Usage:
    - name: conditional_range
      params:
        column: discount_percent
        when: "customer_type == 'premium'"
        min: 10
        max: 50

Fails when:
    - The `when` condition is TRUE AND (column is NULL OR column < min OR column > max)

Passes when:
    - The `when` condition is FALSE (regardless of column value)
    - The `when` condition is TRUE AND column is within [min, max]
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union

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


@register_rule("conditional_range")
class ConditionalRangeRule(BaseRule):
    """
    Fails where column is outside range when a condition is met.

    params:
      - column: str (required) - Column to check range
      - when: str (required) - Condition expression (e.g., "status == 'active'")
      - min: numeric (optional) - Minimum allowed value (inclusive)
      - max: numeric (optional) - Maximum allowed value (inclusive)

    At least one of `min` or `max` must be provided.

    Condition syntax:
      column_name operator value

      Supported operators: ==, !=, >, >=, <, <=
      Supported values: 'string', 123, 123.45, true, false, null

    When the condition is TRUE:
      - NULL in column = failure (can't compare NULL)
      - Value outside [min, max] = failure

    Examples:
      - name: conditional_range
        params:
          column: discount_percent
          when: "customer_type == 'premium'"
          min: 10
          max: 50
    """

    rule_scope = "cross_column"

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        # Validate parameters at construction time
        self._column = self._get_required_param("column", str)
        self._when_expr = self._get_required_param("when", str)
        self._min_val: Optional[Union[int, float]] = params.get("min")
        self._max_val: Optional[Union[int, float]] = params.get("max")

        # At least one bound must be provided
        if self._min_val is None and self._max_val is None:
            raise ValueError(
                "Rule 'conditional_range' requires at least one of 'min' or 'max'"
            )

        # Validate min <= max
        if self._min_val is not None and self._max_val is not None:
            if self._min_val > self._max_val:
                from kontra.errors import RuleParameterError
                raise RuleParameterError(
                    "conditional_range", "min/max",
                    f"min ({self._min_val}) must be <= max ({self._max_val})"
                )

        # Parse the when expression at init time to fail early
        try:
            self._when_column, self._when_op, self._when_value = parse_condition(self._when_expr)
        except ConditionParseError as e:
            raise ValueError(f"Rule 'conditional_range' invalid 'when' expression: {e}") from e

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

    def _build_range_violation_expr(self) -> pl.Expr:
        """Build expression for range violation (NULL or out of range)."""
        col = pl.col(self._column)

        # NULL is a violation
        null_expr = col.is_null()

        # Out of range conditions
        if self._min_val is not None and self._max_val is not None:
            out_of_range = (col < self._min_val) | (col > self._max_val)
        elif self._min_val is not None:
            out_of_range = col < self._min_val
        else:
            out_of_range = col > self._max_val

        return null_expr | out_of_range

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check columns exist before accessing
        col_check = self._check_columns(df, {self._column, self._when_column})
        if col_check is not None:
            return col_check

        # Build condition and range violation expressions
        condition_expr = self._build_condition_expr()
        range_violation_expr = self._build_range_violation_expr()

        # Mask: True = failure
        # Failure = condition is TRUE AND (column is NULL OR outside range)
        mask_expr = condition_expr & range_violation_expr

        # Evaluate the expression to get a Series
        mask = df.select(mask_expr.alias("_mask"))["_mask"]

        message = self._build_message()

        res = super()._failures(df, mask, message)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.CONDITIONAL_RANGE_VIOLATION)
            res["details"] = self._explain_failure(df, mask, res["failed_count"])

        return res

    def _build_message(self) -> str:
        """Build the failure message."""
        if self._min_val is not None and self._max_val is not None:
            return f"{self._column} outside range [{self._min_val}, {self._max_val}] when {self._when_expr}"
        elif self._min_val is not None:
            return f"{self._column} below {self._min_val} when {self._when_expr}"
        else:
            return f"{self._column} above {self._max_val} when {self._when_expr}"

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

        # Add expected bounds
        if self._min_val is not None:
            details["expected_min"] = self._min_val
        if self._max_val is not None:
            details["expected_max"] = self._max_val

        # Filter to rows where condition is true
        condition_mask = df.select(condition_expr.alias("_cond"))["_cond"]
        conditional_df = df.filter(condition_mask)

        if conditional_df.height > 0:
            col = conditional_df[self._column]

            # Count violations by type
            null_count = col.null_count()
            if null_count > 0:
                details["null_count_when_condition"] = int(null_count)

            if self._min_val is not None:
                below_min = (col < self._min_val).sum()
                if below_min > 0:
                    details["below_min_count"] = int(below_min)

            if self._max_val is not None:
                above_max = (col > self._max_val).sum()
                if above_max > 0:
                    details["above_max_count"] = int(above_max)

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
        # Build condition and range violation expressions
        condition_expr = self._build_condition_expr()
        range_violation_expr = self._build_range_violation_expr()

        # Mask: condition is TRUE AND (column is NULL OR outside range)
        expr = condition_expr & range_violation_expr

        message = self._build_message()

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=message,
            columns={self._column, self._when_column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Return SQL spec for SQL pushdown executors."""
        return {
            "kind": "conditional_range",
            "rule_id": self.rule_id,
            "column": self._column,
            "when_column": self._when_column,
            "when_op": self._when_op,
            "when_value": self._when_value,
            "min": self._min_val,
            "max": self._max_val,
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

        # Format the when value
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

        # Build range violation part
        range_parts = [f"{col} IS NULL"]
        if self._min_val is not None:
            range_parts.append(f"{col} < {self._min_val}")
        if self._max_val is not None:
            range_parts.append(f"{col} > {self._max_val}")

        range_violation = " OR ".join(range_parts)

        # Failure = condition is TRUE AND (column is NULL OR outside range)
        return f"({condition}) AND ({range_violation})"
