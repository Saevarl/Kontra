from __future__ import annotations
from typing import Dict, Any, Optional, Union
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


@register_rule("range")
class RangeRule(BaseRule):
    """
    Fails where `column` is outside the specified range [min, max].
    At least one of `min` or `max` must be provided.

    params:
      - column: str (required)
      - min: numeric (optional) - minimum allowed value (inclusive)
      - max: numeric (optional) - maximum allowed value (inclusive)

    NULLs are treated as failures (out of range).

    Examples:
      - name: range
        params:
          column: age
          min: 0
          max: 120

      - name: range
        params:
          column: price
          min: 0  # Only minimum, no upper bound
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from kontra.errors import RuleParameterError

        # Validate required column param
        self._get_required_param("column", str)

        min_val = self.params.get("min")
        max_val = self.params.get("max")

        # Validate at least one bound is provided
        if min_val is None and max_val is None:
            raise RuleParameterError(
                "range", "min/max",
                "at least one of 'min' or 'max' must be provided"
            )

        # Validate min <= max at construction time
        if min_val is not None and max_val is not None:
            if min_val > max_val:
                raise RuleParameterError(
                    "range", "min/max",
                    f"min ({min_val}) must be <= max ({max_val})"
                )

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        min_val = self.params.get("min")
        max_val = self.params.get("max")

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        # Note: min/max validation is done in __init__, so we know at least one is set
        try:
            col = df[column]

            # Build condition for out-of-range values
            if min_val is not None and max_val is not None:
                mask = (col < min_val) | (col > max_val)
            elif min_val is not None:
                mask = col < min_val
            else:
                mask = col > max_val

            # NULLs are also failures
            mask = mask.fill_null(True)

            res = super()._failures(df, mask, self._build_message(column, min_val, max_val))
            res["rule_id"] = self.rule_id

            # Add failure details
            if res["failed_count"] > 0:
                res["failure_mode"] = str(FailureMode.RANGE_VIOLATION)
                res["details"] = self._explain_failure(df, column, min_val, max_val)

            return res
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Rule execution failed: {e}",
            }

    def _explain_failure(
        self,
        df: pl.DataFrame,
        column: str,
        min_val: Optional[Union[int, float]],
        max_val: Optional[Union[int, float]],
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        col = df[column]
        details: Dict[str, Any] = {}

        # Get actual min/max
        actual_min = col.min()
        actual_max = col.max()
        if actual_min is not None:
            details["actual_min"] = actual_min
        if actual_max is not None:
            details["actual_max"] = actual_max

        # Expected bounds
        if min_val is not None:
            details["expected_min"] = min_val
        if max_val is not None:
            details["expected_max"] = max_val

        # Count below min
        if min_val is not None:
            below_min = (col < min_val).sum()
            if below_min > 0:
                details["below_min_count"] = int(below_min)

        # Count above max
        if max_val is not None:
            above_max = (col > max_val).sum()
            if above_max > 0:
                details["above_max_count"] = int(above_max)

        # Count nulls
        null_count = col.null_count()
        if null_count > 0:
            details["null_count"] = int(null_count)

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        min_val = self.params.get("min")
        max_val = self.params.get("max")

        if min_val is None and max_val is None:
            return None

        col = pl.col(column)

        # Build expression for out-of-range values
        if min_val is not None and max_val is not None:
            expr = (col < min_val) | (col > max_val)
        elif min_val is not None:
            expr = col < min_val
        else:
            expr = col > max_val

        # NULLs are also failures
        expr = expr.fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=self._build_message(column, min_val, max_val),
            columns={column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        column = self.params.get("column")
        min_val = self.params.get("min")
        max_val = self.params.get("max")

        if not column or (min_val is None and max_val is None):
            return None

        return {
            "kind": "range",
            "rule_id": self.rule_id,
            "column": column,
            "min": min_val,
            "max": max_val,
        }

    def _build_message(
        self, column: str, min_val: Optional[Union[int, float]], max_val: Optional[Union[int, float]]
    ) -> str:
        if min_val is not None and max_val is not None:
            return f"{column} values outside range [{min_val}, {max_val}]"
        elif min_val is not None:
            return f"{column} values below minimum {min_val}"
        else:
            return f"{column} values above maximum {max_val}"

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        column = self.params.get("column")
        min_val = self.params.get("min")
        max_val = self.params.get("max")

        if not column or (min_val is None and max_val is None):
            return None

        col = f'"{column}"'
        conditions = []

        if min_val is not None:
            conditions.append(f"{col} < {min_val}")
        if max_val is not None:
            conditions.append(f"{col} > {max_val}")

        # NULL is also a failure
        conditions.append(f"{col} IS NULL")

        return " OR ".join(conditions)
