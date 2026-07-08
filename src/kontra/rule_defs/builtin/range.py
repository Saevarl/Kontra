from __future__ import annotations
from datetime import date, datetime
from typing import Dict, Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


def _coerce_temporal(val: Any, target_dtype: Optional[pl.DataType] = None) -> Any:
    """Coerce a string boundary to date/datetime if it looks like one.

    Args:
        val: The boundary value (min or max).
        target_dtype: Optional Polars dtype of the target column for guidance.

    Returns:
        Coerced value, or original value if coercion not applicable.
    """
    if val is None:
        return None

    # Already a temporal type — no coercion needed
    if isinstance(val, (date, datetime)):
        return val

    if not isinstance(val, str):
        return val

    # If we know the target dtype, only coerce for temporal columns
    if target_dtype is not None:
        import polars as pl

        if target_dtype == pl.Date:
            try:
                return date.fromisoformat(val)
            except (ValueError, TypeError):
                return val
        elif target_dtype in (pl.Datetime, pl.Datetime("us"), pl.Datetime("ns"), pl.Datetime("ms")):
            try:
                return datetime.fromisoformat(val)
            except (ValueError, TypeError):
                return val
        elif hasattr(target_dtype, "base_type") and target_dtype.base_type() == pl.Datetime:
            try:
                return datetime.fromisoformat(val)
            except (ValueError, TypeError):
                return val
        # Non-temporal column — don't coerce strings
        return val

    # No dtype hint — try to parse as date first, then datetime
    try:
        return date.fromisoformat(val)
    except (ValueError, TypeError):
        pass
    try:
        return datetime.fromisoformat(val)
    except (ValueError, TypeError):
        pass
    return val


@register_rule("range", _builtin=True)
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

        # Coerce string params to numeric (BUG F-032)
        # Dict format {"name": "range", "params": {"min": "18"}} passes strings
        # from YAML parsing. Coerce if possible, raise clear error otherwise.
        min_val = self._coerce_numeric_param(min_val, "min")
        max_val = self._coerce_numeric_param(max_val, "max")

        # Write coerced values back to params so validate() uses them
        if min_val is not None:
            self.params["min"] = min_val
        if max_val is not None:
            self.params["max"] = max_val

        # Validate min <= max at construction time
        if min_val is not None and max_val is not None:
            if min_val > max_val:
                raise RuleParameterError(
                    "range", "min/max",
                    f"min ({min_val}) must be <= max ({max_val})"
                )

    @staticmethod
    def _coerce_numeric_param(val: Any, param_name: str) -> Any:
        """Coerce a string param to numeric, or raise if not possible.

        YAML parsing and dict-format rules may pass "18" instead of 18.
        Date/datetime strings are left alone (handled by _coerce_temporal at runtime).
        """
        if val is None:
            return None
        # bool is a subclass of int — reject before the int check
        if isinstance(val, bool):
            from kontra.errors import RuleParameterError
            raise RuleParameterError(
                "range", param_name,
                f"must be numeric, got bool ({val})"
            )
        # Already numeric or temporal — OK
        if isinstance(val, (int, float, date, datetime)):
            return val
        if isinstance(val, str):
            # Try numeric coercion first
            try:
                # Prefer int if it looks like an integer
                if "." not in val and "e" not in val.lower():
                    return int(val)
                return float(val)
            except ValueError:
                pass
            # Could be a date/datetime string — leave it for _coerce_temporal
            try:
                date.fromisoformat(val)
                return val
            except (ValueError, TypeError):
                pass
            try:
                datetime.fromisoformat(val)
                return val
            except (ValueError, TypeError):
                pass
            # Not numeric, not temporal
            from kontra.errors import RuleParameterError
            raise RuleParameterError(
                "range", param_name,
                f"must be numeric (or a date/datetime string), got string: '{val}'"
            )
        # Unsupported type
        from kontra.errors import RuleParameterError
        raise RuleParameterError(
            "range", param_name,
            f"must be numeric, got {type(val).__name__}"
        )

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        import polars as pl

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

            # Coerce string boundaries for date/datetime columns
            col_dtype = df.schema[column]
            min_val = _coerce_temporal(min_val, col_dtype)
            max_val = _coerce_temporal(max_val, col_dtype)

            # Build condition for out-of-range values
            if min_val is not None and max_val is not None:
                mask = (col < min_val) | (col > max_val)
            elif min_val is not None:
                mask = col < min_val
            else:
                mask = col > max_val

            # NaN is out-of-range for float columns. In Polars `NaN < min` is
            # False and fill_null does not catch NaN, so a min-only bound would
            # silently let NaN pass. The `> max` path already catches NaN, but
            # we add it consistently so min-only and max-only agree. Gate on
            # float dtype since is_nan() raises on non-float columns.
            if col_dtype.is_float():
                mask = mask | col.is_nan()

            # NULLs are also failures
            mask = mask.fill_null(True)

            res = super()._failures(df, mask, self._build_message(column, min_val, max_val))
            res["rule_id"] = self.rule_id

            # Add failure details
            if res["failed_count"] > 0:
                res["failure_mode"] = str(FailureMode.RANGE_VIOLATION)
                res["details"] = self._explain_failure(df, column, min_val, max_val)

            return res
        except (TypeError, pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError) as e:
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
        col_dtype = df.schema[column]
        min_val = _coerce_temporal(min_val, col_dtype)
        max_val = _coerce_temporal(max_val, col_dtype)
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

        # Coerce string boundaries (best-effort without dtype hint)
        min_val = _coerce_temporal(min_val)
        max_val = _coerce_temporal(max_val)

        # A min-only bound cannot catch NaN in a dtype-safe vectorized expr:
        # is_nan() raises on non-float columns and compile_predicate has no
        # schema to gate on. Route min-only to validate() (which gates is_nan
        # on float dtype). max-only and both-bounds catch NaN via `> max`,
        # matching validate(), so they stay vectorized.
        if max_val is None:
            return None

        def _expr():
            import polars as pl

            col = pl.col(column)
            # Build expression for out-of-range values
            if min_val is not None and max_val is not None:
                expr = (col < min_val) | (col > max_val)
            elif min_val is not None:
                expr = col < min_val
            else:
                expr = col > max_val
            # NULLs are also failures
            return expr.fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr_factory=_expr,
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
