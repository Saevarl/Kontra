from __future__ import annotations
from typing import Dict, Any, List, Optional
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode

@register_rule("not_null")
class NotNullRule(BaseRule):
    """
    Fails where column contains NULL values.

    params:
      - column: str (required) - Column to check
      - include_nan: bool (optional, default: False) - Also treat NaN as null

    Note: By default, NaN values are NOT considered null (Polars behavior).
    Set include_nan=True to catch both NULL and NaN values.
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        # Validate required parameter at construction time
        self._get_required_param("column", str)

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        include_nan = self.params.get("include_nan", False)

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        # Build mask for null (and optionally NaN) values
        mask = df[column].is_null()
        if include_nan:
            # For numeric columns, also check for NaN
            col = df[column]
            if col.dtype.is_float():
                mask = mask | col.is_nan()

        message = f"{column} contains null values"
        if include_nan:
            message = f"{column} contains null or NaN values"

        res = super()._failures(df, mask, message)
        res["rule_id"] = self.rule_id

        # Add failure details
        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.NULL_VALUES)
            res["details"] = self._explain_failure(df, column, res["failed_count"], include_nan)

        return res

    def _explain_failure(
        self, df: pl.DataFrame, column: str, null_count: int, include_nan: bool = False
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        total_rows = df.height
        null_rate = null_count / total_rows if total_rows > 0 else 0

        details: Dict[str, Any] = {
            "null_count": null_count,
            "null_rate": round(null_rate, 4),
            "total_rows": total_rows,
        }

        if include_nan:
            details["includes_nan"] = True

        # Find sample row positions with nulls (first 5)
        if null_count > 0 and null_count <= 1000:
            null_positions: List[int] = []
            col = df[column]
            for i, val in enumerate(col):
                if val is None:
                    null_positions.append(i)
                    if len(null_positions) >= 5:
                        break
            if null_positions:
                details["sample_positions"] = null_positions

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        include_nan = self.params.get("include_nan", False)

        expr = pl.col(column).is_null()
        message = f"{column} contains null values"

        if include_nan:
            # Note: is_nan() only works on float columns, but compile_predicate
            # doesn't have access to the DataFrame schema. The expression will
            # be evaluated at runtime where Polars handles type checking.
            expr = expr | pl.col(column).is_nan()
            message = f"{column} contains null or NaN values"

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=message,
            columns={column},
        )

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        column = self.params["column"]
        include_nan = self.params.get("include_nan", False)

        # Quote column name for safety
        col = f'"{column}"'

        if include_nan:
            # NaN check: value != value is true for NaN
            return f"{col} IS NULL OR {col} != {col}"
        else:
            return f"{col} IS NULL"
