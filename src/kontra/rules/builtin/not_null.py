from __future__ import annotations
from typing import Dict, Any, List, Optional
import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule
from kontra.rules.predicates import Predicate
from kontra.state.types import FailureMode

@register_rule("not_null")
class NotNullRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        mask = df[column].is_null()
        res = super()._failures(df, mask, f"{column} contains null values")
        res["rule_id"] = self.rule_id

        # Add failure details
        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.NULL_VALUES)
            res["details"] = self._explain_failure(df, column, res["failed_count"])

        return res

    def _explain_failure(self, df: pl.DataFrame, column: str, null_count: int) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        total_rows = df.height
        null_rate = null_count / total_rows if total_rows > 0 else 0

        details: Dict[str, Any] = {
            "null_count": null_count,
            "null_rate": round(null_rate, 4),
            "total_rows": total_rows,
        }

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
        expr = pl.col(column).is_null()
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} contains null values",
            columns={column},
        )
