from __future__ import annotations
from typing import Dict, Any, List, Optional
import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule
from kontra.rules.predicates import Predicate
from kontra.state.types import FailureMode

@register_rule("unique")
class UniqueRule(BaseRule):
    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._get_required_param("column", str)

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        col = df[column]

        # SQL semantics: NULLs are not considered duplicates (NULL != NULL)
        # Only check duplicates among non-null values
        non_null_mask = col.is_not_null()
        duplicates = col.is_duplicated() & non_null_mask

        res = super()._failures(df, duplicates, f"{column} has duplicate values")
        res["rule_id"] = self.rule_id

        # Add failure details
        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.DUPLICATE_VALUES)
            res["details"] = self._explain_failure(df, column)

        return res

    def _explain_failure(self, df: pl.DataFrame, column: str) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        # Find duplicated values and their counts
        duplicates_df = (
            df.group_by(column)
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
            .sort("count", descending=True)
            .head(10)  # Top 10 duplicates
        )

        top_duplicates: List[Dict[str, Any]] = []
        for row in duplicates_df.iter_rows(named=True):
            val = row[column]
            count = row["count"]
            top_duplicates.append({
                "value": val,
                "count": count,
            })

        total_duplicates = (
            df.group_by(column)
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
            .height
        )

        return {
            "duplicate_value_count": total_duplicates,
            "top_duplicates": top_duplicates,
        }

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        # SQL semantics: NULLs are not considered duplicates (NULL != NULL)
        col = pl.col(column)
        expr = col.is_duplicated() & col.is_not_null()
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} has duplicate values",
            columns={column},
        )

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        # Unique requires a subquery to find duplicated values
        # This is more complex but still much faster than loading 1M rows
        column = self.params["column"]
        col = f'"{column}"'

        # Find values that appear more than once, then select rows with those values
        # Note: This requires knowing the table name, which we don't have here
        # Return None to fall back to Polars for this rule
        return None
