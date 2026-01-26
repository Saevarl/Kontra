from __future__ import annotations
from typing import Dict, Any, List, Optional
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
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

        # SQL semantics: COUNT(*) - COUNT(DISTINCT col)
        # This counts "extra" rows beyond one unique occurrence
        # NULLs are excluded from DISTINCT count but included in total
        total_count = len(df)
        distinct_count = col.n_unique()  # includes NULL as one value if present

        # Adjust for NULL handling: SQL COUNT(DISTINCT) excludes NULLs
        # but n_unique() counts NULL as a distinct value
        null_count = col.null_count()
        if null_count > 0:
            distinct_count -= 1  # Remove NULL from distinct count

        failed_count = total_count - distinct_count - null_count

        # For sampling, still identify duplicated rows (non-null)
        non_null_mask = col.is_not_null()
        duplicates = col.is_duplicated() & non_null_mask

        # Build result manually to use SQL-semantics count
        res = {
            "rule_id": self.rule_id,
            "name": self.name,
            "passed": failed_count == 0,
            "failed_count": failed_count,
            "message": f"{column} has duplicate values" if failed_count > 0 else f"{column} values are unique",
            "severity": self.params.get("severity", "blocking"),
        }

        # Add failure details and samples
        if failed_count > 0:
            res["failure_mode"] = str(FailureMode.DUPLICATE_VALUES)
            res["details"] = self._explain_failure(df, column)
            # Store mask for sampling (still shows all duplicate rows)
            res["_failure_mask"] = duplicates

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
        # Return None to force fallback to validate() for COUNTING
        # validate() uses SQL semantics: total - distinct (counts "extra" rows)
        # Use sample_predicate() for identifying which rows are duplicates
        return None

    def sample_predicate(self) -> Optional[Predicate]:
        """Return predicate for sampling duplicate rows (not for counting)."""
        column = self.params["column"]
        col = pl.col(column)
        # Identifies all rows participating in duplicates for sampling
        # NULLs are not considered duplicates (NULL != NULL in SQL)
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
