from __future__ import annotations
from typing import Dict, Any, List, Optional, Sequence
import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule
from kontra.rules.predicates import Predicate
from kontra.state.types import FailureMode

@register_rule("allowed_values")
class AllowedValuesRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        allowed_set = set(values)
        mask = (~df[column].is_in(list(values))).fill_null(True)
        res = super()._failures(df, mask, f"{column} contains disallowed values")
        res["rule_id"] = self.rule_id

        # Add detailed explanation for failures
        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.NOVEL_CATEGORY)
            res["details"] = self._explain_failure(df, column, allowed_set)

        return res

    def _explain_failure(self, df: pl.DataFrame, column: str, allowed: set) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        col = df[column]

        # Find unexpected values and their counts
        unexpected = (
            df.filter(~col.is_in(list(allowed)) & col.is_not_null())
            .group_by(column)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)  # Top 10 unexpected values
        )

        unexpected_values: List[Dict[str, Any]] = []
        for row in unexpected.iter_rows(named=True):
            val = row[column]
            count = row["count"]
            unexpected_values.append({
                "value": val,
                "count": count,
            })

        return {
            "expected": sorted([str(v) for v in allowed]),
            "unexpected_values": unexpected_values,
            "suggestion": self._suggest_fix(unexpected_values, allowed) if unexpected_values else None,
        }

    def _suggest_fix(self, unexpected: List[Dict[str, Any]], allowed: set) -> str:
        """Suggest how to fix the validation failure."""
        if not unexpected:
            return ""

        top_unexpected = unexpected[0]
        val = top_unexpected["value"]
        count = top_unexpected["count"]

        # Simple suggestions
        if count > 100:
            return f"Consider adding '{val}' to allowed values (found in {count:,} rows)"

        return f"Found {len(unexpected)} unexpected value(s)"

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]
        expr = (~pl.col(column).is_in(values)).fill_null(True)
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} contains disallowed values",
            columns={column},
        )

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]

        col = f'"{column}"'

        # Build IN list with proper quoting
        quoted_values = []
        for v in values:
            if v is None:
                continue  # NULL handled separately
            elif isinstance(v, str):
                # Escape single quotes
                escaped = v.replace("'", "''")
                quoted_values.append(f"'{escaped}'")
            elif isinstance(v, bool):
                quoted_values.append("TRUE" if v else "FALSE")
            else:
                quoted_values.append(str(v))

        if quoted_values:
            in_list = ", ".join(quoted_values)
            return f"{col} NOT IN ({in_list}) OR {col} IS NULL"
        else:
            # Only NULL in allowed values, everything else fails
            return f"{col} IS NOT NULL"
