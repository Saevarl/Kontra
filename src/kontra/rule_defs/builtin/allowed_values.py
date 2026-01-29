from __future__ import annotations
from typing import Dict, Any, List, Optional, Sequence
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode

@register_rule("allowed_values")
class AllowedValuesRule(BaseRule):
    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._get_required_param("column", str)
        if "values" not in self.params:
            raise ValueError(
                f"Rule '{self.name}' requires parameter 'values' but it was not provided"
            )

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        allowed_set = set(values)
        # Check if NULL is explicitly allowed
        null_allowed = None in allowed_set

        # is_in returns NULL for NULL values, fill_null decides if NULL is violation
        # If NULL is in allowed values, NULL should NOT be a violation (fill_null(False))
        # If NULL is not allowed, NULL IS a violation (fill_null(True))
        mask = (~df[column].is_in(list(values))).fill_null(not null_allowed)
        values_str = self._format_values_list(values)
        res = super()._failures(df, mask, f"{column} value not in [{values_str}]")
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

    def _format_values_list(self, values: Sequence[Any], max_show: int = 5) -> str:
        """Format values list for display, truncating if too long."""
        str_vals = [repr(v) if isinstance(v, str) else str(v) for v in values if v is not None]
        if len(str_vals) <= max_show:
            return ", ".join(str_vals)
        else:
            shown = ", ".join(str_vals[:max_show])
            return f"{shown}, ... ({len(str_vals)} total)"

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]
        # Check if NULL is explicitly allowed
        null_allowed = None in set(values)
        # If NULL is allowed, don't treat NULL as violation
        expr = (~pl.col(column).is_in(values)).fill_null(not null_allowed)
        values_str = self._format_values_list(values)
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} value not in [{values_str}]",
            columns={column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        column = self.params.get("column")
        values = self.params.get("values")

        if not column or values is None:
            return None

        return {
            "kind": "allowed_values",
            "rule_id": self.rule_id,
            "column": column,
            "values": list(values),
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]

        col = f'"{column}"'

        # Check if NULL is explicitly allowed
        null_allowed = None in set(values)

        # Build IN list with proper quoting (exclude None)
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
            if null_allowed:
                # NULL is allowed, only non-null disallowed values are violations
                return f"{col} NOT IN ({in_list}) AND {col} IS NOT NULL"
            else:
                # NULL is not allowed, both disallowed values AND NULL are violations
                return f"{col} NOT IN ({in_list}) OR {col} IS NULL"
        else:
            # Only NULL in allowed values (no other values) - everything non-null fails
            if null_allowed:
                return f"{col} IS NOT NULL"
            else:
                # Empty allowed list, no NULL - everything fails (always true filter)
                return "1=1"
