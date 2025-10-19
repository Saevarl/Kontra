from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule
from contra.rules.predicates import Predicate


@register_rule("regex")
class RegexRule(BaseRule):
    """
    Fails where `column` does not match the regex `pattern`. NULLs are failures.

    params:
      - column: str (required)
      - pattern: str (required)

    Notes:
      - Uses vectorized `str.contains` (regex by default in this Polars version).
      - No `regex=`/`strict=` kwargs are passed to maintain compatibility.
    """

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        pattern = self.params["pattern"]
        try:
            mask = (
                ~df[column]
                .cast(pl.Utf8)
                .str.contains(pattern)  # regex by default
            ).fill_null(True)
            res = super()._failures(df, mask, f"{column} failed regex pattern {pattern}")
            res["rule_id"] = self.rule_id
            return res
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Rule execution failed: {e}",
            }

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        pattern = self.params["pattern"]
        expr = (
            ~pl.col(column)
            .cast(pl.Utf8)
            .str.contains(pattern)  # regex by default
        ).fill_null(True)
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} failed regex pattern {pattern}",
            columns={column},
        )
