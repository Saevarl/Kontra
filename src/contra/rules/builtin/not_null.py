from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule
from contra.rules.predicates import Predicate

@register_rule("not_null")
class NotNullRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        mask = df[column].is_null()
        res = super()._failures(df, mask, f"{column} contains null values")
        res["rule_id"] = self.rule_id
        return res

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        expr = pl.col(column).is_null()
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} contains null values",
            columns={column},
        )
