from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule
from contra.rules.predicates import Predicate

@register_rule("unique")
class UniqueRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        duplicates = df[column].is_duplicated()
        res = super()._failures(df, duplicates, f"{column} has duplicate values")
        res["rule_id"] = self.rule_id
        return res

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        expr = pl.col(column).is_duplicated()
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} has duplicate values",
            columns={column},
        )
