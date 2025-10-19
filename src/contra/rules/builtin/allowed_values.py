from __future__ import annotations
from typing import Dict, Any, Optional, Sequence
import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule
from contra.rules.predicates import Predicate

@register_rule("allowed_values")
class AllowedValuesRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        values: Sequence[Any] = self.params["values"]
        mask = (~df[column].is_in(list(values))).fill_null(True)
        res = super()._failures(df, mask, f"{column} contains disallowed values")
        res["rule_id"] = self.rule_id
        return res

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
