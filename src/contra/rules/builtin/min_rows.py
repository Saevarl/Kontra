from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule

@register_rule("min_rows")
class MinRowsRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        min_count = int(self.params["value"])
        h = int(df.height)
        passed = h >= min_count
        return {
            "rule_id": self.rule_id,
            "passed": passed,
            "failed_count": 0 if passed else (min_count - h),
            "message": f"Dataset has {h} rows, requires at least {min_count}",
        }

    def compile_predicate(self):
        return None  # dataset-level scalar check
