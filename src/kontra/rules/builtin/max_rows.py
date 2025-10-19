from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("max_rows")
class MaxRowsRule(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        max_count = int(self.params["value"])
        h = int(df.height)
        passed = h <= max_count
        return {
            "rule_id": self.rule_id,
            "passed": passed,
            "failed_count": 0 if passed else (h - max_count),
            "message": f"Dataset has {h} rows, exceeds max {max_count}",
        }

    def compile_predicate(self):
        return None  # dataset-level scalar check
