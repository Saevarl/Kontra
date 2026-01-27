from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode

@register_rule("max_rows")
class MaxRowsRule(BaseRule):
    rule_scope = "dataset"
    supports_tally = False

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Accept both 'value' and 'threshold' for backwards compatibility
        max_count = int(self.params.get("value", self.params.get("threshold", 0)))
        h = int(df.height)
        passed = h <= max_count

        if passed:
            message = f"Dataset has {h:,} rows (max allowed: {max_count:,})"
        else:
            message = f"Dataset has {h:,} rows, exceeds max {max_count:,}"

        result: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "passed": passed,
            "failed_count": 0 if passed else (h - max_count),
            "message": message,
        }

        if not passed:
            result["failure_mode"] = str(FailureMode.ROW_COUNT_HIGH)
            result["details"] = {
                "actual_rows": h,
                "maximum_allowed": max_count,
                "excess": h - max_count,
            }

        return result

    def compile_predicate(self):
        return None  # dataset-level scalar check
