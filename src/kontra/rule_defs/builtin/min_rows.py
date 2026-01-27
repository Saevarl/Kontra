from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode

@register_rule("min_rows")
class MinRowsRule(BaseRule):
    rule_scope = "dataset"
    supports_tally = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Validate threshold at construction time
        threshold = self.params.get("value", self.params.get("threshold", 0))
        if threshold is not None and int(threshold) < 0:
            from kontra.errors import RuleParameterError
            raise RuleParameterError(
                "min_rows", "threshold",
                f"must be non-negative, got {threshold}"
            )

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Accept both 'value' and 'threshold' for backwards compatibility
        min_count = int(self.params.get("value", self.params.get("threshold", 0)))
        h = int(df.height)
        passed = h >= min_count

        result: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "passed": passed,
            "failed_count": 0 if passed else (min_count - h),
            "message": f"Dataset has {h} rows, requires at least {min_count}",
        }

        if not passed:
            result["failure_mode"] = str(FailureMode.ROW_COUNT_LOW)
            result["details"] = {
                "actual_rows": h,
                "minimum_required": min_count,
                "shortfall": min_count - h,
            }

        return result

    def compile_predicate(self):
        return None  # dataset-level scalar check
