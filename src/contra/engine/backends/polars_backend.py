from __future__ import annotations

"""
Polars backend (wrapper)

Thin adapter that delegates to the existing RuleExecutionPlan.execute_compiled.
This preserves current behavior exactly; the ValidationEngine can swap this in
without any observable change in outputs.
"""

from typing import Any, Dict, List, Callable

import polars as pl

from .base import ValidationBackend


class PolarsBackend(ValidationBackend):
    name = "polars"

    def __init__(self, executor: Callable[[pl.DataFrame, Any], List[Dict[str, Any]]]):
        """
        Args:
          executor: usually RuleExecutionPlan.execute_compiled
        """
        self._executor = executor

    def supports(self, connector_caps: int) -> bool:
        # Polars works everywhere we can materialize a DataFrame locally.
        return True

    def compile(self, compiled_plan: Any) -> Any:
        # No transformation required for Polars; pass through.
        return compiled_plan

    def execute(self, df: pl.DataFrame, compiled_artifact: Any) -> Dict[str, Any]:
        results = self._executor(df, compiled_artifact)
        return {"results": results}

    def introspect(self, df: pl.DataFrame) -> Dict[str, Any]:
        return {
            "row_count": int(df.height),
            "available_cols": list(df.columns),
        }
