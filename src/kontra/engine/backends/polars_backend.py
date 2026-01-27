# src/kontra/engine/backends/polars_backend.py
from __future__ import annotations

"""
Polars Backend (Adapter)

Thin adapter that defers execution to the RuleExecutionPlan's compiled executor.
Keeps the backend boundary explicit and behavior deterministic.
"""

from typing import Any, Callable, Dict, List, Optional

import polars as pl


class PolarsBackend:
    name = "polars"

    def __init__(self, executor: Callable[..., List[Dict[str, Any]]]):
        """
        Parameters
        ----------
        executor : callable
            Function that evaluates the compiled plan against a materialized
            Polars DataFrame (typically RuleExecutionPlan.execute_compiled).
        """
        self._executor = executor

    def supports(self, connector_caps: int) -> bool:
        """Capability hook reserved for future; always True for local DataFrames."""
        return True

    def compile(self, compiled_plan: Any) -> Any:
        """No-op for Polars: pass through the compiled plan."""
        return compiled_plan

    def execute(
        self,
        df: pl.DataFrame,
        compiled_artifact: Any,
        rule_tally_map: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        """Execute the compiled artifact against `df` and wrap results.

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to validate.
        compiled_artifact : Any
            The compiled execution plan.
        rule_tally_map : dict, optional
            Mapping of rule_id -> bool for tally mode. If True, use exact counts;
            if False, use early termination. Defaults to exact counts if not provided.
        """
        results = self._executor(df, compiled_artifact, rule_tally_map)
        return {"results": results}

    def introspect(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Basic observability: row count and available columns."""
        return {
            "row_count": int(df.height),
            "available_cols": list(df.columns),
        }
