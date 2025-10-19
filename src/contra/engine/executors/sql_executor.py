# src/contra/engine/sql/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class SqlExecutor(ABC):
    """
    Minimal interface for pushing a subset of rules down to a SQL engine.

    Responsibilities:
      - Decide if the executor is relevant for a given source/capabilities
      - Compile CompiledPlan.sql_rules → a single SQL string (or None)
      - Execute that SQL and return Contra-style result dicts (per-rule)
      - (Optional) Introspect source width/height for observability

    NOTE: This class does NOT handle data materialization for Polars.
          That is handled by Materializers.
    """

    name: str = "sql-executor"

    @abstractmethod
    def supports(self, connector_caps: int) -> bool:
        """Return True if this executor is relevant for the given connector capabilities."""
        raise NotImplementedError

    @abstractmethod
    def compile(self, compiled_plan: Any) -> Optional[str]:
        """
        Return a single aggregated SQL statement (or None) that computes
        `{rule_id: failed_count}` in a single row.

        The engine will treat None as “no SQL to push”.
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, source_uri: str, compiled_sql: str) -> Dict[str, Any]:
        """
        Run the SQL against the given source URI and return:
          {"results": List[ContraResultDict]}
        """
        raise NotImplementedError

    def introspect(self, source_uri: str) -> Dict[str, Any]:
        """Optional: lightweight {row_count, available_cols} without materializing."""
        return {}
