# src/kontra/engine/executors/base.py
from __future__ import annotations

from typing import Any, Dict, List, Protocol

from kontra.connectors.handle import DatasetHandle


class SqlExecutor(Protocol):
    """
    Protocol for a pluggable, SQL-based rule executor.

    An executor is responsible for:
    1.  Reporting if it can handle a given data source and rule set.
    2.  Compiling a list of Kontra rules into a single SQL query.
    3.  Executing that query and returning results in the Kontra format.
    4.  (Optional) Introspecting the data source for metadata.
    """

    name: str = "sql_executor"

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        """
        Return True if this executor can run against the given handle
        and supports at least one of the provided SQL-compatible rules.
        """
        ...

    def compile(self, sql_specs: List[Dict[str, Any]]) -> Any:
        """
        Compile the list of rule specs into a native, executable query plan
        (e.g., a SQL string).
        """
        ...

    def execute(self, handle: DatasetHandle, compiled_plan: Any) -> Dict[str, Any]:
        """
        Execute the compiled plan against the data in the handle.
        Must return a dict: {"results": [...]}
        """
        ...

    def introspect(self, handle: DatasetHandle) -> Dict[str, Any]:
        """
        Perform lightweight introspection (e.g., row count, column names).
        Must return a dict: {"row_count": int, "available_cols": list[str]}
        """
        ...