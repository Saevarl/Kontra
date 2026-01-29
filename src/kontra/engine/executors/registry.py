# src/kontra/engine/executors/registry.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from kontra.connectors.handle import DatasetHandle

if TYPE_CHECKING:
    from .base import SqlExecutor


# Global registry: maps "executor_name" -> constructor()
_EXECUTORS: Dict[str, Callable[[], SqlExecutor]] = {}


def register_executor(name: str):
    """
    Decorator to register a SQL executor class.
    """

    def deco(cls: Callable[[], SqlExecutor]) -> Callable[[], SqlExecutor]:
        if name in _EXECUTORS:
            raise ValueError(f"Executor '{name}' is already registered.")
        _EXECUTORS[name] = cls
        return cls

    return deco


def pick_executor(
    handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
) -> Optional[SqlExecutor]:
    """
    Find the first registered executor that supports the given handle and rules.
    """
    if not sql_specs:
        return None  # Nothing to push down

    # Iterate over registered constructors
    for name, ctor in _EXECUTORS.items():
        executor = ctor()  # Instantiate the executor
        try:
            if executor.supports(handle, sql_specs):
                return executor
        except (AttributeError, TypeError, ValueError):
            # Be conservative: ignore faulty executors
            continue
    return None


def register_default_executors() -> None:
    """
    Eagerly import built-in executors so their @register_executor
    decorators run and populate the registry.

    NOTE: This is the legacy function that loads ALL executors.
    For lazy loading, use register_executors_for_path() instead.
    """
    # Local import triggers decorator side-effect
    from . import duckdb_sql  # noqa: F401

    # PostgreSQL executor (optional - requires psycopg)
    try:
        from . import postgres_sql  # noqa: F401
    except ImportError:
        pass  # psycopg not installed, skip postgres executor

    # SQL Server executor (optional - requires pymssql)
    try:
        from . import sqlserver_sql  # noqa: F401
    except ImportError:
        pass  # pymssql not installed, skip sqlserver executor


def register_executors_for_path(
    execution_path: str,
    database_type: str | None = None,
) -> None:
    """
    Register only the executors needed for a specific execution path.

    This enables lazy loading - we only import heavy dependencies when needed.

    Args:
        execution_path: "database", "file", or "dataframe"
        database_type: "postgres" or "sqlserver" (required for database path)
    """
    if execution_path == "database":
        # Database path: only load the specific DB executor
        if database_type == "postgres":
            try:
                from . import postgres_sql  # noqa: F401
            except ImportError:
                raise ImportError(
                    "PostgreSQL support requires psycopg. "
                    "Install with: pip install 'psycopg[binary]'"
                )
        elif database_type == "sqlserver":
            try:
                from . import sqlserver_sql  # noqa: F401
            except ImportError:
                raise ImportError(
                    "SQL Server support requires pymssql. "
                    "Install with: pip install pymssql"
                )
        else:
            raise ValueError(f"Unknown database_type: {database_type}")

    elif execution_path in ("file", "dataframe"):
        # File/DataFrame path: load DuckDB executor
        from . import duckdb_sql  # noqa: F401

    else:
        # Unknown path - fall back to loading everything
        register_default_executors()