from __future__ import annotations
from typing import List, Dict, Any, Optional

from .sql_base import SqlRuleExecutor
from .duckdb_sql import DuckDBSqlExecutor

# Global registry (simple list preserves order)
_SQL_EXECUTORS: List[SqlRuleExecutor] = []


def register_sql_executor(executor: SqlRuleExecutor) -> None:
    """Register an SQL executor implementation (in priority order)."""
    _SQL_EXECUTORS.append(executor)


def available_executors() -> List[SqlRuleExecutor]:
    return list(_SQL_EXECUTORS)


def pick_sql_executor(source_uri: str, connector_caps: int, sql_specs: List[Dict[str, Any]]) -> Optional[SqlRuleExecutor]:
    """
    Choose the first executor that supports both the source and the rule specs.
    Return None if no executor applies (engine will skip pushdown).
    """
    for exec_ in _SQL_EXECUTORS:
        try:
            if exec_.supports_source(source_uri, connector_caps) and exec_.supports_rules(sql_specs):
                return exec_
        except Exception:
            # Be conservative: ignore faulty executors.
            continue
    return None


# Register built-ins (DuckDB first, can add Snowflake/Postgres later)
register_sql_executor(DuckDBSqlExecutor())
