from __future__ import annotations

"""
Executors package

Exports:
  - SqlRuleExecutor (Protocol)
  - DuckDBSqlExecutor (implementation)
"""

from .sql_base import SqlRuleExecutor  # re-export for convenience
from .duckdb_sql import DuckDBSqlExecutor  # concrete executor
