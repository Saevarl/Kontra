# src/kontra/engine/executors/database_base.py
"""
Base class for database SQL executors (PostgreSQL, SQL Server).

This module provides shared implementation for compile() and execute() methods,
reducing code duplication between database-specific executors.

Each subclass must define:
  - DIALECT: "postgres" or "sqlserver"
  - SUPPORTED_RULES: Set of rule kinds this executor supports
  - _get_connection_ctx(): Connection context manager
  - _get_table_reference(): Fully-qualified table reference
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Set

from kontra.connectors.handle import DatasetHandle
from kontra.engine.sql_utils import (
    esc_ident,
    agg_unique,
    agg_min_rows,
    agg_max_rows,
    agg_allowed_values,
    agg_freshness,
    agg_range,
    agg_regex,
    agg_compare,
    agg_conditional_not_null,
    agg_conditional_range,
    exists_not_null,
    results_from_row,
    Dialect,
)

from .base import SqlExecutor


class DatabaseSqlExecutor(SqlExecutor, ABC):
    """
    Abstract base class for database-backed SQL executors.

    Provides shared implementation for compile() and execute() methods.
    Subclasses must implement dialect-specific connection and table handling.
    """

    # Subclasses must define these
    DIALECT: Dialect
    SUPPORTED_RULES: Set[str]

    @property
    @abstractmethod
    def name(self) -> str:
        """Executor name for registry."""
        ...

    @abstractmethod
    @contextmanager
    def _get_connection_ctx(self, handle: DatasetHandle):
        """
        Get a database connection context manager.

        For BYOC, yields the external connection directly.
        For URI-based, yields a new owned connection.
        """
        ...

    @abstractmethod
    def _get_table_reference(self, handle: DatasetHandle) -> str:
        """
        Get the fully-qualified table reference for the handle.

        Returns: "schema.table" format with proper escaping.
        """
        ...

    @abstractmethod
    def _supports_scheme(self, scheme: str, handle: DatasetHandle) -> bool:
        """
        Check if this executor supports the given URI scheme.

        Args:
            scheme: The URI scheme (lowercase)
            handle: The dataset handle for additional context (e.g., dialect)

        Returns:
            True if this executor can handle the scheme
        """
        ...

    def _esc(self, name: str) -> str:
        """Escape an identifier for this dialect."""
        return esc_ident(name, self.DIALECT)

    def _assemble_single_row(self, selects: List[str], table: str) -> str:
        """Build a single-row aggregate query from multiple SELECT expressions."""
        if not selects:
            return "SELECT 0 AS __no_sql_rules__;"
        return f"SELECT {', '.join(selects)} FROM {table};"

    def _assemble_exists_query(self, exists_exprs: List[str]) -> str:
        """Build a query with multiple EXISTS checks."""
        if not exists_exprs:
            return ""
        return f"SELECT {', '.join(exists_exprs)};"

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        """Check if this executor can handle the given handle and rules."""
        scheme = (handle.scheme or "").lower()

        if not self._supports_scheme(scheme, handle):
            return False

        # Must have at least one supported rule
        return any(
            s.get("kind") in self.SUPPORTED_RULES
            for s in (sql_specs or [])
        )

    def compile(self, sql_specs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compile rule specs into two-phase execution plan.

        Phase 1: EXISTS checks for not_null rules (fast, early-terminate)
        Phase 2: Aggregate query for remaining rules

        Returns:
            {
                "exists_specs": [...],      # Phase 1: not_null rules
                "aggregate_selects": [...], # Phase 2: aggregate expressions
                "aggregate_specs": [...],   # Phase 2: specs for aggregates
                "supported_specs": [...],   # All supported specs
            }
        """
        exists_specs: List[Dict[str, Any]] = []
        aggregate_selects: List[str] = []
        aggregate_specs: List[Dict[str, Any]] = []
        supported_specs: List[Dict[str, Any]] = []

        for spec in sql_specs or []:
            kind = spec.get("kind")
            rule_id = spec.get("rule_id")

            if not (kind and rule_id):
                continue

            # Skip unsupported rules
            if kind not in self.SUPPORTED_RULES:
                continue

            if kind == "not_null":
                col = spec.get("column")
                if isinstance(col, str) and col:
                    exists_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "unique":
                col = spec.get("column")
                if isinstance(col, str) and col:
                    aggregate_selects.append(agg_unique(col, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "min_rows":
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_min_rows(int(threshold), rule_id, self.DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "max_rows":
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_max_rows(int(threshold), rule_id, self.DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "allowed_values":
                col = spec.get("column")
                values = spec.get("values", [])
                if isinstance(col, str) and col and values:
                    aggregate_selects.append(agg_allowed_values(col, values, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "freshness":
                col = spec.get("column")
                max_age_seconds = spec.get("max_age_seconds")
                if isinstance(col, str) and col and isinstance(max_age_seconds, int):
                    aggregate_selects.append(agg_freshness(col, max_age_seconds, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "range":
                col = spec.get("column")
                min_val = spec.get("min")
                max_val = spec.get("max")
                if isinstance(col, str) and col and (min_val is not None or max_val is not None):
                    aggregate_selects.append(agg_range(col, min_val, max_val, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "regex":
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    aggregate_selects.append(agg_regex(col, pattern, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "compare":
                left = spec.get("left")
                right = spec.get("right")
                op = spec.get("op")
                if (isinstance(left, str) and left and
                    isinstance(right, str) and right and
                    isinstance(op, str) and op):
                    aggregate_selects.append(agg_compare(left, right, op, rule_id, self.DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_not_null":
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op):
                    aggregate_selects.append(
                        agg_conditional_not_null(col, when_column, when_op, when_value, rule_id, self.DIALECT)
                    )
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_range":
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")
                min_val = spec.get("min")
                max_val = spec.get("max")
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op and
                    (min_val is not None or max_val is not None)):
                    aggregate_selects.append(
                        agg_conditional_range(col, when_column, when_op, when_value, min_val, max_val, rule_id, self.DIALECT)
                    )
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

        return {
            "exists_specs": exists_specs,
            "aggregate_selects": aggregate_selects,
            "aggregate_specs": aggregate_specs,
            "supported_specs": supported_specs,
        }

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: Dict[str, Any],
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute the compiled plan in two phases.

        Phase 1: EXISTS checks for not_null (fast, can early-terminate)
        Phase 2: Aggregate query for remaining rules

        Returns:
            {"results": [...], "staging": None}
        """
        exists_specs = compiled_plan.get("exists_specs", [])
        aggregate_selects = compiled_plan.get("aggregate_selects", [])

        if not exists_specs and not aggregate_selects:
            return {"results": [], "staging": None}

        table = self._get_table_reference(handle)
        results: List[Dict[str, Any]] = []

        # Build rule_kinds mapping from specs
        rule_kinds = {}
        for spec in exists_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in compiled_plan.get("aggregate_specs", []):
            rule_kinds[spec["rule_id"]] = spec.get("kind")

        with self._get_connection_ctx(handle) as conn:
            cursor = self._get_cursor(conn)
            try:
                # Phase 1: EXISTS checks for not_null rules
                if exists_specs:
                    exists_exprs = [
                        exists_not_null(
                            spec["column"],
                            spec["rule_id"],
                            table,
                            self.DIALECT
                        )
                        for spec in exists_specs
                    ]
                    exists_sql = self._assemble_exists_query(exists_exprs)
                    cursor.execute(exists_sql)
                    row = cursor.fetchone()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []

                    if row and columns:
                        exists_results = results_from_row(columns, row, is_exists=True, rule_kinds=rule_kinds)
                        results.extend(exists_results)

                # Phase 2: Aggregate query for remaining rules
                if aggregate_selects:
                    agg_sql = self._assemble_single_row(aggregate_selects, table)
                    cursor.execute(agg_sql)
                    row = cursor.fetchone()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []

                    if row and columns:
                        agg_results = results_from_row(columns, row, is_exists=False, rule_kinds=rule_kinds)
                        results.extend(agg_results)
            finally:
                self._close_cursor(cursor)

        return {"results": results, "staging": None}

    def _get_cursor(self, conn):
        """
        Get a cursor from the connection.

        Default implementation calls conn.cursor().
        Subclasses can override for different behavior.
        """
        return conn.cursor()

    def _close_cursor(self, cursor):
        """
        Close a cursor if needed.

        Default implementation does nothing (cursor closed by context manager).
        Subclasses can override for connections that don't use context managers.
        """
        pass
