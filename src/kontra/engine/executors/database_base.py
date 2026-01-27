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
from typing import Any, Dict, List, Optional, Set, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.engine.sql_utils import (
    esc_ident,
    # Aggregate functions (exact counts)
    agg_not_null,
    agg_unique,
    agg_min_rows,
    agg_max_rows,
    agg_allowed_values,
    agg_disallowed_values,
    agg_freshness,
    agg_range,
    agg_length,
    agg_regex,
    agg_contains,
    agg_starts_with,
    agg_ends_with,
    agg_compare,
    agg_conditional_not_null,
    agg_conditional_range,
    # EXISTS functions (early termination)
    exists_not_null,
    exists_unique,
    exists_allowed_values,
    exists_disallowed_values,
    exists_range,
    exists_length,
    exists_regex,
    exists_contains,
    exists_starts_with,
    exists_ends_with,
    exists_compare,
    exists_conditional_not_null,
    exists_conditional_range,
    exists_custom,
    # Utilities
    results_from_row,
    Dialect,
)
from kontra.engine.sql_validator import validate_sql, replace_table_placeholder, to_count_query
from kontra.logging import get_logger

from .base import SqlExecutor

_logger = get_logger(__name__)


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

    def _get_schema_and_table(self, handle: DatasetHandle) -> Tuple[str, str]:
        """
        Get schema and table name separately (for custom SQL placeholder replacement).

        Returns:
            Tuple of (schema, table_name)
        """
        # Default implementation - subclasses should override
        # This extracts from the table reference or connection params
        raise NotImplementedError("Subclass must implement _get_schema_and_table")

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
        Compile rule specs into three-phase execution plan with tally-aware routing.

        Phase 1: EXISTS checks (fast, early-terminate) - used when tally=False
        Phase 2: Aggregate query (exact counts) - used when tally=True
        Phase 3: Custom SQL queries (each executed individually)

        Tally routing:
        - tally=True: Use aggregate (exact count)
        - tally=False/None: Use EXISTS (early stop) if available, else aggregate

        Dataset rules (min_rows, max_rows, freshness) always use aggregate.

        Returns:
            {
                "exists_specs": [...],      # Phase 1: early-stop rules
                "aggregate_selects": [...], # Phase 2: aggregate expressions
                "aggregate_specs": [...],   # Phase 2: specs for aggregates
                "custom_sql_specs": [...],  # Phase 3: custom SQL queries
                "supported_specs": [...],   # All supported specs
            }
        """
        exists_specs: List[Dict[str, Any]] = []
        aggregate_selects: List[str] = []
        aggregate_specs: List[Dict[str, Any]] = []
        custom_sql_specs: List[Dict[str, Any]] = []
        supported_specs: List[Dict[str, Any]] = []

        for spec in sql_specs or []:
            kind = spec.get("kind")
            rule_id = spec.get("rule_id")

            if not (kind and rule_id):
                continue

            # Skip unsupported rules
            if kind not in self.SUPPORTED_RULES:
                continue

            # Get tally setting: True = exact count, False/None = early stop
            tally = spec.get("tally", False)
            use_exists = not tally  # Early stop when tally is False or None

            if kind == "custom_sql_check":
                # Validate SQL is safe using sqlglot before accepting
                user_sql = spec.get("sql")
                if user_sql:
                    # Replace {table} with dummy name for validation
                    # (sqlglot can't parse {table} as valid SQL)
                    test_sql = user_sql.replace("{table}", "_validation_table_")
                    validation = validate_sql(test_sql, dialect=self.DIALECT)
                    if validation.is_safe:
                        custom_sql_specs.append(spec)
                        supported_specs.append(spec)
                    else:
                        _logger.warning(
                            f"custom_sql_check '{rule_id}' rejected for remote execution: "
                            f"{validation.reason}"
                        )
                continue

            if kind == "not_null":
                col = spec.get("column")
                if isinstance(col, str) and col:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_not_null(col, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "unique":
                col = spec.get("column")
                if isinstance(col, str) and col:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_unique(col, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "min_rows":
                # Dataset rule - always aggregate (tally not applicable)
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_min_rows(int(threshold), rule_id, self.DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "max_rows":
                # Dataset rule - always aggregate (tally not applicable)
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_max_rows(int(threshold), rule_id, self.DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "allowed_values":
                col = spec.get("column")
                values = spec.get("values", [])
                if isinstance(col, str) and col and values:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_allowed_values(col, values, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "disallowed_values":
                col = spec.get("column")
                values = spec.get("values", [])
                if isinstance(col, str) and col and values:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_disallowed_values(col, values, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "freshness":
                # Dataset rule - always aggregate (tally not applicable)
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
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_range(col, min_val, max_val, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "length":
                col = spec.get("column")
                min_len = spec.get("min")
                max_len = spec.get("max")
                if isinstance(col, str) and col and (min_len is not None or max_len is not None):
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_length(col, min_len, max_len, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "regex":
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_regex(col, pattern, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "contains":
                col = spec.get("column")
                substring = spec.get("substring")
                if isinstance(col, str) and col and isinstance(substring, str) and substring:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_contains(col, substring, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "starts_with":
                col = spec.get("column")
                prefix = spec.get("prefix")
                if isinstance(col, str) and col and isinstance(prefix, str) and prefix:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_starts_with(col, prefix, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "ends_with":
                col = spec.get("column")
                suffix = spec.get("suffix")
                if isinstance(col, str) and col and isinstance(suffix, str) and suffix:
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(agg_ends_with(col, suffix, rule_id, self.DIALECT))
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "compare":
                left = spec.get("left")
                right = spec.get("right")
                op = spec.get("op")
                if (isinstance(left, str) and left and
                    isinstance(right, str) and right and
                    isinstance(op, str) and op):
                    if use_exists:
                        exists_specs.append(spec)
                    else:
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
                    if use_exists:
                        exists_specs.append(spec)
                    else:
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
                    if use_exists:
                        exists_specs.append(spec)
                    else:
                        aggregate_selects.append(
                            agg_conditional_range(col, when_column, when_op, when_value, min_val, max_val, rule_id, self.DIALECT)
                        )
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "custom_agg":
                # Custom rule with to_sql_agg() and optional to_sql_exists()
                sql_agg = spec.get("sql_agg", {})
                sql_exists = spec.get("sql_exists", {})

                # Try exact dialect match first, then fallback for sqlserver/mssql naming
                agg_expr = sql_agg.get(self.DIALECT)
                if not agg_expr and self.DIALECT == "sqlserver":
                    agg_expr = sql_agg.get("mssql")  # Fallback: mssql -> sqlserver

                exists_expr = sql_exists.get(self.DIALECT)
                if not exists_expr and self.DIALECT == "sqlserver":
                    exists_expr = sql_exists.get("mssql")  # Fallback: mssql -> sqlserver

                if agg_expr:
                    # If user provided to_sql_exists() and tally=False, use EXISTS
                    if use_exists and exists_expr:
                        exists_specs.append(spec)
                    else:
                        # Fall back to aggregate (COUNT) query
                        aggregate_selects.append(f'{agg_expr} AS "{rule_id}"')
                        aggregate_specs.append(spec)
                    supported_specs.append(spec)

        return {
            "exists_specs": exists_specs,
            "aggregate_selects": aggregate_selects,
            "aggregate_specs": aggregate_specs,
            "custom_sql_specs": custom_sql_specs,
            "supported_specs": supported_specs,
        }

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: Dict[str, Any],
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute the compiled plan in three phases.

        Phase 1: EXISTS checks (fast, early-terminate for tally=False)
        Phase 2: Aggregate query for most rules (batched)
        Phase 3: Custom SQL queries (each executed individually)

        Returns:
            {"results": [...], "staging": None}
        """
        exists_specs = compiled_plan.get("exists_specs", [])
        aggregate_selects = compiled_plan.get("aggregate_selects", [])
        custom_sql_specs = compiled_plan.get("custom_sql_specs", [])

        if not exists_specs and not aggregate_selects and not custom_sql_specs:
            return {"results": [], "staging": None}

        table = self._get_table_reference(handle)
        results: List[Dict[str, Any]] = []

        # Build rule_kinds mapping from specs
        rule_kinds = {}
        for spec in exists_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in compiled_plan.get("aggregate_specs", []):
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in custom_sql_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")

        with self._get_connection_ctx(handle) as conn:
            cursor = self._get_cursor(conn)
            try:
                # Phase 1: EXISTS checks (early termination for tally=False)
                if exists_specs:
                    exists_exprs = []
                    for spec in exists_specs:
                        kind = spec.get("kind")
                        rid = spec.get("rule_id")

                        if kind == "not_null":
                            exists_exprs.append(exists_not_null(spec["column"], rid, table, self.DIALECT))
                        elif kind == "unique":
                            exists_exprs.append(exists_unique(spec["column"], rid, table, self.DIALECT))
                        elif kind == "allowed_values":
                            exists_exprs.append(exists_allowed_values(spec["column"], spec["values"], table, rid, self.DIALECT))
                        elif kind == "disallowed_values":
                            exists_exprs.append(exists_disallowed_values(spec["column"], spec["values"], table, rid, self.DIALECT))
                        elif kind == "range":
                            exists_exprs.append(exists_range(spec["column"], spec.get("min"), spec.get("max"), table, rid, self.DIALECT))
                        elif kind == "length":
                            exists_exprs.append(exists_length(spec["column"], spec.get("min"), spec.get("max"), table, rid, self.DIALECT))
                        elif kind == "regex":
                            exists_exprs.append(exists_regex(spec["column"], spec["pattern"], table, rid, self.DIALECT))
                        elif kind == "contains":
                            exists_exprs.append(exists_contains(spec["column"], spec["substring"], table, rid, self.DIALECT))
                        elif kind == "starts_with":
                            exists_exprs.append(exists_starts_with(spec["column"], spec["prefix"], table, rid, self.DIALECT))
                        elif kind == "ends_with":
                            exists_exprs.append(exists_ends_with(spec["column"], spec["suffix"], table, rid, self.DIALECT))
                        elif kind == "compare":
                            exists_exprs.append(exists_compare(spec["left"], spec["right"], spec["op"], table, rid, self.DIALECT))
                        elif kind == "conditional_not_null":
                            exists_exprs.append(exists_conditional_not_null(
                                spec["column"], spec["when_column"], spec["when_op"],
                                spec.get("when_value"), table, rid, self.DIALECT
                            ))
                        elif kind == "conditional_range":
                            exists_exprs.append(exists_conditional_range(
                                spec["column"], spec["when_column"], spec["when_op"],
                                spec.get("when_value"), spec.get("min"), spec.get("max"),
                                table, rid, self.DIALECT
                            ))
                        elif kind == "custom_agg":
                            # Custom rule with to_sql_exists() - user-provided WHERE condition
                            sql_exists = spec.get("sql_exists", {})
                            exists_condition = sql_exists.get(self.DIALECT)
                            if not exists_condition and self.DIALECT == "sqlserver":
                                exists_condition = sql_exists.get("mssql")
                            if exists_condition:
                                exists_exprs.append(exists_custom(exists_condition, table, rid, self.DIALECT))

                    if exists_exprs:
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

                # Phase 3: Custom SQL queries (executed individually)
                if custom_sql_specs:
                    custom_results = self._execute_custom_sql_queries(
                        cursor, handle, custom_sql_specs
                    )
                    results.extend(custom_results)
            finally:
                self._close_cursor(cursor)

        return {"results": results, "staging": None}

    def _execute_custom_sql_queries(
        self,
        cursor,
        handle: DatasetHandle,
        custom_sql_specs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Execute custom SQL queries (Phase 3).

        Each custom_sql_check query is transformed to return a COUNT(*) and executed.
        The user writes a query that selects "violation rows", and we count them.

        Transformation strategy:
        - Simple SELECT: Rewrite to COUNT(*) directly
        - DISTINCT/GROUP BY/LIMIT: Wrap in SELECT COUNT(*) FROM (...) AS _v
        """
        results: List[Dict[str, Any]] = []

        # Get schema and table for placeholder replacement
        try:
            schema, table_name = self._get_schema_and_table(handle)
        except NotImplementedError:
            # Fallback: extract from full table reference
            _logger.warning("_get_schema_and_table not implemented, custom SQL skipped")
            return results

        for spec in custom_sql_specs:
            rule_id = spec["rule_id"]
            user_sql = spec.get("sql", "")

            try:
                # Step 1: Replace {table} placeholder with properly formatted table reference
                formatted_sql = replace_table_placeholder(
                    sql=user_sql,
                    schema=schema,
                    table=table_name,
                    dialect=self.DIALECT,
                )

                # Step 2: Transform to COUNT(*) query
                success, count_sql = to_count_query(formatted_sql, dialect=self.DIALECT)
                if not success:
                    raise ValueError(f"Failed to transform SQL: {count_sql}")

                # Step 3: Execute and read the count
                cursor.execute(count_sql)
                row = cursor.fetchone()

                if row is None or len(row) < 1:
                    raise ValueError("Query returned no result")

                failed_count = int(row[0]) if row[0] is not None else 0
                threshold = spec.get("params", {}).get("threshold", 0)

                passed = failed_count <= threshold
                results.append({
                    "rule_id": rule_id,
                    "passed": passed,
                    "failed_count": failed_count,
                    "message": "Passed" if passed else f"Custom SQL check failed for {failed_count} rows (threshold: {threshold})",
                    "execution_source": self.DIALECT,
                })

            except Exception as e:
                _logger.warning(f"Custom SQL execution failed for '{rule_id}': {e}")
                results.append({
                    "rule_id": rule_id,
                    "passed": False,
                    "failed_count": 1,  # Unknown, but at least 1 issue
                    "message": f"Custom SQL execution failed: {e}",
                    "execution_source": self.DIALECT,
                })

        return results

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
