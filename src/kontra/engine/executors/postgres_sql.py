# src/kontra/engine/executors/postgres_sql.py
"""
PostgreSQL SQL Executor - executes validation rules via SQL pushdown.

Two-phase execution for optimal performance:
  Phase 1: EXISTS checks for not_null rules (can early-terminate)
  Phase 2: Aggregate query for remaining rules

Supports rules:
  - not_null(column) - uses EXISTS (fast early termination)
  - unique(column) - uses COUNT DISTINCT
  - min_rows(threshold) - uses COUNT
  - max_rows(threshold) - uses COUNT
  - allowed_values(column, values) - uses SUM CASE
  - freshness(column, max_age) - uses MAX
  - range(column, min, max) - uses SUM CASE
  - regex(column, pattern) - uses ~ operator
"""

from __future__ import annotations

from typing import Any, Dict, List

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference, get_default_schema, POSTGRESQL
from contextlib import contextmanager
from kontra.engine.sql_utils import (
    esc_ident,
    agg_not_null,
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
)

from .base import SqlExecutor
from .registry import register_executor


# Dialect constant for this executor
DIALECT = "postgres"


def _esc(name: str) -> str:
    """Escape a PostgreSQL identifier using shared utility."""
    return esc_ident(name, DIALECT)


def _assemble_single_row(selects: List[str], table: str) -> str:
    """Build a single-row aggregate query from multiple SELECT expressions."""
    if not selects:
        return "SELECT 0 AS __no_sql_rules__ LIMIT 1;"

    # For PostgreSQL, we can use a single SELECT with multiple aggregates
    return f"SELECT {', '.join(selects)} FROM {table};"


def _assemble_exists_query(exists_exprs: List[str]) -> str:
    """Build a query with multiple EXISTS checks (Phase 1)."""
    if not exists_exprs:
        return ""
    return f"SELECT {', '.join(exists_exprs)};"


@contextmanager
def _get_connection_ctx(handle: DatasetHandle):
    """
    Get a connection context for either BYOC or URI-based handles.

    For BYOC, yields the external connection directly (not owned by us).
    For URI-based, yields a new connection (owned by context manager).
    """
    if handle.scheme == "byoc" and handle.external_conn is not None:
        # BYOC: yield external connection directly, don't close it
        yield handle.external_conn
    elif handle.db_params:
        # URI-based: use our connection manager
        with get_connection(handle.db_params) as conn:
            yield conn
    else:
        raise ValueError("Handle has neither external_conn nor db_params")


def _get_table_reference(handle: DatasetHandle) -> str:
    """
    Get the fully-qualified table reference for a handle.

    Returns: "schema.table" format for PostgreSQL.
    """
    if handle.scheme == "byoc" and handle.table_ref:
        # BYOC: parse table_ref
        _db, schema, table = parse_table_reference(handle.table_ref)
        schema = schema or get_default_schema(POSTGRESQL)
        return f"{_esc(schema)}.{_esc(table)}"
    elif handle.db_params:
        # URI-based: use db_params
        params: PostgresConnectionParams = handle.db_params
        return f"{_esc(params.schema)}.{_esc(params.table)}"
    else:
        raise ValueError("Handle has neither table_ref nor db_params")


@register_executor("postgres")
class PostgresSqlExecutor(SqlExecutor):
    """
    PostgreSQL SQL pushdown executor.

    Supports:
      - not_null(column)
      - unique(column)
      - min_rows(threshold)
      - max_rows(threshold)
      - allowed_values(column, values)
    """

    name = "postgres"

    SUPPORTED_RULES = {"not_null", "unique", "min_rows", "max_rows", "allowed_values", "freshness", "range", "regex", "compare", "conditional_not_null", "conditional_range"}

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        """Check if this executor can handle the given handle and rules."""
        scheme = (handle.scheme or "").lower()

        # BYOC: check dialect for external connections
        if scheme == "byoc" and handle.dialect == "postgresql":
            # Must have external connection
            if handle.external_conn is None:
                return False
            # Must have at least one supported rule
            return any(
                s.get("kind") in self.SUPPORTED_RULES
                for s in (sql_specs or [])
            )

        # URI-based: handle postgres:// URIs
        if scheme not in {"postgres", "postgresql"}:
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

            if kind == "not_null":
                # Phase 1: Use EXISTS for not_null (faster in all cases)
                col = spec.get("column")
                if isinstance(col, str) and col:
                    exists_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "unique":
                # Phase 2: COUNT DISTINCT is faster than EXISTS GROUP BY
                col = spec.get("column")
                if isinstance(col, str) and col:
                    aggregate_selects.append(agg_unique(col, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "min_rows":
                # Phase 2: Need COUNT
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_min_rows(int(threshold), rule_id, DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "max_rows":
                # Phase 2: Need COUNT
                threshold = spec.get("threshold", 0)
                aggregate_selects.append(agg_max_rows(int(threshold), rule_id, DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "allowed_values":
                # Phase 2: SUM CASE is faster than EXISTS NOT IN
                col = spec.get("column")
                values = spec.get("values", [])
                if isinstance(col, str) and col and values:
                    aggregate_selects.append(agg_allowed_values(col, values, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "freshness":
                # Phase 2: Need MAX
                col = spec.get("column")
                max_age_seconds = spec.get("max_age_seconds")
                if isinstance(col, str) and col and isinstance(max_age_seconds, int):
                    aggregate_selects.append(agg_freshness(col, max_age_seconds, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "range":
                # Phase 2: SUM CASE for range check
                col = spec.get("column")
                min_val = spec.get("min")
                max_val = spec.get("max")
                if isinstance(col, str) and col and (min_val is not None or max_val is not None):
                    aggregate_selects.append(agg_range(col, min_val, max_val, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "regex":
                # Phase 2: Regex matching with ~ operator
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    aggregate_selects.append(agg_regex(col, pattern, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "compare":
                # Phase 2: Compare two columns
                left = spec.get("left")
                right = spec.get("right")
                op = spec.get("op")
                if (isinstance(left, str) and left and
                    isinstance(right, str) and right and
                    isinstance(op, str) and op):
                    aggregate_selects.append(agg_compare(left, right, op, rule_id, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_not_null":
                # Phase 2: Check column is not null when condition is met
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")  # Can be None
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op):
                    aggregate_selects.append(
                        agg_conditional_not_null(col, when_column, when_op, when_value, rule_id, DIALECT)
                    )
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_range":
                # Phase 2: Check column is in range when condition is met
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")  # Can be None
                min_val = spec.get("min")
                max_val = spec.get("max")
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op and
                    (min_val is not None or max_val is not None)):
                    aggregate_selects.append(
                        agg_conditional_range(col, when_column, when_op, when_value, min_val, max_val, rule_id, DIALECT)
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
        Execute the compiled plan against PostgreSQL in two phases.

        Phase 1: EXISTS checks for not_null (fast, can early-terminate)
        Phase 2: Aggregate query for remaining rules

        Supports both URI-based connections (handle.db_params) and
        BYOC connections (handle.external_conn).

        Returns:
            {"results": [...]}
        """
        exists_specs = compiled_plan.get("exists_specs", [])
        aggregate_selects = compiled_plan.get("aggregate_selects", [])

        if not exists_specs and not aggregate_selects:
            return {"results": [], "staging": None}

        table = _get_table_reference(handle)
        results: List[Dict[str, Any]] = []

        # Build rule_kinds mapping from specs
        rule_kinds = {}
        for spec in exists_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in compiled_plan.get("aggregate_specs", []):
            rule_kinds[spec["rule_id"]] = spec.get("kind")

        with _get_connection_ctx(handle) as conn:
            with conn.cursor() as cur:
                # Phase 1: EXISTS checks for not_null rules
                if exists_specs:
                    exists_exprs = [
                        exists_not_null(
                            spec["column"],
                            spec["rule_id"],
                            table,
                            DIALECT
                        )
                        for spec in exists_specs
                    ]
                    exists_sql = _assemble_exists_query(exists_exprs)
                    cur.execute(exists_sql)
                    row = cur.fetchone()
                    columns = [desc[0] for desc in cur.description] if cur.description else []

                    if row and columns:
                        exists_results = results_from_row(columns, row, is_exists=True, rule_kinds=rule_kinds)
                        results.extend(exists_results)

                # Phase 2: Aggregate query for remaining rules
                if aggregate_selects:
                    agg_sql = _assemble_single_row(aggregate_selects, table)
                    cur.execute(agg_sql)
                    row = cur.fetchone()
                    columns = [desc[0] for desc in cur.description] if cur.description else []

                    if row and columns:
                        agg_results = results_from_row(columns, row, is_exists=False, rule_kinds=rule_kinds)
                        results.extend(agg_results)

        return {"results": results, "staging": None}

    def introspect(self, handle: DatasetHandle, **kwargs) -> Dict[str, Any]:
        """
        Introspect the PostgreSQL table for metadata.

        Supports both URI-based connections (handle.db_params) and
        BYOC connections (handle.external_conn).

        Returns:
            {"row_count": int, "available_cols": [...]}
        """
        table = _get_table_reference(handle)

        # Get schema and table name for information_schema query
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table_name = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(POSTGRESQL)
        elif handle.db_params:
            params: PostgresConnectionParams = handle.db_params
            schema = params.schema
            table_name = params.table
        else:
            raise ValueError("Handle has neither table_ref nor db_params")

        with _get_connection_ctx(handle) as conn:
            with conn.cursor() as cur:
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()
                n = int(row_count[0]) if row_count else 0

                # Get column names
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table_name),
                )
                cols = [row[0] for row in cur.fetchall()]

        return {"row_count": n, "available_cols": cols, "staging": None}
