# src/kontra/engine/executors/sqlserver_sql.py
"""
SQL Server SQL Executor - executes validation rules via SQL pushdown.

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
  - regex(column, pattern) - uses PATINDEX (limited regex)
"""

from __future__ import annotations

from typing import Any, Dict, List

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection
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
    exists_not_null,
    results_from_row,
)

from .base import SqlExecutor
from .registry import register_executor


# Dialect constant for this executor
DIALECT = "sqlserver"


def _esc(name: str) -> str:
    """Escape a SQL Server identifier using shared utility."""
    return esc_ident(name, DIALECT)


def _assemble_single_row(selects: List[str], table: str) -> str:
    """Build a single-row aggregate query from multiple SELECT expressions."""
    if not selects:
        return "SELECT 0 AS __no_sql_rules__;"

    return f"SELECT {', '.join(selects)} FROM {table};"


def _assemble_exists_query(exists_exprs: List[str]) -> str:
    """Build a query with multiple EXISTS checks (Phase 1)."""
    if not exists_exprs:
        return ""
    return f"SELECT {', '.join(exists_exprs)};"


@register_executor("sqlserver")
class SqlServerSqlExecutor(SqlExecutor):
    """
    SQL Server SQL pushdown executor.

    Supports:
      - not_null(column)
      - unique(column)
      - min_rows(threshold)
      - max_rows(threshold)
      - allowed_values(column, values)
      - freshness(column, max_age_seconds)
    """

    name = "sqlserver"

    SUPPORTED_RULES = {"not_null", "unique", "min_rows", "max_rows", "allowed_values", "freshness", "range", "regex"}

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        """Check if this executor can handle the given handle and rules."""
        # Only handle mssql:// or sqlserver:// URIs
        scheme = (handle.scheme or "").lower()
        if scheme not in {"mssql", "sqlserver"}:
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
                # Phase 2: Pattern matching with PATINDEX (limited regex)
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    aggregate_selects.append(agg_regex(col, pattern, rule_id, DIALECT))
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
        Execute the compiled plan against SQL Server in two phases.

        Phase 1: EXISTS checks for not_null (fast, can early-terminate)
        Phase 2: Aggregate query for remaining rules

        Returns:
            {"results": [...]}
        """
        if not handle.db_params:
            raise ValueError("SQL Server handle missing db_params")

        params: SqlServerConnectionParams = handle.db_params
        exists_specs = compiled_plan.get("exists_specs", [])
        aggregate_selects = compiled_plan.get("aggregate_selects", [])

        if not exists_specs and not aggregate_selects:
            return {"results": [], "staging": None}

        table = f"{_esc(params.schema)}.{_esc(params.table)}"
        results: List[Dict[str, Any]] = []

        # Build rule_kinds mapping from specs
        rule_kinds = {}
        for spec in exists_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in compiled_plan.get("aggregate_specs", []):
            rule_kinds[spec["rule_id"]] = spec.get("kind")

        with get_connection(params) as conn:
            cursor = conn.cursor()

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
                cursor.execute(exists_sql)
                row = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                if row and columns:
                    exists_results = results_from_row(columns, row, is_exists=True, rule_kinds=rule_kinds)
                    results.extend(exists_results)

            # Phase 2: Aggregate query for remaining rules
            if aggregate_selects:
                agg_sql = _assemble_single_row(aggregate_selects, table)
                cursor.execute(agg_sql)
                row = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                if row and columns:
                    agg_results = results_from_row(columns, row, is_exists=False, rule_kinds=rule_kinds)
                    results.extend(agg_results)

        return {"results": results, "staging": None}

    def introspect(self, handle: DatasetHandle, **kwargs) -> Dict[str, Any]:
        """
        Introspect the SQL Server table for metadata.

        Returns:
            {"row_count": int, "available_cols": [...]}
        """
        if not handle.db_params:
            raise ValueError("SQL Server handle missing db_params")

        params: SqlServerConnectionParams = handle.db_params

        with get_connection(params) as conn:
            cursor = conn.cursor()

            # Get row count
            table = f"{_esc(params.schema)}.{_esc(params.table)}"
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()
            n = int(row_count[0]) if row_count else 0

            # Get column names
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (params.schema, params.table),
            )
            cols = [row[0] for row in cursor.fetchall()]

        return {"row_count": n, "available_cols": cols, "staging": None}
