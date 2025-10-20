# src/kontra/engine/executors/duckdb_sql.py
from __future__ import annotations

from typing import Any, Dict, List

# --- Kontra Imports ---
from kontra.engine.backends.duckdb_session import create_duckdb_connection
from kontra.engine.backends.duckdb_utils import (
    esc_ident,
    lit_str,
)
from kontra.connectors.handle import DatasetHandle

from .base import SqlExecutor
from .registry import register_executor

# ------------------------------- Helpers --------------------------------------
# These helpers are pure, stateless, and specific to compiling DuckDB SQL.


def _agg_not_null(col: str, rule_id: str) -> str:
    # Failures = count of NULLs
    return (
        f"SUM(CASE WHEN {esc_ident(col)} IS NULL THEN 1 ELSE 0 END) "
        f"AS {esc_ident(rule_id)}"
    )


def _agg_min_rows(n: int, rule_id: str) -> str:
    # Failures = max(0, required - actual)
    return f"GREATEST(0, {int(n)} - COUNT(*)) AS {esc_ident(rule_id)}"


def _agg_max_rows(n: int, rule_id: str) -> str:
    # Failures = max(0, actual - allowed)
    return f"GREATEST(0, COUNT(*) - {int(n)}) AS {esc_ident(rule_id)}"


def _assemble_single_row(selects: List[str]) -> str:
    """
    Compose N single-aggregate SELECTs into one row via CROSS JOIN of CTEs.
    Each SELECT reads from the _data view (defined at runtime).
    """
    if not selects:
        return "SELECT 0 AS __no_sql_rules__ LIMIT 1;"
    ctes, aliases = [], []
    for i, sel in enumerate(selects):
        nm = f"a{i}"
        ctes.append(f"{nm} AS (SELECT {sel} FROM _data)")
        aliases.append(nm)
    with_clause = "WITH " + ", ".join(ctes)
    cross = " CROSS JOIN ".join(aliases)
    return f"{with_clause} SELECT * FROM {cross};"


def _results_from_single_row_map(
    values: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Map {rule_id: failed_count} → Kontra-style results."""
    out: List[Dict[str, Any]] = []
    for rule_id, failed in values.items():
        if rule_id == "__no_sql_rules__":
            continue
        failed_count = int(failed) if failed is not None else 0
        out.append(
            {
                "rule_id": rule_id,
                "passed": failed_count == 0,
                "failed_count": failed_count,
                "message": "Passed" if failed_count == 0 else "Failed",
                "severity": "ERROR",
                "actions_executed": [],
            }
        )
    return out


# --------------------------- DuckDB SQL Executor ------------------------------


@register_executor("duckdb")
class DuckDBSqlExecutor:
    """
    DuckDB-based SQL pushdown executor.

    Scope (safe v1):
      - not_null(column)
      - min_rows(threshold)
      - max_rows(threshold)

    This class focuses solely on compiling/executing aggregate SQL to compute
    failure counts. Materialization/projection is handled by Materializers.
    """

    name = "duckdb"

    # --- Executor Protocol Implementation ---

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        """
        Check if we support the handle's scheme and at least one rule.
        """
        # 1. Check if the data source URI scheme is supported
        scheme = handle.scheme
        supported_schemes = ("s3", "http", "https", "file", "")
        if scheme not in supported_schemes:
            return False

        # 2. Check if we support at least one of the provided rule kinds
        supported_kinds = {"not_null", "min_rows", "max_rows"}
        return any(
            (spec.get("kind") in supported_kinds) for spec in (sql_specs or [])
        )

    # -------------------- Compile / Execute -----------------------------------

    def compile(self, sql_specs: List[Dict[str, Any]]) -> str:
        """Build a single-row SELECT with aggregates per supported rule."""
        selects: List[str] = []
        for spec in sql_specs or []:
            kind = spec.get("kind")
            rid = spec.get("rule_id")
            if not (kind and rid):
                continue

            if kind == "not_null":
                col = spec.get("column")
                if isinstance(col, str) and col:
                    selects.append(_agg_not_null(col, rid))

            elif kind == "min_rows":
                selects.append(
                    _agg_min_rows(int(spec.get("threshold", 0)), rid)
                )

            elif kind == "max_rows":
                selects.append(
                    _agg_max_rows(int(spec.get("threshold", 0)), rid)
                )

        return _assemble_single_row(selects)

    def execute(
        self, handle: DatasetHandle, compiled_sql: str
    ) -> Dict[str, Any]:
        """
        Execute the compiled SQL against the source.
        The handle provides the URI and all necessary I/O options.

        Returns:
            {"results": [...]}
        """
        con = create_duckdb_connection(handle)

        # Create a view on the source file.
        # TODO: This should be format-aware
        con.execute(
            f"CREATE OR REPLACE VIEW _data AS "
            f"SELECT * FROM read_parquet({lit_str(handle.uri)})"
        )
        cur = con.execute(compiled_sql)
        row = cur.fetchone()
        if row is None:
            return {"results": []}

        cols = [d[0] for d in cur.description] if cur.description else []
        mapping = {c: row[i] for i, c in enumerate(cols)}
        return {"results": _results_from_single_row_map(mapping)}

    # -------------------- Introspection ---------------------------------------

    def introspect(self, handle: DatasetHandle) -> Dict[str, Any]:
        """
        Lightweight introspection to get row count and column names.
        """
        con = create_duckdb_connection(handle)

        # TODO: This should be format-aware
        row_count_result = con.execute(
            "SELECT COUNT(*) AS n FROM read_parquet(?)", [handle.uri]
        ).fetchone()
        n = row_count_result[0] if row_count_result else 0

        cur = con.execute(
            f"SELECT * FROM read_parquet({lit_str(handle.uri)}) LIMIT 0"
        )
        cols = [d[0] for d in cur.description] if cur.description else []

        return {"row_count": int(n), "available_cols": cols}