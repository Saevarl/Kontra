# src/kontra/engine/executors/duckdb_sql.py
from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

# --- Kontra Imports ---
from kontra.backends.duckdb_session import create_duckdb_connection
from kontra.connectors.handle import DatasetHandle

# ------------------------------- Helpers --------------------------------------
# These helpers are pure, stateless, and specific to compiling DuckDB SQL.


def _esc_ident(name: str) -> str:
    """Quote an identifier for DuckDB (double quotes, escape internal quotes)."""
    return '"' + name.replace('"', '""') + '"'


def _lit_str(s: str) -> str:
    """Return a single-quoted SQL string literal with internal quotes escaped."""
    return "'" + s.replace("'", "''") + "'"


def _agg_not_null(col: str, rule_id: str) -> str:
    # Failures = count of NULLs
    return f"SUM(CASE WHEN {_esc_ident(col)} IS NULL THEN 1 ELSE 0 END) AS {_esc_ident(rule_id)}"


def _agg_min_rows(n: int, rule_id: str) -> str:
    # Failures = max(0, required - actual)
    return f"GREATEST(0, {int(n)} - COUNT(*)) AS {_esc_ident(rule_id)}"


def _agg_max_rows(n: int, rule_id: str) -> str:
    # Failures = max(0, actual - allowed)
    return f"GREATEST(0, COUNT(*) - {int(n)}) AS {_esc_ident(rule_id)}"


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


# -------------------------- Session configuration -----------------------------

# All session configuration helpers (_safe_set, _configure_httpfs_from_opts,
# _new_connection_for_source) have been REMOVED.
#
# All connection logic is now centralized in:
#   kontra.backends.duckdb_session.create_duckdb_connection
#

# --------------------------- DuckDB SQL Executor ------------------------------


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

    # -------------------- Capability-ish checks --------------------------------
    # TODO (Phase 2): Move this logic into a `supports` method for the registry.

    @staticmethod
    def supports_source(source_uri: str) -> bool:
        """
        DuckDB can read Parquet from:
          - s3:// (S3 or S3-compatible via httpfs)
          - http(s):// (httpfs)
          - local files (file:// or bare paths)
        """
        scheme = (urlparse(source_uri).scheme or "").lower()
        return scheme in ("s3", "http", "httpss", "file", "")

    @staticmethod
    def supports_rules(sql_specs: List[Dict[str, Any]]) -> bool:
        supported = {"not_null", "min_rows", "max_rows"}
        return any((spec.get("kind") in supported) for spec in (sql_specs or []))

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
        # Get a connection configured for this specific handle
        con = create_duckdb_connection(handle)

        # Create a view on the source file.
        # DDL is not parameterizable; bind URI via f-string literal.
        con.execute(
            f"CREATE OR REPLACE VIEW _data AS "
            f"SELECT * FROM read_parquet({_lit_str(handle.uri)})"
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

        # Get row count
        # Note: Using handle.uri as a parameter is safe
        row_count_result = con.execute(
            "SELECT COUNT(*) AS n FROM read_parquet(?)", [handle.uri]
        ).fetchone()
        n = row_count_result[0] if row_count_result else 0

        # Get column names
        cur = con.execute(
            f"SELECT * FROM read_parquet({_lit_str(handle.uri)}) LIMIT 0"
        )
        cols = [d[0] for d in cur.description] if cur.description else []

        return {"row_count": int(n), "available_cols": cols}