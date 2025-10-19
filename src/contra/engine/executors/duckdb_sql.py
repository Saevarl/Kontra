from __future__ import annotations
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import duckdb

# We keep this executor self-contained; engine passes fs_opts from the handle.


# ------------------------------- Helpers --------------------------------------

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
        ctes.append(f'{nm} AS (SELECT {sel} FROM _data)')
        aliases.append(nm)
    with_clause = "WITH " + ", ".join(ctes)
    cross = " CROSS JOIN ".join(aliases)
    return f"{with_clause} SELECT * FROM {cross};"


def _results_from_single_row_map(values: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Map {rule_id: failed_count} → Contra-style results."""
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

def _safe_set(con: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    try:
        con.execute("SET " + key + " = ?", [value])
    except Exception:
        # Keep portable across DuckDB versions
        pass


def _configure_httpfs_from_opts(con: duckdb.DuckDBPyConnection, fs_opts: Optional[Dict[str, str]]) -> None:
    """
    Apply S3/MinIO-style options to the DuckDB httpfs session.

    Supported keys in fs_opts (all optional):
      - s3_endpoint         (e.g., "localhost:9000")
      - s3_region           (e.g., "us-east-1")
      - s3_url_style        ("path" | "host")
      - s3_use_ssl          ("true" | "false")
      - s3_access_key_id
      - s3_secret_access_key
      - s3_session_token
      - s3_max_connections  (string int, e.g., "64")
      - s3_request_timeout_ms (string int, e.g., "60000")
    """
    if not fs_opts:
        fs_opts = {}

    # Ensure httpfs is present
    try:
        con.execute("INSTALL httpfs;")
    except Exception:
        pass
    try:
        con.execute("LOAD httpfs;")
    except Exception:
        pass

    # Small perf nicety
    _safe_set(con, "enable_object_cache", "true")

    # Credentials (do not log values)
    if fs_opts.get("s3_access_key_id"):
        _safe_set(con, "s3_access_key_id", fs_opts["s3_access_key_id"])
    if fs_opts.get("s3_secret_access_key"):
        _safe_set(con, "s3_secret_access_key", fs_opts["s3_secret_access_key"])
    if fs_opts.get("s3_session_token"):
        _safe_set(con, "s3_session_token", fs_opts["s3_session_token"])

    # Region (harmless for MinIO; important for AWS)
    if fs_opts.get("s3_region"):
        _safe_set(con, "s3_region", fs_opts["s3_region"])

    # Endpoint / style / SSL — this is the crucial bit for MinIO
    endpoint = fs_opts.get("s3_endpoint")
    url_style = fs_opts.get("s3_url_style")
    use_ssl = fs_opts.get("s3_use_ssl")

    if endpoint:
        # Accept raw "host:port" or full URL; store just host:port
        parsed = urlparse(endpoint)
        hostport = parsed.netloc or parsed.path or endpoint
        _safe_set(con, "s3_endpoint", hostport)

        if use_ssl is not None:
            _safe_set(con, "s3_use_ssl", use_ssl)
        else:
            # If the user passed a full URL, infer from scheme; otherwise default to false for MinIO
            _safe_set(con, "s3_use_ssl", "true" if parsed.scheme == "https" else "false")

        # Default to path-style for MinIO unless explicitly overridden
        if not url_style:
            url_style = "path"

    if url_style:
        _safe_set(con, "s3_url_style", url_style)

    # Conservative defaults
    _safe_set(con, "s3_max_connections", fs_opts.get("s3_max_connections", "64"))
    _safe_set(con, "s3_request_timeout_ms", fs_opts.get("s3_request_timeout_ms", "60000"))


def _new_connection_for_source(source_uri: str, fs_opts: Optional[Dict[str, str]]) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection configured for the source and fs_opts.
    """
    con = duckdb.connect()
    _configure_httpfs_from_opts(con, fs_opts)
    # Threads (optional; keep guarded for portability)
    try:
        import os
        nthreads = int(os.getenv("DUCKDB_THREADS") or (os.cpu_count() or 4))
        try:
            con.execute(f"PRAGMA threads={nthreads};")
        except Exception:
            con.execute(f"SET threads = {nthreads};")
    except Exception:
        pass
    return con


# --------------------------- DuckDB SQL Executor ------------------------------

class DuckDBSqlExecutor:
    """
    DuckDB-based SQL pushdown executor.

    Scope (safe v1):
      - not_null(column)
      - min_rows(threshold)
      - max_rows(threshold)

    This class focuses solely on compiling/executing aggregate SQL to compute
    failure counts. Materialization/projection is handled elsewhere.
    """

    name = "duckdb"

    # -------------------- Capability-ish checks --------------------------------

    @staticmethod
    def supports_source(source_uri: str) -> bool:
        """
        DuckDB can read Parquet from:
          - s3:// (S3 or S3-compatible via httpfs)
          - http(s):// (httpfs)
          - local files (file:// or bare paths)
        """
        scheme = (urlparse(source_uri).scheme or "").lower()
        return scheme in ("s3", "http", "https", "file", "")

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
                selects.append(_agg_min_rows(int(spec.get("threshold", 0)), rid))

            elif kind == "max_rows":
                selects.append(_agg_max_rows(int(spec.get("threshold", 0)), rid))

        return _assemble_single_row(selects)

    def execute(self, source_uri: str, compiled_sql: str, *, io_opts: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Execute the compiled SQL against the source. io_opts carries fs/session
        options (e.g., MinIO endpoint). Returns {"results": [...]}.
        """
        con = _new_connection_for_source(source_uri, io_opts)
        # DDL not parameterizable; bind via literal
        con.execute(f"CREATE OR REPLACE VIEW _data AS SELECT * FROM read_parquet({_lit_str(source_uri)})")
        cur = con.execute(compiled_sql)
        row = cur.fetchone()
        if row is None:
            return {"results": []}
        cols = [d[0] for d in cur.description] if cur.description else []
        mapping = {c: row[i] for i, c in enumerate(cols)}
        return {"results": _results_from_single_row_map(mapping)}

    # -------------------- Introspection ---------------------------------------

    def introspect(self, source_uri: str, *, io_opts: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        con = _new_connection_for_source(source_uri, io_opts)
        n = con.execute("SELECT COUNT(*) AS n FROM read_parquet(?)", [source_uri]).fetchone()[0]
        cur = con.execute(f"SELECT * FROM read_parquet({_lit_str(source_uri)}) LIMIT 0")
        cols = [d[0] for d in cur.description] if cur.description else []
        return {"row_count": int(n), "available_cols": cols}
