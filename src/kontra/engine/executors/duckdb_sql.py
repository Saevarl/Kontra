from __future__ import annotations

"""
DuckDB SQL Executor — format-aware with reliable CSV→Parquet staging.

- Parquet sources: read_parquet(...)
- CSV sources:
    csv_mode=auto    → try read_csv_auto(...); on failure stage to Parquet
    csv_mode=duckdb  → read_csv_auto(...) only (propagate errors)
    csv_mode=parquet → always stage CSV→Parquet via DuckDB COPY (forced execution)

Executor computes aggregate failure counts for SQL-capable rules and exposes
light introspection. The engine may reuse staged Parquet for materialization
to avoid a second CSV parse.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

# --- Kontra Imports ---
from kontra.engine.backends.duckdb_session import create_duckdb_connection
from kontra.engine.backends.duckdb_utils import esc_ident, lit_str
from kontra.connectors.handle import DatasetHandle

# Optional: s3fs + polars for fallback when DuckDB httpfs fails
try:
    import s3fs
    import polars as pl
    _HAS_S3FS = True
except ImportError:
    _HAS_S3FS = False

from .base import SqlExecutor
from .registry import register_executor


# ------------------------------- CSV helpers -------------------------------- #

def _is_csv(handle: DatasetHandle) -> bool:
    fmt = (getattr(handle, "format", "") or "").lower()
    if fmt:
        return fmt == "csv"
    uri = (handle.uri or "").lower().split("?", 1)[0]
    return uri.endswith(".csv") or uri.endswith(".csv.gz")


def _install_httpfs(con: duckdb.DuckDBPyConnection, handle: DatasetHandle) -> None:
    scheme = (handle.scheme or "").lower()
    if scheme in {"s3", "http", "https"}:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")


def _stage_csv_to_parquet_with_duckdb(
    con: duckdb.DuckDBPyConnection, source_uri: str
) -> Tuple[str, tempfile.TemporaryDirectory]:
    """
    Force a real CSV scan and Parquet write using DuckDB COPY.

    Returns:
        (parquet_path, tmpdir) — tmpdir MUST be kept alive by the caller.
    """
    tmpdir = tempfile.TemporaryDirectory(prefix="kontra_csv_stage_")
    stage_path = Path(tmpdir.name) / "kontra_stage.parquet"

    # Ensure httpfs is loaded for remote URIs; COPY will stream CSV → Parquet.
    # We explicitly go through a SELECT to allow future CSV options if needed.
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto({lit_str(source_uri)})) "
        f"TO {lit_str(str(stage_path))} (FORMAT PARQUET)"
    )
    return str(stage_path), tmpdir


def _stage_csv_to_parquet_with_s3fs(
    handle: DatasetHandle,
) -> Tuple[str, tempfile.TemporaryDirectory]:
    """
    Fallback: Stage S3 CSV to Parquet using s3fs + Polars.
    Used when DuckDB httpfs fails with connection errors on large files.

    Returns:
        (parquet_path, tmpdir) — tmpdir MUST be kept alive by the caller.
    """
    if not _HAS_S3FS:
        raise ImportError("s3fs and polars required for S3 CSV fallback")

    tmpdir = tempfile.TemporaryDirectory(prefix="kontra_csv_stage_s3fs_")
    stage_path = Path(tmpdir.name) / "kontra_stage.parquet"

    # Build s3fs client from handle's fs_opts
    opts = handle.fs_opts or {}
    s3_kwargs: Dict[str, Any] = {}
    if opts.get("s3_access_key_id") and opts.get("s3_secret_access_key"):
        s3_kwargs["key"] = opts["s3_access_key_id"]
        s3_kwargs["secret"] = opts["s3_secret_access_key"]
    if opts.get("s3_endpoint"):
        endpoint = opts["s3_endpoint"]
        # s3fs expects endpoint_url with scheme
        if not endpoint.startswith(("http://", "https://")):
            # Infer scheme from s3_use_ssl or default to http for custom endpoints
            scheme = "https" if opts.get("s3_use_ssl", "").lower() == "true" else "http"
            endpoint = f"{scheme}://{endpoint}"
        s3_kwargs["endpoint_url"] = endpoint
        # Force path-style for custom endpoints (MinIO)
        s3_kwargs["client_kwargs"] = {"region_name": opts.get("s3_region", "us-east-1")}

    fs = s3fs.S3FileSystem(**s3_kwargs)

    # Strip s3:// prefix for s3fs
    s3_path = handle.uri
    if s3_path.lower().startswith("s3://"):
        s3_path = s3_path[5:]

    # Read CSV with s3fs → Polars → write Parquet
    with fs.open(s3_path, "rb") as f:
        df = pl.read_csv(f)
    df.write_parquet(str(stage_path))

    if os.getenv("KONTRA_VERBOSE"):
        print(f"[INFO] Staged S3 CSV via s3fs+Polars: {handle.uri} → {stage_path}")

    return str(stage_path), tmpdir


def _create_source_view(
    con: duckdb.DuckDBPyConnection,
    handle: DatasetHandle,
    view: str,
    *,
    csv_mode: str = "auto",  # auto | duckdb | parquet
) -> Tuple[Optional[tempfile.TemporaryDirectory], Optional[str], str]:
    """
    Create a DuckDB view named `view` over the dataset (format-aware).

    Returns:
        (owned_tmpdir, staged_parquet_path, mode_used)
    """
    _install_httpfs(con, handle)

    if not _is_csv(handle):
        con.execute(
            f"CREATE OR REPLACE VIEW {esc_ident(view)} AS "
            f"SELECT * FROM read_parquet({lit_str(handle.uri)})"
        )
        return None, None, "parquet"

    mode = (csv_mode or "auto").lower()
    if mode not in {"auto", "duckdb", "parquet"}:
        mode = "auto"

    if mode in {"auto", "duckdb"}:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {esc_ident(view)} AS "
                f"SELECT * FROM read_csv_auto({lit_str(handle.uri)})"
            )
            return None, None, "duckdb"
        except duckdb.Error:
            if mode == "duckdb":
                # Caller asked to use DuckDB CSV strictly; bubble up.
                raise
            con.execute(f"DROP VIEW IF EXISTS {esc_ident(view)}")

    # Explicit staging path (or auto-fallback) using DuckDB COPY
    # For S3 CSV files, DuckDB httpfs can fail with connection errors on large files.
    # In that case, fall back to s3fs + Polars staging.
    try:
        staged_path, tmpdir = _stage_csv_to_parquet_with_duckdb(con, handle.uri)
    except duckdb.Error as e:
        err_str = str(e).lower()
        is_connection_error = (
            "connection error" in err_str
            or "failed to read" in err_str
            or "timeout" in err_str
            or "timed out" in err_str
        )
        is_s3 = (handle.scheme or "").lower() == "s3"

        if is_connection_error and is_s3 and _HAS_S3FS:
            if os.getenv("KONTRA_VERBOSE"):
                print(f"[INFO] DuckDB httpfs failed for S3 CSV, falling back to s3fs+Polars: {e}")
            staged_path, tmpdir = _stage_csv_to_parquet_with_s3fs(handle)
        else:
            raise

    con.execute(
        f"CREATE OR REPLACE VIEW {esc_ident(view)} AS "
        f"SELECT * FROM read_parquet({lit_str(staged_path)})"
    )
    return tmpdir, staged_path, "parquet"


# ------------------------------- SQL helpers -------------------------------- #

def _agg_not_null(col: str, rule_id: str) -> str:
    return (
        f"SUM(CASE WHEN {esc_ident(col)} IS NULL THEN 1 ELSE 0 END) "
        f"AS {esc_ident(rule_id)}"
    )


def _agg_min_rows(n: int, rule_id: str) -> str:
    return f"GREATEST(0, {int(n)} - COUNT(*)) AS {esc_ident(rule_id)}"


def _agg_max_rows(n: int, rule_id: str) -> str:
    return f"GREATEST(0, COUNT(*) - {int(n)}) AS {esc_ident(rule_id)}"


def _agg_freshness(col: str, max_age_seconds: int, rule_id: str) -> str:
    """
    Aggregate expression for freshness rule in DuckDB.

    Returns 0 if data is fresh (MAX(col) >= NOW() - interval), 1 otherwise.
    """
    return (
        f"CASE WHEN MAX({esc_ident(col)}) >= "
        f"(NOW() - INTERVAL '{int(max_age_seconds)} seconds') "
        f"THEN 0 ELSE 1 END AS {esc_ident(rule_id)}"
    )


def _agg_range(col: str, min_val, max_val, rule_id: str) -> str:
    """
    Aggregate expression for range rule in DuckDB.

    Counts values outside [min, max] range. NULLs are counted as failures.
    """
    conditions = []
    if min_val is not None:
        conditions.append(f"{esc_ident(col)} < {min_val}")
    if max_val is not None:
        conditions.append(f"{esc_ident(col)} > {max_val}")

    out_of_range = " OR ".join(conditions) if conditions else "FALSE"

    return (
        f"SUM(CASE WHEN {esc_ident(col)} IS NULL OR ({out_of_range}) THEN 1 ELSE 0 END) "
        f"AS {esc_ident(rule_id)}"
    )


def _agg_regex(col: str, pattern: str, rule_id: str) -> str:
    """
    Aggregate expression for regex rule in DuckDB.

    Counts values that don't match the regex pattern. NULLs are counted as failures.
    Uses DuckDB's regexp_matches() function.
    """
    escaped_pattern = pattern.replace("'", "''")
    return (
        f"SUM(CASE WHEN {esc_ident(col)} IS NULL "
        f"OR NOT regexp_matches(CAST({esc_ident(col)} AS VARCHAR), '{escaped_pattern}') "
        f"THEN 1 ELSE 0 END) AS {esc_ident(rule_id)}"
    )


# SQL comparison operators
_SQL_OP_MAP = {
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "==": "=",
    "!=": "<>",
}


def _agg_compare(left: str, right: str, op: str, rule_id: str) -> str:
    """
    Aggregate expression for compare rule in DuckDB.

    Counts rows where either column is NULL or comparison fails.
    """
    l = esc_ident(left)
    r = esc_ident(right)
    sql_op = _SQL_OP_MAP.get(op, op)
    return (
        f"SUM(CASE WHEN {l} IS NULL OR {r} IS NULL "
        f"OR NOT ({l} {sql_op} {r}) THEN 1 ELSE 0 END) AS {esc_ident(rule_id)}"
    )


def _agg_conditional_not_null(
    column: str, when_column: str, when_op: str, when_value: Any, rule_id: str
) -> str:
    """
    Aggregate expression for conditional_not_null rule in DuckDB.

    Counts rows where condition is TRUE AND column is NULL.
    """
    col = esc_ident(column)
    when_col = esc_ident(when_column)
    sql_op = _SQL_OP_MAP.get(when_op, when_op)

    # Handle NULL value in condition
    if when_value is None:
        if when_op == "==":
            condition = f"{when_col} IS NULL"
        elif when_op == "!=":
            condition = f"{when_col} IS NOT NULL"
        else:
            condition = "1=0"
    else:
        val = lit_str(str(when_value)) if isinstance(when_value, str) else str(when_value)
        if isinstance(when_value, bool):
            val = "TRUE" if when_value else "FALSE"
        condition = f"{when_col} {sql_op} {val}"

    return (
        f"SUM(CASE WHEN ({condition}) AND {col} IS NULL THEN 1 ELSE 0 END) "
        f"AS {esc_ident(rule_id)}"
    )


def _assemble_single_row(selects: List[str]) -> str:
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


def _results_from_single_row_map(values: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                "execution_source": "sql",
            }
        )
    return out


# --------------------------- DuckDB SQL Executor ------------------------------


@register_executor("duckdb")
class DuckDBSqlExecutor(SqlExecutor):
    """
    DuckDB-based SQL pushdown executor:
      - not_null(column)
      - min_rows(threshold)
      - max_rows(threshold)
      - freshness(column, max_age_seconds)
      - range(column, min, max)
    """

    name = "duckdb"

    SUPPORTED_RULES = {"not_null", "min_rows", "max_rows", "freshness", "range", "regex", "compare", "conditional_not_null"}

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        scheme = (handle.scheme or "").lower()
        if scheme not in {"", "file", "s3", "http", "https"}:
            return False
        return any((s.get("kind") in self.SUPPORTED_RULES) for s in (sql_specs or []))

    def compile(self, sql_specs: List[Dict[str, Any]]) -> str:
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
            elif kind == "freshness":
                col = spec.get("column")
                max_age_seconds = spec.get("max_age_seconds")
                if isinstance(col, str) and col and isinstance(max_age_seconds, int):
                    selects.append(_agg_freshness(col, max_age_seconds, rid))
            elif kind == "range":
                col = spec.get("column")
                min_val = spec.get("min")
                max_val = spec.get("max")
                if isinstance(col, str) and col and (min_val is not None or max_val is not None):
                    selects.append(_agg_range(col, min_val, max_val, rid))
            elif kind == "regex":
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    selects.append(_agg_regex(col, pattern, rid))
            elif kind == "compare":
                left = spec.get("left")
                right = spec.get("right")
                op = spec.get("op")
                if (isinstance(left, str) and left and
                    isinstance(right, str) and right and
                    isinstance(op, str) and op in _SQL_OP_MAP):
                    selects.append(_agg_compare(left, right, op, rid))
            elif kind == "conditional_not_null":
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")  # Can be None
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op in _SQL_OP_MAP):
                    selects.append(_agg_conditional_not_null(col, when_column, when_op, when_value, rid))
        return _assemble_single_row(selects)

    def execute(
        self,
        handle: DatasetHandle,
        compiled_sql: str,
        *,
        csv_mode: str = "auto",
    ) -> Dict[str, Any]:
        """
        Execute compiled SQL, honoring csv_mode for CSV URIs.
        Returns:
          {
            "results": [...],
            "staging": {"path": <parquet_path>|None, "tmpdir": <TemporaryDirectory>|None}
          }
        """
        con = create_duckdb_connection(handle)
        view = "_data"
        tmpdir: Optional[tempfile.TemporaryDirectory] = None
        staged_path: Optional[str] = None

        try:
            tmpdir, staged_path, _ = _create_source_view(con, handle, view, csv_mode=csv_mode)
            cur = con.execute(compiled_sql)
            row = cur.fetchone()
            cols = [d[0] for d in cur.description] if (row and cur.description) else []
            mapping = {c: row[i] for i, c in enumerate(cols)} if row else {}
            return {
                "results": _results_from_single_row_map(mapping),
                "staging": {"path": staged_path, "tmpdir": tmpdir},
            }
        except Exception:
            if tmpdir is not None:
                tmpdir.cleanup()
            raise
        finally:
            try:
                con.execute(f"DROP VIEW IF EXISTS {esc_ident(view)};")
            except Exception:
                pass

    def introspect(
        self,
        handle: DatasetHandle,
        *,
        csv_mode: str = "auto",
    ) -> Dict[str, Any]:
        """
        Introspect row count and columns, honoring csv_mode.
        Returns:
          {
            "row_count": int,
            "available_cols": [...],
            "staging": {"path": <parquet_path>|None, "tmpdir": <TemporaryDirectory>|None}
          }
        """
        con = create_duckdb_connection(handle)
        view = "_data"
        tmpdir: Optional[tempfile.TemporaryDirectory] = None
        staged_path: Optional[str] = None

        try:
            tmpdir, staged_path, _ = _create_source_view(con, handle, view, csv_mode=csv_mode)
            nrow = con.execute(f"SELECT COUNT(*) AS n FROM {esc_ident(view)}").fetchone()
            n = int(nrow[0]) if nrow and nrow[0] is not None else 0
            cur = con.execute(f"SELECT * FROM {esc_ident(view)} LIMIT 0")
            cols = [d[0] for d in cur.description] if cur.description else []
            return {
                "row_count": n,
                "available_cols": cols,
                "staging": {"path": staged_path, "tmpdir": tmpdir},
            }
        except Exception:
            if tmpdir is not None:
                tmpdir.cleanup()
            raise
        finally:
            try:
                con.execute(f"DROP VIEW IF EXISTS {esc_ident(view)};")
            except Exception:
                pass
