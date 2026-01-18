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
from kontra.engine.sql_utils import (
    esc_ident as sql_esc_ident,
    agg_min_rows,
    agg_max_rows,
    agg_freshness,
    agg_range,
    agg_regex,
    agg_compare,
    agg_conditional_not_null,
    agg_conditional_range,
    exists_not_null,
    results_from_row,
    SQL_OP_MAP,
    RULE_KIND_TO_FAILURE_MODE,
)

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

# DuckDB dialect constant
DIALECT = "duckdb"


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

    SUPPORTED_RULES = {"not_null", "min_rows", "max_rows", "freshness", "range", "regex", "compare", "conditional_not_null", "conditional_range"}

    def supports(
        self, handle: DatasetHandle, sql_specs: List[Dict[str, Any]]
    ) -> bool:
        scheme = (handle.scheme or "").lower()
        if scheme not in {"", "file", "s3", "http", "https"}:
            return False
        return any((s.get("kind") in self.SUPPORTED_RULES) for s in (sql_specs or []))

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
            rid = spec.get("rule_id")
            if not (kind and rid):
                continue

            if kind == "not_null":
                # Phase 1: Use EXISTS for not_null (faster with early termination)
                col = spec.get("column")
                if isinstance(col, str) and col:
                    exists_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "min_rows":
                aggregate_selects.append(agg_min_rows(int(spec.get("threshold", 0)), rid, DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "max_rows":
                aggregate_selects.append(agg_max_rows(int(spec.get("threshold", 0)), rid, DIALECT))
                aggregate_specs.append(spec)
                supported_specs.append(spec)

            elif kind == "freshness":
                col = spec.get("column")
                max_age_seconds = spec.get("max_age_seconds")
                if isinstance(col, str) and col and isinstance(max_age_seconds, int):
                    aggregate_selects.append(agg_freshness(col, max_age_seconds, rid, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "range":
                col = spec.get("column")
                min_val = spec.get("min")
                max_val = spec.get("max")
                if isinstance(col, str) and col and (min_val is not None or max_val is not None):
                    aggregate_selects.append(agg_range(col, min_val, max_val, rid, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "regex":
                col = spec.get("column")
                pattern = spec.get("pattern")
                if isinstance(col, str) and col and isinstance(pattern, str) and pattern:
                    aggregate_selects.append(agg_regex(col, pattern, rid, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "compare":
                left = spec.get("left")
                right = spec.get("right")
                op = spec.get("op")
                if (isinstance(left, str) and left and
                    isinstance(right, str) and right and
                    isinstance(op, str) and op in SQL_OP_MAP):
                    aggregate_selects.append(agg_compare(left, right, op, rid, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_not_null":
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")  # Can be None
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op in SQL_OP_MAP):
                    aggregate_selects.append(agg_conditional_not_null(col, when_column, when_op, when_value, rid, DIALECT))
                    aggregate_specs.append(spec)
                    supported_specs.append(spec)

            elif kind == "conditional_range":
                col = spec.get("column")
                when_column = spec.get("when_column")
                when_op = spec.get("when_op")
                when_value = spec.get("when_value")  # Can be None
                min_val = spec.get("min")
                max_val = spec.get("max")
                if (isinstance(col, str) and col and
                    isinstance(when_column, str) and when_column and
                    isinstance(when_op, str) and when_op in SQL_OP_MAP and
                    (min_val is not None or max_val is not None)):
                    aggregate_selects.append(agg_conditional_range(col, when_column, when_op, when_value, min_val, max_val, rid, DIALECT))
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
        *,
        csv_mode: str = "auto",
    ) -> Dict[str, Any]:
        """
        Execute the compiled plan in two phases, honoring csv_mode for CSV URIs.

        Phase 1: EXISTS checks for not_null (fast, can early-terminate)
        Phase 2: Aggregate query for remaining rules

        Returns:
          {
            "results": [...],
            "staging": {"path": <parquet_path>|None, "tmpdir": <TemporaryDirectory>|None}
          }
        """
        exists_specs = compiled_plan.get("exists_specs", [])
        aggregate_selects = compiled_plan.get("aggregate_selects", [])

        if not exists_specs and not aggregate_selects:
            return {"results": [], "staging": {"path": None, "tmpdir": None}}

        con = create_duckdb_connection(handle)
        view = "_data"
        tmpdir: Optional[tempfile.TemporaryDirectory] = None
        staged_path: Optional[str] = None
        results: List[Dict[str, Any]] = []

        # Build rule_kinds mapping from specs
        rule_kinds = {}
        for spec in exists_specs:
            rule_kinds[spec["rule_id"]] = spec.get("kind")
        for spec in compiled_plan.get("aggregate_specs", []):
            rule_kinds[spec["rule_id"]] = spec.get("kind")

        try:
            tmpdir, staged_path, _ = _create_source_view(con, handle, view, csv_mode=csv_mode)

            # Phase 1: EXISTS checks for not_null rules
            if exists_specs:
                exists_exprs = [
                    exists_not_null(
                        spec["column"],
                        spec["rule_id"],
                        esc_ident(view),
                        "duckdb"
                    )
                    for spec in exists_specs
                ]
                exists_sql = f"SELECT {', '.join(exists_exprs)};"
                cur = con.execute(exists_sql)
                row = cur.fetchone()
                cols = [d[0] for d in cur.description] if (row and cur.description) else []

                if row and cols:
                    exists_results = results_from_row(cols, row, is_exists=True, rule_kinds=rule_kinds)
                    results.extend(exists_results)

            # Phase 2: Aggregate query for remaining rules
            if aggregate_selects:
                agg_sql = _assemble_single_row(aggregate_selects)
                cur = con.execute(agg_sql)
                row = cur.fetchone()
                cols = [d[0] for d in cur.description] if (row and cur.description) else []

                if row and cols:
                    agg_results = results_from_row(cols, row, is_exists=False, rule_kinds=rule_kinds)
                    results.extend(agg_results)

            return {
                "results": results,
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
