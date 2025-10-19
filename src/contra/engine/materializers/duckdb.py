# src/contra/engine/materializers/duckdb.py
from __future__ import annotations
from typing import List, Optional, Dict, Any
import os
import time

import duckdb
import polars as pl

from contra.connectors.handle import DatasetHandle
from .registry import BaseMaterializer, register_materializer

def _esc_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

@register_materializer("duckdb")
class DuckDBMaterializer(BaseMaterializer):
    """
    Column-pruned materialization via DuckDB httpfs/azure → Arrow → Polars.

    - True column projection for Parquet/CSV on local or remote object stores.
    - Zero/low-copy handoff to Polars via Arrow.
    - Focused on I/O; it does NOT execute rule SQL (that’s the SQL executor’s job).
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self.source = handle.uri
        self._io_debug_enabled = bool(os.getenv("CONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

        self.con = duckdb.connect()
        self._configure_storage(self.handle)
        self._configure_perf()

    # ---------- Materializer API ----------

    def schema(self) -> List[str]:
        cur = self.con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [self.source])
        return [d[0] for d in cur.description] if cur.description else []

    def to_polars(self, columns: Optional[List[str]]):
        # Route through Arrow for consistent, zero/low-copy columnar materialization.
        import pyarrow as pa  # noqa: F401

        cols_sql = ", ".join(_esc_ident(c) for c in (columns or [])) if columns else "*"
        t0 = time.perf_counter()
        if self._is_parquet():
            cur = self.con.execute(f"SELECT {cols_sql} FROM read_parquet(?)", [self.source])
        elif self._is_csv():
            # For CSVs you can pass options via fs_opts later; using defaults for now.
            cur = self.con.execute(f"SELECT {cols_sql} FROM read_csv_auto(?)", [self.source])
        else:
            # Fallback: try DuckDB's autodetect (best effort)
            cur = self.con.execute(f"SELECT {cols_sql} FROM read_parquet(?)", [self.source])
        table = cur.fetch_arrow_table()
        t1 = time.perf_counter()

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "duckdb",
                "mode": "duckdb_project_to_arrow",
                "columns_requested": list(columns or []),
                "column_count": len(columns or []),
                "elapsed_ms": int((t1 - t0) * 1000),
            }
        else:
            self._last_io_debug = None

        return pl.from_arrow(table)

    def io_debug(self) -> Optional[Dict[str, Any]]:
        return self._last_io_debug

    # ---------- Internals ----------

    def _is_parquet(self) -> bool:
        return self.source.lower().endswith(".parquet")

    def _is_csv(self) -> bool:
        uri = self.source.lower()
        return uri.endswith(".csv") or uri.endswith(".csv.gz") or uri.endswith(".csv.zst")

    def _safe_set(self, key: str, value: str) -> None:
        try:
            self.con.execute("SET " + key + " = ?", [value])
        except Exception:
            pass

    def _configure_perf(self) -> None:
        env_threads = os.getenv("DUCKDB_THREADS")
        try:
            nthreads = int(env_threads) if env_threads else (os.cpu_count() or 4)
        except Exception:
            nthreads = os.cpu_count() or 4
        for stmt in (f"PRAGMA threads={int(nthreads)};", f"SET threads = {int(nthreads)};"):
            try:
                self.con.execute(stmt)
                break
            except Exception:
                continue

    def _configure_storage(self, handle: DatasetHandle) -> None:
        """
        Configure httpfs/azure extensions based on the URI scheme and provided fs_opts.
        No secrets are printed. All settings are guarded for portability.
        """
        uri = handle.uri.lower()
        # Load storage extensions
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

        # Optional Azure support in future (kept lazy to avoid dependency if not needed)
        if uri.startswith("abfs://") or uri.startswith("abfss://"):
            try:
                self.con.execute("INSTALL azure;")
                self.con.execute("LOAD azure;")
            except Exception:
                # If azure extension isn't available, httpfs may still work if creds are env-provided.
                pass

        # Enable object cache if available
        self._safe_set("enable_object_cache", "true")

        # Apply fs_opts first; fall back to envs (kept minimal & generic)
        # S3-style options
        ak = handle.fs_opts.get("s3_access_key_id") or os.getenv("AWS_ACCESS_KEY_ID")
        sk = handle.fs_opts.get("s3_secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY")
        st = handle.fs_opts.get("s3_session_token") or os.getenv("AWS_SESSION_TOKEN")
        region = (
            handle.fs_opts.get("s3_region")
            or os.getenv("DUCKDB_S3_REGION")
            or os.getenv("AWS_REGION")
            or "us-east-1"
        )
        endpoint = handle.fs_opts.get("s3_endpoint") or os.getenv("DUCKDB_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL")
        url_style = handle.fs_opts.get("s3_url_style") or os.getenv("DUCKDB_S3_URL_STYLE")
        use_ssl_override = handle.fs_opts.get("s3_use_ssl") or os.getenv("DUCKDB_S3_USE_SSL")
        max_conns = handle.fs_opts.get("s3_max_connections") or os.getenv("DUCKDB_S3_MAX_CONNECTIONS") or "64"

        if ak: self._safe_set("s3_access_key_id", ak)
        if sk: self._safe_set("s3_secret_access_key", sk)
        if st: self._safe_set("s3_session_token", st)
        if region: self._safe_set("s3_region", region)

        if endpoint:
            from urllib.parse import urlparse
            parsed = urlparse(endpoint)
            hostport = parsed.netloc or parsed.path
            self._safe_set("s3_endpoint", hostport)
            if use_ssl_override is not None:
                self._safe_set("s3_use_ssl", use_ssl_override)
            else:
                self._safe_set("s3_use_ssl", "true" if parsed.scheme == "https" else "false")
            if not url_style:
                url_style = "path"
        if url_style:
            self._safe_set("s3_url_style", url_style)

        self._safe_set("s3_max_connections", str(max_conns))
        self._safe_set("s3_request_timeout_ms", "60000")
