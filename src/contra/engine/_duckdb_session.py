from __future__ import annotations
from typing import Optional, Dict
import os
import duckdb
from urllib.parse import urlparse

def _safe_set(con, key: str, value: str) -> None:
    try:
        con.execute("SET " + key + " = ?", [value])
    except Exception:
        pass

def _configure_threads(con) -> None:
    env_threads = os.getenv("DUCKDB_THREADS")
    try:
        nthreads = int(env_threads) if env_threads else (os.cpu_count() or 4)
    except Exception:
        nthreads = os.cpu_count() or 4
    for sql in (f"PRAGMA threads={int(nthreads)};", f"SET threads = {int(nthreads)};"):
        try:
            con.execute(sql)
            break
        except Exception:
            continue

def _use_httpfs_for_s3(con, endpoint: Optional[str]) -> None:
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    _safe_set(con, "enable_object_cache", "true")

    # S3-style knobs (work for AWS S3 and S3-compatible stores like MinIO/R2/lakeFS).
    if ak := os.getenv("AWS_ACCESS_KEY_ID"):
        _safe_set(con, "s3_access_key_id", ak)
    if sk := os.getenv("AWS_SECRET_ACCESS_KEY"):
        _safe_set(con, "s3_secret_access_key", sk)
    if st := os.getenv("AWS_SESSION_TOKEN"):
        _safe_set(con, "s3_session_token", st)

    region = os.getenv("DUCKDB_S3_REGION") or os.getenv("AWS_REGION") or "us-east-1"
    _safe_set(con, "s3_region", region)

    if endpoint:
        parsed = urlparse(endpoint)
        hostport = parsed.netloc or parsed.path
        _safe_set(con, "s3_endpoint", hostport)
        if (val := os.getenv("DUCKDB_S3_USE_SSL")) is not None:
            _safe_set(con, "s3_use_ssl", val)
        else:
            _safe_set(con, "s3_use_ssl", "true" if parsed.scheme == "https" else "false")
        # Default to path-style when custom endpoints are used (MinIO-friendly)
        if not os.getenv("DUCKDB_S3_URL_STYLE"):
            _safe_set(con, "s3_url_style", "path")

    if (style := os.getenv("DUCKDB_S3_URL_STYLE")):
        _safe_set(con, "s3_url_style", style)

    _safe_set(con, "s3_max_connections", os.getenv("DUCKDB_S3_MAX_CONNECTIONS", "64"))
    _safe_set(con, "s3_request_timeout_ms", "60000")

def _use_azure_extension(con) -> None:
    # Native Azure filesystem (Blob / ADLS Gen2). Read support; great for column-pruned reads.
    con.execute("INSTALL azure;")
    con.execute("LOAD azure;")
    # The azure extension reads credentials from standard Azure envs or uses SAS.
    # We don't set secrets here to avoid logging them; DuckDB will resolve via env/secret chain.

def _register_adlfs_via_fsspec(con, account_name: Optional[str] = None) -> None:
    # Fallback path: register an fsspec filesystem (adlfs) when extension is unavailable.
    # Users can control auth via environment (DefaultAzureCredential/SAS/conn string).
    try:
        from adlfs import AzureBlobFileSystem  # type: ignore
        fs = AzureBlobFileSystem(account_name=account_name) if account_name else AzureBlobFileSystem()
        con.register_filesystem(fs)
    except Exception:
        # If adlfs isn't installed, we simply skip; caller should have loaded 'azure' extension.
        pass

def new_duckdb_connection_for_uri(uri: str, *, opts: Optional[Dict[str, str]] = None) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection configured for the given URI scheme.

    - s3://, http(s):// with S3 endpoints -> httpfs (S3 API)
    - abfs://, az://, abfss:// -> azure extension (or fallback to adlfs via fsspec)
    - https:// plain -> httpfs (read-only)
    """
    con = duckdb.connect()
    parsed = urlparse(uri)
    scheme = (parsed.scheme or "").lower()
    _configure_threads(con)

    if scheme in ("s3",):
        _use_httpfs_for_s3(con, endpoint=os.getenv("AWS_ENDPOINT_URL"))
        return con

    if scheme in ("abfs", "abfss", "az"):
        try:
            _use_azure_extension(con)
        except Exception:
            # Optional fallback: attempt fsspec registration (adlfs)
            _register_adlfs_via_fsspec(con, account_name=os.getenv("AZURE_STORAGE_ACCOUNT"))
        return con

    # Generic HTTPS read path (httpfs handles HTTPS)
    if scheme in ("http", "https"):
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        _safe_set(con, "enable_object_cache", "true")
        return con

    # Default: still load httpfs for broad compatibility (local files don't need it)
    try:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
    except Exception:
        pass
    return con
