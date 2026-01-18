# src/kontra/backends/duckdb_session.py
from __future__ import annotations

import os
from typing import Any, Dict
from urllib.parse import urlparse

import duckdb
from kontra.connectors.handle import DatasetHandle

# --- Public API ---


def create_duckdb_connection(handle: DatasetHandle) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection configured specifically for the given DatasetHandle.

    This is the centralized factory for all DuckDB instances in Kontra.
    It inspects the handle's scheme and fs_opts to load the correct
    extensions (httpfs) and apply the necessary configuration
    (e.g., S3 endpoints, credentials, region) for I/O.

    Args:
        handle: The DatasetHandle containing the URI and filesystem options.

    Returns:
        A configured duckdb.DuckDBPyConnection.
    """
    con = duckdb.connect()

    # Apply performance/threading tweaks (reads env, but for runtime, not I/O)
    _configure_threads(con)

    # Apply I/O and credential configuration based on the data source
    match handle.scheme:
        case "s3":
            _configure_s3(con, handle.fs_opts)
        case "abfs" | "abfss" | "az":
            _configure_azure(con, handle.fs_opts)  # Stubbed for future work
        case "http" | "https":
            _configure_http(con, handle.fs_opts)
        case "file" | "":
            # Local files need no special I/O config
            pass
        case _:
            # Best-effort for unknown schemes: load httpfs just in case
            try:
                _configure_http(con, handle.fs_opts)
            except Exception:
                pass  # Ignore if httpfs fails to load

    return con


# --- Internal Helpers ---


def _safe_set(con: duckdb.DuckDBPyConnection, key: str, value: Any) -> None:
    """
    Safely execute a DuckDB SET command, ignoring errors.
    """
    try:
        con.execute(f"SET {key} = ?", [str(value)])
    except Exception:
        # Fails gracefully if the setting doesn't exist (e.g., wrong DuckDB version)
        pass


def _configure_threads(con: duckdb.DuckDBPyConnection) -> None:
    """
    Configure DuckDB thread count based on env vars or CPU count.
    This is a performance tweak, not an I/O secret.
    """
    env_threads = os.getenv("DUCKDB_THREADS")
    try:
        nthreads = int(env_threads) if env_threads else (os.cpu_count() or 4)
    except Exception:
        nthreads = os.cpu_count() or 4

    # Try both PRAGMA (older) and SET (newer) for compatibility
    for sql in (f"PRAGMA threads={int(nthreads)};", f"SET threads = {int(nthreads)};"):
        try:
            con.execute(sql)
            break
        except Exception:
            continue


def _configure_http(
    con: duckdb.DuckDBPyConnection, fs_opts: Dict[str, str]
) -> None:
    """
    Install and load the httpfs extension for reading http(s):// files.
    """
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    _safe_set(con, "enable_object_cache", "true")


def _configure_s3(con: duckdb.DuckDBPyConnection, fs_opts: Dict[str, str]) -> None:
    """
    Configure the httpfs extension for S3-compatible storage (AWS, MinIO, R2).

    Expected fs_opts keys:
    - s3_endpoint
    - s3_region
    - s3_url_style ('path' | 'host')
    - s3_use_ssl ('true' | 'false')
    - s3_access_key_id
    - s3_secret_access_key
    - s3_session_token
    - s3_max_connections
    """
    _configure_http(con, fs_opts)  # S3 depends on httpfs

    # Credentials
    if ak := fs_opts.get("s3_access_key_id"):
        _safe_set(con, "s3_access_key_id", ak)
    if sk := fs_opts.get("s3_secret_access_key"):
        _safe_set(con, "s3_secret_access_key", sk)
    if st := fs_opts.get("s3_session_token"):
        _safe_set(con, "s3_session_token", st)

    # Region
    if region := fs_opts.get("s3_region"):
        _safe_set(con, "s3_region", region)

    # Endpoint (MinIO/S3-compatible)
    endpoint = fs_opts.get("s3_endpoint")
    url_style = fs_opts.get("s3_url_style")
    use_ssl = fs_opts.get("s3_use_ssl")

    if endpoint:
        # Parse "http://host:port" or just "host:port"
        parsed = urlparse(endpoint)
        hostport = parsed.netloc or parsed.path or endpoint
        _safe_set(con, "s3_endpoint", hostport)

        # Infer SSL from endpoint scheme if not explicitly set
        if use_ssl is None:
            use_ssl = "true" if parsed.scheme == "https" else "false"
        _safe_set(con, "s3_use_ssl", use_ssl)

        # Default to path-style for custom endpoints (MinIO-friendly)
        if url_style is None:
            url_style = "path"

    if url_style:
        _safe_set(con, "s3_url_style", url_style)

    # Performance and reliability for large files over S3/HTTP
    # http_timeout is in seconds (default 30s - increase for large files)
    _safe_set(con, "http_timeout", "600")  # 10 minutes for large files
    _safe_set(con, "http_retries", "5")    # More retries for reliability
    _safe_set(con, "http_retry_wait_ms", "2000")  # 2s between retries
    # Disable keep-alive for MinIO/S3-compatible - connection pooling can cause issues
    _safe_set(con, "http_keep_alive", "false")


def _configure_azure(
    con: duckdb.DuckDBPyConnection, fs_opts: Dict[str, str]
) -> None:
    """
    Configure the Azure extension for ADLS Gen2 (abfs://, abfss://) and Azure Blob (az://).

    DuckDB 0.10+ has native Azure support via the 'azure' extension.
    This handles authentication and endpoint configuration.

    Expected fs_opts keys:
    - azure_account_name: Storage account name
    - azure_account_key: Storage account key
    - azure_sas_token: SAS token (alternative to key)
    - azure_connection_string: Full connection string (alternative)
    - azure_tenant_id: For OAuth/service principal
    - azure_client_id: For OAuth/service principal
    - azure_client_secret: For OAuth/service principal
    - azure_endpoint: Custom endpoint (Databricks, sovereign clouds, Azurite)

    Raises:
        RuntimeError: If Azure extension is not available (DuckDB < 0.10.0)
    """
    # Install and load the Azure extension
    try:
        con.execute("INSTALL azure;")
        con.execute("LOAD azure;")
    except Exception as e:
        raise RuntimeError(
            f"Azure extension not available. DuckDB >= 0.10.0 is required for Azure support. "
            f"Error: {e}"
        ) from e

    # Account name (required for key/SAS auth)
    if account_name := fs_opts.get("azure_account_name"):
        _safe_set(con, "azure_storage_account_name", account_name)

    # Account key auth
    if account_key := fs_opts.get("azure_account_key"):
        _safe_set(con, "azure_account_key", account_key)

    # SAS token auth (alternative to account key)
    # Note: DuckDB expects the token without leading '?'
    if sas_token := fs_opts.get("azure_sas_token"):
        # Strip leading '?' if present
        if sas_token.startswith("?"):
            sas_token = sas_token[1:]
        _safe_set(con, "azure_sas_token", sas_token)

    # Connection string auth
    if conn_string := fs_opts.get("azure_connection_string"):
        _safe_set(con, "azure_storage_connection_string", conn_string)

    # OAuth / Service Principal auth
    if tenant_id := fs_opts.get("azure_tenant_id"):
        _safe_set(con, "azure_tenant_id", tenant_id)
    if client_id := fs_opts.get("azure_client_id"):
        _safe_set(con, "azure_client_id", client_id)
    if client_secret := fs_opts.get("azure_client_secret"):
        _safe_set(con, "azure_client_secret", client_secret)

    # Custom endpoint (for Databricks, sovereign clouds, Azurite emulator)
    if endpoint := fs_opts.get("azure_endpoint"):
        _safe_set(con, "azure_endpoint", endpoint)

    # Performance settings (same as S3)
    _safe_set(con, "http_timeout", "600")  # 10 minutes for large files
    _safe_set(con, "http_retries", "5")
    _safe_set(con, "http_retry_wait_ms", "2000")