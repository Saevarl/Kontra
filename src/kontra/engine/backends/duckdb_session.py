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
            except duckdb.Error:
                pass  # Ignore if httpfs fails to load

    return con


# --- Internal Helpers ---


def _safe_set(con: duckdb.DuckDBPyConnection, key: str, value: Any) -> None:
    """
    Safely execute a DuckDB SET command, ignoring errors.
    """
    try:
        con.execute(f"SET {key} = ?", [str(value)])
    except duckdb.Error:
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
    except (ValueError, TypeError):
        nthreads = os.cpu_count() or 4

    # Try both PRAGMA (older) and SET (newer) for compatibility
    for sql in (f"PRAGMA threads={int(nthreads)};", f"SET threads = {int(nthreads)};"):
        try:
            con.execute(sql)
            break
        except duckdb.Error:
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
    This handles authentication via DuckDB's secret manager.

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

    account_name = fs_opts.get("azure_account_name")
    account_key = fs_opts.get("azure_account_key")
    sas_token = fs_opts.get("azure_sas_token")
    conn_string = fs_opts.get("azure_connection_string")
    tenant_id = fs_opts.get("azure_tenant_id")
    client_id = fs_opts.get("azure_client_id")
    client_secret = fs_opts.get("azure_client_secret")
    endpoint = fs_opts.get("azure_endpoint")

    # Build connection string for DuckDB secret
    # Priority: explicit connection_string > account_key > sas_token > service_principal
    if conn_string:
        # User provided full connection string
        _create_azure_secret(con, conn_string)
    elif account_name and account_key:
        # Account key auth
        cs = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
        if endpoint:
            cs += f";BlobEndpoint={endpoint}"
        _create_azure_secret(con, cs)
    elif account_name and sas_token:
        # SAS token auth - strip leading '?' if present
        if sas_token.startswith("?"):
            sas_token = sas_token[1:]
        cs = f"DefaultEndpointsProtocol=https;AccountName={account_name};SharedAccessSignature={sas_token}"
        if endpoint:
            cs += f";BlobEndpoint={endpoint}"
        _create_azure_secret(con, cs)
    elif tenant_id and client_id and client_secret:
        # Service principal auth - use credential chain
        _safe_set(con, "azure_account_name", account_name or "")
        # Set up credential chain for service principal
        con.execute(f"""
            CREATE SECRET azure_sp (
                TYPE AZURE,
                PROVIDER CREDENTIAL_CHAIN,
                ACCOUNT_NAME '{account_name or ""}'
            )
        """)
        # Set the environment variables for the credential chain to pick up
        os.environ.setdefault("AZURE_TENANT_ID", tenant_id)
        os.environ.setdefault("AZURE_CLIENT_ID", client_id)
        os.environ.setdefault("AZURE_CLIENT_SECRET", client_secret)
    elif account_name:
        # Just account name - try credential chain (CLI, managed identity, etc.)
        _safe_set(con, "azure_account_name", account_name)

    # Custom endpoint for Azurite/sovereign clouds
    if endpoint and not conn_string and not (account_name and (account_key or sas_token)):
        _safe_set(con, "azure_endpoint", endpoint)

    # Performance settings (same as S3)
    _safe_set(con, "http_timeout", "600")  # 10 minutes for large files
    _safe_set(con, "http_retries", "5")
    _safe_set(con, "http_retry_wait_ms", "2000")


def _create_azure_secret(con: duckdb.DuckDBPyConnection, connection_string: str) -> None:
    """Create a DuckDB secret for Azure authentication."""
    # Escape single quotes in connection string
    escaped_cs = connection_string.replace("'", "''")
    con.execute(f"""
        CREATE SECRET kontra_azure (
            TYPE AZURE,
            PROVIDER CONFIG,
            CONNECTION_STRING '{escaped_cs}'
        )
    """)