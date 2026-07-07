# src/kontra/connectors/uri_utils.py
"""
Shared URI parsing and filesystem creation utilities.

Centralizes S3, Azure ADLS, and Parquet URI handling used across
engine, preplan, residual, and materializer modules.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow.fs as pafs
    from kontra.connectors.handle import DatasetHandle


def is_s3_uri(val: str | None) -> bool:
    """Check if value is an S3 URI."""
    return isinstance(val, str) and val.lower().startswith("s3://")


def is_azure_uri(val: str | None) -> bool:
    """Check if URI is an Azure storage URI (ADLS Gen2 or Blob)."""
    if not isinstance(val, str):
        return False
    lower = val.lower()
    return lower.startswith(("abfs://", "abfss://", "az://"))


def validate_azure_account_key(key: str) -> None:
    """
    Reject account keys that are not valid base64 BEFORE they reach DuckDB.

    Azure storage account keys are always base64. A malformed key otherwise
    surfaces as an opaque auth/HTTP failure at query time — and because the
    key is embedded into a ';'-delimited connection string for DuckDB's
    secret manager, a key containing ';' or '=' in the wrong place could
    silently alter the connection-string fields.

    Raises:
        AzureCredentialError: key is empty, has non-base64 characters,
            or has broken padding.
    """
    import base64
    import binascii

    from kontra.errors import AzureCredentialError

    if not key or not key.strip():
        raise AzureCredentialError("account key is empty")
    if key != key.strip():
        raise AzureCredentialError("account key has leading/trailing whitespace")
    if len(key) % 4 != 0:
        raise AzureCredentialError(
            f"account key length ({len(key)}) is not a multiple of 4 — "
            "base64 requires '='-padded groups of 4 (key may be truncated)"
        )
    try:
        base64.b64decode(key, validate=True)
    except (binascii.Error, ValueError) as e:
        raise AzureCredentialError(f"account key is not valid base64: {e}") from e


def azure_transport_option(fs_opts: "Optional[Dict[str, str]]" = None) -> Optional[str]:
    """
    Transport adapter for DuckDB's Azure extension: 'default', 'curl', or None.

    The Azure SDK's default transport commonly fails to locate the CA bundle
    inside slim/container Linux images (opaque SSL/connection errors at query
    time); the 'curl' transport searches the standard certificate paths. So:

    Resolution order:
      1. fs_opts['azure_transport'] (storage_options key: 'transport')
      2. KONTRA_AZURE_TRANSPORT env var
      3. 'curl' on Linux (containers are the motivating case)
      4. None elsewhere — keep DuckDB's platform default

    Returns None to mean "don't set the option".

    Raises:
        ValueError: explicit value is neither 'default' nor 'curl'.
    """
    import os
    import sys

    value = (fs_opts or {}).get("azure_transport") or os.getenv("KONTRA_AZURE_TRANSPORT")
    if value:
        value = value.strip().lower()
        if value not in ("default", "curl"):
            raise ValueError(
                f"Invalid Azure transport {value!r}: expected 'default' or 'curl' "
                "(storage_options 'transport' / KONTRA_AZURE_TRANSPORT)"
            )
        return value
    if sys.platform.startswith("linux"):
        return "curl"
    return None


def is_parquet(path: str | None) -> bool:
    """Check if path points to a Parquet file."""
    return isinstance(path, str) and path.lower().endswith(".parquet")


def s3_uri_to_path(uri: str) -> str:
    """Convert s3://bucket/key to bucket/key (PyArrow S3FileSystem format)."""
    if uri.lower().startswith("s3://"):
        return uri[5:]  # Strip 's3://'
    return uri


def azure_uri_to_path(uri: str) -> str:
    """
    Convert Azure URI to container/path format for PyArrow AzureFileSystem.

    abfss://container@account.dfs.core.windows.net/path -> container/path
    """
    from urllib.parse import urlparse
    parsed = urlparse(uri)
    # netloc is "container@account.dfs.core.windows.net"
    if "@" in parsed.netloc:
        container = parsed.netloc.split("@", 1)[0]
    else:
        container = parsed.netloc.split(".")[0]
    path_part = parsed.path.lstrip("/")
    return f"{container}/{path_part}"


def create_s3_filesystem(handle: "DatasetHandle") -> "pafs.S3FileSystem":
    """
    Create a PyArrow S3FileSystem from handle's fs_opts (populated from env vars).
    Supports MinIO and other S3-compatible storage via custom endpoints.
    """
    import pyarrow.fs as pafs

    opts = handle.fs_opts or {}

    # Map our fs_opts keys to PyArrow S3FileSystem kwargs
    kwargs: Dict[str, Any] = {}
    if opts.get("s3_access_key_id") and opts.get("s3_secret_access_key"):
        kwargs["access_key"] = opts["s3_access_key_id"]
        kwargs["secret_key"] = opts["s3_secret_access_key"]
    if opts.get("s3_session_token"):
        kwargs["session_token"] = opts["s3_session_token"]
    if opts.get("s3_region"):
        kwargs["region"] = opts["s3_region"]
    if opts.get("s3_endpoint"):
        # PyArrow expects endpoint_override without the scheme
        endpoint = opts["s3_endpoint"]
        # Strip scheme if present and set scheme kwarg
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
            kwargs["scheme"] = "http"
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
            kwargs["scheme"] = "https"
        kwargs["endpoint_override"] = endpoint

    # MinIO and some S3-compatible storage require path-style URLs (not virtual-hosted)
    # DUCKDB_S3_URL_STYLE=path -> force_virtual_addressing=False
    url_style = opts.get("s3_url_style", "").lower()
    if url_style == "path":
        kwargs["force_virtual_addressing"] = False
    elif url_style == "host":
        kwargs["force_virtual_addressing"] = True
    # If endpoint is set but no url_style, default to path-style (common for MinIO)
    elif opts.get("s3_endpoint"):
        kwargs["force_virtual_addressing"] = False

    return pafs.S3FileSystem(**kwargs)


def create_azure_filesystem(handle: "DatasetHandle") -> "pafs.FileSystem":
    """
    Create a PyArrow AzureFileSystem from handle's fs_opts (populated from env vars).
    Supports account key and SAS token authentication.

    Priority: account_key > sas_token (only one auth method should be used)
    """
    import pyarrow.fs as pafs

    opts = handle.fs_opts or {}

    kwargs: Dict[str, Any] = {}
    if opts.get("azure_account_name"):
        kwargs["account_name"] = opts["azure_account_name"]

    # Use only ONE auth method - account_key takes priority over sas_token
    # PyArrow can crash or behave unexpectedly when both are provided
    if opts.get("azure_account_key"):
        validate_azure_account_key(opts["azure_account_key"])
        kwargs["account_key"] = opts["azure_account_key"]
    elif opts.get("azure_sas_token"):
        # PyArrow requires SAS token WITH the leading '?'
        sas = opts["azure_sas_token"]
        if not sas.startswith("?"):
            sas = "?" + sas
        kwargs["sas_token"] = sas

    return pafs.AzureFileSystem(**kwargs)
