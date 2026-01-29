# src/kontra/connectors/handle.py
from __future__ import annotations

"""
DatasetHandle — a normalized, engine-agnostic view of a dataset location.

Why this exists
---------------
Materializers (DuckDB/Polars) and SQL executors shouldn't have to parse URIs
or chase environment variables. This small value object centralizes that logic:

  - `uri`:     the original string you passed (e.g., "s3://bucket/key.parquet")
  - `scheme`:  parsed scheme: "s3", "file", "https", "" (bare local), "byoc", etc.
  - `path`:    the path we should hand to the backend (typically the original URI)
  - `format`:  best-effort file format: "parquet" | "csv" | "postgres" | "sqlserver" | "unknown"
  - `fs_opts`: normalized filesystem options pulled from env (e.g., S3 creds,
               region, endpoint, URL style). These are safe to pass to a DuckDB
               httpfs session or other backends.

BYOC (Bring Your Own Connection) support:
  - `external_conn`: User-provided database connection object
  - `dialect`:       Database dialect ("postgresql", "sqlserver")
  - `table_ref`:     Table reference ("schema.table" or "db.schema.table")
  - `owned`:         If True, Kontra closes the connection. If False (BYOC), user closes it.

This object is intentionally tiny and immutable. If a connector later wants to
enrich it (e.g., SAS tokens for ADLS), we can extend `fs_opts` without touching
the engine or materializers.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import os
import re
from urllib.parse import urlparse


def mask_credentials(uri: str) -> str:
    """
    Mask credentials in a URI for safe display in logs/output.

    Handles patterns like:
    - postgres://user:password@host/db -> postgres://user:***@host/db
    - mssql://sa:Secret123!@host/db -> mssql://sa:***@host/db
    - Any URI with ://user:password@ pattern (including @ in password)

    Args:
        uri: URI that may contain credentials

    Returns:
        URI with password masked as '***'
    """
    if not uri or "://" not in uri:
        return uri

    # Use urlparse for robust handling of credentials
    try:
        parsed = urlparse(uri)
        if parsed.password:
            # Reconstruct with masked password
            # netloc format: user:password@host:port
            if parsed.port:
                new_netloc = f"{parsed.username}:***@{parsed.hostname}:{parsed.port}"
            else:
                new_netloc = f"{parsed.username}:***@{parsed.hostname}"
            return uri.replace(parsed.netloc, new_netloc)
    except (ValueError, AttributeError):
        pass  # URI parsing failed

    return uri


@dataclass(frozen=True)
class DatasetHandle:
    uri: str
    scheme: str
    path: str
    format: str
    fs_opts: Dict[str, str]
    # Database connection parameters (for URI-based connections)
    db_params: Optional[Any] = field(default=None)

    # BYOC (Bring Your Own Connection) fields
    external_conn: Optional[Any] = field(default=None)  # User's connection object
    dialect: Optional[str] = field(default=None)        # "postgresql" | "sqlserver"
    table_ref: Optional[str] = field(default=None)      # "schema.table" or "db.schema.table"
    owned: bool = field(default=True)                   # True = we close, False = user closes

    # ------------------------------ Constructors ------------------------------

    @staticmethod
    def from_connection(conn: Any, table: str) -> "DatasetHandle":
        """
        Create a DatasetHandle from a BYOC (Bring Your Own Connection) database connection.

        This allows users to pass their own database connection objects (psycopg2,
        pyodbc, SQLAlchemy, etc.) while Kontra still performs SQL pushdown and preplan.

        Args:
            conn: A database connection object (psycopg2, pyodbc, SQLAlchemy engine, etc.)
            table: Table reference: "table", "schema.table", or "database.schema.table"

        Returns:
            DatasetHandle configured for BYOC mode

        Examples:
            >>> import psycopg2
            >>> conn = psycopg2.connect(host="localhost", dbname="mydb")
            >>> handle = DatasetHandle.from_connection(conn, "public.users")

            >>> import pyodbc
            >>> conn = pyodbc.connect("DRIVER={ODBC Driver 17};SERVER=...")
            >>> handle = DatasetHandle.from_connection(conn, "dbo.orders")

        Notes:
            - Kontra does NOT close the connection (owned=False). User manages lifecycle.
            - SQL pushdown and preplan still work using the provided connection.
            - The `dialect` is auto-detected from the connection type.
            - SQLAlchemy engines/connections are automatically unwrapped to raw DBAPI.
        """
        from kontra.connectors.detection import (
            detect_connection_dialect,
            unwrap_sqlalchemy_connection,
        )

        # Detect dialect before unwrapping (SQLAlchemy has better dialect info)
        dialect = detect_connection_dialect(conn)

        # Unwrap SQLAlchemy to raw DBAPI connection (has .cursor() method)
        raw_conn = unwrap_sqlalchemy_connection(conn)

        return DatasetHandle(
            uri=f"byoc://{dialect}/{table}",
            scheme="byoc",
            path=table,
            format=dialect,
            fs_opts={},
            db_params=None,
            external_conn=raw_conn,
            dialect=dialect,
            table_ref=table,
            owned=False,  # User owns the connection, not Kontra
        )

    @staticmethod
    def from_uri(
        uri: str,
        storage_options: Optional[Dict[str, Any]] = None,
    ) -> "DatasetHandle":
        """
        Create a DatasetHandle from a user-provided URI or path.

        Examples:
          - "s3://my-bucket/data/users.parquet"
          - "/data/users.parquet"         (scheme = "")
          - "file:///data/users.csv"      (scheme = "file")
          - "https://example.com/x.parquet"

        Args:
            uri: Path or URI to the dataset
            storage_options: Optional dict of cloud storage credentials.
                For S3/MinIO:
                  - aws_access_key_id, aws_secret_access_key
                  - aws_region (required for Polars)
                  - endpoint_url (for MinIO/S3-compatible)
                For Azure:
                  - account_name, account_key, sas_token, etc.
                These override environment variables when provided.

        Notes:
          - We keep `path` equal to the original `uri` so engines that accept
            URIs directly (DuckDB: read_parquet) can use it verbatim.
          - `fs_opts` is populated from environment variables, then merged with
            storage_options (storage_options take precedence).
        """
        parsed = urlparse(uri)
        scheme = (parsed.scheme or "").lower()
        lower = uri.lower()

        # Very light format inference (enough for materializer selection)
        if lower.endswith(".parquet"):
            fmt = "parquet"
        elif lower.endswith(".csv") or lower.endswith(".tsv"):
            fmt = "csv"  # TSV is CSV with tab separator (auto-detected by Polars)
        else:
            fmt = "unknown"

        # Defaults: pass the original URI through to backends that accept URIs
        path = uri

        # Filesystem options (extensible). For now we focus on S3-compatible settings;
        # other filesystems can add their own keys without breaking callers.
        fs_opts: Dict[str, str] = {}

        if scheme == "s3":
            _inject_s3_env(fs_opts)
            # Merge user-provided storage_options (takes precedence over env vars)
            if storage_options:
                _merge_s3_storage_options(fs_opts, storage_options)

        # Azure Data Lake Storage / Azure Blob Storage
        if scheme in ("abfs", "abfss", "az"):
            _inject_azure_env(fs_opts)
            # Merge user-provided storage_options (takes precedence over env vars)
            if storage_options:
                _merge_azure_storage_options(fs_opts, storage_options)

        # HTTP(S): typically public or signed URLs. No defaults needed here.
        # Local `""`/`file` schemes: no fs_opts.

        # PostgreSQL: resolve connection parameters from URI + environment
        db_params = None
        if scheme in ("postgres", "postgresql"):
            from kontra.connectors.postgres import resolve_connection_params

            db_params = resolve_connection_params(uri)
            fmt = "postgres"

        # SQL Server: resolve connection parameters from URI + environment
        if scheme in ("mssql", "sqlserver"):
            from kontra.connectors.sqlserver import resolve_connection_params as resolve_sqlserver_params

            db_params = resolve_sqlserver_params(uri)
            fmt = "sqlserver"

        return DatasetHandle(
            uri=uri, scheme=scheme, path=path, format=fmt, fs_opts=fs_opts, db_params=db_params
        )


# ------------------------------ Helpers ---------------------------------------


def _inject_s3_env(opts: Dict[str, str]) -> None:
    """
    Read S3/MinIO-related environment variables and copy them into `opts` using
    the normalized keys that our DuckDB session factory/materializer expect.

    We *don’t* log or print these values anywhere; the caller just passes them to
    the backend session config. All keys are optional.
    """
    # Credentials
    ak = os.getenv("AWS_ACCESS_KEY_ID")
    sk = os.getenv("AWS_SECRET_ACCESS_KEY")
    st = os.getenv("AWS_SESSION_TOKEN")

    # Region (prefer DUCKDB_S3_REGION when provided, else AWS_REGION, else default)
    region = os.getenv("DUCKDB_S3_REGION") or os.getenv("AWS_REGION") or "us-east-1"

    # Endpoint / style (MinIO/custom endpoints)
    endpoint = os.getenv("DUCKDB_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL")
    url_style = os.getenv("DUCKDB_S3_URL_STYLE")  # 'path' | 'host'
    use_ssl = os.getenv("DUCKDB_S3_USE_SSL")      # 'true' | 'false'
    max_conns = os.getenv("DUCKDB_S3_MAX_CONNECTIONS") or "64"

    if ak:
        opts["s3_access_key_id"] = ak
    if sk:
        opts["s3_secret_access_key"] = sk
    if st:
        opts["s3_session_token"] = st
    if region:
        opts["s3_region"] = region
    if endpoint:
        # Keep the full endpoint string; the DuckDB session factory will parse it.
        opts["s3_endpoint"] = endpoint
    if url_style:
        opts["s3_url_style"] = url_style
    if use_ssl:
        opts["s3_use_ssl"] = use_ssl
    if max_conns:
        opts["s3_max_connections"] = str(max_conns)


def _inject_azure_env(opts: Dict[str, str]) -> None:
    """
    Read Azure Storage environment variables and copy them into `opts` using
    normalized keys that our DuckDB session factory expects.

    Supports multiple auth methods:
    - Account key: AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
    - SAS token: AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_SAS_TOKEN
    - Connection string: AZURE_STORAGE_CONNECTION_STRING
    - Service principal (OAuth): AZURE_TENANT_ID + AZURE_CLIENT_ID + AZURE_CLIENT_SECRET

    All keys are optional. DuckDB's azure extension will use what's available.
    """
    # Account name (required for most auth methods)
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    if account_name:
        opts["azure_account_name"] = account_name

    # Account key auth (accept both common env var names)
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY") or os.getenv("AZURE_STORAGE_ACCESS_KEY")
    if account_key:
        opts["azure_account_key"] = account_key

    # SAS token auth (alternative to account key)
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    if sas_token:
        opts["azure_sas_token"] = sas_token

    # Connection string auth (contains account name + key/SAS)
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_string:
        opts["azure_connection_string"] = conn_string

    # OAuth / Service Principal auth
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if tenant_id:
        opts["azure_tenant_id"] = tenant_id
    if client_id:
        opts["azure_client_id"] = client_id
    if client_secret:
        opts["azure_client_secret"] = client_secret

    # Custom endpoint (for Databricks, sovereign clouds, Azurite emulator)
    endpoint = os.getenv("AZURE_STORAGE_ENDPOINT")
    if endpoint:
        opts["azure_endpoint"] = endpoint


def _merge_s3_storage_options(opts: Dict[str, str], storage_options: Dict[str, Any]) -> None:
    """
    Merge user-provided storage_options into fs_opts for S3.

    Maps Polars-style keys to our internal normalized keys.
    User values take precedence over env-var derived values.

    Polars storage_options keys:
      - aws_access_key_id -> s3_access_key_id
      - aws_secret_access_key -> s3_secret_access_key
      - aws_session_token -> s3_session_token
      - aws_region -> s3_region
      - endpoint_url -> s3_endpoint
    """
    # Mapping from Polars/user keys to our internal keys
    key_map = {
        "aws_access_key_id": "s3_access_key_id",
        "aws_secret_access_key": "s3_secret_access_key",
        "aws_session_token": "s3_session_token",
        "aws_region": "s3_region",
        "region": "s3_region",  # Alternative key
        "endpoint_url": "s3_endpoint",
    }

    for user_key, internal_key in key_map.items():
        if user_key in storage_options and storage_options[user_key] is not None:
            opts[internal_key] = str(storage_options[user_key])

    # Also accept our internal keys directly (pass-through)
    internal_keys = [
        "s3_access_key_id",
        "s3_secret_access_key",
        "s3_session_token",
        "s3_region",
        "s3_endpoint",
        "s3_url_style",
        "s3_use_ssl",
    ]
    for key in internal_keys:
        if key in storage_options and storage_options[key] is not None:
            opts[key] = str(storage_options[key])


def _merge_azure_storage_options(opts: Dict[str, str], storage_options: Dict[str, Any]) -> None:
    """
    Merge user-provided storage_options into fs_opts for Azure.

    Maps common Azure keys to our internal normalized keys.
    User values take precedence over env-var derived values.
    """
    # Mapping from user keys to our internal keys
    key_map = {
        "account_name": "azure_account_name",
        "account_key": "azure_account_key",
        "sas_token": "azure_sas_token",
        "connection_string": "azure_connection_string",
        "tenant_id": "azure_tenant_id",
        "client_id": "azure_client_id",
        "client_secret": "azure_client_secret",
        "endpoint": "azure_endpoint",
    }

    for user_key, internal_key in key_map.items():
        if user_key in storage_options and storage_options[user_key] is not None:
            opts[internal_key] = str(storage_options[user_key])

    # Also accept our internal keys directly (pass-through)
    internal_keys = [
        "azure_account_name",
        "azure_account_key",
        "azure_sas_token",
        "azure_connection_string",
        "azure_tenant_id",
        "azure_client_id",
        "azure_client_secret",
        "azure_endpoint",
    ]
    for key in internal_keys:
        if key in storage_options and storage_options[key] is not None:
            opts[key] = str(storage_options[key])
