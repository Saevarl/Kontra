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
from urllib.parse import urlparse


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
        """
        from kontra.connectors.detection import detect_connection_dialect

        dialect = detect_connection_dialect(conn)

        return DatasetHandle(
            uri=f"byoc://{dialect}/{table}",
            scheme="byoc",
            path=table,
            format=dialect,
            fs_opts={},
            db_params=None,
            external_conn=conn,
            dialect=dialect,
            table_ref=table,
            owned=False,  # User owns the connection, not Kontra
        )

    @staticmethod
    def from_uri(uri: str) -> "DatasetHandle":
        """
        Create a DatasetHandle from a user-provided URI or path.

        Examples:
          - "s3://my-bucket/data/users.parquet"
          - "/data/users.parquet"         (scheme = "")
          - "file:///data/users.csv"      (scheme = "file")
          - "https://example.com/x.parquet"

        Notes:
          - We keep `path` equal to the original `uri` so engines that accept
            URIs directly (DuckDB: read_parquet) can use it verbatim.
          - `fs_opts` is populated from environment variables where appropriate.
            It’s OK if it’s empty (e.g., local files).
        """
        parsed = urlparse(uri)
        scheme = (parsed.scheme or "").lower()
        lower = uri.lower()

        # Very light format inference (enough for materializer selection)
        if lower.endswith(".parquet"):
            fmt = "parquet"
        elif lower.endswith(".csv"):
            fmt = "csv"
        else:
            fmt = "unknown"

        # Defaults: pass the original URI through to backends that accept URIs
        path = uri

        # Filesystem options (extensible). For now we focus on S3-compatible settings;
        # other filesystems can add their own keys without breaking callers.
        fs_opts: Dict[str, str] = {}

        if scheme == "s3":
            _inject_s3_env(fs_opts)

        # HTTP(S): typically public or signed URLs. No defaults needed here.
        # abfs/abfss (Azure) can be handled when we introduce the Azure materializer/executor.
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
