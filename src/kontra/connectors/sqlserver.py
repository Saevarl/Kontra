# src/kontra/connectors/sqlserver.py
"""
SQL Server connection utilities for Kontra.

Supports multiple authentication methods:
1. Full URI: mssql://user:pass@host:port/database/schema.table
2. Environment variables: MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE
3. SQLSERVER_URL (similar to DATABASE_URL pattern)

Priority: URI values > SQLSERVER_URL > MSSQL_XXX env vars > defaults

Authentication modes (the ``auth`` parameter). These cover Azure SQL Database
and Azure SQL Managed Instance:
    - "sql" (default): username/password via pymssql (today's behavior, unchanged)
    - "entra_default": DefaultAzureCredential-style chain via the ODBC driver
      (Authentication=ActiveDirectoryDefault). Tries env service principal, then
      managed identity, then az-cli. Recommended default for Entra.
    - "entra_mi": Azure Managed Identity via pyodbc / ODBC Driver
      (Authentication=ActiveDirectoryMsi). System-assigned by default; pass a
      ``client_id`` for user-assigned identities.
    - "entra_service_principal": Azure AD app registration via the ODBC driver
      (Authentication=ActiveDirectoryServicePrincipal). Uses ``client_id`` as UID
      and ``client_secret`` as PWD. The tenant is the directory of the SQL
      resource; msodbcsql18 has no dedicated tenant/Authority connection keyword,
      so ``tenant_id`` / AZURE_TENANT_ID is carried but not injected.
    - "entra_interactive": interactive browser login via the ODBC driver
      (Authentication=ActiveDirectoryInteractive). For dev workstations.

The ``auth`` (and optional ``client_id``) values are resolved with priority:
    URI query string (``?auth=entra_mi&client_id=...``)
      > named-datasource config (baked into the URI query by resolve_datasource)
      > env vars (MSSQL_AUTH, AZURE_CLIENT_ID)
      > default ("sql")

For Entra modes the Microsoft ODBC driver acquires the token itself, so Kontra
does NOT depend on azure-identity. Install the optional extra:
    pip install 'kontra[sqlserver-entra]'
and a Microsoft ODBC driver (msodbcsql18) on the host.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .db_utils import (
    DbConnectionConfig,
    resolve_connection_params as _resolve_params,
)


# --------------------------------------------------------------------------- #
# Authentication modes
# --------------------------------------------------------------------------- #

AUTH_SQL = "sql"
AUTH_ENTRA_DEFAULT = "entra_default"
AUTH_ENTRA_MI = "entra_mi"
AUTH_ENTRA_SERVICE_PRINCIPAL = "entra_service_principal"
AUTH_ENTRA_INTERACTIVE = "entra_interactive"

ALLOWED_AUTH_MODES = (
    AUTH_SQL,
    AUTH_ENTRA_DEFAULT,
    AUTH_ENTRA_MI,
    AUTH_ENTRA_SERVICE_PRINCIPAL,
    AUTH_ENTRA_INTERACTIVE,
)

# Map Kontra auth mode -> ODBC "Authentication=" keyword.
_ENTRA_ODBC_AUTH = {
    AUTH_ENTRA_DEFAULT: "ActiveDirectoryDefault",
    AUTH_ENTRA_MI: "ActiveDirectoryMsi",
    AUTH_ENTRA_SERVICE_PRINCIPAL: "ActiveDirectoryServicePrincipal",
    AUTH_ENTRA_INTERACTIVE: "ActiveDirectoryInteractive",
}


def validate_auth_mode(auth: str) -> str:
    """Validate an auth mode string, returning the normalized value.

    Raises ValueError (listing allowed values) for anything unrecognized.
    """
    normalized = (auth or "").strip().lower()
    if normalized not in ALLOWED_AUTH_MODES:
        allowed = ", ".join(repr(a) for a in ALLOWED_AUTH_MODES)
        raise ValueError(
            f"Invalid SQL Server auth mode {auth!r}. Allowed values: {allowed}."
        )
    return normalized


# SQL Server-specific configuration for parameter resolution
_MSSQL_CONFIG = DbConnectionConfig(
    default_host="localhost",
    default_port=1433,
    default_user="sa",
    default_schema="dbo",
    env_host="MSSQL_HOST",
    env_port="MSSQL_PORT",
    env_user="MSSQL_USER",
    env_password="MSSQL_PASSWORD",
    env_database="MSSQL_DATABASE",
    env_url="SQLSERVER_URL",
    db_name="SQL Server",
    uri_example="mssql://user:pass@host:1433/database/schema.table",
    env_example="MSSQL_DATABASE",
)


@dataclass
class SqlServerConnectionParams:
    """Resolved SQL Server connection parameters."""

    host: str
    port: int
    user: str
    password: Optional[str]
    database: str
    schema: str
    table: str
    # Entra ID / Azure AD authentication (default "sql" = pymssql user/pass)
    auth: str = AUTH_SQL
    client_id: Optional[str] = None       # user-assigned MI / service principal app id
    client_secret: Optional[str] = None   # service principal secret
    tenant_id: Optional[str] = None       # carried for service principal (see module docs)

    def to_dict(self) -> Dict[str, Any]:
        """Return connection kwargs for pymssql.connect()."""
        return {
            "server": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }

    @property
    def qualified_table(self) -> str:
        """Return schema.table identifier."""
        return f"{self.schema}.{self.table}"


def _first_query_value(query: Dict[str, list], key: str) -> Optional[str]:
    """Return the first value for a parsed query-string key, or None."""
    values = query.get(key)
    if values:
        return values[0]
    return None


def resolve_connection_params(uri: str) -> SqlServerConnectionParams:
    """
    Resolve SQL Server connection parameters from URI + environment.

    URI format:
        mssql://user:pass@host:port/database/schema.table
        mssql:///dbo.users  (uses env vars for connection)
        sqlserver://...  (alias for mssql://)

    Optional query string controls Entra ID auth:
        mssql://host/db/dbo.users?auth=entra_mi
        mssql://host/db/dbo.users?auth=entra_mi&client_id=<user-assigned-id>
        mssql://host/db/dbo.users?auth=entra_service_principal&client_id=<app>&client_secret=<secret>

    Priority: URI values > SQLSERVER_URL > MSSQL_XXX env vars > defaults

    Auth resolution priority: URI query (?auth=/?client_id=...) > MSSQL_AUTH /
    AZURE_CLIENT_ID env vars > default ("sql"). Named-datasource config is baked
    into the URI query by resolve_datasource(), so it participates as "URI query".

    Raises:
        ValueError: If required parameters (database, table) cannot be resolved,
            or if an invalid auth mode is supplied.
    """
    resolved = _resolve_params(uri, _MSSQL_CONFIG)

    query = parse_qs(urlparse(uri).query)

    auth_raw = _first_query_value(query, "auth") or os.getenv("MSSQL_AUTH") or AUTH_SQL
    auth = validate_auth_mode(auth_raw)

    client_id = _first_query_value(query, "client_id") or os.getenv("AZURE_CLIENT_ID")
    client_secret = (
        _first_query_value(query, "client_secret") or os.getenv("AZURE_CLIENT_SECRET")
    )
    tenant_id = _first_query_value(query, "tenant_id") or os.getenv("AZURE_TENANT_ID")

    return SqlServerConnectionParams(
        host=resolved.host,
        port=resolved.port,
        user=resolved.user,
        password=resolved.password,
        database=resolved.database,  # type: ignore (validated in _resolve_params)
        schema=resolved.schema,
        table=resolved.table,  # type: ignore (validated in _resolve_params)
        auth=auth,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
    )


# --------------------------------------------------------------------------- #
# Connection factory
# --------------------------------------------------------------------------- #


def get_connection(params: SqlServerConnectionParams):
    """
    Create a SQL Server connection from resolved parameters.

    - auth="sql"  -> pymssql (username/password), byte-identical to legacy behavior.
    - auth="entra_*" -> pyodbc using a Microsoft ODBC driver that acquires an
      Entra ID (Azure AD) token itself. No password required.

    Returns:
        A DBAPI-style connection (pymssql.Connection or pyodbc.Connection).
    """
    auth = getattr(params, "auth", AUTH_SQL) or AUTH_SQL

    if auth == AUTH_SQL:
        return _connect_pymssql(params)

    if auth in _ENTRA_ODBC_AUTH:
        return _connect_entra_pyodbc(params, auth)

    # Defensive: resolve_connection_params validates, but params may be built
    # directly in tests / callers.
    validate_auth_mode(auth)
    raise ValueError(f"Unsupported SQL Server auth mode: {auth!r}")


def _connect_pymssql(params: SqlServerConnectionParams):
    """Create a pymssql connection (auth='sql', legacy path)."""
    try:
        import pymssql
    except ImportError as e:
        raise ImportError(
            "pymssql is required for SQL Server support.\n"
            "Install with: pip install pymssql"
        ) from e

    try:
        return pymssql.connect(**params.to_dict())
    except pymssql.OperationalError as e:
        raise ConnectionError(
            f"SQL Server connection failed: {e}\n\n"
            f"Connection details:\n"
            f"  Host: {params.host}:{params.port}\n"
            f"  Database: {params.database}\n"
            f"  User: {params.user}\n\n"
            "Check your connection settings or set environment variables:\n"
            "  export MSSQL_HOST=localhost\n"
            "  export MSSQL_PORT=1433\n"
            "  export MSSQL_USER=your_user\n"
            "  export MSSQL_PASSWORD=your_password\n"
            "  export MSSQL_DATABASE=your_database"
        ) from e


def _odbc_escape(value: Any) -> str:
    """
    Escape a value for an ODBC connection string.

    ODBC connection strings use ``KEY=VALUE`` pairs separated by ``;``. Values
    containing ``;``, ``{``, ``}``, ``=`` or leading/trailing whitespace must be
    wrapped in braces, with any literal ``}`` doubled. This prevents a value such
    as a hostname/database containing ``;`` from injecting extra keywords.
    """
    s = str(value)
    if s == "":
        return s
    if any(ch in s for ch in ";{}=") or s != s.strip():
        return "{" + s.replace("}", "}}") + "}"
    return s


def _pick_odbc_driver(pyodbc_module) -> Optional[str]:
    """
    Pick the newest installed 'ODBC Driver NN for SQL Server'.

    Returns the driver name (e.g. "ODBC Driver 18 for SQL Server") or None if
    no Microsoft SQL Server ODBC driver is installed.
    """
    candidates = []
    for name in pyodbc_module.drivers():
        match = re.match(r"ODBC Driver (\d+) for SQL Server\s*$", name.strip())
        if match:
            candidates.append((int(match.group(1)), name.strip()))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def build_entra_connection_string(
    driver: str,
    params: SqlServerConnectionParams,
    auth: str,
) -> str:
    """
    Build an ODBC connection string for an Entra ID auth mode.

    Encrypt=yes is mandatory (Azure SQL and Managed Instance require encryption).
    Values are brace-escaped to prevent connection-string injection.
    """
    odbc_auth = _ENTRA_ODBC_AUTH[auth]
    parts = [
        f"Driver={{{driver}}}",
        f"Server={_odbc_escape(params.host)},{int(params.port)}",
        f"Database={_odbc_escape(params.database)}",
        f"Authentication={odbc_auth}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
    ]

    # User-assigned managed identity: UID carries the identity's client id.
    if auth == AUTH_ENTRA_MI and params.client_id:
        parts.append(f"UID={_odbc_escape(params.client_id)}")

    # Service principal: UID=app id, PWD=secret. (msodbcsql18 has no dedicated
    # tenant/Authority keyword; the tenant is the directory of the SQL resource.
    # tenant_id / AZURE_TENANT_ID is carried on params but intentionally not
    # injected, since an unrecognized keyword would break the driver.)
    if auth == AUTH_ENTRA_SERVICE_PRINCIPAL:
        if params.client_id:
            parts.append(f"UID={_odbc_escape(params.client_id)}")
        if params.client_secret:
            parts.append(f"PWD={_odbc_escape(params.client_secret)}")

    return ";".join(parts) + ";"


def _connect_entra_pyodbc(params: SqlServerConnectionParams, auth: str):
    """Create a pyodbc connection using an Entra ID auth mode."""
    try:
        import pyodbc
    except ImportError as e:
        raise ImportError(
            "Entra ID (Azure AD) authentication for SQL Server requires 'pyodbc' "
            "and a Microsoft ODBC driver (msodbcsql18).\n"
            "Install with: pip install 'kontra[sqlserver-entra]'\n"
            "And install the ODBC driver: "
            "https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
        ) from e

    driver = _pick_odbc_driver(pyodbc)
    if driver is None:
        raise RuntimeError(
            "No Microsoft ODBC Driver for SQL Server is installed "
            "(looked for 'ODBC Driver 18 for SQL Server' / "
            "'ODBC Driver 17 for SQL Server').\n"
            "Entra ID authentication needs the driver to acquire a token.\n"
            "On Azure compute install 'msodbcsql18':\n"
            "  https://learn.microsoft.com/sql/connect/odbc/"
            "download-odbc-driver-for-sql-server"
        )

    conn_str = build_entra_connection_string(driver, params, auth)

    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        detail = (
            f"SQL Server Entra ID connection failed: {e}\n\n"
            f"Connection details:\n"
            f"  Host: {params.host}:{params.port}\n"
            f"  Database: {params.database}\n"
            f"  Auth: {auth}\n"
            f"  Driver: {driver}\n"
        )
        if params.client_id:
            detail += f"  Client ID: {params.client_id}\n"
        detail += (
            "\nEnsure this host has an Entra identity with access to the database, "
            "that the identity is mapped to a database user, and that the ODBC "
            "driver (msodbcsql18) is installed."
        )
        raise ConnectionError(detail) from e
