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
    - "entra_password": Entra username/password via the ODBC driver
      (Authentication=ActiveDirectoryPassword). The Entra UPN and password ride
      the normal user/password fields. Legacy flow; does not work for accounts
      requiring MFA.

The ``auth`` (and optional ``client_id``) values are resolved with priority:
    URI query string (``?auth=entra_mi&client_id=...``)
      > named-datasource config (baked into the URI query by resolve_datasource)
      > env vars (MSSQL_AUTH, AZURE_CLIENT_ID)
      > default ("sql")

On Linux/macOS the Microsoft ODBC driver acquires the token itself via the
``Authentication=ActiveDirectory*`` keywords, so azure-identity is not needed
there. On Windows those keywords are rejected by msodbcsql18 for the token modes
(entra_default / entra_mi / entra_service_principal); Kontra instead acquires an
access token with azure-identity and passes it to pyodbc via
``attrs_before={1256: token_struct}`` (1256 = SQL_COPT_SS_ACCESS_TOKEN). Install
the optional extra (it bundles azure-identity for the Windows path):
    pip install 'kontra[sqlserver-entra]'
and a Microsoft ODBC driver (msodbcsql18) on the host.
"""

from __future__ import annotations

import os
import platform
import re
import struct
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
AUTH_ENTRA_PASSWORD = "entra_password"

ALLOWED_AUTH_MODES = (
    AUTH_SQL,
    AUTH_ENTRA_DEFAULT,
    AUTH_ENTRA_MI,
    AUTH_ENTRA_SERVICE_PRINCIPAL,
    AUTH_ENTRA_INTERACTIVE,
    AUTH_ENTRA_PASSWORD,
)

# Map Kontra auth mode -> ODBC "Authentication=" keyword.
_ENTRA_ODBC_AUTH = {
    AUTH_ENTRA_DEFAULT: "ActiveDirectoryDefault",
    AUTH_ENTRA_MI: "ActiveDirectoryMsi",
    AUTH_ENTRA_SERVICE_PRINCIPAL: "ActiveDirectoryServicePrincipal",
    AUTH_ENTRA_INTERACTIVE: "ActiveDirectoryInteractive",
    AUTH_ENTRA_PASSWORD: "ActiveDirectoryPassword",
}

# Entra auth modes where a token is acquired by azure-identity rather than
# ridden through user/password fields. On Windows the msodbcsql18 driver REJECTS
# the "Authentication=ActiveDirectory*" keywords for these modes (they are
# Linux/macOS-only driver keywords), so on Windows we acquire the token with
# azure-identity and hand it to pyodbc via attrs_before instead.
# (entra_interactive and entra_password keep the Authentication= keyword path on
# every platform.)
_ENTRA_TOKEN_MODES = (
    AUTH_ENTRA_DEFAULT,
    AUTH_ENTRA_MI,
    AUTH_ENTRA_SERVICE_PRINCIPAL,
)

# SQL_COPT_SS_ACCESS_TOKEN: pyodbc pre-connect attribute for an Azure AD access
# token. The AAD scope for Azure SQL Database / Managed Instance.
_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AZURE_SQL_SCOPE = "https://database.windows.net/.default"


def _is_windows() -> bool:
    """Return True when running on Windows (patch point for tests)."""
    return platform.system() == "Windows"


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

    # Entra username/password (ActiveDirectoryPassword): the Entra UPN and
    # password ride the ordinary user/password fields (URI userinfo,
    # MSSQL_USER/MSSQL_PASSWORD, or datasource config).
    if auth == AUTH_ENTRA_PASSWORD:
        if not params.user or not params.password:
            raise ValueError(
                "auth='entra_password' requires a username (Entra UPN, e.g. "
                "user@tenant.com) and password — set them in the URI, the "
                "datasource config, or MSSQL_USER/MSSQL_PASSWORD"
            )
        parts.append(f"UID={_odbc_escape(params.user)}")
        parts.append(f"PWD={_odbc_escape(params.password)}")

    return ";".join(parts) + ";"


def build_token_connection_string(
    driver: str,
    params: SqlServerConnectionParams,
) -> str:
    """
    Build an ODBC connection string for the Windows access-token path.

    This is used only when Kontra acquires an Entra ID token itself (Windows +
    a token auth mode) and passes it via pyodbc ``attrs_before``. It deliberately
    contains NO ``Authentication=`` keyword (msodbcsql18 rejects the
    ActiveDirectory* keywords on Windows) and no UID/PWD - the token carries the
    identity. Encrypt=yes is mandatory for Azure SQL / Managed Instance.
    """
    parts = [
        f"Driver={{{driver}}}",
        f"Server={_odbc_escape(params.host)},{int(params.port)}",
        f"Database={_odbc_escape(params.database)}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
    ]
    return ";".join(parts) + ";"


def _encode_access_token(token: str) -> bytes:
    """
    Encode an Entra ID access token into the pyodbc SQL_COPT_SS_ACCESS_TOKEN
    struct: a little-endian int32 length prefix followed by the UTF-16-LE token.
    """
    token_bytes = token.encode("utf-16-le")
    return struct.pack("<i", len(token_bytes)) + token_bytes


def _acquire_entra_token(params: SqlServerConnectionParams, auth: str) -> str:
    """
    Acquire an Azure AD access token for Azure SQL using azure-identity.

    Credential selection mirrors the ODBC Authentication modes:
      entra_default            -> DefaultAzureCredential
      entra_mi                 -> ManagedIdentityCredential (client_id if given)
      entra_service_principal  -> ClientSecretCredential(tenant, client, secret)

    Raises ImportError (Windows-specific, actionable) if azure-identity is not
    installed, and ValueError if a service principal is missing credentials.
    """
    try:
        from azure.identity import (
            ClientSecretCredential,
            DefaultAzureCredential,
            ManagedIdentityCredential,
        )
    except ImportError as e:
        raise ImportError(
            "On Windows, Entra token authentication "
            "(auth='entra_default' / 'entra_mi' / 'entra_service_principal') "
            "requires the 'azure-identity' package. The Microsoft ODBC driver's "
            "'Authentication=ActiveDirectory*' keywords are Linux/macOS-only, so "
            "on Windows Kontra must acquire the access token itself and pass it "
            "to the driver.\n"
            "Install it with: pip install 'kontra[sqlserver-entra]'\n"
            "  (or: pip install azure-identity)\n"
            "Alternatively, use auth='entra_password' (works on Windows without "
            "azure-identity)."
        ) from e

    if auth == AUTH_ENTRA_DEFAULT:
        credential = DefaultAzureCredential()
    elif auth == AUTH_ENTRA_MI:
        credential = (
            ManagedIdentityCredential(client_id=params.client_id)
            if params.client_id
            else ManagedIdentityCredential()
        )
    elif auth == AUTH_ENTRA_SERVICE_PRINCIPAL:
        if not (params.tenant_id and params.client_id and params.client_secret):
            raise ValueError(
                "auth='entra_service_principal' on Windows requires tenant_id, "
                "client_id and client_secret (set them in the URI query, the "
                "datasource config, or AZURE_TENANT_ID / AZURE_CLIENT_ID / "
                "AZURE_CLIENT_SECRET)."
            )
        credential = ClientSecretCredential(
            tenant_id=params.tenant_id,
            client_id=params.client_id,
            client_secret=params.client_secret,
        )
    else:  # pragma: no cover - guarded by _ENTRA_TOKEN_MODES at call site
        raise ValueError(f"Auth mode {auth!r} does not use token acquisition.")

    return credential.get_token(_AZURE_SQL_SCOPE).token


def _connect_entra_windows_token(pyodbc, driver: str, params: SqlServerConnectionParams, auth: str):
    """
    Windows token path: acquire an Entra ID token via azure-identity and pass it
    to pyodbc through attrs_before (SQL_COPT_SS_ACCESS_TOKEN), with a connection
    string that omits the Authentication= keyword.
    """
    token = _acquire_entra_token(params, auth)
    token_struct = _encode_access_token(token)
    conn_str = build_token_connection_string(driver, params)

    try:
        return pyodbc.connect(
            conn_str,
            attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        )
    except pyodbc.Error as e:
        detail = (
            f"SQL Server Entra ID connection failed (Windows token path): {e}\n\n"
            f"Connection details:\n"
            f"  Host: {params.host}:{params.port}\n"
            f"  Database: {params.database}\n"
            f"  Auth: {auth}\n"
            f"  Driver: {driver}\n"
        )
        if params.client_id:
            detail += f"  Client ID: {params.client_id}\n"
        detail += (
            "\nEnsure this host has an Entra identity with access to the "
            "database, that the identity is mapped to a database user, and that "
            "the ODBC driver (msodbcsql18) is installed."
        )
        raise ConnectionError(detail) from e


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

    # Windows: the ActiveDirectory* Authentication keywords are not supported by
    # msodbcsql18. Acquire the token via azure-identity and pass it through
    # attrs_before instead. Non-Windows keeps the Authentication= string path.
    if _is_windows() and auth in _ENTRA_TOKEN_MODES:
        return _connect_entra_windows_token(pyodbc, driver, params, auth)

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
