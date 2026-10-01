# src/kontra/connectors/trino.py
"""
Trino connection utilities for Kontra.

Trino is a distributed SQL query engine; a table lives in a catalog (a
connector such as Iceberg, Hive or PostgreSQL), so the path carries three
parts.

URI form:
    trino://user[:password]@host:8080/catalog/schema.table
    trinos://user:password@host:443/catalog/schema.table   (HTTPS)

``catalog/schema/table`` is accepted as well. A password enables HTTP basic
authentication, which Trino only accepts over HTTPS (``trinos://``).

Owned connections run with the session time zone set to UTC, so naive
timestamps compare the way the Polars tier reads them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from kontra.connectors.db_utils import mask_credentials

_DEFAULT_HTTP_PORT = 8080
_DEFAULT_HTTPS_PORT = 443


@dataclass
class TrinoConnectionParams:
    """Resolved Trino connection parameters."""

    host: str
    port: int
    user: str
    password: str | None
    catalog: str
    schema: str
    table: str
    secure: bool = False

    @property
    def database(self) -> str:
        """The catalog, under the name the other database backends use."""
        return self.catalog

    def connect_kwargs(self) -> dict:
        """Keyword arguments for ``trino.dbapi.connect()`` (without auth)."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "catalog": self.catalog,
            "schema": self.schema,
            "http_scheme": "https" if self.secure else "http",
            "timezone": "UTC",
            "source": "kontra",
        }


def resolve_connection_params(uri: str) -> TrinoConnectionParams:
    """
    Parse a ``trino://`` (or ``trinos://`` for HTTPS) URI into params.

    Raises:
        ValueError: If the URI is missing a catalog, schema or table.
    """
    parsed = urlparse(uri)
    secure = parsed.scheme.lower() == "trinos"

    host = parsed.hostname or "localhost"
    port = parsed.port or (_DEFAULT_HTTPS_PORT if secure else _DEFAULT_HTTP_PORT)
    user = unquote(parsed.username) if parsed.username else "kontra"
    password = unquote(parsed.password) if parsed.password else None

    # Path: /<catalog>/<schema>.<table>  or  /<catalog>/<schema>/<table>
    parts = [unquote(p) for p in (parsed.path or "").split("/") if p]
    if len(parts) == 2 and "." in parts[1]:
        schema, table = parts[1].split(".", 1)
        parts = [parts[0], schema, table]
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            "Trino URI must include a catalog, schema and table: "
            "trino://user@host:8080/catalog/schema.table "
            f"(got: {mask_credentials(uri)!r})"
        )
    catalog, schema, table = parts

    return TrinoConnectionParams(
        host=host,
        port=port,
        user=user,
        password=password,
        catalog=catalog,
        schema=schema,
        table=table,
        secure=secure,
    )


def get_connection(params: TrinoConnectionParams) -> Any:
    """
    Create a Trino DBAPI connection from resolved parameters.

    Returns:
        trino.dbapi.Connection (usable as a context manager)
    """
    try:
        import trino
    except ImportError as e:
        raise ImportError(
            "Trino support requires 'trino'.\nInstall with: pip install 'kontra[trino]'"
        ) from e

    kwargs = params.connect_kwargs()
    if params.password:
        kwargs["auth"] = trino.auth.BasicAuthentication(params.user, params.password)
    return trino.dbapi.connect(**kwargs)
