# src/kontra/state/backends/__init__.py
"""
State storage backends.

Backends provide pluggable persistence for validation state:
- LocalStore: Filesystem storage in .kontra/state/
- S3Store: S3-compatible object storage
- PostgresStore: PostgreSQL database
- SQLServerStore: SQL Server database
"""

from .base import StateBackend
from .local import LocalStore

# Default store factory
_default_store: LocalStore | None = None


def get_default_store() -> LocalStore:
    """
    Get the default state store.

    Uses .kontra/state/ in the current working directory.
    Lazily initialized on first call.
    """
    global _default_store
    if _default_store is None:
        _default_store = LocalStore()
    return _default_store


def get_store(backend: str = "local") -> StateBackend:
    """
    Get a state store by backend identifier.

    Args:
        backend: Backend identifier. Options:
            - "local" or "": LocalStore (default)
            - "s3://bucket/prefix": S3Store
            - "postgres://..." or "postgresql://...": PostgresStore
            - "mssql://..." or "sqlserver://...": SQLServerStore

    Returns:
        A StateBackend instance

    Environment Variables:
        For S3:
            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL, AWS_REGION

        For PostgreSQL:
            PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, DATABASE_URL

        For SQL Server:
            MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE, MSSQL_DRIVER
    """
    if not backend or backend == "local":
        return get_default_store()

    if backend.startswith("s3://"):
        from .s3 import S3Store
        return S3Store(backend)

    if backend.startswith("postgres://") or backend.startswith("postgresql://"):
        from .postgres import PostgresStore
        return PostgresStore(backend)

    if backend.startswith("mssql://") or backend.startswith("sqlserver://"):
        from .sqlserver import SQLServerStore
        return SQLServerStore(backend)

    raise ValueError(f"Unknown state backend: {backend}")


__all__ = [
    "StateBackend",
    "LocalStore",
    "get_default_store",
    "get_store",
]
