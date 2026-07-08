# src/kontra/state/backends/__init__.py
"""
State storage backends.

Backends provide pluggable persistence for validation state:
- LocalStore: Filesystem storage in .kontra/state/
- S3Store: S3-compatible object storage
- PostgresStore: PostgreSQL database
- SQLServerStore: SQL Server database
"""

import os
import threading

from .base import StateBackend
from .local import LocalStore

# Default store factory.
#
# The default LocalStore resolves its base_path to ``<cwd>/.kontra/state`` at
# construction time. Long-lived processes (services, MCP servers) or tests may
# change the working directory after the first call, so we key the cached
# singleton on the cwd it was built for and rebuild it when the cwd changes.
# Repeated calls from the same directory reuse the cached instance.
_default_store: LocalStore | None = None
_default_store_cwd: str | None = None
_default_store_lock = threading.Lock()


def get_default_store() -> LocalStore:
    """
    Get the default state store.

    Uses .kontra/state/ in the current working directory. The store is cached
    per working directory: if the cwd changes between calls (e.g. in a
    long-lived service or after ``kontra.set_config``), the store is rebuilt so
    reads and writes always target the current directory's state tree.
    """
    global _default_store, _default_store_cwd
    current_cwd = os.getcwd()
    # Fast path: same cwd as the cached store, no locking needed.
    if _default_store is not None and _default_store_cwd == current_cwd:
        return _default_store
    with _default_store_lock:
        # Re-check under the lock in case another thread just rebuilt it.
        if _default_store is None or _default_store_cwd != current_cwd:
            _default_store = LocalStore()
            _default_store_cwd = current_cwd
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
