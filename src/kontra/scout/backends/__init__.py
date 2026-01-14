# src/kontra/scout/backends/__init__.py
"""
Scout profiler backends - pluggable data source adapters.
"""

from .base import ProfilerBackend
from .duckdb_backend import DuckDBBackend

__all__ = ["ProfilerBackend", "DuckDBBackend"]

# PostgreSQL backend (optional - requires psycopg)
try:
    from .postgres_backend import PostgreSQLBackend

    __all__.append("PostgreSQLBackend")
except ImportError:
    pass
