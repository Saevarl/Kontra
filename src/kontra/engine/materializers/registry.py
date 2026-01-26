# src/kontra/engine/materializers/registry.py
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List

from kontra.connectors.handle import DatasetHandle

if TYPE_CHECKING:
    # Import from the new base file
    from .base import BaseMaterializer as Materializer
    from .duckdb import DuckDBMaterializer  # noqa: F401
    from .polars_connector import PolarsConnectorMaterializer  # noqa: F401
    from .postgres import PostgresMaterializer  # noqa: F401
    from .sqlserver import SqlServerMaterializer  # noqa: F401


# Registry: materializer_name -> ctor(handle) function
_MATS: Dict[str, Callable[[DatasetHandle], Materializer]] = {}
# Simple order for picking when multiple can handle a handle
_ORDER: List[str] = []


def register_materializer(name: str):
    """
    Decorator to register a materializer class under a stable name.
    The class must implement the Materializer protocol.
    """

    def deco(cls: Callable[[DatasetHandle], Materializer]) -> Callable[
        [DatasetHandle], Materializer
    ]:
        if name in _MATS:
            raise ValueError(f"Materializer '{name}' is already registered.")
        _MATS[name] = cls
        if name not in _ORDER:
            _ORDER.append(name)
        cls.materializer_name = name  # friendly label for stats.io
        return cls

    return deco


def pick_materializer(handle: DatasetHandle) -> Materializer:
    """
    Choose the best materializer for the given dataset handle.

    Policy (v1.4 - BYOC support):
      - BYOC handles use the materializer matching their dialect.
      - PostgreSQL URIs use the PostgreSQL materializer.
      - SQL Server URIs use the SQL Server materializer.
      - Remote files (s3, http) with known formats use DuckDB materializer.
      - Otherwise, fall back to PolarsConnector materializer.

    This logic is INDEPENDENT of the projection flag.
    """
    # BYOC: route based on dialect
    if handle.scheme == "byoc":
        if handle.dialect == "postgresql":
            ctor = _MATS.get("postgres")
            if ctor:
                return ctor(handle)
            raise RuntimeError(
                "PostgreSQL materializer not registered. "
                "Ensure psycopg is installed: pip install 'psycopg[binary]'"
            )
        elif handle.dialect == "sqlserver":
            ctor = _MATS.get("sqlserver")
            if ctor:
                return ctor(handle)
            raise RuntimeError(
                "SQL Server materializer not registered. "
                "Ensure pymssql is installed: pip install pymssql"
            )
        else:
            raise RuntimeError(
                f"Unsupported BYOC dialect: {handle.dialect}. "
                "Supported: postgresql, sqlserver"
            )

    # PostgreSQL: use dedicated materializer
    if handle.scheme in ("postgres", "postgresql"):
        ctor = _MATS.get("postgres")
        if ctor:
            return ctor(handle)
        raise RuntimeError(
            "PostgreSQL materializer not registered. "
            "Ensure psycopg is installed: pip install 'psycopg[binary]'"
        )

    # SQL Server: use dedicated materializer
    if handle.scheme in ("mssql", "sqlserver"):
        ctor = _MATS.get("sqlserver")
        if ctor:
            return ctor(handle)
        raise RuntimeError(
            "SQL Server materializer not registered. "
            "Ensure pymssql is installed: pip install pymssql"
        )

    # Remote files with known formats: use DuckDB for efficient I/O
    # Includes S3, HTTP(S), and Azure (ADLS Gen2, Azure Blob)
    is_remote = handle.scheme in ("s3", "http", "https", "abfs", "abfss", "az")
    is_known_format = handle.format in ("parquet", "csv")

    if is_remote and is_known_format:
        ctor = _MATS.get("duckdb")
        if ctor:
            return ctor(handle)

    # Fallback for local files or unknown formats
    ctor = _MATS.get("polars-connector")
    if not ctor:
        raise RuntimeError(
            "No default materializer registered (polars-connector missing)"
        )
    return ctor(handle)


def register_default_materializers() -> None:
    """
    Eagerly import built-in materializers so their @register_materializer
    decorators run and populate the registry.

    NOTE: This is the legacy function that loads ALL materializers.
    For lazy loading, use register_materializers_for_path() instead.
    """
    # Local imports to trigger decorator side-effects
    from . import duckdb  # noqa: F401
    from . import polars_connector  # noqa: F401

    # PostgreSQL materializer (optional - requires psycopg)
    try:
        from . import postgres  # noqa: F401
    except ImportError:
        pass  # psycopg not installed, skip postgres materializer

    # SQL Server materializer (optional - requires pymssql)
    try:
        from . import sqlserver  # noqa: F401
    except ImportError:
        pass  # pymssql not installed, skip sqlserver materializer


def register_materializers_for_path(
    execution_path: str,
    database_type: str | None = None,
) -> None:
    """
    Register only the materializers needed for a specific execution path.

    This enables lazy loading - we only import heavy dependencies when needed.

    Args:
        execution_path: "database", "file", or "dataframe"
        database_type: "postgres" or "sqlserver" (required for database path)
    """
    if execution_path == "database":
        # Database path: only load the specific DB materializer
        if database_type == "postgres":
            try:
                from . import postgres  # noqa: F401
            except ImportError:
                raise ImportError(
                    "PostgreSQL support requires psycopg. "
                    "Install with: pip install 'psycopg[binary]'"
                )
        elif database_type == "sqlserver":
            try:
                from . import sqlserver  # noqa: F401
            except ImportError:
                raise ImportError(
                    "SQL Server support requires pymssql. "
                    "Install with: pip install pymssql"
                )
        else:
            raise ValueError(f"Unknown database_type: {database_type}")

    elif execution_path in ("file", "dataframe"):
        # File/DataFrame path: load DuckDB and Polars materializers
        from . import duckdb  # noqa: F401
        from . import polars_connector  # noqa: F401

    else:
        # Unknown path - fall back to loading everything
        register_default_materializers()