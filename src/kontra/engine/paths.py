# src/kontra/engine/paths.py
"""
Lightweight execution path detection.

This module determines which execution path to use (database, file, dataframe)
WITHOUT importing heavy dependencies like polars, duckdb, or database connectors.

This is the foundation for lazy loading - we detect the path first, then only
import what's needed for that specific path.
"""

from typing import Literal, Any

# Execution path types
ExecutionPath = Literal["database", "file", "dataframe"]


def detect_execution_path(data: Any, *, table: str | None = None) -> ExecutionPath:
    """
    Detect execution path WITHOUT importing heavy dependencies.

    This function must be fast and import-free. It uses string checks and
    type name inspection to avoid importing polars, duckdb, psycopg2, etc.

    Args:
        data: The data input (URI string, DataFrame, connection, etc.)
        table: Optional table name (indicates BYOC pattern if present with connection)

    Returns:
        "database" - PostgreSQL, SQL Server, or BYOC connection
        "file" - Local file, S3, Azure ADLS, HTTP
        "dataframe" - Polars/Pandas DataFrame, list[dict], dict

    Examples:
        >>> detect_execution_path("postgres://localhost/db/users")
        'database'
        >>> detect_execution_path("s3://bucket/data.parquet")
        'file'
        >>> detect_execution_path(pl.DataFrame({"a": [1, 2, 3]}))
        'dataframe'
    """
    # 1. Check for DataFrame by type name (avoid importing polars/pandas)
    type_name = type(data).__name__
    module_name = type(data).__module__

    # Polars DataFrame/LazyFrame
    if module_name.startswith("polars") and type_name in ("DataFrame", "LazyFrame"):
        return "dataframe"

    # Pandas DataFrame
    if module_name.startswith("pandas") and type_name == "DataFrame":
        return "dataframe"

    # 2. Check for list[dict] or dict (inline data)
    if isinstance(data, (list, dict)) and not isinstance(data, str):
        return "dataframe"

    # 3. Check for database connection objects (BYOC pattern)
    if _is_database_connection(data):
        return "database"

    # 4. String URI checks
    if isinstance(data, str):
        return _detect_path_from_uri(data)

    # 5. Default to dataframe (will error later if invalid)
    return "dataframe"


def _detect_path_from_uri(uri: str) -> ExecutionPath:
    """
    Detect execution path from a URI string.

    Args:
        uri: URI or file path string

    Returns:
        ExecutionPath based on URI scheme/pattern
    """
    uri_lower = uri.lower().strip()

    # Database URIs
    if uri_lower.startswith(("postgres://", "postgresql://")):
        return "database"
    if uri_lower.startswith(("mssql://", "sqlserver://")):
        return "database"

    # Named datasource pattern: "datasource_name.table"
    # These could be database or file - need to resolve via config
    # For now, treat as file (config resolution happens later)
    if "." in uri and not uri_lower.startswith(("http://", "https://", "s3://", "az://", "abfs://", "abfss://", "gs://")):
        # Could be "prod_db.users" (database) or "data.parquet" (file)
        # Check for file extensions
        if _has_file_extension(uri_lower):
            return "file"
        # Assume named datasource - will be resolved later
        # For lazy loading purposes, we need to resolve this
        return _resolve_named_datasource_path(uri)

    # Everything else is a file (local, S3, Azure, HTTP, etc.)
    return "file"


def _has_file_extension(uri: str) -> bool:
    """Check if URI has a known data file extension."""
    extensions = (
        ".parquet", ".pq",
        ".csv", ".tsv",
        ".json", ".ndjson", ".jsonl",
        ".arrow", ".feather", ".ipc",
    )
    return any(uri.endswith(ext) for ext in extensions)


def _is_database_connection(obj: Any) -> bool:
    """
    Check if object is a database connection WITHOUT importing DB libraries.

    Uses duck typing and module name checks to avoid imports.
    """
    type_name = type(obj).__name__
    module_name = type(obj).__module__

    # psycopg2 connection
    if module_name.startswith("psycopg2") and "connection" in type_name.lower():
        return True

    # psycopg3 connection
    if module_name.startswith("psycopg") and "Connection" in type_name:
        return True

    # pymssql connection
    if module_name.startswith("pymssql") and "Connection" in type_name:
        return True

    # pyodbc connection
    if module_name == "pyodbc" and type_name == "Connection":
        return True

    # SQLAlchemy connection/engine
    if "sqlalchemy" in module_name.lower():
        if "Connection" in type_name or "Engine" in type_name:
            return True

    # Generic check: has cursor() method and looks like a DB connection
    if hasattr(obj, "cursor") and hasattr(obj, "commit"):
        return True

    return False


def _resolve_named_datasource_path(reference: str) -> ExecutionPath:
    """
    Resolve a named datasource reference to determine its execution path.

    This imports the config module (lightweight) to resolve the datasource.
    If resolution fails, defaults to "file".

    Args:
        reference: Named datasource like "prod_db.users"

    Returns:
        ExecutionPath based on resolved URI
    """
    try:
        from kontra.config.settings import resolve_datasource
        resolved_uri = resolve_datasource(reference)
        # Recursively detect path from resolved URI
        return _detect_path_from_uri(resolved_uri)
    except (ValueError, ImportError, KeyError):
        # Not a named datasource or config not available
        # Default to file - will error later if invalid
        return "file"


def get_database_type(uri: str) -> Literal["postgres", "sqlserver"] | None:
    """
    Get the specific database type from a URI.

    Args:
        uri: Database URI string

    Returns:
        "postgres", "sqlserver", or None if not a database URI
    """
    uri_lower = uri.lower().strip()

    if uri_lower.startswith(("postgres://", "postgresql://")):
        return "postgres"
    if uri_lower.startswith(("mssql://", "sqlserver://")):
        return "sqlserver"

    return None
