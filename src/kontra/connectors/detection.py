# src/kontra/connectors/detection.py
"""
Connection type detection for BYOC (Bring Your Own Connection) pattern.

Detects database dialect from connection objects so Kontra can use the
correct SQL executor and materializer.

Supported connection types:
- psycopg / psycopg2 / psycopg3 → PostgreSQL
- pg8000 → PostgreSQL
- pyodbc → SQL Server (or detected via getinfo)
- pymssql → SQL Server
- SQLAlchemy engine/connection → detected from dialect
"""

from __future__ import annotations

from typing import Any, Tuple, Optional


# Dialect constants
POSTGRESQL = "postgresql"
SQLSERVER = "sqlserver"


def detect_connection_dialect(conn: Any) -> str:
    """
    Detect database dialect from a connection object.

    Args:
        conn: A database connection object (psycopg, pyodbc, SQLAlchemy, etc.)

    Returns:
        Dialect string: "postgresql" or "sqlserver"

    Raises:
        ValueError: If connection type cannot be detected

    Examples:
        >>> import psycopg2
        >>> conn = psycopg2.connect(...)
        >>> detect_connection_dialect(conn)
        'postgresql'

        >>> import pyodbc
        >>> conn = pyodbc.connect("DRIVER={ODBC Driver 17};SERVER=...")
        >>> detect_connection_dialect(conn)
        'sqlserver'
    """
    module = type(conn).__module__
    class_name = type(conn).__name__

    # PostgreSQL drivers
    if module.startswith("psycopg"):
        return POSTGRESQL
    if module.startswith("pg8000"):
        return POSTGRESQL
    if "postgres" in module.lower():
        return POSTGRESQL

    # SQL Server drivers
    if module.startswith("pymssql"):
        return SQLSERVER

    # pyodbc - generic ODBC, need to inspect
    if module == "pyodbc":
        return _detect_pyodbc_dialect(conn)

    # SQLAlchemy
    if module.startswith("sqlalchemy"):
        return _detect_sqlalchemy_dialect(conn)

    raise ValueError(
        f"Unknown connection type: {module}.{class_name}\n\n"
        "Supported connection types:\n"
        "  - psycopg / psycopg2 / psycopg3 (PostgreSQL)\n"
        "  - pg8000 (PostgreSQL)\n"
        "  - pyodbc (SQL Server, PostgreSQL via ODBC)\n"
        "  - pymssql (SQL Server)\n"
        "  - SQLAlchemy engine or connection"
    )


def _detect_pyodbc_dialect(conn: Any) -> str:
    """
    Detect dialect from a pyodbc connection.

    pyodbc is generic - it can connect to SQL Server, PostgreSQL, MySQL, etc.
    We use getinfo(SQL_DBMS_NAME) to detect the actual database.
    """
    try:
        import pyodbc
        dbms_name = conn.getinfo(pyodbc.SQL_DBMS_NAME).lower()

        if "sql server" in dbms_name or "microsoft" in dbms_name:
            return SQLSERVER
        if "postgres" in dbms_name:
            return POSTGRESQL

        # Default for pyodbc (most common use case)
        return SQLSERVER

    except (AttributeError, TypeError, ValueError):
        # If getinfo fails, assume SQL Server (most common pyodbc use)
        return SQLSERVER


def _detect_sqlalchemy_dialect(conn: Any) -> str:
    """
    Detect dialect from a SQLAlchemy engine or connection.

    SQLAlchemy connections/engines have a dialect attribute that tells us
    the database type.
    """
    dialect_name = None

    # SQLAlchemy Engine
    if hasattr(conn, "dialect"):
        dialect_name = conn.dialect.name

    # SQLAlchemy Connection (has engine attribute)
    elif hasattr(conn, "engine") and hasattr(conn.engine, "dialect"):
        dialect_name = conn.engine.dialect.name

    if dialect_name:
        dialect_lower = dialect_name.lower()
        if "postgres" in dialect_lower:
            return POSTGRESQL
        if "mssql" in dialect_lower or "sqlserver" in dialect_lower:
            return SQLSERVER

        raise ValueError(
            f"Unsupported SQLAlchemy dialect: {dialect_name}\n\n"
            "Supported dialects: postgresql, mssql"
        )

    raise ValueError(
        "Could not detect dialect from SQLAlchemy connection.\n"
        "Make sure you're passing an Engine or Connection object."
    )


def is_cursor_object(obj: Any) -> bool:
    """
    Check if an object appears to be a database cursor (not a connection).

    Cursors are returned by connection.cursor() and have execute/fetch methods
    but NOT a cursor() method themselves.

    This helps catch a common mistake: passing cursor instead of connection.

    Args:
        obj: Any Python object

    Returns:
        True if the object appears to be a database cursor
    """
    if obj is None:
        return False

    class_name = type(obj).__name__.lower()

    # Explicit cursor class names
    if "cursor" in class_name:
        return True

    # Has execute/fetchone but NOT cursor() method = likely a cursor
    has_execute = hasattr(obj, "execute") and callable(getattr(obj, "execute", None))
    has_fetch = hasattr(obj, "fetchone") and callable(getattr(obj, "fetchone", None))
    has_cursor_method = hasattr(obj, "cursor") and callable(getattr(obj, "cursor", None))

    if has_execute and has_fetch and not has_cursor_method:
        return True

    return False


def is_database_connection(obj: Any) -> bool:
    """
    Check if an object appears to be a database connection.

    This is a heuristic check - we look for common connection attributes
    and module names.

    Args:
        obj: Any Python object

    Returns:
        True if the object appears to be a database connection
    """
    if obj is None:
        return False

    # First check it's not a cursor
    if is_cursor_object(obj):
        return False

    module = type(obj).__module__

    # Known database driver modules
    known_modules = (
        "psycopg",
        "psycopg2",
        "pg8000",
        "pyodbc",
        "pymssql",
        "sqlalchemy",
    )

    for known in known_modules:
        if module.startswith(known):
            return True

    # Check for common connection attributes
    if hasattr(obj, "cursor") and callable(getattr(obj, "cursor", None)):
        return True

    # SQLAlchemy engine
    if hasattr(obj, "dialect") and hasattr(obj, "connect"):
        return True

    return False


def is_sqlalchemy_object(obj: Any) -> bool:
    """
    Check if an object is a SQLAlchemy Engine or Connection.

    Args:
        obj: Any Python object

    Returns:
        True if the object is a SQLAlchemy engine or connection
    """
    module = type(obj).__module__
    return module.startswith("sqlalchemy")


def unwrap_sqlalchemy_connection(obj: Any) -> Any:
    """
    Extract the raw DBAPI connection from a SQLAlchemy Engine or Connection.

    SQLAlchemy objects don't have a .cursor() method, but we can get the
    underlying DBAPI connection which does.

    Args:
        obj: A SQLAlchemy Engine or Connection object

    Returns:
        The raw DBAPI connection object

    Raises:
        ValueError: If the object cannot be unwrapped
    """
    # If it's not SQLAlchemy, return as-is
    if not is_sqlalchemy_object(obj):
        return obj

    # SQLAlchemy 2.x Engine - use raw_connection()
    if hasattr(obj, "raw_connection"):
        return obj.raw_connection()

    # SQLAlchemy 2.x Connection - get underlying connection
    if hasattr(obj, "connection"):
        dbapi_conn = obj.connection
        # In SQLAlchemy 2.x, this might be another wrapper
        if hasattr(dbapi_conn, "dbapi_connection"):
            return dbapi_conn.dbapi_connection
        return dbapi_conn

    # SQLAlchemy 1.x Engine
    if hasattr(obj, "connect"):
        sa_conn = obj.connect()
        if hasattr(sa_conn, "connection"):
            return sa_conn.connection
        return sa_conn

    raise ValueError(
        f"Cannot extract DBAPI connection from SQLAlchemy object: {type(obj).__name__}\n"
        "Try passing engine.raw_connection() instead."
    )


def parse_table_reference(table: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Parse a table reference into (database, schema, table) components.

    Formats:
        - "table" → (None, None, "table")
        - "schema.table" → (None, "schema", "table")
        - "database.schema.table" → ("database", "schema", "table")

    Args:
        table: Table reference string

    Returns:
        Tuple of (database, schema, table_name)

    Raises:
        ValueError: If table reference has too many parts
    """
    parts = table.split(".")

    if len(parts) == 1:
        return None, None, parts[0]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    elif len(parts) == 3:
        return parts[0], parts[1], parts[2]
    else:
        raise ValueError(
            f"Invalid table reference: {table}\n\n"
            "Expected format: table, schema.table, or database.schema.table"
        )


def get_default_schema(dialect: str) -> str:
    """Get the default schema for a dialect."""
    if dialect == POSTGRESQL:
        return "public"
    elif dialect == SQLSERVER:
        return "dbo"
    return "public"
