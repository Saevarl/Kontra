# src/kontra/scout/dtype_mapping.py
"""
Unified dtype mapping for Scout profiler.

Provides consistent type normalization across all backends (DuckDB, PostgreSQL, SQL Server).
This module consolidates dtype mappings that were previously duplicated across backend files.
"""

from __future__ import annotations

from typing import Dict

# Normalized type categories
NUMERIC_TYPES = {"int", "float"}
TEMPORAL_TYPES = {"date", "time", "datetime", "interval"}
STRING_TYPES = {"string"}
BOOLEAN_TYPES = {"bool"}
BINARY_TYPES = {"binary"}

# Master dtype mapping (lowercase keys for case-insensitive lookup)
# Maps raw database types to normalized Kontra types
DTYPE_MAP: Dict[str, str] = {
    # Integer types (common)
    "tinyint": "int",
    "smallint": "int",
    "integer": "int",
    "int": "int",
    "bigint": "int",
    "hugeint": "int",
    "int2": "int",
    "int4": "int",
    "int8": "int",
    "int16": "int",
    "int32": "int",
    "int64": "int",
    "int128": "int",
    "serial": "int",
    "bigserial": "int",
    # Unsigned integers (DuckDB)
    "utinyint": "int",
    "usmallint": "int",
    "uinteger": "int",
    "ubigint": "int",
    "uint8": "int",
    "uint16": "int",
    "uint32": "int",
    "uint64": "int",
    # Float types (common)
    "float": "float",
    "float4": "float",
    "float8": "float",
    "real": "float",
    "double": "float",
    "double precision": "float",
    "decimal": "float",
    "numeric": "float",
    # Float types (SQL Server)
    "money": "float",
    "smallmoney": "float",
    # Boolean types
    "boolean": "bool",
    "bool": "bool",
    "bit": "bool",  # SQL Server
    # String types (common)
    "varchar": "string",
    "char": "string",
    "bpchar": "string",  # PostgreSQL blank-padded char
    "text": "string",
    "string": "string",
    "character varying": "string",
    "character": "string",
    # String types (SQL Server)
    "nvarchar": "string",
    "nchar": "string",
    "ntext": "string",
    # Date types
    "date": "date",
    # Time types
    "time": "time",
    "time without time zone": "time",
    "time with time zone": "time",
    # Datetime types (common)
    "timestamp": "datetime",
    "timestamp with time zone": "datetime",
    "timestamp without time zone": "datetime",
    "timestamptz": "datetime",
    # Datetime types (SQL Server)
    "datetime": "datetime",
    "datetime2": "datetime",
    "smalldatetime": "datetime",
    "datetimeoffset": "datetime",
    # Interval
    "interval": "interval",
    # Binary types (common)
    "blob": "binary",
    "bytea": "binary",  # PostgreSQL
    # Binary types (SQL Server)
    "binary": "binary",
    "varbinary": "binary",
    "image": "binary",
    # UUID / special string types
    "uuid": "string",
    "json": "string",
    "jsonb": "string",
    "uniqueidentifier": "string",  # SQL Server UUID
    "xml": "string",  # SQL Server
}


def normalize_dtype(raw_type: str) -> str:
    """
    Normalize a raw database type to a simplified Kontra type name.

    Args:
        raw_type: Raw type string from database (e.g., "VARCHAR(255)", "BIGINT")

    Returns:
        Normalized type: "int", "float", "string", "bool", "date", "datetime",
                        "time", "interval", "binary", or "unknown"

    Examples:
        >>> normalize_dtype("VARCHAR(255)")
        'string'
        >>> normalize_dtype("DECIMAL(10,2)")
        'float'
        >>> normalize_dtype("bigint")
        'int'
    """
    # Lowercase and strip whitespace for case-insensitive matching
    lower = raw_type.lower().strip()

    # Handle parameterized types like DECIMAL(10,2) or VARCHAR(255)
    base = lower.split("(")[0].strip()

    return DTYPE_MAP.get(base, "unknown")


def is_numeric_type(normalized_type: str) -> bool:
    """Check if a normalized type is numeric."""
    return normalized_type in NUMERIC_TYPES


def is_temporal_type(normalized_type: str) -> bool:
    """Check if a normalized type is temporal (date/time)."""
    return normalized_type in TEMPORAL_TYPES


def is_string_type(normalized_type: str) -> bool:
    """Check if a normalized type is a string."""
    return normalized_type in STRING_TYPES
