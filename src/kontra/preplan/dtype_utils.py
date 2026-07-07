# src/kontra/preplan/dtype_utils.py
"""
Shared dtype matching for database preplan modules.

Table-driven approach: each dialect provides a type mapping,
and dtype_matches() does the lookup.
"""

from __future__ import annotations

from typing import Dict, List


# PostgreSQL type map: expected -> set of matching pg types (data_type or udt_name)
PG_TYPE_MAP: Dict[str, List[str]] = {
    # Integer family
    "int": ["integer", "smallint", "bigint", "int2", "int4", "int8"],
    "integer": ["integer", "smallint", "bigint", "int2", "int4", "int8"],
    "int8": ["smallint", "int2"],
    "int16": ["smallint", "int2"],
    "int32": ["integer", "int4"],
    "int64": ["bigint", "int8"],
    # Float family
    "float": ["double precision", "real", "numeric", "float4", "float8"],
    "float64": ["double precision", "real", "numeric", "float4", "float8"],
    "double": ["double precision", "real", "numeric", "float4", "float8"],
    "float32": ["real", "float4"],
    "numeric": ["integer", "smallint", "bigint", "double precision", "real", "numeric"],
    # String family
    "string": ["character varying", "text", "character", "varchar", "char", "bpchar"],
    "str": ["character varying", "text", "character", "varchar", "char", "bpchar"],
    "utf8": ["character varying", "text", "character", "varchar", "char", "bpchar"],
    "text": ["character varying", "text", "character", "varchar", "char", "bpchar"],
    # Boolean
    "bool": ["boolean", "bool"],
    "boolean": ["boolean", "bool"],
    # Date/time (prefix-matched separately)
}

# SQL Server type map: expected -> set of matching type names from sys.types
SS_TYPE_MAP: Dict[str, List[str]] = {
    # Integer family
    "int": ["tinyint", "smallint", "int", "bigint"],
    "integer": ["tinyint", "smallint", "int", "bigint"],
    "int8": ["tinyint", "smallint"],
    "int16": ["tinyint", "smallint"],
    "int32": ["int"],
    "int64": ["bigint"],
    # Float family
    "float": ["float", "real", "decimal", "numeric", "money", "smallmoney"],
    "float64": ["float", "real", "decimal", "numeric", "money", "smallmoney"],
    "double": ["float", "real", "decimal", "numeric", "money", "smallmoney"],
    "float32": ["real"],
    "numeric": ["tinyint", "smallint", "int", "bigint", "float", "real", "decimal", "numeric"],
    # String family
    "string": ["char", "varchar", "text", "nchar", "nvarchar", "ntext"],
    "str": ["char", "varchar", "text", "nchar", "nvarchar", "ntext"],
    "utf8": ["char", "varchar", "text", "nchar", "nvarchar", "ntext"],
    "text": ["char", "varchar", "text", "nchar", "nvarchar", "ntext"],
    # Boolean
    "bool": ["bit"],
    "boolean": ["bit"],
    # Date/time
    "date": ["date"],
    "datetime": ["datetime", "datetime2", "smalldatetime", "datetimeoffset"],
    "time": ["time"],
}


def pg_dtype_matches(pg_type: str, udt_name: str, expected: str) -> bool:
    """
    Check if PostgreSQL data type matches expected dtype specification.

    Args:
        pg_type: data_type from information_schema (e.g., 'integer', 'character varying')
        udt_name: udt_name from information_schema (e.g., 'int4', 'varchar')
        expected: User's expected dtype (e.g., 'int', 'string', 'int64')
    """
    pg_type = (pg_type or "").lower()
    udt_name = (udt_name or "").lower()
    expected = expected.lower()

    # Table-driven lookup
    accepted = PG_TYPE_MAP.get(expected)
    if accepted is not None:
        return pg_type in accepted or udt_name in accepted

    # Date/time prefix matching (not table-driven due to prefix semantics)
    if expected == "date":
        return pg_type == "date" or udt_name == "date"
    if expected == "datetime":
        return pg_type.startswith("timestamp") or udt_name.startswith("timestamp")
    if expected == "time":
        return pg_type.startswith("time") or udt_name.startswith("time")

    # Exact match fallback
    return expected == pg_type or expected == udt_name


def ss_dtype_matches(sql_type: str, expected: str) -> bool:
    """
    Check if SQL Server data type matches expected dtype specification.

    Args:
        sql_type: Type name from sys.types (e.g., 'int', 'varchar', 'bigint')
        expected: User's expected dtype (e.g., 'int', 'string', 'int64')
    """
    sql_type = (sql_type or "").lower()
    expected = expected.lower()

    # Table-driven lookup
    accepted = SS_TYPE_MAP.get(expected)
    if accepted is not None:
        return sql_type in accepted

    # Exact match fallback
    return expected == sql_type
