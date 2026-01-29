# src/kontra/preplan/sqlserver.py
"""
SQL Server preplan - use metadata for rule decisions.

Uses SQL Server system views:
- sys.dm_db_partition_stats for row count estimates
- sys.columns for nullability constraints
- sys.indexes + sys.index_columns for uniqueness constraints

Note: Unlike PostgreSQL's pg_stats, SQL Server doesn't expose null_frac directly.
We use constraint metadata instead (NOT NULL, UNIQUE indexes).
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection

from .types import PrePlan, Decision


# Predicate format: (rule_id, column, op, value)
Predicate = Tuple[str, str, str, Any]


def _sqlserver_dtype_matches(sql_type: str, expected: str) -> bool:
    """
    Check if SQL Server data type matches expected dtype specification.

    Args:
        sql_type: Type name from sys.types (e.g., 'int', 'varchar', 'bigint')
        expected: User's expected dtype (e.g., 'int', 'string', 'int64')
    """
    sql_type = (sql_type or "").lower()
    expected = expected.lower()

    # Integer family
    if expected in ("int", "integer"):
        return sql_type in ("tinyint", "smallint", "int", "bigint")
    if expected in ("int8", "int16"):
        return sql_type in ("tinyint", "smallint")
    if expected == "int32":
        return sql_type == "int"
    if expected == "int64":
        return sql_type == "bigint"

    # Float family
    if expected in ("float", "float64", "double"):
        return sql_type in ("float", "real", "decimal", "numeric", "money", "smallmoney")
    if expected == "float32":
        return sql_type == "real"
    if expected == "numeric":
        return sql_type in ("tinyint", "smallint", "int", "bigint", "float", "real", "decimal", "numeric")

    # String family
    if expected in ("string", "str", "utf8", "text"):
        return sql_type in ("char", "varchar", "text", "nchar", "nvarchar", "ntext")

    # Boolean
    if expected in ("bool", "boolean"):
        return sql_type == "bit"

    # Date/time
    if expected == "date":
        return sql_type == "date"
    if expected == "datetime":
        return sql_type in ("datetime", "datetime2", "smalldatetime", "datetimeoffset")
    if expected == "time":
        return sql_type == "time"

    # Exact match fallback
    return expected == sql_type


def fetch_sqlserver_metadata(
    params: SqlServerConnectionParams,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch SQL Server metadata for preplan decisions.

    Returns:
        {
            "__table__": {"row_estimate": int, "page_count": int},
            "column_name": {
                "is_nullable": bool,     # From column definition
                "is_identity": bool,     # Identity column
                "has_unique_constraint": bool,  # Unique index or constraint
            },
            ...
        }
    """
    with get_connection(params) as conn:
        cursor = conn.cursor()

        # Table-level stats from sys.dm_db_partition_stats
        cursor.execute(
            """
            SELECT SUM(row_count) AS row_estimate,
                   SUM(used_page_count) AS page_count
            FROM sys.dm_db_partition_stats ps
            JOIN sys.objects o ON ps.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = %s AND o.name = %s AND ps.index_id IN (0, 1)
            """,
            (params.schema, params.table),
        )
        row = cursor.fetchone()
        table_stats = {
            "row_estimate": int(row[0]) if row and row[0] else 0,
            "page_count": int(row[1]) if row and row[1] else 0,
        }

        # Column-level metadata with type information
        cursor.execute(
            """
            SELECT
                c.name AS column_name,
                c.is_nullable,
                c.is_identity,
                t.name AS type_name
            FROM sys.columns c
            JOIN sys.objects o ON c.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE s.name = %s AND o.name = %s
            """,
            (params.schema, params.table),
        )

        result: Dict[str, Dict[str, Any]] = {"__table__": table_stats}
        for col_row in cursor.fetchall():
            col_name, is_nullable, is_identity, type_name = col_row
            result[col_name] = {
                "is_nullable": bool(is_nullable),
                "is_identity": bool(is_identity),
                "has_unique_constraint": False,  # Will be updated below
                "type_name": type_name,
            }

        # Check for unique constraints/indexes
        cursor.execute(
            """
            SELECT c.name AS column_name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.objects o ON i.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = %s AND o.name = %s
              AND (i.is_unique = 1 OR i.is_primary_key = 1)
              AND ic.is_included_column = 0
            GROUP BY c.name
            HAVING COUNT(*) = 1  -- Single-column unique constraint
            """,
            (params.schema, params.table),
        )

        for row in cursor.fetchall():
            col_name = row[0]
            if col_name in result:
                result[col_name]["has_unique_constraint"] = True

        return result


def preplan_sqlserver(
    handle: DatasetHandle,
    required_columns: List[str],
    predicates: List[Predicate],
) -> PrePlan:
    """
    Metadata-only pre-planner for SQL Server tables.

    Supports decisions for:
      - not_null: if column is NOT NULL (is_nullable = 0) -> pass_meta
      - unique: if column has unique index/constraint -> pass_meta

    Args:
        handle: DatasetHandle with db_params
        required_columns: Columns needed for validation
        predicates: List of (rule_id, column, op, value) tuples

    Returns:
        PrePlan with rule decisions based on SQL Server metadata
    """
    if not handle.db_params:
        raise ValueError("SQL Server handle missing db_params")

    params: SqlServerConnectionParams = handle.db_params
    metadata = fetch_sqlserver_metadata(params)

    table_stats = metadata.get("__table__", {})
    row_estimate = table_stats.get("row_estimate", 0)

    rule_decisions: Dict[str, Decision] = {}
    fail_details: Dict[str, Dict[str, Any]] = {}

    for rule_id, column, op, value in predicates:
        col_meta = metadata.get(column)

        if col_meta is None:
            # Column not found in metadata
            rule_decisions[rule_id] = "unknown"
            continue

        is_nullable = col_meta.get("is_nullable", True)
        is_identity = col_meta.get("is_identity", False)
        has_unique = col_meta.get("has_unique_constraint", False)

        if op == "not_null":
            # If column is defined as NOT NULL, it definitely has no nulls
            if not is_nullable:
                rule_decisions[rule_id] = "pass_meta"
            else:
                # Column allows nulls - may or may not have any
                rule_decisions[rule_id] = "unknown"

        elif op == "unique":
            # If column has unique constraint or is identity, it's unique
            if has_unique or is_identity:
                rule_decisions[rule_id] = "pass_meta"
            else:
                rule_decisions[rule_id] = "unknown"

        elif op == "dtype":
            # Check column type from sys.types
            type_name = col_meta.get("type_name")
            if type_name is not None:
                if _sqlserver_dtype_matches(type_name, value):
                    rule_decisions[rule_id] = "pass_meta"
                else:
                    rule_decisions[rule_id] = "fail_meta"
                    fail_details[rule_id] = {"expected": value, "actual": type_name}
            else:
                rule_decisions[rule_id] = "unknown"

        else:
            # Other ops - would need actual data statistics
            rule_decisions[rule_id] = "unknown"

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],  # Not applicable for SQL Server
        rule_decisions=rule_decisions,
        stats={
            "total_rows": row_estimate,  # Use total_rows for consistency with engine
            "row_estimate": row_estimate,  # Keep for backwards compatibility
            "columns_with_metadata": len([k for k in metadata if k != "__table__"]),
        },
        fail_details=fail_details,
    )


def can_preplan_sqlserver(handle: DatasetHandle) -> bool:
    """Check if SQL Server preplan is applicable for this handle."""
    return handle.scheme in ("mssql", "sqlserver") and handle.db_params is not None
