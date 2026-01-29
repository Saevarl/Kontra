# src/kontra/preplan/postgres.py
"""
PostgreSQL preplan - use pg_stats for metadata-only rule decisions.

Similar to Parquet metadata preplan, but uses PostgreSQL's statistics catalog.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection

from .types import PrePlan, Decision


# Predicate format: (rule_id, column, op, value)
Predicate = Tuple[str, str, str, Any]


def _pg_dtype_matches(pg_type: str, udt_name: str, expected: str) -> bool:
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

    # Integer family
    if expected in ("int", "integer"):
        return pg_type in ("integer", "smallint", "bigint") or udt_name in ("int2", "int4", "int8")
    if expected == "int8" or expected == "int16":
        return pg_type == "smallint" or udt_name == "int2"
    if expected == "int32":
        return pg_type == "integer" or udt_name == "int4"
    if expected == "int64":
        return pg_type == "bigint" or udt_name == "int8"

    # Float family
    if expected in ("float", "float64", "double"):
        return pg_type in ("double precision", "real", "numeric") or udt_name in ("float4", "float8", "numeric")
    if expected == "float32":
        return pg_type == "real" or udt_name == "float4"
    if expected == "numeric":
        return pg_type in ("integer", "smallint", "bigint", "double precision", "real", "numeric")

    # String family
    if expected in ("string", "str", "utf8", "text"):
        return pg_type in ("character varying", "text", "character", "varchar", "char") or udt_name in ("varchar", "text", "bpchar")

    # Boolean
    if expected in ("bool", "boolean"):
        return pg_type == "boolean" or udt_name == "bool"

    # Date/time
    if expected == "date":
        return pg_type == "date" or udt_name == "date"
    if expected == "datetime":
        return pg_type.startswith("timestamp") or udt_name.startswith("timestamp")
    if expected == "time":
        return pg_type.startswith("time") or udt_name.startswith("time")

    # Exact match fallback
    return expected == pg_type or expected == udt_name


def fetch_pg_stats_for_preplan(
    params: PostgresConnectionParams,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch PostgreSQL statistics for preplan decisions.

    Returns:
        {
            "__table__": {"row_estimate": int, "page_count": int},
            "column_name": {
                "null_frac": float,      # 0.0-1.0 fraction of nulls
                "n_distinct": float,     # -1 = unique, >0 = count, <0 = fraction
                "most_common_vals": str, # Array literal
            },
            ...
        }
    """
    with get_connection(params) as conn:
        with conn.cursor() as cur:
            # Table-level stats from pg_class
            cur.execute(
                """
                SELECT reltuples::bigint AS row_estimate,
                       relpages AS page_count
                FROM pg_class
                WHERE relname = %s
                  AND relnamespace = %s::regnamespace
                """,
                (params.table, params.schema),
            )
            row = cur.fetchone()
            table_stats = {
                "row_estimate": row[0] if row else 0,
                "page_count": row[1] if row else 0,
            }

            # Column-level stats from pg_stats joined with column types
            cur.execute(
                """
                SELECT
                    c.column_name,
                    s.null_frac,
                    s.n_distinct,
                    s.most_common_vals::text,
                    c.data_type,
                    c.udt_name
                FROM information_schema.columns c
                LEFT JOIN pg_stats s
                    ON s.schemaname = c.table_schema
                    AND s.tablename = c.table_name
                    AND s.attname = c.column_name
                WHERE c.table_schema = %s AND c.table_name = %s
                """,
                (params.schema, params.table),
            )

            result: Dict[str, Dict[str, Any]] = {"__table__": table_stats}
            for col_row in cur.fetchall():
                col_name, null_frac, n_distinct, mcv, data_type, udt_name = col_row
                result[col_name] = {
                    "null_frac": null_frac,
                    "n_distinct": n_distinct,
                    "most_common_vals": mcv,
                    "data_type": data_type,
                    "udt_name": udt_name,
                }

            return result


def preplan_postgres(
    handle: DatasetHandle,
    required_columns: List[str],
    predicates: List[Predicate],
) -> PrePlan:
    """
    Metadata-only pre-planner for PostgreSQL tables using pg_stats.

    Supports decisions for:
      - not_null: if null_frac == 0 -> pass_meta
      - unique: if n_distinct == -1 -> pass_meta (PostgreSQL's way of saying "all unique")

    Args:
        handle: DatasetHandle with db_params
        required_columns: Columns needed for validation
        predicates: List of (rule_id, column, op, value) tuples

    Returns:
        PrePlan with rule decisions based on pg_stats
    """
    if not handle.db_params:
        raise ValueError("PostgreSQL handle missing db_params")

    params: PostgresConnectionParams = handle.db_params
    pg_stats = fetch_pg_stats_for_preplan(params)

    table_stats = pg_stats.get("__table__", {})
    row_estimate = table_stats.get("row_estimate", 0)

    rule_decisions: Dict[str, Decision] = {}
    fail_details: Dict[str, Dict[str, Any]] = {}

    for rule_id, column, op, value in predicates:
        col_stats = pg_stats.get(column)

        if col_stats is None:
            # No stats for this column (ANALYZE not run or column doesn't exist)
            rule_decisions[rule_id] = "unknown"
            continue

        null_frac = col_stats.get("null_frac")
        n_distinct = col_stats.get("n_distinct")

        if op == "not_null":
            # If null_frac is exactly 0, the column has no nulls
            if null_frac is not None and null_frac == 0:
                rule_decisions[rule_id] = "pass_meta"
            elif null_frac is not None and null_frac > 0:
                # We know there ARE nulls, but pg_stats doesn't give exact count
                # So we can't prove pass, but we know the rule will likely fail
                # Be conservative: mark as unknown (actual execution will determine)
                rule_decisions[rule_id] = "unknown"
            else:
                rule_decisions[rule_id] = "unknown"

        elif op == "unique":
            # n_distinct == -1 means PostgreSQL detected all values are unique
            # n_distinct < 0 (other than -1) means n_distinct is a fraction of rows
            if n_distinct is not None:
                if n_distinct == -1:
                    # All values are unique AND no nulls (unique constraint behavior)
                    if null_frac == 0:
                        rule_decisions[rule_id] = "pass_meta"
                    else:
                        # Unique values but has nulls - need to check if nulls cause dups
                        rule_decisions[rule_id] = "unknown"
                elif n_distinct < 0:
                    # Fraction - close to -1 means high uniqueness but not guaranteed
                    rule_decisions[rule_id] = "unknown"
                else:
                    # n_distinct > 0: exact count or estimate
                    # If n_distinct equals row_estimate, likely unique
                    if row_estimate > 0 and n_distinct >= row_estimate:
                        rule_decisions[rule_id] = "pass_meta"
                    else:
                        rule_decisions[rule_id] = "unknown"
            else:
                rule_decisions[rule_id] = "unknown"

        elif op == "dtype":
            # Check column type from information_schema
            data_type = col_stats.get("data_type")
            udt_name = col_stats.get("udt_name")
            if data_type is not None:
                if _pg_dtype_matches(data_type, udt_name, value):
                    rule_decisions[rule_id] = "pass_meta"
                else:
                    rule_decisions[rule_id] = "fail_meta"
                    # Use udt_name if available, else data_type for actual
                    actual = udt_name if udt_name else data_type
                    fail_details[rule_id] = {"expected": value, "actual": actual}
            else:
                rule_decisions[rule_id] = "unknown"

        else:
            # Other ops (>=, <=, ==, etc.) - pg_stats doesn't have min/max for general use
            # (histogram_bounds could be used but it's complex)
            rule_decisions[rule_id] = "unknown"

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],  # Not applicable for PostgreSQL
        rule_decisions=rule_decisions,
        stats={
            "total_rows": row_estimate,  # Use total_rows for consistency with engine
            "row_estimate": row_estimate,  # Keep for backwards compatibility
            "columns_with_stats": len([k for k in pg_stats if k != "__table__"]),
        },
        fail_details=fail_details,
    )


def can_preplan_postgres(handle: DatasetHandle) -> bool:
    """Check if PostgreSQL preplan is applicable for this handle."""
    return handle.scheme in ("postgres", "postgresql") and handle.db_params is not None
