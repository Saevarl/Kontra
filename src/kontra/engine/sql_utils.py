# src/kontra/engine/sql_utils.py
"""
Shared SQL utilities for all database executors.

This module provides dialect-aware SQL escaping and common aggregate
expression builders to reduce code duplication across executors.
"""

from __future__ import annotations

from typing import Any, List, Literal, Optional

Dialect = Literal["duckdb", "postgres", "sqlserver"]


# =============================================================================
# Identifier and Literal Escaping
# =============================================================================

def esc_ident(name: str, dialect: Dialect = "duckdb") -> str:
    """
    Escape a SQL identifier (column name, table name) for the given dialect.

    - DuckDB/PostgreSQL: "name" with " doubled
    - SQL Server: [name] with ] doubled
    """
    if dialect == "sqlserver":
        return "[" + name.replace("]", "]]") + "]"
    else:  # duckdb, postgres
        return '"' + name.replace('"', '""') + '"'


def lit_str(value: str, dialect: Dialect = "duckdb") -> str:
    """
    Escape a string literal for SQL. All dialects use single quotes.
    """
    return "'" + value.replace("'", "''") + "'"


def lit_value(value: Any, dialect: Dialect = "duckdb") -> str:
    """
    Convert a Python value to a SQL literal.
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, str):
        return lit_str(value, dialect)
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return lit_str(str(value), dialect)


# =============================================================================
# Common Aggregate Expression Builders
# =============================================================================

def agg_not_null(col: str, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Count NULL values in a column."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)
    return f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {r}"


def agg_unique(col: str, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Count duplicate values in a column."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)
    return f"(COUNT(*) - COUNT(DISTINCT {c})) AS {r}"


def agg_min_rows(threshold: int, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Check if row count >= threshold. Returns deficit if below."""
    r = esc_ident(rule_id, dialect)
    n = int(threshold)
    if dialect == "sqlserver":
        # SQL Server doesn't have GREATEST
        return f"CASE WHEN COUNT(*) >= {n} THEN 0 ELSE {n} - COUNT(*) END AS {r}"
    else:
        return f"GREATEST(0, {n} - COUNT(*)) AS {r}"


def agg_max_rows(threshold: int, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Check if row count <= threshold. Returns excess if above."""
    r = esc_ident(rule_id, dialect)
    n = int(threshold)
    if dialect == "sqlserver":
        return f"CASE WHEN COUNT(*) <= {n} THEN 0 ELSE COUNT(*) - {n} END AS {r}"
    else:
        return f"GREATEST(0, COUNT(*) - {n}) AS {r}"


def agg_allowed_values(
    col: str, values: List[Any], rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values not in the allowed set."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    val_list = ", ".join(
        lit_str(str(v), dialect) if isinstance(v, str) else str(v)
        for v in values
    )

    if dialect == "sqlserver":
        cast_col = f"CAST({c} AS NVARCHAR(MAX))"
    elif dialect == "postgres":
        cast_col = f"{c}::text"
    else:
        cast_col = c

    return (
        f"SUM(CASE WHEN {c} IS NOT NULL AND {cast_col} NOT IN ({val_list}) "
        f"THEN 1 ELSE 0 END) AS {r}"
    )


def agg_freshness(
    col: str, max_age_seconds: int, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Check if MAX(column) is within max_age_seconds of now."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)
    secs = int(max_age_seconds)

    if dialect == "sqlserver":
        threshold = f"DATEADD(SECOND, -{secs}, GETUTCDATE())"
    else:  # duckdb, postgres use similar syntax
        threshold = f"(NOW() - INTERVAL '{secs} seconds')"

    return f"CASE WHEN MAX({c}) >= {threshold} THEN 0 ELSE 1 END AS {r}"


def agg_range(
    col: str,
    min_val: Optional[Any],
    max_val: Optional[Any],
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """Count values outside [min, max] range. NULLs are failures."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    conditions = []
    if min_val is not None:
        conditions.append(f"{c} < {min_val}")
    if max_val is not None:
        conditions.append(f"{c} > {max_val}")

    out_of_range = " OR ".join(conditions) if conditions else "0=1"

    return (
        f"SUM(CASE WHEN {c} IS NULL OR ({out_of_range}) THEN 1 ELSE 0 END) AS {r}"
    )


def agg_regex(
    col: str, pattern: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values that don't match the regex pattern. NULLs are failures."""
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)
    escaped_pattern = pattern.replace("'", "''")

    if dialect == "sqlserver":
        # SQL Server uses PATINDEX with LIKE-style patterns (limited regex)
        return (
            f"SUM(CASE WHEN {c} IS NULL "
            f"OR PATINDEX('%{escaped_pattern}%', CAST({c} AS NVARCHAR(MAX))) = 0 "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    elif dialect == "postgres":
        # PostgreSQL uses ~ operator for regex
        return (
            f"SUM(CASE WHEN {c} IS NULL "
            f"OR NOT ({c}::text ~ '{escaped_pattern}') "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # duckdb
        # DuckDB uses regexp_matches()
        return (
            f"SUM(CASE WHEN {c} IS NULL "
            f"OR NOT regexp_matches(CAST({c} AS VARCHAR), '{escaped_pattern}') "
            f"THEN 1 ELSE 0 END) AS {r}"
        )


# =============================================================================
# EXISTS Expression Builders (for early-termination patterns)
# =============================================================================

def exists_not_null(
    col: str, rule_id: str, table: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for not_null rule - stops at first NULL found.
    Returns 1 if any NULL exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL) "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL LIMIT 1) AS {r}"
        )


# =============================================================================
# Result Parsing
# =============================================================================

# SQL comparison operators
SQL_OP_MAP = {
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "==": "=",
    "!=": "<>",
}


def agg_compare(
    left: str,
    right: str,
    op: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    Count rows where the comparison fails or either column is NULL.

    Args:
        left: Left column name
        right: Right column name
        op: Comparison operator (>, >=, <, <=, ==, !=)
        rule_id: Rule identifier for alias
        dialect: SQL dialect

    Returns:
        SQL aggregate expression
    """
    l = esc_ident(left, dialect)
    r_col = esc_ident(right, dialect)
    r_id = esc_ident(rule_id, dialect)
    sql_op = SQL_OP_MAP.get(op, op)

    # Count failures: NULL in either column OR comparison is false
    return (
        f"SUM(CASE WHEN {l} IS NULL OR {r_col} IS NULL "
        f"OR NOT ({l} {sql_op} {r_col}) THEN 1 ELSE 0 END) AS {r_id}"
    )


def agg_conditional_not_null(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    Count rows where column is NULL when condition is met.

    Args:
        column: Column that must not be null
        when_column: Column in the condition
        when_op: Condition operator
        when_value: Condition value
        rule_id: Rule identifier for alias
        dialect: SQL dialect

    Returns:
        SQL aggregate expression
    """
    col = esc_ident(column, dialect)
    when_col = esc_ident(when_column, dialect)
    r_id = esc_ident(rule_id, dialect)
    sql_op = SQL_OP_MAP.get(when_op, when_op)

    # Handle NULL value in condition
    if when_value is None:
        if when_op == "==":
            condition = f"{when_col} IS NULL"
        elif when_op == "!=":
            condition = f"{when_col} IS NOT NULL"
        else:
            condition = "1=0"  # Other operators with NULL -> always false
    else:
        val = lit_value(when_value, dialect)
        condition = f"{when_col} {sql_op} {val}"

    # Count failures: condition is TRUE AND column is NULL
    return (
        f"SUM(CASE WHEN ({condition}) AND {col} IS NULL THEN 1 ELSE 0 END) AS {r_id}"
    )


# Mapping from rule kind to failure_mode
RULE_KIND_TO_FAILURE_MODE = {
    "not_null": "null_values",
    "unique": "duplicate_values",
    "allowed_values": "novel_category",
    "min_rows": "row_count_low",
    "max_rows": "row_count_high",
    "range": "range_violation",
    "freshness": "freshness_lag",
    "regex": "pattern_mismatch",
    "dtype": "schema_drift",
    "custom_sql_check": "custom_check_failed",
    "compare": "comparison_failed",
    "conditional_not_null": "conditional_null",
}


def results_from_row(
    columns: List[str],
    values: tuple,
    is_exists: bool = False,
    rule_kinds: Optional[dict] = None,
) -> List[dict]:
    """
    Convert a single-row SQL result to Kontra result format.

    Args:
        columns: Column names (rule IDs)
        values: Result values
        is_exists: If True, values are booleans (True=violation, False=pass)
                   If False, values are counts (0=pass, >0=violation count)
        rule_kinds: Optional dict mapping rule_id -> rule_kind for failure_mode
    """
    rule_kinds = rule_kinds or {}
    out = []
    for i, col in enumerate(columns):
        if col == "__no_sql_rules__":
            continue

        rule_id = col
        val = values[i]

        # Get failure_mode from rule kind
        rule_kind = rule_kinds.get(rule_id)
        failure_mode = RULE_KIND_TO_FAILURE_MODE.get(rule_kind) if rule_kind else None

        if is_exists:
            has_violation = bool(val) if val is not None else False
            result = {
                "rule_id": rule_id,
                "passed": not has_violation,
                "failed_count": 1 if has_violation else 0,
                "message": "Passed" if not has_violation else "Failed",
                "severity": "ERROR",
                "actions_executed": [],
                "execution_source": "sql",
            }
            if has_violation and failure_mode:
                result["failure_mode"] = failure_mode
            out.append(result)
        else:
            failed_count = int(val) if val is not None else 0
            result = {
                "rule_id": rule_id,
                "passed": failed_count == 0,
                "failed_count": failed_count,
                "message": "Passed" if failed_count == 0 else "Failed",
                "severity": "ERROR",
                "actions_executed": [],
                "execution_source": "sql",
            }
            if failed_count > 0 and failure_mode:
                result["failure_mode"] = failure_mode
            out.append(result)

    return out
