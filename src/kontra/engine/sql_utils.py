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


def exists_unique(
    col: str, rule_id: str, table: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for unique rule - stops at first duplicate found.
    Returns 1 if any duplicate exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # Subquery finds groups with COUNT > 1
    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS ("
            f"SELECT 1 FROM {table} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} HAVING COUNT(*) > 1"
            f") THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} HAVING COUNT(*) > 1 LIMIT 1) AS {r}"
        )


def exists_allowed_values(
    col: str, values: List[Any], table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for allowed_values rule - stops at first disallowed value.
    Returns 1 if any value not in allowed set exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    val_list = ", ".join(
        lit_str(str(v), dialect) if isinstance(v, str) else str(v)
        for v in values
    )

    if dialect == "sqlserver":
        cast_col = f"CAST({c} AS NVARCHAR(MAX))"
        return (
            f"(SELECT CASE WHEN EXISTS ("
            f"SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {cast_col} NOT IN ({val_list})"
            f") THEN 1 ELSE 0 END) AS {r}"
        )
    elif dialect == "postgres":
        cast_col = f"{c}::text"
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {cast_col} NOT IN ({val_list}) LIMIT 1) AS {r}"
        )
    else:  # duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {c} NOT IN ({val_list}) LIMIT 1) AS {r}"
        )


def exists_disallowed_values(
    col: str, values: List[Any], table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for disallowed_values rule - stops at first disallowed value.
    Returns 1 if any value in disallowed set exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    if not values:
        # No disallowed values = always passes
        return f"0 AS {r}"

    val_list = ", ".join(
        lit_str(str(v), dialect) if isinstance(v, str) else str(v)
        for v in values
        if v is not None
    )

    if dialect == "sqlserver":
        cast_col = f"CAST({c} AS NVARCHAR(MAX))"
        return (
            f"(SELECT CASE WHEN EXISTS ("
            f"SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {cast_col} IN ({val_list})"
            f") THEN 1 ELSE 0 END) AS {r}"
        )
    elif dialect == "postgres":
        cast_col = f"{c}::text"
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {cast_col} IN ({val_list}) LIMIT 1) AS {r}"
        )
    else:  # duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NOT NULL AND {c} IN ({val_list}) LIMIT 1) AS {r}"
        )


def exists_range(
    col: str,
    min_val: Optional[Any],
    max_val: Optional[Any],
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for range rule - stops at first out-of-range value.
    Returns 1 if any value outside [min, max] or NULL exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    conditions = [f"{c} IS NULL"]
    if min_val is not None:
        conditions.append(f"{c} < {min_val}")
    if max_val is not None:
        conditions.append(f"{c} > {max_val}")

    violation = " OR ".join(conditions)

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {violation}) "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {violation} LIMIT 1) AS {r}"
        )


def exists_length(
    col: str,
    min_len: Optional[int],
    max_len: Optional[int],
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for length rule - stops at first invalid length.
    Returns 1 if any value with invalid length exists, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # SQL Server uses LEN(), others use LENGTH()
    if dialect == "sqlserver":
        len_func = f"LEN({c})"
    else:
        len_func = f"LENGTH({c})"

    conditions = [f"{c} IS NULL"]
    if min_len is not None:
        conditions.append(f"{len_func} < {int(min_len)}")
    if max_len is not None:
        conditions.append(f"{len_func} > {int(max_len)}")

    violation = " OR ".join(conditions)

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {violation}) "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {violation} LIMIT 1) AS {r}"
        )


def exists_regex(
    col: str, pattern: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for regex rule - stops at first non-matching value.
    Returns 1 if any value doesn't match pattern or is NULL, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)
    escaped_pattern = pattern.replace("'", "''")

    if dialect == "sqlserver":
        # SQL Server uses PATINDEX with LIKE-style patterns
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR PATINDEX('%{escaped_pattern}%', CAST({c} AS NVARCHAR(MAX))) = 0) "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    elif dialect == "postgres":
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR NOT ({c}::text ~ '{escaped_pattern}') LIMIT 1) AS {r}"
        )
    else:  # duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR NOT regexp_matches(CAST({c} AS VARCHAR), '{escaped_pattern}') LIMIT 1) AS {r}"
        )


def exists_contains(
    col: str, substring: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for contains rule - stops at first non-containing value.
    Returns 1 if any value doesn't contain substring or is NULL, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    escaped = escape_like_pattern(substring)
    pattern = f"%{escaped}%"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\') THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\' LIMIT 1) AS {r}"
        )


def exists_starts_with(
    col: str, prefix: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for starts_with rule - stops at first non-matching value.
    Returns 1 if any value doesn't start with prefix or is NULL, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    escaped = escape_like_pattern(prefix)
    pattern = f"{escaped}%"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\') THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\' LIMIT 1) AS {r}"
        )


def exists_ends_with(
    col: str, suffix: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for ends_with rule - stops at first non-matching value.
    Returns 1 if any value doesn't end with suffix or is NULL, 0 otherwise.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    escaped = escape_like_pattern(suffix)
    pattern = f"%{escaped}"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\') THEN 1 ELSE 0 END) AS {r}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {c} IS NULL "
            f"OR {c} NOT LIKE '{pattern}' ESCAPE '\\' LIMIT 1) AS {r}"
        )


def exists_compare(
    left: str,
    right: str,
    op: str,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for compare rule - stops at first comparison failure.
    Returns 1 if any comparison fails or either column is NULL, 0 otherwise.
    """
    l = esc_ident(left, dialect)
    r_col = esc_ident(right, dialect)
    r_id = esc_ident(rule_id, dialect)
    sql_op = SQL_OP_MAP.get(op, op)

    violation = f"{l} IS NULL OR {r_col} IS NULL OR NOT ({l} {sql_op} {r_col})"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {violation}) "
            f"THEN 1 ELSE 0 END) AS {r_id}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {violation} LIMIT 1) AS {r_id}"
        )


def exists_conditional_not_null(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for conditional_not_null rule - stops at first violation.
    Returns 1 if any row has column NULL when condition is TRUE, 0 otherwise.
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
            condition = "1=0"
    else:
        val = lit_value(when_value, dialect)
        condition = f"{when_col} {sql_op} {val}"

    violation = f"({condition}) AND {col} IS NULL"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {violation}) "
            f"THEN 1 ELSE 0 END) AS {r_id}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {violation} LIMIT 1) AS {r_id}"
        )


def exists_conditional_range(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    min_val: Any,
    max_val: Any,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for conditional_range rule - stops at first violation.
    Returns 1 if any row is outside range when condition is TRUE, 0 otherwise.
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
            condition = "1=0"
    else:
        val = lit_value(when_value, dialect)
        condition = f"{when_col} {sql_op} {val}"

    # Build range violation part
    range_parts = [f"{col} IS NULL"]
    if min_val is not None:
        range_parts.append(f"{col} < {min_val}")
    if max_val is not None:
        range_parts.append(f"{col} > {max_val}")
    range_violation = " OR ".join(range_parts)

    violation = f"({condition}) AND ({range_violation})"

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {violation}) "
            f"THEN 1 ELSE 0 END) AS {r_id}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {violation} LIMIT 1) AS {r_id}"
        )


def exists_custom(
    where_condition: str,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for custom rules - user provides the WHERE condition.
    Returns 1 if any row matches the condition (violation exists), 0 otherwise.

    Parameters
    ----------
    where_condition : str
        The WHERE clause condition that identifies violations.
        Example: '"score" <= 0' or '"status" NOT IN (''active'', ''inactive'')'
    table : str
        The table or view name (already escaped/formatted).
    rule_id : str
        Unique identifier for this rule (used as column alias).
    dialect : str
        SQL dialect ('duckdb', 'postgres', 'sqlserver', 'mssql').
    """
    r_id = esc_ident(rule_id, dialect)

    if dialect == "sqlserver":
        return (
            f"(SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {where_condition}) "
            f"THEN 1 ELSE 0 END) AS {r_id}"
        )
    else:  # postgres, duckdb
        return (
            f"EXISTS (SELECT 1 FROM {table} WHERE {where_condition} LIMIT 1) AS {r_id}"
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


def agg_conditional_range(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    min_val: Any,
    max_val: Any,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    Count rows where column is outside range when condition is met.

    Args:
        column: Column to check range
        when_column: Column in the condition
        when_op: Condition operator
        when_value: Condition value
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)
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

    # Build range violation part: NULL OR outside range
    range_parts = [f"{col} IS NULL"]
    if min_val is not None:
        range_parts.append(f"{col} < {min_val}")
    if max_val is not None:
        range_parts.append(f"{col} > {max_val}")
    range_violation = " OR ".join(range_parts)

    # Count failures: condition is TRUE AND (column is NULL OR outside range)
    return (
        f"SUM(CASE WHEN ({condition}) AND ({range_violation}) THEN 1 ELSE 0 END) AS {r_id}"
    )


# Mapping from rule kind to failure_mode
RULE_KIND_TO_FAILURE_MODE = {
    "not_null": "null_values",
    "unique": "duplicate_values",
    "allowed_values": "novel_category",
    "disallowed_values": "disallowed_value",
    "min_rows": "row_count_low",
    "max_rows": "row_count_high",
    "range": "range_violation",
    "length": "length_violation",
    "freshness": "freshness_lag",
    "regex": "pattern_mismatch",
    "contains": "pattern_mismatch",
    "starts_with": "pattern_mismatch",
    "ends_with": "pattern_mismatch",
    "dtype": "schema_drift",
    "custom_sql_check": "custom_check_failed",
    "compare": "comparison_failed",
    "conditional_not_null": "conditional_null",
    "conditional_range": "conditional_range_violation",
}


# =============================================================================
# String Validation Aggregate Expression Builders
# =============================================================================

def escape_like_pattern(value: str, escape_char: str = "\\") -> str:
    """
    Escape special characters in a LIKE pattern value.

    LIKE special characters: %, _, and the escape character itself.

    Args:
        value: The literal string to escape
        escape_char: The escape character to use (default: backslash)

    Returns:
        Escaped string safe for use in LIKE patterns
    """
    # Order matters: escape the escape char first
    for c in (escape_char, "%", "_"):
        value = value.replace(c, escape_char + c)
    return value


def agg_disallowed_values(
    col: str, values: List[Any], rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that ARE in the disallowed set.

    Inverse of allowed_values: fails if value IS in the list.
    NULL values are NOT failures (NULL is not in any list).
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    if not values:
        # No disallowed values means nothing can fail
        return f"0 AS {r}"

    val_list = ", ".join(
        lit_str(str(v), dialect) if isinstance(v, str) else str(v)
        for v in values
        if v is not None  # NULL in disallowed list doesn't make sense
    )

    if dialect == "sqlserver":
        cast_col = f"CAST({c} AS NVARCHAR(MAX))"
    elif dialect == "postgres":
        cast_col = f"{c}::text"
    else:
        cast_col = c

    # Failure = value IS in the disallowed list (and not null)
    return (
        f"SUM(CASE WHEN {c} IS NOT NULL AND {cast_col} IN ({val_list}) "
        f"THEN 1 ELSE 0 END) AS {r}"
    )


def agg_length(
    col: str,
    min_len: Optional[int],
    max_len: Optional[int],
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    Count values where string length is outside [min_len, max_len].

    NULL values are failures (can't measure length of NULL).
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # SQL Server uses LEN(), others use LENGTH()
    if dialect == "sqlserver":
        len_func = f"LEN({c})"
    else:
        len_func = f"LENGTH({c})"

    conditions = [f"{c} IS NULL"]
    if min_len is not None:
        conditions.append(f"{len_func} < {int(min_len)}")
    if max_len is not None:
        conditions.append(f"{len_func} > {int(max_len)}")

    violation = " OR ".join(conditions)
    return f"SUM(CASE WHEN {violation} THEN 1 ELSE 0 END) AS {r}"


def agg_contains(
    col: str, substring: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that do NOT contain the substring.

    Uses LIKE for efficiency (faster than regex).
    NULL values are failures.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # Escape LIKE special characters in the substring
    escaped = escape_like_pattern(substring)
    pattern = f"%{escaped}%"

    if dialect == "sqlserver":
        # SQL Server LIKE is case-insensitive by default (depends on collation)
        # Use ESCAPE clause for backslash
        return (
            f"SUM(CASE WHEN {c} IS NULL OR {c} NOT LIKE '{pattern}' ESCAPE '\\' "
            f"THEN 1 ELSE 0 END) AS {r}"
        )
    else:
        # DuckDB and PostgreSQL
        return (
            f"SUM(CASE WHEN {c} IS NULL OR {c} NOT LIKE '{pattern}' ESCAPE '\\' "
            f"THEN 1 ELSE 0 END) AS {r}"
        )


def agg_starts_with(
    col: str, prefix: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that do NOT start with the prefix.

    Uses LIKE for efficiency (faster than regex).
    NULL values are failures.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # Escape LIKE special characters in the prefix
    escaped = escape_like_pattern(prefix)
    pattern = f"{escaped}%"

    return (
        f"SUM(CASE WHEN {c} IS NULL OR {c} NOT LIKE '{pattern}' ESCAPE '\\' "
        f"THEN 1 ELSE 0 END) AS {r}"
    )


def agg_ends_with(
    col: str, suffix: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that do NOT end with the suffix.

    Uses LIKE for efficiency (faster than regex).
    NULL values are failures.
    """
    c = esc_ident(col, dialect)
    r = esc_ident(rule_id, dialect)

    # Escape LIKE special characters in the suffix
    escaped = escape_like_pattern(suffix)
    pattern = f"%{escaped}"

    return (
        f"SUM(CASE WHEN {c} IS NULL OR {c} NOT LIKE '{pattern}' ESCAPE '\\' "
        f"THEN 1 ELSE 0 END) AS {r}"
    )


def _generate_rule_message(
    rule_kind: Optional[str],
    failed_count: int,
    is_tally: bool,
    rule_id: str,
) -> str:
    """
    Generate a descriptive message for a rule result.

    Args:
        rule_kind: The type of rule (e.g., "not_null", "unique", "range")
        failed_count: Number of violations (1 for EXISTS mode)
        is_tally: True if exact count (COUNT mode), False if lower bound (EXISTS mode)
        rule_id: The rule ID (used to extract column name if needed)
    """
    if failed_count == 0:
        return "Passed"

    # Extract column name from rule_id (format: COL:column:rule_kind or DATASET:rule_kind)
    column = None
    if rule_id.startswith("COL:"):
        parts = rule_id.split(":")
        if len(parts) >= 2:
            column = parts[1]

    # Count prefix: "At least 1" for EXISTS, exact count for tally
    if is_tally:
        count_str = str(failed_count)
        row_str = "row" if failed_count == 1 else "rows"
    else:
        count_str = "At least 1"
        row_str = "row"

    # Generate rule-specific messages
    if rule_kind == "not_null":
        col_part = f" in {column}" if column else ""
        return f"{count_str} null value{'' if not is_tally or failed_count == 1 else 's'} found{col_part}"

    elif rule_kind == "unique":
        col_part = f" in {column}" if column else ""
        return f"{count_str} duplicate {row_str}{col_part}"

    elif rule_kind == "allowed_values":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with disallowed value{col_part}"

    elif rule_kind == "disallowed_values":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with disallowed value{col_part}"

    elif rule_kind == "range":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} out of range{col_part}"

    elif rule_kind == "length":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with invalid length{col_part}"

    elif rule_kind == "regex":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} failed regex match{col_part}"

    elif rule_kind == "contains":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} missing required substring{col_part}"

    elif rule_kind == "starts_with":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with invalid prefix{col_part}"

    elif rule_kind == "ends_with":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with invalid suffix{col_part}"

    elif rule_kind == "compare":
        return f"{count_str} {row_str} failed comparison"

    elif rule_kind == "conditional_not_null":
        return f"{count_str} {row_str} with null value when condition met"

    elif rule_kind == "conditional_range":
        return f"{count_str} {row_str} out of range when condition met"

    elif rule_kind == "min_rows":
        return f"Dataset has {failed_count} fewer rows than required minimum"

    elif rule_kind == "max_rows":
        return f"Dataset has {failed_count} more rows than allowed maximum"

    elif rule_kind == "freshness":
        return "Data is stale"

    else:
        # Generic fallback
        return f"{count_str} {row_str} failed validation"


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
            failed_count = 1 if has_violation else 0
            message = _generate_rule_message(
                rule_kind, failed_count, is_tally=False, rule_id=rule_id
            )
            result = {
                "rule_id": rule_id,
                "passed": not has_violation,
                "failed_count": failed_count,
                "tally": False,
                "message": message,
                "severity": "ERROR",
                "actions_executed": [],
                "execution_source": "sql",
            }
            if has_violation and failure_mode:
                result["failure_mode"] = failure_mode
            out.append(result)
        else:
            failed_count = int(val) if val is not None else 0
            message = _generate_rule_message(
                rule_kind, failed_count, is_tally=True, rule_id=rule_id
            )
            result = {
                "rule_id": rule_id,
                "passed": failed_count == 0,
                "failed_count": failed_count,
                "tally": True,
                "message": message,
                "severity": "ERROR",
                "actions_executed": [],
                "execution_source": "sql",
            }
            if failed_count > 0 and failure_mode:
                result["failure_mode"] = failure_mode
            out.append(result)

    return out
