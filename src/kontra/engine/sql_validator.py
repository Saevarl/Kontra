# src/kontra/engine/sql_validator.py
"""
SQL validation using sqlglot for safe remote execution.

Ensures user-provided SQL is read-only before executing on production databases.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError


# Statement types that are NOT allowed (write operations)
FORBIDDEN_STATEMENT_TYPES: Set[type] = {
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.Merge,
    exp.Grant,
    exp.Revoke,
    exp.Command,  # Generic command execution
}

# Function names that could have side effects (case-insensitive)
FORBIDDEN_FUNCTIONS: Set[str] = {
    # PostgreSQL
    "pg_sleep",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "set_config",
    "dblink",
    "dblink_exec",
    "lo_import",
    "lo_export",
    "pg_file_write",
    "pg_read_file",
    "pg_ls_dir",
    # SQL Server
    "xp_cmdshell",
    "xp_regread",
    "xp_regwrite",
    "sp_executesql",
    "sp_oacreate",
    "openrowset",
    "opendatasource",
    "bulk",
    # Generic dangerous
    "exec",
    "execute",
    "call",
    "sleep",
}


@dataclass
class ValidationResult:
    """Result of SQL validation."""

    is_safe: bool
    reason: Optional[str] = None
    parsed_sql: Optional[str] = None  # Normalized SQL if parsing succeeded
    dialect: Optional[str] = None


def validate_sql(
    sql: str,
    dialect: str = "postgres",
    allow_cte: bool = True,
    allow_subqueries: bool = True,
) -> ValidationResult:
    """
    Validate that SQL is safe for remote execution.

    A SQL statement is considered safe if:
    1. It parses successfully
    2. It's a SELECT statement (not INSERT, UPDATE, DELETE, etc.)
    3. It doesn't contain forbidden functions
    4. It doesn't contain multiple statements (no SQL injection via ;)

    Args:
        sql: The SQL statement to validate
        dialect: SQL dialect for parsing ("postgres", "tsql", "duckdb")
        allow_cte: Allow WITH clauses (CTEs)
        allow_subqueries: Allow subqueries in WHERE/FROM

    Returns:
        ValidationResult with is_safe=True if SQL is safe, False otherwise
    """
    sql = sql.strip()

    if not sql:
        return ValidationResult(is_safe=False, reason="Empty SQL statement")

    # Map dialect names
    dialect_map = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "sqlserver": "tsql",
        "mssql": "tsql",
        "tsql": "tsql",
        "duckdb": "duckdb",
    }
    sqlglot_dialect = dialect_map.get(dialect.lower(), "postgres")

    try:
        # Parse SQL - this will catch syntax errors
        statements = sqlglot.parse(sql, dialect=sqlglot_dialect)
    except ParseError as e:
        return ValidationResult(
            is_safe=False,
            reason=f"SQL parse error: {e}",
            dialect=sqlglot_dialect,
        )

    # Must be exactly one statement (no SQL injection via semicolons)
    if len(statements) != 1:
        return ValidationResult(
            is_safe=False,
            reason=f"Expected 1 statement, found {len(statements)}. Multiple statements not allowed.",
            dialect=sqlglot_dialect,
        )

    stmt = statements[0]

    if stmt is None:
        return ValidationResult(
            is_safe=False,
            reason="Failed to parse SQL statement",
            dialect=sqlglot_dialect,
        )

    # Check statement type - must be SELECT (or WITH for CTEs)
    is_select = isinstance(stmt, exp.Select)
    is_cte_select = isinstance(stmt, exp.With) and allow_cte

    if not (is_select or is_cte_select):
        stmt_type = type(stmt).__name__
        return ValidationResult(
            is_safe=False,
            reason=f"Only SELECT statements allowed, found: {stmt_type}",
            dialect=sqlglot_dialect,
        )

    # Check for forbidden statement types anywhere in the AST
    for node in stmt.walk():
        node_type = type(node)
        if node_type in FORBIDDEN_STATEMENT_TYPES:
            return ValidationResult(
                is_safe=False,
                reason=f"Forbidden operation: {node_type.__name__}",
                dialect=sqlglot_dialect,
            )

    # Check for forbidden functions
    forbidden_found = _check_forbidden_functions(stmt)
    if forbidden_found:
        return ValidationResult(
            is_safe=False,
            reason=f"Forbidden function: {forbidden_found}",
            dialect=sqlglot_dialect,
        )

    # Check for subqueries if not allowed
    if not allow_subqueries:
        for node in stmt.walk():
            if isinstance(node, exp.Subquery):
                return ValidationResult(
                    is_safe=False,
                    reason="Subqueries not allowed",
                    dialect=sqlglot_dialect,
                )

    # SQL is safe - return normalized version
    try:
        normalized = stmt.sql(dialect=sqlglot_dialect)
    except Exception:
        normalized = sql  # Fallback to original if normalization fails

    return ValidationResult(
        is_safe=True,
        parsed_sql=normalized,
        dialect=sqlglot_dialect,
    )


def _check_forbidden_functions(stmt: exp.Expression) -> Optional[str]:
    """
    Check for forbidden function calls in the AST.

    Returns the name of the forbidden function if found, None otherwise.
    """
    for node in stmt.walk():
        if isinstance(node, exp.Func):
            func_name = node.name.lower() if node.name else ""
            if func_name in FORBIDDEN_FUNCTIONS:
                return func_name

        # Also check for CALL statements disguised as functions
        if isinstance(node, exp.Anonymous):
            name = node.name.lower() if hasattr(node, "name") and node.name else ""
            if name in FORBIDDEN_FUNCTIONS:
                return name

    return None


def transpile_sql(
    sql: str,
    from_dialect: str,
    to_dialect: str,
) -> Tuple[bool, str]:
    """
    Transpile SQL from one dialect to another.

    Args:
        sql: The SQL statement to transpile
        from_dialect: Source dialect ("postgres", "tsql", "duckdb")
        to_dialect: Target dialect

    Returns:
        Tuple of (success, result_sql_or_error)
    """
    dialect_map = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "sqlserver": "tsql",
        "mssql": "tsql",
        "tsql": "tsql",
        "duckdb": "duckdb",
    }

    src = dialect_map.get(from_dialect.lower(), from_dialect)
    dst = dialect_map.get(to_dialect.lower(), to_dialect)

    try:
        result = sqlglot.transpile(sql, read=src, write=dst)
        if result:
            return True, result[0]
        return False, "Transpilation returned empty result"
    except Exception as e:
        return False, str(e)


def format_table_reference(
    schema: str,
    table: str,
    dialect: str,
) -> str:
    """
    Format a table reference for a specific SQL dialect.

    Args:
        schema: Schema name (e.g., "public", "dbo")
        table: Table name
        dialect: SQL dialect ("postgres", "sqlserver", "duckdb")

    Returns:
        Properly quoted table reference
    """
    dialect = dialect.lower()

    if dialect in ("postgres", "postgresql", "duckdb"):
        # PostgreSQL/DuckDB: "schema"."table"
        return f'"{schema}"."{table}"'
    elif dialect in ("sqlserver", "mssql", "tsql"):
        # SQL Server: [schema].[table]
        return f"[{schema}].[{table}]"
    else:
        # Default: schema.table
        return f"{schema}.{table}"


def replace_table_placeholder(
    sql: str,
    schema: str,
    table: str,
    dialect: str,
    placeholder: str = "{table}",
) -> str:
    """
    Replace {table} placeholder with properly formatted table reference.

    Args:
        sql: SQL with placeholder
        schema: Schema name
        table: Table name
        dialect: SQL dialect
        placeholder: Placeholder string to replace (default: "{table}")

    Returns:
        SQL with placeholder replaced
    """
    table_ref = format_table_reference(schema, table, dialect)
    return sql.replace(placeholder, table_ref)


def to_count_query(sql: str, dialect: str = "postgres") -> Tuple[bool, str]:
    """
    Transform a SELECT query into a COUNT(*) query for violation counting.

    Strategy:
    - Simple SELECT (no DISTINCT, GROUP BY, LIMIT): Rewrite SELECT to COUNT(*)
    - Complex SELECT (has DISTINCT, GROUP BY, or LIMIT): Wrap in COUNT(*)

    Examples:
        SELECT * FROM t WHERE x < 0
        → SELECT COUNT(*) FROM t WHERE x < 0

        SELECT DISTINCT region FROM t
        → SELECT COUNT(*) FROM (SELECT DISTINCT region FROM t) AS _v

        SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1
        → SELECT COUNT(*) FROM (SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1) AS _v

    Args:
        sql: The SELECT query to transform
        dialect: SQL dialect ("postgres", "sqlserver", "duckdb")

    Returns:
        Tuple of (success, transformed_sql_or_error)
    """
    # Map dialect names
    dialect_map = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "sqlserver": "tsql",
        "mssql": "tsql",
        "tsql": "tsql",
        "duckdb": "duckdb",
    }
    sqlglot_dialect = dialect_map.get(dialect.lower(), "postgres")

    try:
        parsed = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
    except ParseError as e:
        return False, f"SQL parse error: {e}"

    if parsed is None:
        return False, "Failed to parse SQL"

    # Verify it's a SELECT statement (or WITH/CTE)
    if not isinstance(parsed, (exp.Select, exp.With)):
        return False, f"Expected SELECT statement, got {type(parsed).__name__}"

    # Check if we need to wrap (complex query) or can rewrite (simple query)
    needs_wrap = _needs_wrapping(parsed)

    if needs_wrap:
        # Wrap: SELECT COUNT(*) FROM (...) AS _v
        result = _wrap_in_count(parsed, sqlglot_dialect)
    else:
        # Rewrite: Replace SELECT expressions with COUNT(*)
        result = _rewrite_to_count(parsed, sqlglot_dialect)

    return True, result


def _needs_wrapping(parsed: exp.Expression) -> bool:
    """
    Check if a query needs wrapping vs simple rewriting.

    Needs wrapping if:
    - Has DISTINCT (changing SELECT would change result set)
    - Has GROUP BY (rewriting would return multiple rows)
    - Has LIMIT/OFFSET (rewriting would ignore the limit)
    - Has UNION/INTERSECT/EXCEPT (compound queries)
    """
    # Check for DISTINCT in the main SELECT
    if isinstance(parsed, exp.Select):
        if parsed.args.get("distinct"):
            return True

    # Check for GROUP BY
    if parsed.find(exp.Group):
        return True

    # Check for LIMIT or OFFSET
    if parsed.find(exp.Limit) or parsed.find(exp.Offset):
        return True

    # Check for set operations (UNION, INTERSECT, EXCEPT)
    if parsed.find(exp.Union) or parsed.find(exp.Intersect) or parsed.find(exp.Except):
        return True

    # Check for WITH (CTE) - wrap to be safe
    if isinstance(parsed, exp.With):
        return True

    return False


def _wrap_in_count(parsed: exp.Expression, dialect: str) -> str:
    """
    Wrap a query in SELECT COUNT(*) FROM (...) AS _v.
    """
    # Create: SELECT COUNT(*) FROM (original_query) AS _v
    count_star = exp.Count(this=exp.Star())

    # Handle different expression types
    if hasattr(parsed, "subquery"):
        subquery = parsed.subquery(alias="_v")
    else:
        # Fallback: wrap in parentheses manually
        subquery = exp.Subquery(this=parsed, alias=exp.TableAlias(this=exp.Identifier(this="_v")))

    wrapped = exp.Select(expressions=[count_star]).from_(subquery)

    return wrapped.sql(dialect=dialect)


def _rewrite_to_count(parsed: exp.Expression, dialect: str) -> str:
    """
    Rewrite a simple SELECT to use COUNT(*) instead of column expressions.

    SELECT a, b, c FROM t WHERE x < 0
    → SELECT COUNT(*) FROM t WHERE x < 0
    """
    if not isinstance(parsed, exp.Select):
        # Fallback to wrapping for non-SELECT
        return _wrap_in_count(parsed, dialect)

    # Create COUNT(*) expression
    count_star = exp.Count(this=exp.Star())

    # Replace the SELECT expressions with COUNT(*)
    parsed.set("expressions", [count_star])

    # Remove any DISTINCT (shouldn't be here, but just in case)
    if parsed.args.get("distinct"):
        parsed.set("distinct", None)

    return parsed.sql(dialect=dialect)
