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


# Statement types that are NOT allowed (write operations and external access)
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
    exp.Copy,     # COPY command (file I/O)
    exp.Set,      # SET commands (configuration changes)
    exp.Use,      # USE database (context switching)
    exp.Attach,   # ATTACH external databases (SEC-002)
}

# Table prefixes/schemas that are forbidden (system catalogs)
# These allow information disclosure attacks
FORBIDDEN_TABLE_PREFIXES: Set[str] = {
    # PostgreSQL system catalogs
    "pg_",
    # SQL Server system views
    "sys.",
    # Standard information schema (both PostgreSQL and SQL Server)
    "information_schema.",
}

# Specific system tables to block (without prefix, for tables accessed directly)
FORBIDDEN_TABLES: Set[str] = {
    # PostgreSQL sensitive tables
    "pg_shadow",
    "pg_authid",
    "pg_roles",
    "pg_user",
    "pg_database",
    "pg_tablespace",
    "pg_settings",
    "pg_stat_activity",
    "pg_stat_user_tables",
    # SQL Server sensitive tables
    "syslogins",
    "sysobjects",
    "syscolumns",
    "sysusers",
    "sysdatabases",
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
    # DuckDB file access (SEC-001: arbitrary file read)
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_json",
    "read_json_auto",
    "read_json_objects",
    "read_blob",
    "read_text",
    "read_ndjson",
    "read_ndjson_auto",
    "read_ndjson_objects",
    # DuckDB file listing/globbing
    "glob",
    "list_files",
    # DuckDB external access
    "httpfs_get",
    "http_get",
    "s3_get",
    # DuckDB query functions that could bypass table reference
    "query",
    "query_table",
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

    # Check for system catalog access (information disclosure)
    forbidden_table = _check_forbidden_tables(stmt)
    if forbidden_table:
        return ValidationResult(
            is_safe=False,
            reason=f"Access to system catalog not allowed: {forbidden_table}",
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
    except (AttributeError, ValueError):
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
            # Check function name via multiple methods
            func_name = node.name.lower() if node.name else ""
            if func_name in FORBIDDEN_FUNCTIONS:
                return func_name

            # Check sql_name() for functions like ReadCSV, ReadParquet
            # that have specific class types
            try:
                sql_name = node.sql_name().lower() if hasattr(node, "sql_name") else ""
                if sql_name in FORBIDDEN_FUNCTIONS:
                    return sql_name
            except (AttributeError, TypeError):
                pass

            # Check class name directly for specific types
            class_name = type(node).__name__.lower()
            # Map class names to function names
            class_to_func = {
                "readcsv": "read_csv",
                "readparquet": "read_parquet",
                "readjson": "read_json",
            }
            if class_name in class_to_func:
                mapped_name = class_to_func[class_name]
                if mapped_name in FORBIDDEN_FUNCTIONS:
                    return mapped_name

        # Also check for CALL statements disguised as functions
        if isinstance(node, exp.Anonymous):
            name = node.name.lower() if hasattr(node, "name") and node.name else ""
            if name in FORBIDDEN_FUNCTIONS:
                return name

    return None


def _check_forbidden_tables(stmt: exp.Expression) -> Optional[str]:
    """
    Check for access to forbidden system catalog tables.

    Walks the AST looking for table references that match system catalog
    patterns (pg_*, sys.*, information_schema.*).

    Returns the forbidden table reference if found, None otherwise.
    """
    for node in stmt.walk():
        # Check Table nodes (direct table references)
        if isinstance(node, exp.Table):
            table_name = _get_full_table_name(node)
            if table_name:
                forbidden = _is_forbidden_table(table_name)
                if forbidden:
                    return forbidden

    return None


def _get_full_table_name(table_node: exp.Table) -> Optional[str]:
    """
    Extract the full table name from a Table node, including schema if present.

    Returns schema.table or just table name.
    """
    parts = []

    # Get catalog (database) if present
    if table_node.catalog:
        parts.append(str(table_node.catalog))

    # Get schema if present
    if table_node.db:
        parts.append(str(table_node.db))

    # Get table name
    if table_node.name:
        parts.append(str(table_node.name))

    if parts:
        return ".".join(parts)
    return None


def _is_forbidden_table(table_ref: str) -> Optional[str]:
    """
    Check if a table reference matches forbidden patterns.

    Args:
        table_ref: Full table reference (e.g., "pg_user", "sys.tables", "information_schema.columns")

    Returns:
        The forbidden table reference if matched, None otherwise.
    """
    table_lower = table_ref.lower()

    # Check exact matches first
    # Handle both "pg_user" and "public.pg_user" etc.
    table_parts = table_lower.split(".")
    base_table = table_parts[-1]  # Last part is the table name

    if base_table in FORBIDDEN_TABLES:
        return table_ref

    # Check prefixes (handles schema.table patterns)
    for prefix in FORBIDDEN_TABLE_PREFIXES:
        # Check if the full reference starts with the prefix
        if table_lower.startswith(prefix):
            return table_ref
        # Also check if any part starts with the prefix
        for part in table_parts:
            if part.startswith(prefix.rstrip(".")):
                return table_ref

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
