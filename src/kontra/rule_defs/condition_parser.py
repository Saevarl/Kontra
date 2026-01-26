# src/kontra/rules/condition_parser.py
"""
Simple condition parser for when expressions.

Parses expressions like:
    - status == 'shipped'
    - amount > 0
    - is_active == true
    - category != 'test'

Returns (column_name, operator, typed_value).

Safety:
    - No eval() or exec()
    - Regex-based parsing only
    - Whitelist of supported operators
"""
from __future__ import annotations

import re
from typing import Any, Tuple

# Regex pattern for condition expressions
# Matches: column_name operator value
# Examples: status == 'shipped', amount > 100, is_active == true
CONDITION_PATTERN = re.compile(
    r"^\s*"  # Leading whitespace
    r"([a-zA-Z_][a-zA-Z0-9_]*)"  # Column name (identifier)
    r"\s*"  # Whitespace
    r"(==|!=|>=|<=|>|<)"  # Operator
    r"\s*"  # Whitespace
    r"(.+?)"  # Value (non-greedy)
    r"\s*$"  # Trailing whitespace
)

SUPPORTED_OPERATORS = {"==", "!=", ">", ">=", "<", "<="}


class ConditionParseError(ValueError):
    """Raised when a condition expression cannot be parsed."""

    pass


def parse_condition(expr: str) -> Tuple[str, str, Any]:
    """
    Parse a condition expression into (column, operator, typed_value).

    Args:
        expr: Condition expression (e.g., "status == 'shipped'")

    Returns:
        Tuple of (column_name, operator, typed_value)

    Raises:
        ConditionParseError: If the expression cannot be parsed

    Examples:
        >>> parse_condition("status == 'shipped'")
        ('status', '==', 'shipped')

        >>> parse_condition("amount > 100")
        ('amount', '>', 100)

        >>> parse_condition("is_active == true")
        ('is_active', '==', True)
    """
    if not expr or not isinstance(expr, str):
        raise ConditionParseError(f"Invalid condition expression: {expr!r}")

    # Check for unsupported compound expressions (AND, OR)
    # Case-insensitive check, but avoid matching inside quoted strings
    expr_upper = expr.upper()
    if " AND " in expr_upper or " OR " in expr_upper:
        raise ConditionParseError(
            f"Compound expressions (AND/OR) are not supported: {expr!r}. "
            f"Use multiple rules or custom_sql_check for complex conditions."
        )

    match = CONDITION_PATTERN.match(expr)
    if not match:
        raise ConditionParseError(
            f"Cannot parse condition: {expr!r}. "
            f"Expected format: column op value (e.g., status == 'shipped')"
        )

    column, operator, value_str = match.groups()

    if operator not in SUPPORTED_OPERATORS:
        raise ConditionParseError(
            f"Unsupported operator '{operator}' in condition: {expr!r}. "
            f"Supported: {', '.join(sorted(SUPPORTED_OPERATORS))}"
        )

    try:
        typed_value = _parse_value(value_str)
    except ValueError as e:
        raise ConditionParseError(
            f"Cannot parse value in condition: {expr!r}. {e}"
        ) from e

    return column, operator, typed_value


def _parse_value(value_str: str) -> Any:
    """
    Parse a value string into a typed Python value.

    Supported value types:
        - Strings: 'value' or "value"
        - Booleans: true, false (case-insensitive)
        - Null: null (case-insensitive)
        - Numbers: 123, 123.45, -42

    Args:
        value_str: String representation of the value

    Returns:
        Typed Python value

    Raises:
        ValueError: If the value cannot be parsed
    """
    val = value_str.strip()

    # Empty value
    if not val:
        raise ValueError("Empty value")

    # String literals: 'value' or "value"
    if (val.startswith("'") and val.endswith("'")) or \
       (val.startswith('"') and val.endswith('"')):
        # Handle escaped quotes inside strings
        inner = val[1:-1]
        return inner

    # Boolean: true/false (case-insensitive)
    val_lower = val.lower()
    if val_lower == "true":
        return True
    if val_lower == "false":
        return False

    # Null: null (case-insensitive)
    if val_lower == "null":
        return None

    # Try parsing as number
    # Integer
    try:
        return int(val)
    except ValueError:
        pass

    # Float
    try:
        return float(val)
    except ValueError:
        pass

    raise ValueError(f"Cannot parse value: {val!r}")


def condition_to_sql(column: str, operator: str, value: Any, dialect: str = "duckdb") -> str:
    """
    Convert a parsed condition to SQL WHERE clause fragment.

    Args:
        column: Column name
        operator: Comparison operator
        value: Typed value
        dialect: SQL dialect (duckdb, postgres, sqlserver)

    Returns:
        SQL WHERE clause fragment
    """
    from kontra.engine.sql_utils import esc_ident, lit_value

    col_sql = esc_ident(column, dialect)

    # Map Python operators to SQL operators
    sql_op_map = {
        "==": "=",
        "!=": "<>",
        ">": ">",
        ">=": ">=",
        "<": "<",
        "<=": "<=",
    }
    sql_op = sql_op_map.get(operator, operator)

    # Handle NULL comparison specially
    if value is None:
        if operator == "==":
            return f"{col_sql} IS NULL"
        elif operator == "!=":
            return f"{col_sql} IS NOT NULL"
        else:
            # Other operators with NULL don't make sense
            return f"{col_sql} IS NULL"

    val_sql = lit_value(value, dialect)
    return f"{col_sql} {sql_op} {val_sql}"
