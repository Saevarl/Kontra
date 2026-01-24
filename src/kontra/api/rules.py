# src/kontra/api/rules.py
"""
Rule helper functions for inline rule definitions.

Usage:
    from kontra import rules

    result = kontra.validate(df, rules=[
        rules.not_null("user_id"),
        rules.unique("email"),
        rules.range("age", min=0, max=150),
    ])

    # Multiple rules on same column with custom IDs:
    result = kontra.validate(df, rules=[
        rules.range("score", min=0, max=100, id="score_full_range"),
        rules.range("score", min=80, max=100, id="score_strict_range"),
    ])
"""

from typing import Any, Dict, List, Optional, Union


def _build_rule(
    name: str,
    params: Dict[str, Any],
    severity: str,
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a rule dict, optionally with custom id and context."""
    rule: Dict[str, Any] = {
        "name": name,
        "params": params,
        "severity": severity,
    }
    if id is not None:
        rule["id"] = id
    if context is not None:
        rule["context"] = context
    return rule


def not_null(
    column: str,
    severity: str = "blocking",
    include_nan: bool = False,
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must not contain null values.

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        include_nan: If True, also treat NaN as null (default: False)
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        By default, NaN values are NOT considered null (Polars behavior).
        Set include_nan=True to catch both NULL and NaN values in float columns.

    Returns:
        Rule dict for use with kontra.validate()
    """
    params: Dict[str, Any] = {"column": column}
    if include_nan:
        params["include_nan"] = True

    return _build_rule("not_null", params, severity, id, context)


def unique(
    column: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must be unique (no duplicates).

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("unique", {"column": column}, severity, id, context)


def dtype(
    column: str,
    type: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must have the specified data type.

    Args:
        column: Column name to check
        type: Expected type (int64, float64, string, datetime, bool, etc.)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("dtype", {"column": column, "type": type}, severity, id, context)


def range(
    column: str,
    min: Optional[Union[int, float]] = None,
    max: Optional[Union[int, float]] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must be within the specified range.

    Args:
        column: Column name to check
        min: Minimum allowed value (inclusive)
        max: Maximum allowed value (inclusive)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If neither min nor max is provided, or if min > max
    """
    # Validate at least one bound is provided
    if min is None and max is None:
        raise ValueError("range rule: at least one of 'min' or 'max' must be provided")

    # Validate min <= max
    if min is not None and max is not None and min > max:
        raise ValueError(f"range rule: min ({min}) must be <= max ({max})")

    params: Dict[str, Any] = {"column": column}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max

    return _build_rule("range", params, severity, id, context)


def allowed_values(
    column: str,
    values: List[Any],
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must be in the allowed set.

    Args:
        column: Column name to check
        values: List of allowed values
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("allowed_values", {"column": column, "values": values}, severity, id, context)


def regex(
    column: str,
    pattern: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must match the regex pattern.

    Args:
        column: Column name to check
        pattern: Regular expression pattern
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("regex", {"column": column, "pattern": pattern}, severity, id, context)


def min_rows(
    threshold: int,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Dataset must have at least this many rows.

    Args:
        threshold: Minimum row count (must be >= 0)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If threshold is negative
    """
    if threshold < 0:
        raise ValueError(f"min_rows threshold must be non-negative, got {threshold}")

    return _build_rule("min_rows", {"threshold": threshold}, severity, id, context)


def max_rows(
    threshold: int,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Dataset must have at most this many rows.

    Args:
        threshold: Maximum row count
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("max_rows", {"threshold": threshold}, severity, id, context)


def freshness(
    column: str,
    max_age: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column timestamp must be within max_age of now.

    Args:
        column: Datetime column to check
        max_age: Maximum age (e.g., "24h", "7d", "1w")
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("freshness", {"column": column, "max_age": max_age}, severity, id, context)


def custom_sql_check(
    sql: str,
    threshold: int = 0,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Custom SQL check must return at most `threshold` rows.

    Args:
        sql: SQL query that returns rows that violate the rule
        threshold: Maximum allowed violations (default: 0)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple custom checks)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("custom_sql_check", {"sql": sql, "threshold": threshold}, severity, id, context)


def compare(
    left: str,
    right: str,
    op: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compare two columns using a comparison operator.

    Args:
        left: Left column name
        right: Right column name
        op: Comparison operator: ">", ">=", "<", "<=", "==", "!="
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple compare rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        Rows where either column is NULL are counted as failures.
        You cannot meaningfully compare NULL values.

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        # Ensure end_date >= start_date
        rules.compare("end_date", "start_date", ">=")
    """
    return _build_rule("compare", {"left": left, "right": right, "op": op}, severity, id, context)


def conditional_not_null(
    column: str,
    when: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must not be NULL when a condition is met.

    Args:
        column: Column that must not be null
        when: Condition expression (e.g., "status == 'shipped'")
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Condition syntax:
        column_name operator value

        Supported operators: ==, !=, >, >=, <, <=
        Supported values: 'string', 123, 123.45, true, false, null

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        # shipping_date must not be null when status is 'shipped'
        rules.conditional_not_null("shipping_date", "status == 'shipped'")
    """
    return _build_rule("conditional_not_null", {"column": column, "when": when}, severity, id, context)


def conditional_range(
    column: str,
    when: str,
    min: Optional[Union[int, float]] = None,
    max: Optional[Union[int, float]] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must be within range when a condition is met.

    Args:
        column: Column to check range
        when: Condition expression (e.g., "customer_type == 'premium'")
        min: Minimum allowed value (inclusive)
        max: Maximum allowed value (inclusive)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    At least one of `min` or `max` must be provided.

    Condition syntax:
        column_name operator value

        Supported operators: ==, !=, >, >=, <, <=
        Supported values: 'string', 123, 123.45, true, false, null

    When the condition is TRUE:
        - NULL in column = failure (can't compare NULL)
        - Value outside [min, max] = failure

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        # discount_percent must be between 10 and 50 for premium customers
        rules.conditional_range("discount_percent", "customer_type == 'premium'", min=10, max=50)
    """
    params = {"column": column, "when": when}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max
    return _build_rule("conditional_range", params, severity, id, context)


# Module-level access for `from kontra import rules` then `rules.not_null(...)`
class _RulesModule:
    """
    Namespace for rule helper functions.

    This allows using rules.not_null() syntax.
    """

    not_null = staticmethod(not_null)
    unique = staticmethod(unique)
    dtype = staticmethod(dtype)
    range = staticmethod(range)
    allowed_values = staticmethod(allowed_values)
    regex = staticmethod(regex)
    min_rows = staticmethod(min_rows)
    max_rows = staticmethod(max_rows)
    freshness = staticmethod(freshness)
    custom_sql_check = staticmethod(custom_sql_check)
    compare = staticmethod(compare)
    conditional_not_null = staticmethod(conditional_not_null)
    conditional_range = staticmethod(conditional_range)

    def __repr__(self) -> str:
        return "<kontra.rules module>"


# Export the module instance
rules = _RulesModule()
