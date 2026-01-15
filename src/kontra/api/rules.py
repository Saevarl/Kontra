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
) -> Dict[str, Any]:
    """Build a rule dict, optionally with custom id."""
    rule: Dict[str, Any] = {
        "name": name,
        "params": params,
        "severity": severity,
    }
    if id is not None:
        rule["id"] = id
    return rule


def not_null(
    column: str,
    severity: str = "blocking",
    include_nan: bool = False,
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column must not contain null values.

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        include_nan: If True, also treat NaN as null (default: False)
        id: Custom rule ID (use when applying multiple rules to same column)

    Note:
        By default, NaN values are NOT considered null (Polars behavior).
        Set include_nan=True to catch both NULL and NaN values in float columns.

    Returns:
        Rule dict for use with kontra.validate()
    """
    params: Dict[str, Any] = {"column": column}
    if include_nan:
        params["include_nan"] = True

    return _build_rule("not_null", params, severity, id)


def unique(
    column: str,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column values must be unique (no duplicates).

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("unique", {"column": column}, severity, id)


def dtype(
    column: str,
    type: str,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column must have the specified data type.

    Args:
        column: Column name to check
        type: Expected type (int64, float64, string, datetime, bool, etc.)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("dtype", {"column": column, "type": type}, severity, id)


def range(
    column: str,
    min: Optional[Union[int, float]] = None,
    max: Optional[Union[int, float]] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column values must be within the specified range.

    Args:
        column: Column name to check
        min: Minimum allowed value (inclusive)
        max: Maximum allowed value (inclusive)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    params: Dict[str, Any] = {"column": column}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max

    return _build_rule("range", params, severity, id)


def allowed_values(
    column: str,
    values: List[Any],
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column values must be in the allowed set.

    Args:
        column: Column name to check
        values: List of allowed values
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("allowed_values", {"column": column, "values": values}, severity, id)


def regex(
    column: str,
    pattern: str,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column values must match the regex pattern.

    Args:
        column: Column name to check
        pattern: Regular expression pattern
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("regex", {"column": column, "pattern": pattern}, severity, id)


def min_rows(
    threshold: int,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Dataset must have at least this many rows.

    Args:
        threshold: Minimum row count
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("min_rows", {"threshold": threshold}, severity, id)


def max_rows(
    threshold: int,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Dataset must have at most this many rows.

    Args:
        threshold: Maximum row count
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("max_rows", {"threshold": threshold}, severity, id)


def freshness(
    column: str,
    max_age: str,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Column timestamp must be within max_age of now.

    Args:
        column: Datetime column to check
        max_age: Maximum age (e.g., "24h", "7d", "1w")
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("freshness", {"column": column, "max_age": max_age}, severity, id)


def custom_sql_check(
    sql: str,
    severity: str = "blocking",
    id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Custom SQL check must return 0 rows (no violations).

    Args:
        sql: SQL query that returns rows that violate the rule
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple custom checks)

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("custom_sql_check", {"sql": sql}, severity, id)


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

    def __repr__(self) -> str:
        return "<kontra.rules module>"


# Export the module instance
rules = _RulesModule()
