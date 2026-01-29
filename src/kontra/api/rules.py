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


def _validate_column(column: Any, rule_name: str) -> str:
    """Validate that column is a non-empty string."""
    if column is None:
        raise ValueError(f"{rule_name}() requires a column name, got None")
    if not isinstance(column, str):
        raise ValueError(f"{rule_name}() column must be a string, got {type(column).__name__}")
    if not column.strip():
        raise ValueError(f"{rule_name}() column name cannot be empty")
    return column


def _build_rule(
    name: str,
    params: Dict[str, Any],
    severity: str,
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a rule dict, optionally with custom id, tally, and context."""
    rule: Dict[str, Any] = {
        "name": name,
        "params": params,
        "severity": severity,
    }
    if id is not None:
        rule["id"] = id
    if tally is not None:
        rule["tally"] = tally
    if context is not None:
        rule["context"] = context
    return rule


def not_null(
    column: str,
    severity: str = "blocking",
    include_nan: bool = False,
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must not contain null values.

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        include_nan: If True, also treat NaN as null (default: False)
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        By default, NaN values are NOT considered null (Polars behavior).
        Set include_nan=True to catch both NULL and NaN values in float columns.

    Returns:
        Rule dict for use with kontra.validate()
    """
    _validate_column(column, "not_null")
    params: Dict[str, Any] = {"column": column}
    if include_nan:
        params["include_nan"] = True

    return _build_rule("not_null", params, severity, id, tally, context)


def unique(
    column: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must be unique (no duplicates).

    Args:
        column: Column name to check
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    _validate_column(column, "unique")
    return _build_rule("unique", {"column": column}, severity, id, tally, context)


def dtype(
    column: str,
    type: Optional[str] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    *,
    dtype: Optional[str] = None,  # Alias for type (more intuitive)
) -> Dict[str, Any]:
    """
    Column must have the specified data type.

    Args:
        column: Column name to check
        type: Expected type (int64, float64, string, datetime, bool, etc.)
        dtype: Alias for type (use either, dtype is preferred)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        dtype is a schema-level rule and does not support tally (binary pass/fail).

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        rules.dtype("age", "int64")
        rules.dtype("age", dtype="int64")  # Same thing
    """
    _validate_column(column, "dtype")
    # Accept either 'type' or 'dtype' parameter (dtype takes precedence)
    actual_type = dtype if dtype is not None else type
    if actual_type is None:
        raise ValueError("dtype() requires 'type' or 'dtype' parameter")
    return _build_rule("dtype", {"column": column, "type": actual_type}, severity, id, None, context)


def range(
    column: str,
    min: Optional[Union[int, float]] = None,
    max: Optional[Union[int, float]] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
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
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If neither min nor max is provided, if min > max, or if min/max are not numeric
    """
    _validate_column(column, "range")

    # Validate at least one bound is provided
    if min is None and max is None:
        raise ValueError("range rule: at least one of 'min' or 'max' must be provided")

    # Validate min/max are numeric
    if min is not None and not isinstance(min, (int, float)):
        raise ValueError(f"range rule: min must be numeric, got {type(min).__name__}")
    if max is not None and not isinstance(max, (int, float)):
        raise ValueError(f"range rule: max must be numeric, got {type(max).__name__}")

    # Validate min <= max
    if min is not None and max is not None and min > max:
        raise ValueError(f"range rule: min ({min}) must be <= max ({max})")

    params: Dict[str, Any] = {"column": column}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max

    return _build_rule("range", params, severity, id, tally, context)


def allowed_values(
    column: str,
    values: List[Any],
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must be in the allowed set.

    Args:
        column: Column name to check
        values: List of allowed values
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()
    """
    _validate_column(column, "allowed_values")
    return _build_rule("allowed_values", {"column": column, "values": values}, severity, id, tally, context)


def regex(
    column: str,
    pattern: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must match the regex pattern.

    Args:
        column: Column name to check
        pattern: Regular expression pattern
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If pattern is not a valid regex
    """
    import re
    _validate_column(column, "regex")
    # Validate regex pattern early - fail fast with helpful message
    try:
        re.compile(pattern)
    except re.error as e:
        pos_info = f" at position {e.pos}" if e.pos is not None else ""
        raise ValueError(f"Invalid regex pattern{pos_info}: {e.msg}\n  Pattern: {pattern}")
    return _build_rule("regex", {"column": column, "pattern": pattern}, severity, id, tally, context)


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

    Note:
        min_rows is a dataset-level rule and does not support tally (binary pass/fail).

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If threshold is negative or not an integer
    """
    if not isinstance(threshold, int) or isinstance(threshold, bool):
        raise ValueError(f"min_rows() threshold must be an integer, got {type(threshold).__name__}")
    if threshold < 0:
        raise ValueError(f"min_rows threshold must be non-negative, got {threshold}")

    return _build_rule("min_rows", {"threshold": threshold}, severity, id, None, context)


def max_rows(
    threshold: int,
    severity: str = "blocking",
    id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Dataset must have at most this many rows.

    Args:
        threshold: Maximum row count (must be a non-negative integer)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        max_rows is a dataset-level rule and does not support tally (binary pass/fail).

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If threshold is negative or not an integer
    """
    if not isinstance(threshold, int) or isinstance(threshold, bool):
        raise ValueError(f"max_rows() threshold must be an integer, got {type(threshold).__name__}")
    if threshold < 0:
        raise ValueError(f"max_rows threshold must be non-negative, got {threshold}")
    return _build_rule("max_rows", {"threshold": threshold}, severity, id, None, context)


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

    Note:
        freshness is a dataset-level rule and does not support tally (binary pass/fail).

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If max_age is invalid or not provided
    """
    _validate_column(column, "freshness")

    # Validate max_age format
    if max_age is None:
        raise ValueError("freshness() requires max_age parameter")
    if not isinstance(max_age, str):
        raise ValueError(f"freshness() max_age must be a string, got {type(max_age).__name__}")

    # Validate max_age is parseable
    from kontra.rule_defs.builtin.freshness import parse_duration
    try:
        parse_duration(max_age)
    except ValueError as e:
        raise ValueError(f"freshness() invalid max_age: {e}") from None

    return _build_rule("freshness", {"column": column, "max_age": max_age}, severity, id, None, context)


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

    Note:
        custom_sql_check does not support tally (user controls the SQL).

    Returns:
        Rule dict for use with kontra.validate()
    """
    return _build_rule("custom_sql_check", {"sql": sql, "threshold": threshold}, severity, id, None, context)


def compare(
    left: str,
    right: str,
    op: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
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
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        Rows where either column is NULL are counted as failures.
        You cannot meaningfully compare NULL values.

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        # Ensure end_date >= start_date
        rules.compare("end_date", "start_date", ">=")

    Raises:
        ValueError: If op is not a valid comparison operator
    """
    _validate_column(left, "compare (left)")
    _validate_column(right, "compare (right)")
    valid_ops = {">", ">=", "<", "<=", "==", "!="}
    if op not in valid_ops:
        raise ValueError(f"Invalid comparison operator '{op}'. Must be one of: {', '.join(sorted(valid_ops))}")
    return _build_rule("compare", {"left": left, "right": right, "op": op}, severity, id, tally, context)


def conditional_not_null(
    column: str,
    when: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column must not be NULL when a condition is met.

    Args:
        column: Column that must not be null
        when: Condition expression (e.g., "status == 'shipped'")
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules)
        tally: Count all violations (True) or early-stop (False/None)
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
    _validate_column(column, "conditional_not_null")
    return _build_rule("conditional_not_null", {"column": column, "when": when}, severity, id, tally, context)


def conditional_range(
    column: str,
    when: str,
    min: Optional[Union[int, float]] = None,
    max: Optional[Union[int, float]] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
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
        tally: Count all violations (True) or early-stop (False/None)
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

    Raises:
        ValueError: If neither min nor max is provided
    """
    _validate_column(column, "conditional_range")
    if min is None and max is None:
        raise ValueError("conditional_range requires at least one of 'min' or 'max'")
    params = {"column": column, "when": when}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max
    return _build_rule("conditional_range", params, severity, id, tally, context)


def disallowed_values(
    column: str,
    values: List[Any],
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must NOT be in the disallowed set.

    Inverse of allowed_values: fails if value IS in the list.

    Args:
        column: Column name to check
        values: List of disallowed values
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        NULL values are NOT failures (NULL is not in any list).

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        rules.disallowed_values("status", ["deleted", "banned", "spam"])
    """
    _validate_column(column, "disallowed_values")
    return _build_rule("disallowed_values", {"column": column, "values": values}, severity, id, tally, context)


def length(
    column: str,
    min: Optional[int] = None,
    max: Optional[int] = None,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column string length must be within the specified range.

    Args:
        column: Column name to check
        min: Minimum length (inclusive)
        max: Maximum length (inclusive)
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    At least one of `min` or `max` must be provided.

    Note:
        NULL values are failures (can't measure length of NULL).

    Returns:
        Rule dict for use with kontra.validate()

    Raises:
        ValueError: If neither min nor max is provided, if min > max, or if min/max are not integers

    Example:
        rules.length("username", min=3, max=50)
    """
    _validate_column(column, "length")

    if min is None and max is None:
        raise ValueError("length rule: at least one of 'min' or 'max' must be provided")

    # Validate min/max are integers (length must be whole number)
    if min is not None and not isinstance(min, int):
        raise ValueError(f"length rule: min must be an integer, got {type(min).__name__}")
    if max is not None and not isinstance(max, int):
        raise ValueError(f"length rule: max must be an integer, got {type(max).__name__}")

    if min is not None and max is not None and min > max:
        raise ValueError(f"length rule: min ({min}) must be <= max ({max})")

    params: Dict[str, Any] = {"column": column}
    if min is not None:
        params["min"] = min
    if max is not None:
        params["max"] = max

    return _build_rule("length", params, severity, id, tally, context)


def contains(
    column: str,
    substring: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must contain the specified substring.

    Uses literal substring matching for efficiency.
    For regex patterns, use the `regex` rule instead.

    Args:
        column: Column name to check
        substring: Substring that must be present
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        NULL values are failures (can't search in NULL).

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        rules.contains("email", "@")
    """
    _validate_column(column, "contains")
    if not substring:
        raise ValueError("contains rule: substring cannot be empty")
    return _build_rule("contains", {"column": column, "substring": substring}, severity, id, tally, context)


def starts_with(
    column: str,
    prefix: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must start with the specified prefix.

    Uses LIKE pattern matching for efficiency (faster than regex).

    Args:
        column: Column name to check
        prefix: Prefix that must be present at the start
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        NULL values are failures (can't check NULL).

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        rules.starts_with("url", "https://")
    """
    _validate_column(column, "starts_with")
    if not prefix:
        raise ValueError("starts_with rule: prefix cannot be empty")
    return _build_rule("starts_with", {"column": column, "prefix": prefix}, severity, id, tally, context)


def ends_with(
    column: str,
    suffix: str,
    severity: str = "blocking",
    id: Optional[str] = None,
    tally: Optional[bool] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Column values must end with the specified suffix.

    Uses LIKE pattern matching for efficiency (faster than regex).

    Args:
        column: Column name to check
        suffix: Suffix that must be present at the end
        severity: "blocking" | "warning" | "info"
        id: Custom rule ID (use when applying multiple rules to same column)
        tally: Count all violations (True) or early-stop (False/None)
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)

    Note:
        NULL values are failures (can't check NULL).

    Returns:
        Rule dict for use with kontra.validate()

    Example:
        rules.ends_with("filename", ".csv")
    """
    _validate_column(column, "ends_with")
    if not suffix:
        raise ValueError("ends_with rule: suffix cannot be empty")
    return _build_rule("ends_with", {"column": column, "suffix": suffix}, severity, id, tally, context)


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
    disallowed_values = staticmethod(disallowed_values)
    regex = staticmethod(regex)
    length = staticmethod(length)
    contains = staticmethod(contains)
    starts_with = staticmethod(starts_with)
    ends_with = staticmethod(ends_with)
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
