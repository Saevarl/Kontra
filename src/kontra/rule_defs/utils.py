# src/kontra/rule_defs/utils.py
"""
Shared utilities for rule implementations.

These helpers are used across multiple builtin rules to avoid code duplication.
"""

from __future__ import annotations


def escape_like_pattern(value: str, escape_char: str = "\\") -> str:
    """
    Escape SQL LIKE special characters: %, _, and the escape char.

    Used by contains, starts_with, ends_with rules for SQL filter generation.

    Args:
        value: The literal string to escape
        escape_char: The escape character (default: backslash)

    Returns:
        Escaped string safe for use in LIKE patterns

    Example:
        >>> escape_like_pattern("100%")
        '100\\%'
        >>> escape_like_pattern("under_score")
        'under\\_score'
    """
    for c in (escape_char, "%", "_"):
        value = value.replace(c, escape_char + c)
    return value
