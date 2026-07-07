# src/kontra/rule_defs/builtin/contains.py
"""
Contains rule - Column must contain the specified substring.

Uses literal substring matching (not regex) for maximum efficiency.
For regex patterns, use the `regex` rule instead.

Usage:
    - name: contains
      params:
        column: email
        substring: "@"

Fails when:
    - Value does NOT contain the substring
    - Value is NULL (can't search in NULL)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.builtin.string_pattern import StringPatternRule


@register_rule("contains", _builtin=True)
class ContainsRule(StringPatternRule):
    """
    Fails where column value does NOT contain the substring.

    params:
      - column: str (required) - Column to check
      - substring: str (required) - Substring that must be present

    This rule uses literal matching, not regex. For regex patterns,
    use the `regex` rule instead.

    NULL handling:
      - NULL values are failures (can't search in NULL)
    """

    _rule_kind = "contains"
    _param_name = "substring"
    _failure_verb = "contain"

    def _polars_match(self, series: pl.Expr) -> pl.Expr:
        return series.str.contains(self._pattern_value, literal=True)

    def _like_pattern(self, escaped: str) -> str:
        return f"%{escaped}%"
