# src/kontra/rule_defs/builtin/starts_with.py
"""
Starts with rule - Column must start with the specified prefix.

Uses LIKE pattern matching for maximum efficiency (faster than regex).

Usage:
    - name: starts_with
      params:
        column: url
        prefix: "https://"

Fails when:
    - Value does NOT start with the prefix
    - Value is NULL (can't check NULL)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.builtin.string_pattern import StringPatternRule


@register_rule("starts_with", _builtin=True)
class StartsWithRule(StringPatternRule):
    """
    Fails where column value does NOT start with the prefix.

    params:
      - column: str (required) - Column to check
      - prefix: str (required) - Prefix that must be present

    NULL handling:
      - NULL values are failures (can't check NULL)
    """

    _rule_kind = "starts_with"
    _param_name = "prefix"
    _failure_verb = "start with"

    def _polars_match(self, series: pl.Expr) -> pl.Expr:
        return series.str.starts_with(self._pattern_value)

    def _like_pattern(self, escaped: str) -> str:
        return f"{escaped}%"
