# src/kontra/rule_defs/builtin/ends_with.py
"""
Ends with rule - Column must end with the specified suffix.

Uses LIKE pattern matching for maximum efficiency (faster than regex).

Usage:
    - name: ends_with
      params:
        column: filename
        suffix: ".csv"

Fails when:
    - Value does NOT end with the suffix
    - Value is NULL (can't check NULL)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.builtin.string_pattern import StringPatternRule


@register_rule("ends_with", _builtin=True)
class EndsWithRule(StringPatternRule):
    """
    Fails where column value does NOT end with the suffix.

    params:
      - column: str (required) - Column to check
      - suffix: str (required) - Suffix that must be present

    NULL handling:
      - NULL values are failures (can't check NULL)
    """

    _rule_kind = "ends_with"
    _param_name = "suffix"
    _failure_verb = "end with"

    def _polars_match(self, series: pl.Expr) -> pl.Expr:
        return series.str.ends_with(self._pattern_value)

    def _like_pattern(self, escaped: str) -> str:
        return f"%{escaped}"
