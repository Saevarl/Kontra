# src/kontra/rules/builtin/contains.py
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

from typing import Any, Dict, List, Optional, Set

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


def _escape_like_pattern(value: str, escape_char: str = "\\") -> str:
    """Escape LIKE special characters: %, _, and the escape char."""
    for c in (escape_char, "%", "_"):
        value = value.replace(c, escape_char + c)
    return value


@register_rule("contains")
class ContainsRule(BaseRule):
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

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        self._substring = self._get_required_param("substring", str)

        if not self._substring:
            raise ValueError("Rule 'contains' substring cannot be empty")

    def required_columns(self) -> Set[str]:
        return {self._column}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check column exists before accessing
        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        # Use literal=True for efficiency (not regex)
        contains_result = df[self._column].cast(pl.Utf8).str.contains(
            self._substring, literal=True
        )

        # Failure = does NOT contain OR is NULL
        mask = (~contains_result).fill_null(True)

        msg = f"{self._column} does not contain '{self._substring}'"
        res = super()._failures(df, mask, msg)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.PATTERN_MISMATCH)
            res["details"] = self._explain_failure(df, mask)

        return res

    def _explain_failure(self, df: pl.DataFrame, mask: pl.Series) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        details: Dict[str, Any] = {
            "column": self._column,
            "expected_substring": self._substring,
        }

        # Sample failing values
        failed_df = df.filter(mask).head(5)
        samples: List[Any] = []
        for val in failed_df[self._column]:
            samples.append(val)

        if samples:
            details["sample_failures"] = samples

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        # Use literal=True for efficiency
        contains_expr = pl.col(self._column).cast(pl.Utf8).str.contains(
            self._substring, literal=True
        )
        expr = (~contains_expr).fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{self._column} does not contain '{self._substring}'",
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        return {
            "kind": "contains",
            "rule_id": self.rule_id,
            "column": self._column,
            "substring": self._substring,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """Generate SQL filter for sampling failing rows."""
        col = f'"{self._column}"'

        # Escape LIKE special characters
        escaped = _escape_like_pattern(self._substring)
        pattern = f"%{escaped}%"

        # Failure = does NOT contain OR is NULL
        return f"{col} IS NULL OR {col} NOT LIKE '{pattern}' ESCAPE '\\'"
