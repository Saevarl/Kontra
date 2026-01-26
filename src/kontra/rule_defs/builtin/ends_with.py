# src/kontra/rules/builtin/ends_with.py
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


@register_rule("ends_with")
class EndsWithRule(BaseRule):
    """
    Fails where column value does NOT end with the suffix.

    params:
      - column: str (required) - Column to check
      - suffix: str (required) - Suffix that must be present

    NULL handling:
      - NULL values are failures (can't check NULL)
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        self._suffix = self._get_required_param("suffix", str)

        if not self._suffix:
            raise ValueError("Rule 'ends_with' suffix cannot be empty")

    def required_columns(self) -> Set[str]:
        return {self._column}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check column exists before accessing
        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        # Use Polars str.ends_with for efficiency
        ends_result = df[self._column].cast(pl.Utf8).str.ends_with(self._suffix)

        # Failure = does NOT end with OR is NULL
        mask = (~ends_result).fill_null(True)

        msg = f"{self._column} does not end with '{self._suffix}'"
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
            "expected_suffix": self._suffix,
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
        ends_expr = pl.col(self._column).cast(pl.Utf8).str.ends_with(self._suffix)
        expr = (~ends_expr).fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{self._column} does not end with '{self._suffix}'",
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        return {
            "kind": "ends_with",
            "rule_id": self.rule_id,
            "column": self._column,
            "suffix": self._suffix,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """Generate SQL filter for sampling failing rows."""
        col = f'"{self._column}"'

        # Escape LIKE special characters
        escaped = _escape_like_pattern(self._suffix)
        pattern = f"%{escaped}"

        # Failure = does NOT end with OR is NULL
        return f"{col} IS NULL OR {col} NOT LIKE '{pattern}' ESCAPE '\\'"
