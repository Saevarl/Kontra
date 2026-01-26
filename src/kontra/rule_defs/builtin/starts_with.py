# src/kontra/rules/builtin/starts_with.py
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


@register_rule("starts_with")
class StartsWithRule(BaseRule):
    """
    Fails where column value does NOT start with the prefix.

    params:
      - column: str (required) - Column to check
      - prefix: str (required) - Prefix that must be present

    NULL handling:
      - NULL values are failures (can't check NULL)
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        self._prefix = self._get_required_param("prefix", str)

        if not self._prefix:
            raise ValueError("Rule 'starts_with' prefix cannot be empty")

    def required_columns(self) -> Set[str]:
        return {self._column}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check column exists before accessing
        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        # Use Polars str.starts_with for efficiency
        starts_result = df[self._column].cast(pl.Utf8).str.starts_with(self._prefix)

        # Failure = does NOT start with OR is NULL
        mask = (~starts_result).fill_null(True)

        msg = f"{self._column} does not start with '{self._prefix}'"
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
            "expected_prefix": self._prefix,
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
        starts_expr = pl.col(self._column).cast(pl.Utf8).str.starts_with(self._prefix)
        expr = (~starts_expr).fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{self._column} does not start with '{self._prefix}'",
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        return {
            "kind": "starts_with",
            "rule_id": self.rule_id,
            "column": self._column,
            "prefix": self._prefix,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """Generate SQL filter for sampling failing rows."""
        col = f'"{self._column}"'

        # Escape LIKE special characters
        escaped = _escape_like_pattern(self._prefix)
        pattern = f"{escaped}%"

        # Failure = does NOT start with OR is NULL
        return f"{col} IS NULL OR {col} NOT LIKE '{pattern}' ESCAPE '\\'"
