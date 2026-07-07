# src/kontra/rule_defs/builtin/string_pattern.py
"""
Base class for string pattern rules (contains, starts_with, ends_with).

Subclasses only override pattern-building methods.
"""
from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.predicates import Predicate
from kontra.rule_defs.utils import escape_like_pattern
from kontra.state.types import FailureMode


class StringPatternRule(BaseRule):
    """
    Base class for string pattern matching rules.

    Subclasses must define:
      - _rule_kind: str (e.g., "contains")
      - _param_name: str (e.g., "substring")
      - _failure_verb: str (e.g., "contain")
      - _polars_match(series, pattern): Polars match expression
      - _like_pattern(escaped): SQL LIKE pattern string
    """

    _rule_kind: str
    _param_name: str
    _failure_verb: str

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        self._pattern_value = self._get_required_param(self._param_name, str)

        if not self._pattern_value:
            raise ValueError(f"Rule '{self._rule_kind}' {self._param_name} cannot be empty")

    def required_columns(self) -> Set[str]:
        return {self._column}

    @abstractmethod
    def _polars_match(self, series: pl.Expr) -> pl.Expr:
        """Return a Polars expression that is True where the value matches."""

    @abstractmethod
    def _like_pattern(self, escaped: str) -> str:
        """Return the SQL LIKE pattern (e.g., '%escaped%')."""

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        import polars as pl

        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        match_result = self._polars_match(df[self._column].cast(pl.Utf8))
        mask = (~match_result).fill_null(True)

        msg = f"{self._column} does not {self._failure_verb} '{self._pattern_value}'"
        res = super()._failures(df, mask, msg)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.PATTERN_MISMATCH)
            details: Dict[str, Any] = {
                "column": self._column,
                f"expected_{self._param_name}": self._pattern_value,
            }
            failed_df = df.filter(mask).head(5)
            samples: List[Any] = [val for val in failed_df[self._column]]
            if samples:
                details["sample_failures"] = samples
            res["details"] = details

        return res

    def compile_predicate(self) -> Optional[Predicate]:
        def _expr():
            import polars as pl

            match_expr = self._polars_match(pl.col(self._column).cast(pl.Utf8))
            return (~match_expr).fill_null(True)

        return Predicate(
            rule_id=self.rule_id,
            expr_factory=_expr,
            message=f"{self._column} does not {self._failure_verb} '{self._pattern_value}'",
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        return {
            "kind": self._rule_kind,
            "rule_id": self.rule_id,
            "column": self._column,
            self._param_name: self._pattern_value,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        col = f'"{self._column}"'
        escaped = escape_like_pattern(self._pattern_value)
        pattern = self._like_pattern(escaped)
        return f"{col} IS NULL OR {col} NOT LIKE '{pattern}' ESCAPE '\\'"
