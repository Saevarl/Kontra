from __future__ import annotations
from typing import Dict, Any, List, Optional
import re
import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode
from kontra.errors import RuleParameterError


@register_rule("regex")
class RegexRule(BaseRule):
    """
    Fails where `column` does not match the regex `pattern`. NULLs are failures.

    params:
      - column: str (required)
      - pattern: str (required)

    Notes:
      - Uses vectorized `str.contains` (regex by default in this Polars version).
      - No `regex=`/`strict=` kwargs are passed to maintain compatibility.
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        # Validate regex pattern early to provide helpful error message
        pattern = params.get("pattern", "")
        try:
            re.compile(pattern)
        except re.error as e:
            pos_info = f" at position {e.pos}" if e.pos is not None else ""
            raise RuleParameterError(
                "regex",
                "pattern",
                f"Invalid regex pattern{pos_info}: {e.msg}\n  Pattern: {pattern}"
            )

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        pattern = self.params["pattern"]

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        try:
            mask = (
                ~df[column]
                .cast(pl.Utf8)
                .str.contains(pattern)  # regex by default
            ).fill_null(True)
            res = super()._failures(df, mask, f"{column} failed regex pattern {pattern}")
            res["rule_id"] = self.rule_id

            # Add failure details
            if res["failed_count"] > 0:
                res["failure_mode"] = str(FailureMode.PATTERN_MISMATCH)
                res["details"] = self._explain_failure(df, column, pattern, mask)

            return res
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Rule execution failed: {e}",
            }

    def _explain_failure(
        self, df: pl.DataFrame, column: str, pattern: str, mask: pl.Series
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        details: Dict[str, Any] = {
            "pattern": pattern,
        }

        # Sample non-matching values (first 5)
        failed_df = df.filter(mask)
        if failed_df.height > 0:
            sample_values: List[Any] = []
            for val in failed_df[column].head(5):
                sample_values.append(val)
            if sample_values:
                details["sample_mismatches"] = sample_values

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        column = self.params["column"]
        pattern = self.params["pattern"]
        expr = (
            ~pl.col(column)
            .cast(pl.Utf8)
            .str.contains(pattern)  # regex by default
        ).fill_null(True)
        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=f"{column} failed regex pattern {pattern}",
            columns={column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification for regex rule."""
        column = self.params.get("column")
        pattern = self.params.get("pattern")

        if not column or not pattern:
            return None

        return {
            "kind": "regex",
            "rule_id": self.rule_id,
            "column": column,
            "pattern": pattern,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        column = self.params.get("column")
        pattern = self.params.get("pattern")

        if not column or not pattern:
            return None

        col = f'"{column}"'
        # Escape single quotes in pattern
        escaped_pattern = pattern.replace("'", "''")

        if dialect in ("postgres", "postgresql"):
            # PostgreSQL uses ~ for regex match, !~ for non-match
            return f"{col} !~ '{escaped_pattern}' OR {col} IS NULL"
        elif dialect == "duckdb":
            # DuckDB uses regexp_matches
            return f"NOT regexp_matches({col}, '{escaped_pattern}') OR {col} IS NULL"
        elif dialect == "mssql":
            # SQL Server doesn't have native regex - skip SQL filter
            return None
        else:
            return None
