# src/kontra/rules/builtin/length.py
"""
Length rule - Column string length must be within specified bounds.

Usage:
    - name: length
      params:
        column: username
        min: 3
        max: 50

Fails when:
    - String length < min (if min specified)
    - String length > max (if max specified)
    - Value is NULL (can't measure length of NULL)

At least one of `min` or `max` must be specified.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.rule_defs.predicates import Predicate
from kontra.state.types import FailureMode


@register_rule("length")
class LengthRule(BaseRule):
    """
    Fails where string length is outside [min, max] bounds.

    params:
      - column: str (required) - Column to check
      - min: int (optional) - Minimum length (inclusive)
      - max: int (optional) - Maximum length (inclusive)

    At least one of min or max must be provided.

    NULL handling:
      - NULL values are failures (can't measure length of NULL)
    """

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._column = self._get_required_param("column", str)
        self._min_len: Optional[int] = params.get("min")
        self._max_len: Optional[int] = params.get("max")

        # Validate at least one bound is provided
        if self._min_len is None and self._max_len is None:
            raise ValueError(
                f"Rule 'length' requires at least one of 'min' or 'max' parameters"
            )

        # Validate min <= max if both provided
        if self._min_len is not None and self._max_len is not None:
            if self._min_len > self._max_len:
                raise ValueError(
                    f"Rule 'length' min ({self._min_len}) must be <= max ({self._max_len})"
                )

        # Validate non-negative
        if self._min_len is not None and self._min_len < 0:
            raise ValueError(f"Rule 'length' min must be non-negative, got {self._min_len}")
        if self._max_len is not None and self._max_len < 0:
            raise ValueError(f"Rule 'length' max must be non-negative, got {self._max_len}")

    def required_columns(self) -> Set[str]:
        return {self._column}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Check column exists before accessing
        col_check = self._check_columns(df, {self._column})
        if col_check is not None:
            return col_check

        # Get string length (cast to string first to handle non-string columns)
        length_col = df[self._column].cast(pl.Utf8).str.len_chars()

        # Build mask: True = failure
        mask = df[self._column].is_null()  # NULL is failure

        if self._min_len is not None:
            mask = mask | (length_col < self._min_len)
        if self._max_len is not None:
            mask = mask | (length_col > self._max_len)

        # Build message
        if self._min_len is not None and self._max_len is not None:
            msg = f"{self._column} length not in range [{self._min_len}, {self._max_len}]"
        elif self._min_len is not None:
            msg = f"{self._column} length < {self._min_len}"
        else:
            msg = f"{self._column} length > {self._max_len}"

        res = super()._failures(df, mask, msg)
        res["rule_id"] = self.rule_id

        if res["failed_count"] > 0:
            res["failure_mode"] = str(FailureMode.RANGE_VIOLATION)
            res["details"] = self._explain_failure(df, length_col, mask)

        return res

    def _explain_failure(
        self, df: pl.DataFrame, length_col: pl.Series, mask: pl.Series
    ) -> Dict[str, Any]:
        """Generate detailed failure explanation."""
        details: Dict[str, Any] = {
            "column": self._column,
        }
        if self._min_len is not None:
            details["min_length"] = self._min_len
        if self._max_len is not None:
            details["max_length"] = self._max_len

        # Sample failing values with their lengths
        failed_df = df.filter(mask).with_columns(
            length_col.filter(mask).alias("_length")
        ).head(5)

        samples: List[Dict[str, Any]] = []
        for row in failed_df.iter_rows(named=True):
            val = row[self._column]
            length = row.get("_length")
            samples.append({
                "value": val,
                "length": length,
            })

        if samples:
            details["sample_failures"] = samples

        return details

    def compile_predicate(self) -> Optional[Predicate]:
        # Get string length
        length_expr = pl.col(self._column).cast(pl.Utf8).str.len_chars()

        # Build mask: True = failure
        expr = pl.col(self._column).is_null()

        if self._min_len is not None:
            expr = expr | (length_expr < self._min_len)
        if self._max_len is not None:
            expr = expr | (length_expr > self._max_len)

        # Build message
        if self._min_len is not None and self._max_len is not None:
            msg = f"{self._column} length not in range [{self._min_len}, {self._max_len}]"
        elif self._min_len is not None:
            msg = f"{self._column} length < {self._min_len}"
        else:
            msg = f"{self._column} length > {self._max_len}"

        return Predicate(
            rule_id=self.rule_id,
            expr=expr,
            message=msg,
            columns={self._column},
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL pushdown specification."""
        return {
            "kind": "length",
            "rule_id": self.rule_id,
            "column": self._column,
            "min": self._min_len,
            "max": self._max_len,
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """Generate SQL filter for sampling failing rows."""
        col = f'"{self._column}"'

        # SQL Server uses LEN(), others use LENGTH()
        if dialect in ("mssql", "sqlserver"):
            len_func = f"LEN({col})"
        else:
            len_func = f"LENGTH({col})"

        conditions = [f"{col} IS NULL"]
        if self._min_len is not None:
            conditions.append(f"{len_func} < {self._min_len}")
        if self._max_len is not None:
            conditions.append(f"{len_func} > {self._max_len}")

        return " OR ".join(conditions)
