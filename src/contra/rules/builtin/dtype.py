from __future__ import annotations
from typing import Dict, Any, Optional, Set

import polars as pl

from contra.rules.base import BaseRule
from contra.rules.registry import register_rule


@register_rule("dtype")
class DtypeRule(BaseRule):
    """
    Dataset-level strict dtype check for a single column.

    params:
      - column: str        # required
      - type: str          # required (e.g., "int64", "float64", "utf8", "string", "boolean", "date", "datetime")
      - mode: "strict"     # optional; only "strict" supported for now (default)

    Semantics:
      - Treat Polars' "Utf8" and "String" as equivalent string types.
      - Map common aliases to Polars dtypes and compare dtype objects, not their string names.
    """

    _STRING_ALIASES = {"utf8", "string", "str", "text"}

    _DTYPE_MAP = {
        # integers
        "int8": pl.Int8, "int16": pl.Int16, "int32": pl.Int32, "int64": pl.Int64,
        "uint8": pl.UInt8, "uint16": pl.UInt16, "uint32": pl.UInt32, "uint64": pl.UInt64,
        # floats
        "float32": pl.Float32, "float64": pl.Float64, "double": pl.Float64, "float": pl.Float64,
        # booleans
        "bool": pl.Boolean, "boolean": pl.Boolean,
        # temporal
        "date": pl.Date, "datetime": pl.Datetime,
        # strings handled via alias set above
    }

    def _normalize_expected(self, typ: str):
        """Return a tuple (kind, dtype_or_set). kind='string' or 'dtype'."""
        t = (typ or "").strip().lower()

        # strings: accept Utf8 or String, regardless of Polars version
        if t in self._STRING_ALIASES:
            return "string", {pl.Utf8, getattr(pl, "String", pl.Utf8)}

        # mapped numeric/temporal dtypes
        if t in self._DTYPE_MAP:
            return "dtype", self._DTYPE_MAP[t]

        # tolerate 'utf-8' variant
        if t.replace("-", "") in self._STRING_ALIASES:
            return "string", {pl.Utf8, getattr(pl, "String", pl.Utf8)}

        # fall back: try to resolve dynamically, else raise
        raise ValueError(f"Unsupported/unknown dtype alias: '{typ}'")

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        expected_type = self.params.get("type")
        mode = (self.params.get("mode") or "strict").lower()

        if mode != "strict":
            # only strict supported right now
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Unsupported dtype mode '{mode}'; only 'strict' is implemented.",
            }

        if column not in df.columns:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Column '{column}' not found for dtype check",
            }

        try:
            kind, exp = self._normalize_expected(expected_type)
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Invalid expected dtype '{expected_type}': {e}",
            }

        actual = df[column].dtype

        if kind == "string":
            passed = (actual in exp)
        else:
            passed = (actual == exp)

        return {
            "rule_id": self.rule_id,
            "passed": bool(passed),
            "failed_count": 0 if passed else int(df.height),
            "message": (
                "Passed"
                if passed
                else f"{column} expected {expected_type}, found {actual}"
            ),
        }
    
    def required_columns(self) -> Set[str]:
        # strict dtype check inspects the column's dtype; ensure it's loaded
        col = self.params.get("column")
        return {col} if isinstance(col, str) else set()
