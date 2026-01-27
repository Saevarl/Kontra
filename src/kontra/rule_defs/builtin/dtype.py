from __future__ import annotations
from typing import Dict, Any, Optional, Set, Tuple

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode


@register_rule("dtype")
class DtypeRule(BaseRule):
    """
    Dtype — schema-level type check for a single column.

    Params
    ------
      - column: str            # required
      - type:   str            # required
        Accepts either:
          * exact physical types: int8/int16/int32/int64, uint8/uint16/uint32/uint64,
                                  float32/float64 (or float/double as aliases),
                                  boolean/bool, utf8/string/str/text, date, datetime, time
          * logical families:    int/integer, float, numeric, string/str

      - mode: "strict"         # optional (default). Future: may support relaxed modes.

    Semantics
    ---------
    - Exact types require an exact match (e.g., "int16" passes only if the column is Int16).
    - Family types accept any member of the family (e.g., "int" accepts Int8/16/32/64).
    - Strings: "utf8", "string", "str", "text" are treated as the same family (Utf8 or String).
    - We do NOT cast — we only validate. (Casting hints may come via planner/materializers later.)

    Results
    -------
    - On mismatch or invalid config, `failed_count == nrows` (schema-level violation).
    - Message is deterministic: "<col> expected <expected>, found <ActualDtype>".
    """

    rule_scope = "schema"
    supports_tally = False

    # Valid type names (for error message)
    _VALID_TYPES = [
        # Exact types
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "float64", "float", "double",
        "bool", "boolean",
        "date", "datetime", "time",
        "utf8", "string", "str", "text",
        # Family types
        "int", "integer", "numeric",
    ]

    # ---- Aliases / Maps -----------------------------------------------------

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from kontra.errors import RuleParameterError

        expected_type = self.params.get("type")
        if expected_type is not None:
            label, allowed = self._normalize_expected(str(expected_type))
            if allowed is None:
                raise RuleParameterError(
                    "dtype", "type",
                    f"unknown type '{expected_type}'. Valid types: {', '.join(sorted(self._VALID_TYPES))}"
                )

    _STRING_ALIASES = {"utf8", "string", "str", "text"}

    # Exact physical types (single-member sets treated as "exact")
    _EXACT_MAP = {
        # signed ints
        "int8": {pl.Int8}, "int16": {pl.Int16}, "int32": {pl.Int32}, "int64": {pl.Int64},
        # unsigned ints
        "uint8": {pl.UInt8}, "uint16": {pl.UInt16}, "uint32": {pl.UInt32}, "uint64": {pl.UInt64},
        # floats
        "float32": {pl.Float32}, "float64": {pl.Float64},
        "float": {pl.Float64}, "double": {pl.Float64},  # common aliases treated as exact Float64
        # booleans
        "bool": {pl.Boolean}, "boolean": {pl.Boolean},
        # temporal
        "date": {pl.Date}, "datetime": {pl.Datetime}, "time": {pl.Time},
    }

    # Logical families (multi-member sets)
    _FAMILY_MAP = {
        "int": {pl.Int8, pl.Int16, pl.Int32, pl.Int64},
        "integer": {pl.Int8, pl.Int16, pl.Int32, pl.Int64},
        "float": {pl.Float32, pl.Float64},
        "numeric": {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64},
        "string": {pl.Utf8, getattr(pl, "String", pl.Utf8)},  # tolerate both Utf8 and String
        "str": {pl.Utf8, getattr(pl, "String", pl.Utf8)},
        "text": {pl.Utf8, getattr(pl, "String", pl.Utf8)},
        "utf8": {pl.Utf8, getattr(pl, "String", pl.Utf8)},
    }

    # ---- Normalization ------------------------------------------------------

    @staticmethod
    def _dtype_label(dt: pl.DataType) -> str:
        """Stable, user-friendly label for actual dtype in messages."""
        # Polars dtypes stringify nicely (e.g., "Int64", "Utf8").
        # Keep that behavior, but ensure Utf8/String variants read cleanly.
        if dt == pl.Utf8:
            return "Utf8"
        # Some Polars versions may have pl.String; prefer "Utf8" in messages for consistency.
        if getattr(pl, "String", None) and dt == getattr(pl, "String"):
            return "Utf8"
        return str(dt)

    def _normalize_expected(self, typ: str) -> Tuple[str, Optional[set]]:
        """
        Returns (label, allowed_set).
          - label: string echoed in error messages ("int16", "int", "date", ...)
          - allowed_set: a set of acceptable Polars dtypes (None if unknown)
        """
        t = (typ or "").strip().lower()
        if not t:
            return "<unspecified>", None

        # tolerate hyphen variants like "utf-8"
        t_no_dash = t.replace("-", "")

        # Family first (covers "string", "str", "utf8", etc.)
        if t in self._FAMILY_MAP:
            return t, self._FAMILY_MAP[t]
        if t_no_dash in self._FAMILY_MAP:
            return t_no_dash, self._FAMILY_MAP[t_no_dash]

        # Exact physical types (single-member sets)
        if t in self._EXACT_MAP:
            return t, self._EXACT_MAP[t]

        return t, None

    # ---- Rule contract ------------------------------------------------------

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params.get("column")
        expected_type = self.params.get("type")
        mode = (self.params.get("mode") or "strict").lower()

        if mode != "strict":
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Unsupported dtype mode '{mode}'; only 'strict' is implemented.",
            }

        if not isinstance(column, str) or not column:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": "Missing required 'column' parameter for dtype rule",
            }

        if column not in df.columns:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Column '{column}' not found for dtype check",
            }

        label, allowed = self._normalize_expected(str(expected_type) if expected_type is not None else "")
        if allowed is None:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Invalid expected dtype '{expected_type}'",
            }

        actual = df[column].dtype
        # Use equality comparison instead of set membership because parametric
        # types like Datetime(time_unit='us') have different hashes than pl.Datetime
        # but are equal via __eq__
        passed = any(actual == a for a in allowed)

        result: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "passed": bool(passed),
            "failed_count": 0 if passed else int(df.height),
            "message": "Passed" if passed else f"{column} expected {label}, found {self._dtype_label(actual)}",
        }

        if not passed:
            result["failure_mode"] = str(FailureMode.SCHEMA_DRIFT)
            result["details"] = {
                "expected_type": label,
                "actual_type": self._dtype_label(actual),
                "column": column,
            }

        return result

    def required_columns(self) -> Set[str]:
        # dtype check inspects the column’s dtype; ensure it is loaded (for projection).
        col = self.params.get("column")
        return {col} if isinstance(col, str) else set()
