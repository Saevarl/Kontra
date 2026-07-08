"""
Regression tests for rule-audit fixes:

1. regex: engine-invalid patterns (backreferences) rejected at construction.
2. range: min-only bound treats NaN as a violation (float columns).
3. dtype: "float" is a family alias (matches Float32/Float64); "double" stays exact Float64.
4. max_rows: negative thresholds rejected at construction (mirrors min_rows).
"""
from __future__ import annotations

import math

import pytest
import polars as pl

import kontra
from kontra import rules
from kontra.errors import RuleParameterError
from kontra.rule_defs.builtin.regex import RegexRule
from kontra.rule_defs.builtin.range import RangeRule
from kontra.rule_defs.builtin.max_rows import MaxRowsRule


# ---------------------------------------------------------------------------
# Finding 1: regex — engine-invalid patterns fail at construction
# ---------------------------------------------------------------------------


class TestRegexEnginePatternValidation:
    def test_valid_pattern_constructs_and_works(self):
        """A pattern valid in both Python re and the Polars engine still works."""
        rule = RegexRule("regex", {"column": "email", "pattern": r"^[^@]+@[^@]+$"})
        assert rule is not None

        df = pl.DataFrame({"email": ["a@b.com", "bad", None]})
        result = kontra.validate(
            df, rules=[rules.regex("email", r"^[^@]+@[^@]+$")], tally=True
        )
        assert not result.passed
        # "bad" and None are failures, "a@b.com" passes
        assert result.rules[0].failed_count == 2

    def test_backreference_raises_at_construction(self):
        """Backreference is valid in Python re but rejected by the Rust engine."""
        with pytest.raises(RuleParameterError) as exc:
            RegexRule("regex", {"column": "x", "pattern": r"(a)\1"})
        assert "execution engine" in str(exc.value)

    def test_backreference_via_validate_raises(self):
        """The construction error propagates through kontra.validate()."""
        df = pl.DataFrame({"x": ["aa", "ab"]})
        with pytest.raises(RuleParameterError):
            kontra.validate(df, rules=[rules.regex("x", r"(a)\1")])

    def test_lookahead_still_rejected(self):
        """Pre-existing lookahead guard is unaffected."""
        with pytest.raises(RuleParameterError):
            RegexRule("regex", {"column": "x", "pattern": r"(?=foo)"})


# ---------------------------------------------------------------------------
# Finding 2: range — min-only bound catches NaN on float columns
# ---------------------------------------------------------------------------


class TestRangeNaNViolation:
    def test_min_only_float_nan_is_violation(self):
        """range(min=0) on a float column treats NaN as out-of-range."""
        df = pl.DataFrame({"v": [10.0, float("nan"), -5.0, None]})
        result = kontra.validate(
            df, rules=[rules.range("v", min=0)], tally=True
        )
        assert not result.passed
        # NaN, -5.0, and None are all violations; 10.0 passes.
        assert result.rules[0].failed_count == 3

    def test_min_only_int_unaffected(self):
        """Integer min-only range is unchanged (no NaN possible)."""
        df = pl.DataFrame({"v": [10, 5, -5, 0]})
        result = kontra.validate(
            df, rules=[rules.range("v", min=0)], tally=True
        )
        assert not result.passed
        assert result.rules[0].failed_count == 1  # only -5

    def test_max_only_float_nan_still_violation(self):
        """max-only already caught NaN; behavior preserved."""
        df = pl.DataFrame({"v": [1.0, float("nan"), 200.0, None]})
        result = kontra.validate(
            df, rules=[rules.range("v", max=100)], tally=True
        )
        assert not result.passed
        # NaN, 200.0, None are violations; 1.0 passes.
        assert result.rules[0].failed_count == 3

    def test_string_column_range_does_not_crash(self):
        """is_nan gating on dtype avoids the not_null-style crash on non-float.

        is_float() is False for Utf8, so is_nan() is never called on a string
        column. The rule must run without raising InvalidOperationError.
        """
        df = pl.DataFrame({"v": ["a", "b", "c"]})
        # Should not raise (regression guard for the is_nan gating).
        result = kontra.validate(df, rules=[rules.range("v", min=0)])
        assert result is not None

    def test_compile_predicate_min_only_returns_none(self):
        """Min-only routes to validate() (dtype-safe NaN handling)."""
        rule = RangeRule("range", {"column": "v", "min": 0})
        assert rule.compile_predicate() is None

    def test_compile_predicate_max_only_vectorized(self):
        """Max-only stays vectorized (catches NaN via > max)."""
        rule = RangeRule("range", {"column": "v", "max": 100})
        assert rule.compile_predicate() is not None

    def test_compile_predicate_both_bounds_vectorized(self):
        rule = RangeRule("range", {"column": "v", "min": 0, "max": 100})
        assert rule.compile_predicate() is not None


@pytest.mark.integration
class TestRangeNaNTierEquivalence:
    """All tiers must agree on pass/fail for the NaN range fix."""

    def _make_parquet(self, tmp_path):
        df = pl.DataFrame({"v": [10.0, float("nan"), -5.0, None]})
        path = tmp_path / "range_nan.parquet"
        df.write_parquet(str(path))
        return str(path)

    def _run(self, contract_path, preplan, pushdown):
        from kontra.engine.engine import ValidationEngine

        eng = ValidationEngine(
            contract_path=contract_path,
            preplan=preplan,
            pushdown=pushdown,
            emit_report=False,
            stats_mode="summary",
            tally=True,
        )
        return eng.run()

    def test_all_tiers_agree_pass_fail(self, tmp_path, write_contract):
        data = self._make_parquet(tmp_path)
        cpath = write_contract(
            dataset=data,
            rules=[{"name": "range", "params": {"column": "v", "min": 0}}],
        )

        combos = [
            ("on", "off"),   # preplan
            ("off", "on"),   # sql pushdown
            ("off", "off"),  # pure polars
        ]
        passed_flags = []
        for preplan, pushdown in combos:
            out = self._run(cpath, preplan, pushdown)
            rule_res = next(r for r in out["results"] if r["rule_id"] == "COL:v:range")
            passed_flags.append(rule_res["passed"])

        # -5.0 and None guarantee a real violation in every tier.
        assert passed_flags == [False, False, False]


# ---------------------------------------------------------------------------
# Finding 3: dtype — "float" is a family alias, "double" stays exact
# ---------------------------------------------------------------------------


class TestDtypeFloatFamily:
    def test_float_matches_float32(self):
        """type:'float' is a family alias and matches Float32."""
        df = pl.DataFrame({"v": pl.Series([1.0, 2.0], dtype=pl.Float32)})
        result = kontra.validate(df, rules=[rules.dtype("v", "float")])
        assert result.passed

    def test_float_matches_float64(self):
        df = pl.DataFrame({"v": pl.Series([1.0, 2.0], dtype=pl.Float64)})
        result = kontra.validate(df, rules=[rules.dtype("v", "float")])
        assert result.passed

    def test_double_is_exact_float64(self):
        """type:'double' stays an exact Float64 alias (does NOT match Float32)."""
        df32 = pl.DataFrame({"v": pl.Series([1.0], dtype=pl.Float32)})
        assert not kontra.validate(df32, rules=[rules.dtype("v", "double")]).passed

        df64 = pl.DataFrame({"v": pl.Series([1.0], dtype=pl.Float64)})
        assert kontra.validate(df64, rules=[rules.dtype("v", "double")]).passed


# ---------------------------------------------------------------------------
# Finding 4: max_rows — negative threshold rejected at construction
# ---------------------------------------------------------------------------


class TestMaxRowsNegativeThreshold:
    def test_negative_value_raises(self):
        with pytest.raises(RuleParameterError):
            MaxRowsRule("max_rows", {"value": -5})

    def test_negative_threshold_alias_raises(self):
        with pytest.raises(RuleParameterError):
            MaxRowsRule("max_rows", {"threshold": -1})

    def test_zero_is_allowed(self):
        rule = MaxRowsRule("max_rows", {"value": 0})
        assert rule is not None

    def test_positive_is_allowed(self):
        rule = MaxRowsRule("max_rows", {"value": 100})
        assert rule is not None

    def test_parity_with_min_rows(self):
        """max_rows now mirrors min_rows negative-threshold rejection."""
        from kontra.rule_defs.builtin.min_rows import MinRowsRule

        with pytest.raises(RuleParameterError):
            MinRowsRule("min_rows", {"value": -5})
        with pytest.raises(RuleParameterError):
            MaxRowsRule("max_rows", {"value": -5})
