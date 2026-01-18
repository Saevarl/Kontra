# tests/test_severity_weights.py
"""Tests for severity weights and quality score (v0.6 LLM enhancements)."""

import pytest
import polars as pl

from kontra.api.results import ValidationResult, RuleResult


class TestSeverityWeights:
    """Test severity weight propagation and RuleResult integration."""

    def test_rule_result_severity_weight_default_none(self):
        """Severity weight is None by default."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=True,
            failed_count=0,
            message="Passed",
            severity="blocking",
        )
        assert rule.severity_weight is None

    def test_rule_result_severity_weight_set(self):
        """Severity weight can be set explicitly."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=5,
            message="5 nulls",
            severity="blocking",
            severity_weight=1.0,
        )
        assert rule.severity_weight == 1.0

    def test_rule_result_to_dict_includes_weight(self):
        """to_dict includes severity_weight when set."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=5,
            message="5 nulls",
            severity="blocking",
            severity_weight=0.8,
        )
        d = rule.to_dict()
        assert d["severity_weight"] == 0.8

    def test_rule_result_to_dict_excludes_none_weight(self):
        """to_dict excludes severity_weight when None."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=True,
            failed_count=0,
            message="Passed",
            severity="blocking",
        )
        d = rule.to_dict()
        assert "severity_weight" not in d

    def test_rule_result_to_llm_includes_weight(self):
        """to_llm includes weight in compact format."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=5,
            message="5 nulls",
            severity="blocking",
            severity_weight=1.0,
        )
        llm = rule.to_llm()
        assert "[w=1.0]" in llm

    def test_rule_result_to_llm_excludes_none_weight(self):
        """to_llm excludes weight when None."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=5,
            message="5 nulls",
            severity="blocking",
        )
        llm = rule.to_llm()
        assert "[w=" not in llm


class TestQualityScore:
    """Test quality score calculation."""

    def test_quality_score_none_without_weights(self):
        """Quality score is None when no weights configured."""
        result = ValidationResult(
            passed=False,
            dataset="test",
            total_rows=100,
            total_rules=2,
            passed_count=1,
            failed_count=1,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=False,
                    failed_count=10,
                    message="10 nulls",
                    severity="blocking",
                    # No severity_weight
                ),
            ],
        )
        assert result.quality_score is None

    def test_quality_score_none_with_zero_rows(self):
        """Quality score is None when total_rows is 0."""
        result = ValidationResult(
            passed=True,
            dataset="test",
            total_rows=0,
            total_rules=1,
            passed_count=1,
            failed_count=0,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="DATASET:min_rows",
                    name="min_rows",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    severity_weight=1.0,
                ),
            ],
        )
        assert result.quality_score is None

    def test_quality_score_perfect_all_pass(self):
        """Quality score is 1.0 when all rules pass."""
        result = ValidationResult(
            passed=True,
            dataset="test",
            total_rows=100,
            total_rules=2,
            passed_count=2,
            failed_count=0,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    severity_weight=1.0,
                ),
                RuleResult(
                    rule_id="COL:name:unique",
                    name="unique",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="warning",
                    severity_weight=0.5,
                ),
            ],
        )
        assert result.quality_score == 1.0

    def test_quality_score_calculation(self):
        """Quality score calculated correctly from weighted violations."""
        # Setup:
        # - Rule 1: blocking (weight 1.0), 10 failures
        # - Rule 2: warning (weight 0.5), 4 failures
        # - Rule 3: blocking (weight 1.0), 0 failures (passed)
        # - Total rows: 100
        #
        # Weighted violations: 10*1.0 + 4*0.5 + 0*1.0 = 12
        # Total weight: 1.0 + 0.5 + 1.0 = 2.5
        # Max weighted violations: 100 * 2.5 = 250
        # Violation rate: 12 / 250 = 0.048
        # Quality score: 1.0 - 0.048 = 0.952

        result = ValidationResult(
            passed=False,
            dataset="test",
            total_rows=100,
            total_rules=3,
            passed_count=1,
            failed_count=1,
            warning_count=1,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=False,
                    failed_count=10,
                    message="10 nulls",
                    severity="blocking",
                    severity_weight=1.0,
                ),
                RuleResult(
                    rule_id="COL:name:unique",
                    name="unique",
                    passed=False,
                    failed_count=4,
                    message="4 dupes",
                    severity="warning",
                    severity_weight=0.5,
                ),
                RuleResult(
                    rule_id="COL:status:allowed_values",
                    name="allowed_values",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    severity_weight=1.0,
                ),
            ],
        )

        assert result.quality_score == pytest.approx(0.952, rel=0.001)

    def test_quality_score_partial_weights(self):
        """Quality score only considers rules with weights."""
        result = ValidationResult(
            passed=False,
            dataset="test",
            total_rows=100,
            total_rules=2,
            passed_count=0,
            failed_count=2,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=False,
                    failed_count=10,
                    message="10 nulls",
                    severity="blocking",
                    severity_weight=1.0,  # Has weight
                ),
                RuleResult(
                    rule_id="COL:name:unique",
                    name="unique",
                    passed=False,
                    failed_count=50,
                    message="50 dupes",
                    severity="blocking",
                    # No weight - not counted
                ),
            ],
        )

        # Only rule with weight counts
        # Weighted violations: 10*1.0 = 10
        # Total weight: 1.0
        # Max: 100 * 1.0 = 100
        # Rate: 10/100 = 0.1
        # Score: 1.0 - 0.1 = 0.9
        assert result.quality_score == pytest.approx(0.9, rel=0.001)

    def test_quality_score_to_dict(self):
        """to_dict includes quality_score when present."""
        result = ValidationResult(
            passed=True,
            dataset="test",
            total_rows=100,
            total_rules=1,
            passed_count=1,
            failed_count=0,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    severity_weight=1.0,
                ),
            ],
        )

        d = result.to_dict()
        assert "quality_score" in d
        assert d["quality_score"] == 1.0

    def test_quality_score_to_dict_excludes_when_none(self):
        """to_dict excludes quality_score when None."""
        result = ValidationResult(
            passed=True,
            dataset="test",
            total_rows=100,
            total_rules=1,
            passed_count=1,
            failed_count=0,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    # No weight
                ),
            ],
        )

        d = result.to_dict()
        assert "quality_score" not in d

    def test_quality_score_to_llm(self):
        """to_llm includes quality score in header."""
        result = ValidationResult(
            passed=False,
            dataset="test",
            total_rows=100,
            total_rules=1,
            passed_count=0,
            failed_count=1,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=False,
                    failed_count=10,
                    message="10 nulls",
                    severity="blocking",
                    severity_weight=1.0,
                ),
            ],
        )

        llm = result.to_llm()
        assert "[score=" in llm
        assert "0.90" in llm  # 1.0 - 10/100 = 0.90

    def test_quality_score_to_llm_excludes_when_none(self):
        """to_llm excludes score when None."""
        result = ValidationResult(
            passed=True,
            dataset="test",
            total_rows=100,
            total_rules=1,
            passed_count=1,
            failed_count=0,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    # No weight
                ),
            ],
        )

        llm = result.to_llm()
        assert "[score=" not in llm


class TestConfigIntegration:
    """Test config file integration for severity weights."""

    def test_effective_config_severity_weights_default_none(self):
        """EffectiveConfig has severity_weights=None by default."""
        from kontra.config.settings import EffectiveConfig

        cfg = EffectiveConfig()
        assert cfg.severity_weights is None

    def test_effective_config_severity_weights_in_to_dict(self):
        """EffectiveConfig.to_dict includes severity_weights when set."""
        from kontra.config.settings import EffectiveConfig

        cfg = EffectiveConfig()
        cfg.severity_weights = {"blocking": 1.0, "warning": 0.5}

        d = cfg.to_dict()
        assert "severity_weights" in d
        assert d["severity_weights"] == {"blocking": 1.0, "warning": 0.5}

    def test_effective_config_severity_weights_excluded_when_none(self):
        """EffectiveConfig.to_dict excludes severity_weights when None."""
        from kontra.config.settings import EffectiveConfig

        cfg = EffectiveConfig()
        d = cfg.to_dict()
        assert "severity_weights" not in d
