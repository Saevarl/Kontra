# tests/test_severity.py
"""Tests for rule severity levels."""

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import pytest
import polars as pl
import yaml

from kontra.state.types import Severity, RuleState, StateSummary, ValidationState
from kontra.config.models import RuleSpec, Contract
from kontra.rule_defs.factory import RuleFactory
from kontra.engine.engine import ValidationEngine


class TestSeverityEnum:
    """Tests for the Severity enum."""

    def test_severity_values(self):
        """All severity levels exist."""
        assert Severity.BLOCKING.value == "blocking"
        assert Severity.WARNING.value == "warning"
        assert Severity.INFO.value == "info"

    def test_severity_str(self):
        """Severity has string representation."""
        assert str(Severity.BLOCKING) == "blocking"
        assert str(Severity.WARNING) == "warning"
        assert str(Severity.INFO) == "info"

    def test_severity_from_str(self):
        """Severity can be parsed from string."""
        assert Severity.from_str("blocking") == Severity.BLOCKING
        assert Severity.from_str("warning") == Severity.WARNING
        assert Severity.from_str("info") == Severity.INFO
        # Default to blocking for invalid values
        assert Severity.from_str("invalid") == Severity.BLOCKING
        assert Severity.from_str(None) == Severity.BLOCKING


class TestRuleSpecSeverity:
    """Tests for severity in RuleSpec model."""

    def test_default_severity_is_blocking(self):
        """RuleSpec defaults to blocking severity."""
        spec = RuleSpec(name="not_null", params={"column": "id"})
        assert spec.severity == "blocking"

    def test_explicit_severity(self):
        """RuleSpec accepts explicit severity."""
        spec = RuleSpec(name="not_null", params={"column": "id"}, severity="warning")
        assert spec.severity == "warning"

        spec = RuleSpec(name="not_null", params={"column": "id"}, severity="info")
        assert spec.severity == "info"

    def test_invalid_severity_raises(self):
        """Invalid severity values raise validation error."""
        with pytest.raises(Exception):  # Pydantic validation error
            RuleSpec(name="not_null", params={"column": "id"}, severity="critical")


class TestRuleFactorySeverity:
    """Tests for severity propagation through rule factory."""

    def test_factory_sets_severity_on_rules(self):
        """Factory passes severity to rule instances."""
        specs = [
            RuleSpec(name="not_null", params={"column": "id"}, severity="blocking"),
            RuleSpec(name="not_null", params={"column": "email"}, severity="warning"),
            RuleSpec(name="unique", params={"column": "id"}, severity="info"),
        ]
        factory = RuleFactory(specs)
        rules = factory.build_rules()

        assert len(rules) == 3
        assert rules[0].severity == "blocking"
        assert rules[1].severity == "warning"
        assert rules[2].severity == "info"


class TestRuleStateSeverity:
    """Tests for severity in RuleState."""

    def test_rule_state_default_severity(self):
        """RuleState defaults to blocking severity."""
        state = RuleState(
            rule_id="COL:id:not_null",
            rule_name="not_null",
            passed=True,
            failed_count=0,
            execution_source="polars",
        )
        assert state.severity == "blocking"

    def test_rule_state_explicit_severity(self):
        """RuleState accepts explicit severity."""
        state = RuleState(
            rule_id="COL:id:not_null",
            rule_name="not_null",
            passed=False,
            failed_count=10,
            execution_source="polars",
            severity="warning",
        )
        assert state.severity == "warning"

    def test_rule_state_to_dict_includes_severity(self):
        """RuleState.to_dict() includes severity."""
        state = RuleState(
            rule_id="COL:id:not_null",
            rule_name="not_null",
            passed=False,
            failed_count=10,
            execution_source="polars",
            severity="warning",
        )
        d = state.to_dict()
        assert d["severity"] == "warning"

    def test_rule_state_from_dict_preserves_severity(self):
        """RuleState.from_dict() preserves severity."""
        d = {
            "rule_id": "COL:id:not_null",
            "rule_name": "not_null",
            "passed": False,
            "failed_count": 10,
            "execution_source": "polars",
            "severity": "info",
        }
        state = RuleState.from_dict(d)
        assert state.severity == "info"

    def test_rule_state_from_result_preserves_severity(self):
        """RuleState.from_result() preserves severity."""
        result = {
            "rule_id": "COL:id:not_null",
            "passed": False,
            "failed_count": 10,
            "message": "id contains null values",
            "execution_source": "polars",
            "severity": "warning",
        }
        state = RuleState.from_result(result)
        assert state.severity == "warning"


class TestStateSummarySeverity:
    """Tests for severity counts in StateSummary."""

    def test_summary_severity_defaults(self):
        """StateSummary defaults severity counts to 0."""
        summary = StateSummary(
            passed=True,
            total_rules=5,
            passed_rules=5,
            failed_rules=0,
        )
        assert summary.blocking_failures == 0
        assert summary.warning_failures == 0
        assert summary.info_failures == 0

    def test_summary_severity_counts(self):
        """StateSummary tracks severity counts."""
        summary = StateSummary(
            passed=False,
            total_rules=10,
            passed_rules=7,
            failed_rules=3,
            blocking_failures=1,
            warning_failures=1,
            info_failures=1,
        )
        assert summary.blocking_failures == 1
        assert summary.warning_failures == 1
        assert summary.info_failures == 1

    def test_summary_to_dict_includes_severity(self):
        """StateSummary.to_dict() includes severity counts."""
        summary = StateSummary(
            passed=False,
            total_rules=10,
            passed_rules=7,
            failed_rules=3,
            blocking_failures=1,
            warning_failures=2,
            info_failures=0,
        )
        d = summary.to_dict()
        assert d["blocking_failures"] == 1
        assert d["warning_failures"] == 2
        assert d["info_failures"] == 0

    def test_summary_from_dict_preserves_severity(self):
        """StateSummary.from_dict() preserves severity counts."""
        d = {
            "passed": False,
            "total_rules": 10,
            "passed_rules": 7,
            "failed_rules": 3,
            "blocking_failures": 2,
            "warning_failures": 1,
            "info_failures": 0,
        }
        summary = StateSummary.from_dict(d)
        assert summary.blocking_failures == 2
        assert summary.warning_failures == 1
        assert summary.info_failures == 0


class TestValidationEngineSeverity:
    """Tests for severity in validation engine."""

    @pytest.fixture
    def temp_data(self, tmp_path):
        """Create temporary test data."""
        df = pl.DataFrame({
            "id": [1, 2, 3, None, 5],  # 1 null
            "email": ["a@x.com", "b@x.com", "b@x.com", "d@x.com", "e@x.com"],  # 1 duplicate
            "status": ["active", "inactive", "active", "unknown", "active"],  # 1 bad value
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        return path

    @pytest.fixture
    def contract_with_severities(self, temp_data, tmp_path):
        """Create contract with mixed severities."""
        contract = {
            "name": "test_severity",
            "dataset": str(temp_data),
            "rules": [
                {"name": "not_null", "params": {"column": "id"}, "severity": "blocking"},
                {"name": "unique", "params": {"column": "email"}, "severity": "warning"},
                {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive"]}, "severity": "info"},
            ]
        }
        path = tmp_path / "contract.yml"
        with open(path, "w") as f:
            yaml.dump(contract, f)
        return path

    def test_engine_propagates_severity_to_results(self, contract_with_severities):
        """Engine includes severity in rule results."""
        engine = ValidationEngine(
            contract_path=str(contract_with_severities),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        results = result["results"]
        assert len(results) == 3

        # Find each rule by ID
        id_rule = next(r for r in results if "id" in r["rule_id"])
        email_rule = next(r for r in results if "email" in r["rule_id"])
        status_rule = next(r for r in results if "status" in r["rule_id"])

        assert id_rule["severity"] == "blocking"
        assert email_rule["severity"] == "warning"
        assert status_rule["severity"] == "info"

    def test_engine_summary_includes_severity_counts(self, contract_with_severities):
        """Engine summary includes severity counts."""
        engine = ValidationEngine(
            contract_path=str(contract_with_severities),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        summary = result["summary"]
        assert summary["blocking_failures"] == 1  # not_null failed
        assert summary["warning_failures"] == 1   # unique failed
        assert summary["info_failures"] == 1      # allowed_values failed

    def test_engine_passes_with_only_warning_failures(self, temp_data, tmp_path):
        """Validation passes if only warning/info failures."""
        # Create data with only warning-level failures
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],  # all valid (no nulls)
            "email": ["a@x.com", "b@x.com", "b@x.com", "d@x.com", "e@x.com"],  # duplicate
        })
        data_path = tmp_path / "test2.parquet"
        df.write_parquet(data_path)

        contract = {
            "name": "test_warning_only",
            "dataset": str(data_path),
            "rules": [
                {"name": "not_null", "params": {"column": "id"}, "severity": "blocking"},
                {"name": "unique", "params": {"column": "email"}, "severity": "warning"},
            ]
        }
        contract_path = tmp_path / "contract2.yml"
        with open(contract_path, "w") as f:
            yaml.dump(contract, f)

        engine = ValidationEngine(
            contract_path=str(contract_path),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        # Should pass because only warning failure
        assert result["summary"]["passed"] is True
        assert result["summary"]["blocking_failures"] == 0
        assert result["summary"]["warning_failures"] == 1


class TestValidationStateSeverityLLM:
    """Tests for severity in LLM output."""

    def test_to_llm_includes_severity_breakdown(self):
        """to_llm() shows severity breakdown for failures."""
        rules = [
            RuleState("COL:id:not_null", "not_null", False, 10, "polars", severity="blocking"),
            RuleState("COL:email:unique", "unique", False, 5, "polars", severity="warning"),
            RuleState("COL:status:allowed_values", "allowed_values", True, 0, "polars"),
        ]
        summary = StateSummary(
            passed=False,
            total_rules=3,
            passed_rules=1,
            failed_rules=2,
            blocking_failures=1,
            warning_failures=1,
            info_failures=0,
        )
        state = ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="def456",
            contract_name="test",
            dataset_uri="data.parquet",
            run_at=datetime.now(timezone.utc),
            summary=summary,
            rules=rules,
        )

        llm_output = state.to_llm()

        # Should show severity breakdown
        assert "1 blocking" in llm_output
        assert "1 warning" in llm_output
        # Warning rule should show [warning] tag
        assert "[warning]" in llm_output

    def test_to_llm_no_severity_for_blocking_rules(self):
        """to_llm() doesn't show severity tag for blocking rules."""
        rules = [
            RuleState("COL:id:not_null", "not_null", False, 10, "polars", severity="blocking"),
        ]
        summary = StateSummary(
            passed=False,
            total_rules=1,
            passed_rules=0,
            failed_rules=1,
            blocking_failures=1,
        )
        state = ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="def456",
            contract_name="test",
            dataset_uri="data.parquet",
            run_at=datetime.now(timezone.utc),
            summary=summary,
            rules=rules,
        )

        llm_output = state.to_llm()

        # Should NOT show [blocking] tag (it's the default)
        assert "[blocking]" not in llm_output
        assert "COL:id:not_null" in llm_output
