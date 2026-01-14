# tests/test_state.py
"""
Tests for validation state management.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kontra.state.types import ValidationState, RuleState, StateSummary, StateDiff, RuleDiff
from kontra.state.fingerprint import (
    fingerprint_contract,
    fingerprint_from_name_and_uri,
)
from kontra.state.backends.local import LocalStore


# ---------------------------------------------------------------------------
# RuleState Tests
# ---------------------------------------------------------------------------


class TestRuleState:
    """Tests for RuleState dataclass."""

    def test_to_dict_minimal(self):
        """Test serialization with minimal fields."""
        rule = RuleState(
            rule_id="COL:user_id:not_null",
            rule_name="not_null",
            passed=True,
            failed_count=0,
            execution_source="metadata",
        )

        d = rule.to_dict()

        assert d["rule_id"] == "COL:user_id:not_null"
        assert d["rule_name"] == "not_null"
        assert d["passed"] is True
        assert d["failed_count"] == 0
        assert d["execution_source"] == "metadata"
        assert "failure_mode" not in d  # Optional field not included

    def test_to_dict_with_details(self):
        """Test serialization with failure details."""
        rule = RuleState(
            rule_id="COL:status:allowed_values",
            rule_name="allowed_values",
            passed=False,
            failed_count=42,
            execution_source="sql",
            failure_mode="novel_category",
            details={"unexpected_values": [{"value": "archived", "count": 42}]},
            message="status contains disallowed values",
            column="status",
        )

        d = rule.to_dict()

        assert d["passed"] is False
        assert d["failed_count"] == 42
        assert d["failure_mode"] == "novel_category"
        assert d["details"]["unexpected_values"][0]["value"] == "archived"
        assert d["column"] == "status"

    def test_from_dict_roundtrip(self):
        """Test serialization roundtrip."""
        original = RuleState(
            rule_id="COL:email:regex",
            rule_name="regex",
            passed=False,
            failed_count=15,
            execution_source="polars",
            failure_mode="pattern_violation",
            column="email",
        )

        d = original.to_dict()
        restored = RuleState.from_dict(d)

        assert restored.rule_id == original.rule_id
        assert restored.passed == original.passed
        assert restored.failed_count == original.failed_count
        assert restored.failure_mode == original.failure_mode

    def test_from_result(self):
        """Test creating from validation engine result."""
        result = {
            "rule_id": "COL:user_id:not_null",
            "name": "not_null",
            "passed": True,
            "failed_count": 0,
            "execution_source": "metadata",
            "message": "user_id has no nulls",
        }

        rule = RuleState.from_result(result)

        assert rule.rule_id == "COL:user_id:not_null"
        assert rule.column == "user_id"  # Extracted from rule_id
        assert rule.passed is True


# ---------------------------------------------------------------------------
# ValidationState Tests
# ---------------------------------------------------------------------------


class TestValidationState:
    """Tests for ValidationState dataclass."""

    def test_to_dict(self):
        """Test full serialization."""
        state = ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="def456",
            contract_name="users_contract",
            dataset_uri="data/users.parquet",
            run_at=datetime(2024, 1, 13, 10, 30, 0, tzinfo=timezone.utc),
            summary=StateSummary(
                passed=True,
                total_rules=5,
                passed_rules=5,
                failed_rules=0,
                row_count=1000000,
            ),
            rules=[
                RuleState(
                    rule_id="COL:user_id:not_null",
                    rule_name="not_null",
                    passed=True,
                    failed_count=0,
                    execution_source="metadata",
                ),
            ],
            duration_ms=1234,
        )

        d = state.to_dict()

        assert d["schema_version"] == "1.0"
        assert d["contract_fingerprint"] == "abc123"
        assert d["contract_name"] == "users_contract"
        assert d["summary"]["passed"] is True
        assert d["summary"]["row_count"] == 1000000
        assert len(d["rules"]) == 1
        assert d["duration_ms"] == 1234

    def test_json_roundtrip(self):
        """Test JSON serialization roundtrip."""
        original = ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="def456",
            contract_name="test_contract",
            dataset_uri="s3://bucket/data.parquet",
            run_at=datetime(2024, 1, 13, 10, 30, 0, tzinfo=timezone.utc),
            summary=StateSummary(
                passed=False,
                total_rules=3,
                passed_rules=2,
                failed_rules=1,
            ),
            rules=[
                RuleState(
                    rule_id="COL:status:allowed_values",
                    rule_name="allowed_values",
                    passed=False,
                    failed_count=42,
                    execution_source="sql",
                ),
            ],
        )

        json_str = original.to_json()
        restored = ValidationState.from_json(json_str)

        assert restored.contract_fingerprint == original.contract_fingerprint
        assert restored.contract_name == original.contract_name
        assert restored.summary.passed == original.summary.passed
        assert restored.summary.failed_rules == 1
        assert len(restored.rules) == 1
        assert restored.rules[0].failed_count == 42

    def test_get_failed_rules(self):
        """Test filtering failed rules."""
        state = ValidationState(
            contract_fingerprint="abc",
            dataset_fingerprint=None,
            contract_name="test",
            dataset_uri="test.parquet",
            run_at=datetime.now(timezone.utc),
            summary=StateSummary(passed=False, total_rules=3, passed_rules=2, failed_rules=1),
            rules=[
                RuleState("r1", "not_null", True, 0, "metadata"),
                RuleState("r2", "unique", True, 0, "sql"),
                RuleState("r3", "allowed_values", False, 10, "sql"),
            ],
        )

        failed = state.get_failed_rules()

        assert len(failed) == 1
        assert failed[0].rule_id == "r3"

    def test_to_llm(self):
        """Test LLM-optimized rendering."""
        state = ValidationState(
            contract_fingerprint="abc123def456",
            dataset_fingerprint=None,
            contract_name="users_contract",
            dataset_uri="data/users.parquet",
            run_at=datetime(2024, 1, 13, 10, 30, 0, tzinfo=timezone.utc),
            summary=StateSummary(passed=False, total_rules=5, passed_rules=3, failed_rules=2),
            rules=[
                RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
                RuleState("COL:email:not_null", "not_null", True, 0, "metadata"),
                RuleState("DATASET:min_rows", "min_rows", True, 0, "sql"),
                RuleState("COL:status:allowed_values", "allowed_values", False, 42, "sql",
                         failure_mode="novel_category"),
                RuleState("COL:age:range", "range", False, 5, "polars"),
            ],
        )

        llm_output = state.to_llm()

        # Check key elements are present
        assert "users_contract" in llm_output
        assert "FAILED" in llm_output
        assert "3/5 passed" in llm_output
        assert "Failed (2)" in llm_output
        assert "COL:status:allowed_values" in llm_output
        assert "novel_category" in llm_output
        assert "Passed (3)" in llm_output
        assert "fingerprint: abc123def456" in llm_output

        # Should be significantly smaller than JSON
        json_size = len(state.to_json())
        llm_size = len(llm_output)
        assert llm_size < json_size / 2  # At least 2x smaller


# ---------------------------------------------------------------------------
# Fingerprint Tests
# ---------------------------------------------------------------------------


class TestFingerprint:
    """Tests for fingerprinting utilities."""

    def test_fingerprint_from_name_and_uri(self):
        """Test simple fingerprinting."""
        fp1 = fingerprint_from_name_and_uri("my_contract", "data/users.parquet")
        fp2 = fingerprint_from_name_and_uri("my_contract", "data/users.parquet")
        fp3 = fingerprint_from_name_and_uri("other_contract", "data/users.parquet")

        assert fp1 == fp2  # Same inputs = same fingerprint
        assert fp1 != fp3  # Different name = different fingerprint
        assert len(fp1) == 16  # 16 hex chars

    def test_fingerprint_stability(self):
        """Test that fingerprints are stable across runs."""
        # This specific input should always produce the same hash
        fp = fingerprint_from_name_and_uri("test_contract", "test.parquet")

        # The fingerprint should be deterministic
        assert isinstance(fp, str)
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)


# ---------------------------------------------------------------------------
# LocalStore Tests
# ---------------------------------------------------------------------------


class TestLocalStore:
    """Tests for LocalStore backend."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LocalStore(base_path=tmpdir)
            yield store

    def _make_state(self, contract_fp: str = "abc123", passed: bool = True) -> ValidationState:
        """Helper to create test states."""
        return ValidationState(
            contract_fingerprint=contract_fp,
            dataset_fingerprint="data123",
            contract_name="test_contract",
            dataset_uri="data/test.parquet",
            run_at=datetime.now(timezone.utc),
            summary=StateSummary(
                passed=passed,
                total_rules=3,
                passed_rules=3 if passed else 2,
                failed_rules=0 if passed else 1,
            ),
            rules=[
                RuleState("r1", "not_null", True, 0, "metadata"),
                RuleState("r2", "unique", passed, 0 if passed else 5, "sql"),
            ],
        )

    def test_save_and_get_latest(self, temp_store):
        """Test basic save and retrieve."""
        state = self._make_state()
        temp_store.save(state)

        retrieved = temp_store.get_latest("abc123")

        assert retrieved is not None
        assert retrieved.contract_fingerprint == "abc123"
        assert retrieved.summary.passed is True

    def test_get_latest_no_history(self, temp_store):
        """Test get_latest when no history exists."""
        result = temp_store.get_latest("nonexistent")
        assert result is None

    def test_get_history(self, temp_store):
        """Test retrieving multiple states."""
        import time

        # Save multiple states with small delays
        for i in range(3):
            state = self._make_state(passed=(i % 2 == 0))
            temp_store.save(state)
            time.sleep(0.01)  # Ensure different timestamps

        history = temp_store.get_history("abc123", limit=10)

        assert len(history) == 3
        # Should be newest first
        assert history[0].run_at >= history[1].run_at
        assert history[1].run_at >= history[2].run_at

    def test_get_history_with_limit(self, temp_store):
        """Test history limit."""
        import time

        for i in range(5):
            state = self._make_state()
            temp_store.save(state)
            time.sleep(0.01)

        history = temp_store.get_history("abc123", limit=2)

        assert len(history) == 2

    def test_multiple_contracts(self, temp_store):
        """Test storing states for different contracts."""
        state1 = self._make_state(contract_fp="contract_a")
        state2 = self._make_state(contract_fp="contract_b")

        temp_store.save(state1)
        temp_store.save(state2)

        retrieved_a = temp_store.get_latest("contract_a")
        retrieved_b = temp_store.get_latest("contract_b")

        assert retrieved_a is not None
        assert retrieved_b is not None
        assert retrieved_a.contract_fingerprint == "contract_a"
        assert retrieved_b.contract_fingerprint == "contract_b"

    def test_list_contracts(self, temp_store):
        """Test listing all contracts."""
        temp_store.save(self._make_state(contract_fp="aaaa111122223333"))
        temp_store.save(self._make_state(contract_fp="bbbb444455556666"))

        contracts = temp_store.list_contracts()

        assert len(contracts) == 2
        assert "aaaa111122223333" in contracts
        assert "bbbb444455556666" in contracts

    def test_delete_old(self, temp_store):
        """Test retention policy."""
        import time

        # Save 5 states
        for i in range(5):
            state = self._make_state()
            temp_store.save(state)
            time.sleep(0.01)

        # Delete keeping only 2
        deleted = temp_store.delete_old("abc123", keep_count=2)

        assert deleted == 3
        history = temp_store.get_history("abc123")
        assert len(history) == 2

    def test_clear_single_contract(self, temp_store):
        """Test clearing a single contract's history."""
        temp_store.save(self._make_state(contract_fp="keep_me"))
        temp_store.save(self._make_state(contract_fp="delete_me"))

        deleted = temp_store.clear("delete_me")

        assert deleted >= 1
        assert temp_store.get_latest("delete_me") is None
        assert temp_store.get_latest("keep_me") is not None

    def test_clear_all(self, temp_store):
        """Test clearing all state."""
        temp_store.save(self._make_state(contract_fp="contract_a"))
        temp_store.save(self._make_state(contract_fp="contract_b"))

        deleted = temp_store.clear()

        assert deleted >= 2
        assert temp_store.list_contracts() == []


# ---------------------------------------------------------------------------
# StateDiff Tests
# ---------------------------------------------------------------------------


class TestStateDiff:
    """Tests for StateDiff computation and rendering."""

    def _make_state(
        self,
        passed: bool = True,
        rules: list = None,
        run_at: datetime = None,
    ) -> ValidationState:
        """Helper to create test states."""
        if rules is None:
            rules = [
                RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
                RuleState("COL:email:not_null", "not_null", True, 0, "metadata"),
                RuleState("DATASET:min_rows", "min_rows", True, 0, "sql"),
            ]

        passed_count = sum(1 for r in rules if r.passed)
        failed_count = len(rules) - passed_count

        return ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="data123",
            contract_name="test_contract",
            dataset_uri="data/test.parquet",
            run_at=run_at or datetime.now(timezone.utc),
            summary=StateSummary(
                passed=passed,
                total_rules=len(rules),
                passed_rules=passed_count,
                failed_rules=failed_count,
            ),
            rules=rules,
        )

    def test_no_changes(self):
        """Test diff when nothing changed."""
        before = self._make_state()
        after = self._make_state()

        diff = StateDiff.compute(before, after)

        assert not diff.status_changed
        assert not diff.has_regressions
        assert not diff.has_improvements
        assert len(diff.new_failures) == 0
        assert len(diff.resolved) == 0
        assert len(diff.regressions) == 0
        assert len(diff.improvements) == 0
        assert len(diff.unchanged) == 3

    def test_new_failure(self):
        """Test diff when a rule starts failing."""
        before = self._make_state(rules=[
            RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
            RuleState("COL:email:not_null", "not_null", True, 0, "metadata"),
        ])
        after = self._make_state(passed=False, rules=[
            RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
            RuleState("COL:email:not_null", "not_null", False, 15, "metadata"),  # Now failing
        ])

        diff = StateDiff.compute(before, after)

        assert diff.status_changed  # was passing, now failing
        assert diff.has_regressions
        assert len(diff.new_failures) == 1
        assert diff.new_failures[0].rule_id == "COL:email:not_null"
        assert diff.new_failures[0].after_count == 15

    def test_resolved_failure(self):
        """Test diff when a failure is resolved."""
        before = self._make_state(passed=False, rules=[
            RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
            RuleState("COL:email:not_null", "not_null", False, 15, "metadata"),  # Was failing
        ])
        after = self._make_state(rules=[
            RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
            RuleState("COL:email:not_null", "not_null", True, 0, "metadata"),  # Now passing
        ])

        diff = StateDiff.compute(before, after)

        assert diff.status_changed  # was failing, now passing
        assert diff.has_improvements
        assert not diff.has_regressions
        assert len(diff.resolved) == 1
        assert diff.resolved[0].rule_id == "COL:email:not_null"

    def test_regression_count_increase(self):
        """Test diff when failure count increases."""
        before = self._make_state(passed=False, rules=[
            RuleState("COL:email:not_null", "not_null", False, 10, "metadata"),
        ])
        after = self._make_state(passed=False, rules=[
            RuleState("COL:email:not_null", "not_null", False, 25, "metadata"),  # Count increased
        ])

        diff = StateDiff.compute(before, after)

        assert not diff.status_changed  # Both failing
        assert diff.has_regressions
        assert len(diff.regressions) == 1
        assert diff.regressions[0].delta == 15

    def test_improvement_count_decrease(self):
        """Test diff when failure count decreases."""
        before = self._make_state(passed=False, rules=[
            RuleState("COL:email:not_null", "not_null", False, 25, "metadata"),
        ])
        after = self._make_state(passed=False, rules=[
            RuleState("COL:email:not_null", "not_null", False, 10, "metadata"),  # Count decreased
        ])

        diff = StateDiff.compute(before, after)

        assert diff.has_improvements
        assert not diff.has_regressions
        assert len(diff.improvements) == 1
        assert diff.improvements[0].delta == -15

    def test_to_llm(self):
        """Test LLM-optimized rendering."""
        before = self._make_state(rules=[
            RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
        ])
        after = self._make_state(passed=False, rules=[
            RuleState("COL:user_id:not_null", "not_null", False, 42, "metadata",
                      failure_mode="null_spike"),
        ])

        diff = StateDiff.compute(before, after)
        llm_output = diff.to_llm()

        assert "REGRESSION" in llm_output
        assert "New Blocking Failures" in llm_output  # Now grouped by severity
        assert "COL:user_id:not_null" in llm_output
        assert "null_spike" in llm_output
        assert "fingerprint" in llm_output

    def test_to_json(self):
        """Test JSON serialization."""
        before = self._make_state()
        after = self._make_state()

        diff = StateDiff.compute(before, after)
        json_str = diff.to_json()

        # Should be valid JSON
        import json
        data = json.loads(json_str)

        assert "before_run_at" in data
        assert "after_run_at" in data
        assert "has_regressions" in data
        assert "new_failures" in data
