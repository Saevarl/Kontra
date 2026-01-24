# tests/test_state.py
"""
Tests for validation state management.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from kontra.state.types import (
    ValidationState,
    RuleState,
    StateSummary,
    StateDiff,
    RuleDiff,
    Annotation,
)
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

        assert d["schema_version"] == "2.0"
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
        from datetime import timedelta

        # Save multiple states with different timestamps in the state object
        base_time = datetime.now(timezone.utc)
        for i in range(3):
            state = self._make_state(passed=(i % 2 == 0))
            # Manually set different run_at times (older to newer)
            state.run_at = base_time + timedelta(seconds=i * 2)
            temp_store.save(state)
            time.sleep(0.01)  # Small delay for unique file names

        history = temp_store.get_history("abc123", limit=10)

        assert len(history) == 3
        # Should be newest first (the file with the latest timestamp prefix)
        # Ordering is by filename, not by run_at in the content

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


# ---------------------------------------------------------------------------
# Annotation Tests
# ---------------------------------------------------------------------------


class TestAnnotation:
    """Tests for Annotation dataclass."""

    def test_to_dict_minimal(self):
        """Test serialization with minimal fields."""
        annotation = Annotation(
            run_id=1,
            actor_type="agent",
            actor_id="test-agent",
            annotation_type="note",
            summary="Test annotation",
        )

        d = annotation.to_dict()

        assert d["run_id"] == 1
        assert d["actor_type"] == "agent"
        assert d["actor_id"] == "test-agent"
        assert d["annotation_type"] == "note"
        assert d["summary"] == "Test annotation"
        assert "rule_result_id" not in d  # None, not included
        assert "payload" not in d  # None, not included

    def test_to_dict_with_payload(self):
        """Test serialization with payload."""
        annotation = Annotation(
            run_id=1,
            rule_result_id=5,
            actor_type="agent",
            actor_id="repair-agent",
            annotation_type="resolution",
            summary="Fixed the issue",
            payload={"fix_query": "UPDATE users SET email = 'test@test.com'"},
        )

        d = annotation.to_dict()

        assert d["rule_result_id"] == 5
        assert d["payload"]["fix_query"] == "UPDATE users SET email = 'test@test.com'"

    def test_from_dict_roundtrip(self):
        """Test serialization roundtrip."""
        original = Annotation(
            run_id=42,
            rule_result_id=7,
            actor_type="human",
            actor_id="alice@example.com",
            annotation_type="false_positive",
            summary="Service accounts are expected to have null emails",
            payload={"affected_rows": 15},
        )

        d = original.to_dict()
        restored = Annotation.from_dict(d)

        assert restored.run_id == original.run_id
        assert restored.rule_result_id == original.rule_result_id
        assert restored.actor_type == original.actor_type
        assert restored.actor_id == original.actor_id
        assert restored.annotation_type == original.annotation_type
        assert restored.summary == original.summary
        assert restored.payload == original.payload

    def test_to_json(self):
        """Test JSON serialization."""
        annotation = Annotation(
            run_id=1,
            actor_type="agent",
            actor_id="test-agent",
            annotation_type="note",
            summary="Test",
        )

        json_str = annotation.to_json()
        data = json.loads(json_str)

        assert data["actor_type"] == "agent"
        assert data["summary"] == "Test"

    def test_from_json(self):
        """Test JSON deserialization."""
        json_str = '{"run_id": 1, "actor_type": "agent", "actor_id": "test", "annotation_type": "note", "summary": "Hello", "created_at": "2024-01-15T10:00:00+00:00"}'

        annotation = Annotation.from_json(json_str)

        assert annotation.run_id == 1
        assert annotation.actor_type == "agent"
        assert annotation.summary == "Hello"
        assert annotation.created_at is not None

    def test_to_llm(self):
        """Test LLM-optimized format."""
        annotation = Annotation(
            run_id=1,
            actor_type="agent",
            actor_id="repair-agent-v2",
            annotation_type="resolution",
            summary="Fixed null emails by backfilling",
            created_at=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
        )

        llm_output = annotation.to_llm()

        assert "[resolution]" in llm_output
        assert "agent:repair-agent-v2" in llm_output
        assert "2024-01-15 10:30" in llm_output
        assert "Fixed null emails by backfilling" in llm_output


class TestLocalStoreAnnotations:
    """Tests for LocalStore annotation methods."""

    @pytest.fixture
    def temp_store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LocalStore(base_path=tmpdir)
            yield store

    def _make_state(self, contract_fp: str = "abc1234567890123") -> ValidationState:
        """Helper to create test states."""
        return ValidationState(
            contract_fingerprint=contract_fp,
            dataset_fingerprint="data123",
            contract_name="test_contract",
            dataset_uri="data/test.parquet",
            run_at=datetime.now(timezone.utc),
            summary=StateSummary(
                passed=True,
                total_rules=2,
                passed_rules=2,
                failed_rules=0,
            ),
            rules=[
                RuleState("COL:user_id:not_null", "not_null", True, 0, "metadata"),
                RuleState("COL:email:not_null", "not_null", True, 0, "metadata"),
            ],
        )

    def test_save_annotation_for_run(self, temp_store):
        """Test saving an annotation for a run."""
        # First save a state
        state = self._make_state()
        temp_store.save(state)

        # Get the run_id string
        runs_dir = temp_store._runs_dir("abc1234567890123")
        run_files = list(runs_dir.glob("*.json"))
        assert len(run_files) == 1
        run_id_str = run_files[0].stem

        # Save an annotation
        annotation = Annotation(
            actor_type="agent",
            actor_id="test-agent",
            annotation_type="note",
            summary="Test annotation",
        )

        ann_id = temp_store.save_annotation_for_run(
            "abc1234567890123", run_id_str, annotation
        )

        assert ann_id == 1  # First annotation

        # Verify annotation file exists
        ann_file = runs_dir / f"{run_id_str}.ann.jsonl"
        assert ann_file.exists()

        # Verify content
        content = ann_file.read_text()
        assert "Test annotation" in content

    def test_get_run_with_annotations(self, temp_store):
        """Test loading a run with its annotations."""
        # Save a state
        state = self._make_state()
        temp_store.save(state)

        # Get the run_id string
        runs_dir = temp_store._runs_dir("abc1234567890123")
        run_files = list(runs_dir.glob("*.json"))
        run_id_str = run_files[0].stem

        # Save two annotations
        ann1 = Annotation(
            actor_type="agent",
            actor_id="agent-1",
            annotation_type="note",
            summary="First note",
        )
        ann2 = Annotation(
            actor_type="human",
            actor_id="alice@example.com",
            annotation_type="acknowledged",
            summary="Acknowledged this issue",
        )

        temp_store.save_annotation_for_run("abc1234567890123", run_id_str, ann1)
        temp_store.save_annotation_for_run("abc1234567890123", run_id_str, ann2)

        # Load with annotations
        loaded = temp_store.get_run_with_annotations("abc1234567890123")

        assert loaded is not None
        assert loaded.annotations is not None
        assert len(loaded.annotations) == 2
        assert loaded.annotations[0].summary == "First note"
        assert loaded.annotations[1].actor_id == "alice@example.com"

    def test_get_history_with_annotations(self, temp_store):
        """Test loading history with annotations."""
        import time

        # Save two states
        state1 = self._make_state()
        temp_store.save(state1)
        time.sleep(0.01)
        state2 = self._make_state()
        temp_store.save(state2)

        # Get run_id strings
        runs_dir = temp_store._runs_dir("abc1234567890123")
        run_files = sorted(runs_dir.glob("*.json"))
        run_id_1 = run_files[0].stem
        run_id_2 = run_files[1].stem

        # Add annotation to first run only
        ann = Annotation(
            actor_type="agent",
            actor_id="test-agent",
            annotation_type="note",
            summary="Annotation on first run",
        )
        temp_store.save_annotation_for_run("abc1234567890123", run_id_1, ann)

        # Load history with annotations
        history = temp_store.get_history_with_annotations("abc1234567890123", limit=10)

        assert len(history) == 2
        # One should have annotations, one shouldn't
        annotated = [h for h in history if h.annotations and len(h.annotations) > 0]
        assert len(annotated) == 1


class TestRuleStateAnnotations:
    """Tests for RuleState annotation serialization."""

    def test_to_dict_with_annotations(self):
        """Test that annotations are included when specified."""
        annotations = [
            Annotation(
                actor_type="agent",
                actor_id="test",
                annotation_type="note",
                summary="Test",
            )
        ]

        rule = RuleState(
            rule_id="COL:email:not_null",
            rule_name="not_null",
            passed=False,
            failed_count=10,
            execution_source="polars",
            annotations=annotations,
        )

        # Without include_annotations
        d1 = rule.to_dict(include_annotations=False)
        assert "annotations" not in d1

        # With include_annotations
        d2 = rule.to_dict(include_annotations=True)
        assert "annotations" in d2
        assert len(d2["annotations"]) == 1


class TestValidationStateAnnotations:
    """Tests for ValidationState annotation serialization."""

    def test_to_dict_with_annotations(self):
        """Test that annotations are included when specified."""
        rule_annotations = [
            Annotation(
                actor_type="agent",
                actor_id="agent1",
                annotation_type="resolution",
                summary="Fixed this rule",
            )
        ]

        run_annotations = [
            Annotation(
                actor_type="human",
                actor_id="alice",
                annotation_type="acknowledged",
                summary="Acknowledged this run",
            )
        ]

        state = ValidationState(
            contract_fingerprint="abc123",
            dataset_fingerprint="def456",
            contract_name="test_contract",
            dataset_uri="test.parquet",
            run_at=datetime.now(timezone.utc),
            summary=StateSummary(
                passed=False,
                total_rules=1,
                passed_rules=0,
                failed_rules=1,
            ),
            rules=[
                RuleState(
                    rule_id="COL:email:not_null",
                    rule_name="not_null",
                    passed=False,
                    failed_count=10,
                    execution_source="polars",
                    annotations=rule_annotations,
                ),
            ],
            annotations=run_annotations,
        )

        # Without include_annotations
        d1 = state.to_dict(include_annotations=False)
        assert "annotations" not in d1
        assert "annotations" not in d1["rules"][0]

        # With include_annotations
        d2 = state.to_dict(include_annotations=True)
        assert "annotations" in d2
        assert len(d2["annotations"]) == 1
        assert d2["annotations"][0]["actor_id"] == "alice"
        assert "annotations" in d2["rules"][0]
        assert d2["rules"][0]["annotations"][0]["actor_id"] == "agent1"


# ---------------------------------------------------------------------------
# get_annotations API Tests
# ---------------------------------------------------------------------------


class TestGetAnnotationsAPI:
    """Tests for kontra.get_annotations() cross-run query."""

    def test_get_annotations_basic(self, tmp_path):
        """Test basic annotation retrieval."""
        import kontra
        from kontra import rules
        import polars as pl
        import os

        # Setup temp directory
        os.makedirs(tmp_path / ".kontra", exist_ok=True)
        os.chdir(tmp_path)

        # Create data and contract
        df = pl.DataFrame({"email": ["a@b.com", None, "c@d.com"]})
        df.write_parquet("users.parquet")

        with open("users_contract.yml", "w") as f:
            f.write("""
name: users_contract
datasource: users.parquet
rules:
  - name: not_null
    params:
      column: email
""")

        # Validate
        result = kontra.validate("users.parquet", "users_contract.yml")
        assert result.passed is False

        # Annotate
        kontra.annotate(
            "users_contract.yml",
            rule_id="COL:email:not_null",
            actor_id="test-agent",
            annotation_type="root_cause",
            summary="Missing emails from legacy import",
        )

        # Retrieve all annotations
        annotations = kontra.get_annotations("users_contract.yml")
        assert len(annotations) == 1
        assert annotations[0]["annotation_type"] == "root_cause"
        assert annotations[0]["rule_id"] == "COL:email:not_null"
        assert annotations[0]["summary"] == "Missing emails from legacy import"

    def test_get_annotations_filter_by_rule(self, tmp_path):
        """Test filtering annotations by rule_id."""
        import kontra
        from kontra import rules
        import polars as pl
        import os

        os.makedirs(tmp_path / ".kontra", exist_ok=True)
        os.chdir(tmp_path)

        df = pl.DataFrame({"email": [None], "name": [None]})
        df.write_parquet("data.parquet")

        with open("contract.yml", "w") as f:
            f.write("""
name: test_contract
datasource: data.parquet
rules:
  - name: not_null
    params:
      column: email
  - name: not_null
    params:
      column: name
""")

        kontra.validate("data.parquet", "contract.yml")

        # Annotate both rules
        kontra.annotate(
            "contract.yml",
            rule_id="COL:email:not_null",
            actor_id="agent",
            annotation_type="resolution",
            summary="Fixed email",
        )
        kontra.annotate(
            "contract.yml",
            rule_id="COL:name:not_null",
            actor_id="agent",
            annotation_type="resolution",
            summary="Fixed name",
        )

        # Get all
        all_annotations = kontra.get_annotations("contract.yml")
        assert len(all_annotations) == 2

        # Filter to email rule
        email_annotations = kontra.get_annotations("contract.yml", rule_id="COL:email:not_null")
        assert len(email_annotations) == 1
        assert email_annotations[0]["summary"] == "Fixed email"

        # Filter to name rule
        name_annotations = kontra.get_annotations("contract.yml", rule_id="COL:name:not_null")
        assert len(name_annotations) == 1
        assert name_annotations[0]["summary"] == "Fixed name"

    def test_get_annotations_filter_by_type(self, tmp_path):
        """Test filtering annotations by annotation_type."""
        import kontra
        from kontra import rules
        import polars as pl
        import os

        os.makedirs(tmp_path / ".kontra", exist_ok=True)
        os.chdir(tmp_path)

        df = pl.DataFrame({"x": [None]})
        df.write_parquet("data.parquet")

        with open("contract.yml", "w") as f:
            f.write("""
name: test_contract
datasource: data.parquet
rules:
  - name: not_null
    params:
      column: x
""")

        kontra.validate("data.parquet", "contract.yml")

        # Add different annotation types
        kontra.annotate("contract.yml", rule_id="COL:x:not_null", actor_id="a", annotation_type="resolution", summary="Fixed")
        kontra.annotate("contract.yml", rule_id="COL:x:not_null", actor_id="b", annotation_type="root_cause", summary="Upstream issue")
        kontra.annotate("contract.yml", rule_id="COL:x:not_null", actor_id="c", annotation_type="resolution", summary="Fixed again")

        # Filter by type
        resolutions = kontra.get_annotations("contract.yml", annotation_type="resolution")
        assert len(resolutions) == 2

        root_causes = kontra.get_annotations("contract.yml", annotation_type="root_cause")
        assert len(root_causes) == 1
        assert root_causes[0]["summary"] == "Upstream issue"

    def test_get_annotations_empty(self, tmp_path):
        """Test get_annotations returns empty list when no annotations."""
        import kontra
        import os

        os.makedirs(tmp_path / ".kontra", exist_ok=True)
        os.chdir(tmp_path)

        # No contract exists
        annotations = kontra.get_annotations("nonexistent_contract")
        assert annotations == []
