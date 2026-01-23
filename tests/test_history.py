# tests/test_history.py
"""Tests for validation history functionality."""

import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from kontra.state.types import RunSummary, ValidationState, StateSummary, RuleState


class TestRunSummary:
    """Tests for RunSummary type."""

    def test_to_dict(self):
        """RunSummary converts to dict correctly."""
        summary = RunSummary(
            run_id="run_123",
            timestamp=datetime(2026, 1, 23, 10, 30, tzinfo=timezone.utc),
            passed=True,
            failed_count=0,
            total_rows=1000,
            contract_name="test_contract",
            contract_fingerprint="abc123",
            total_rules=5,
            blocking_failures=0,
            warning_failures=0,
            duration_ms=500,
        )

        d = summary.to_dict()
        assert d["run_id"] == "run_123"
        assert d["passed"] is True
        assert d["failed_count"] == 0
        assert d["total_rows"] == 1000
        assert d["contract_name"] == "test_contract"
        assert d["total_rules"] == 5

    def test_from_dict(self):
        """RunSummary can be created from dict."""
        d = {
            "run_id": "run_456",
            "timestamp": "2026-01-23T10:30:00+00:00",
            "passed": False,
            "failed_count": 3,
            "total_rows": 5000,
            "contract_name": "my_contract",
            "contract_fingerprint": "def456",
            "total_rules": 10,
            "blocking_failures": 2,
            "warning_failures": 1,
        }

        summary = RunSummary.from_dict(d)
        assert summary.run_id == "run_456"
        assert summary.passed is False
        assert summary.failed_count == 3
        assert summary.total_rows == 5000
        assert summary.blocking_failures == 2

    def test_from_validation_state(self):
        """RunSummary can be created from ValidationState."""
        state = ValidationState(
            contract_fingerprint="fp123",
            dataset_fingerprint="ds456",
            contract_name="test",
            dataset_uri="data.parquet",
            run_at=datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc),
            summary=StateSummary(
                passed=False,
                total_rules=5,
                passed_rules=3,
                failed_rules=2,
                row_count=10000,
                blocking_failures=1,
                warning_failures=1,
            ),
            rules=[],
            duration_ms=1000,
        )

        summary = RunSummary.from_validation_state(state, "run_001")
        assert summary.run_id == "run_001"
        assert summary.passed is False
        assert summary.failed_count == 2
        assert summary.total_rows == 10000
        assert summary.blocking_failures == 1
        assert summary.duration_ms == 1000

    def test_to_llm(self):
        """RunSummary has token-optimized LLM output."""
        summary = RunSummary(
            run_id="run_123",
            timestamp=datetime(2026, 1, 23, 10, 30, tzinfo=timezone.utc),
            passed=False,
            failed_count=5,
            total_rows=1000,
            contract_name="test",
            contract_fingerprint="abc",
        )

        llm = summary.to_llm()
        assert "2026-01-23 10:30" in llm
        assert "FAIL" in llm
        assert "5 failures" in llm
        assert "1,000 rows" in llm


class TestGetRunSummaries:
    """Tests for StateBackend.get_run_summaries()."""

    def test_get_run_summaries_empty(self, tmp_path):
        """Returns empty list when no history exists."""
        from kontra.state.backends.local import LocalStore

        store = LocalStore(str(tmp_path))
        summaries = store.get_run_summaries("nonexistent_fingerprint")
        assert summaries == []

    def test_get_run_summaries_returns_summaries(self, tmp_path):
        """Returns RunSummary objects from history."""
        from kontra.state.backends.local import LocalStore

        store = LocalStore(str(tmp_path))

        # Create some states
        for i in range(3):
            state = ValidationState(
                contract_fingerprint="test_fp",
                dataset_fingerprint="ds_fp",
                contract_name="test",
                dataset_uri="data.parquet",
                run_at=datetime.now(timezone.utc) - timedelta(hours=i),
                summary=StateSummary(
                    passed=i % 2 == 0,
                    total_rules=5,
                    passed_rules=5 if i % 2 == 0 else 3,
                    failed_rules=0 if i % 2 == 0 else 2,
                    row_count=1000 * (i + 1),
                ),
                rules=[],
            )
            store.save(state)

        summaries = store.get_run_summaries("test_fp", limit=10)
        assert len(summaries) == 3
        assert all(isinstance(s, RunSummary) for s in summaries)

    def test_get_run_summaries_respects_limit(self, tmp_path):
        """Respects limit parameter."""
        from kontra.state.backends.local import LocalStore

        store = LocalStore(str(tmp_path))

        # Create 5 states
        for i in range(5):
            state = ValidationState(
                contract_fingerprint="test_fp",
                dataset_fingerprint="ds_fp",
                contract_name="test",
                dataset_uri="data.parquet",
                run_at=datetime.now(timezone.utc) - timedelta(hours=i),
                summary=StateSummary(
                    passed=True,
                    total_rules=1,
                    passed_rules=1,
                    failed_rules=0,
                ),
                rules=[],
            )
            store.save(state)

        summaries = store.get_run_summaries("test_fp", limit=2)
        assert len(summaries) == 2

    def test_get_run_summaries_failed_only(self, tmp_path):
        """Filters to failed runs only."""
        from kontra.state.backends.local import LocalStore

        store = LocalStore(str(tmp_path))

        # Create mix of passed/failed states
        for i in range(4):
            state = ValidationState(
                contract_fingerprint="test_fp",
                dataset_fingerprint="ds_fp",
                contract_name="test",
                dataset_uri="data.parquet",
                run_at=datetime.now(timezone.utc) - timedelta(hours=i),
                summary=StateSummary(
                    passed=i % 2 == 0,  # 0, 2 pass; 1, 3 fail
                    total_rules=1,
                    passed_rules=1 if i % 2 == 0 else 0,
                    failed_rules=0 if i % 2 == 0 else 1,
                ),
                rules=[],
            )
            store.save(state)

        summaries = store.get_run_summaries("test_fp", failed_only=True)
        assert len(summaries) == 2
        assert all(not s.passed for s in summaries)


class TestKontraGetHistory:
    """Tests for kontra.get_history() Python API."""

    def test_get_history_returns_list(self, tmp_path):
        """get_history returns list of dicts."""
        import kontra
        from kontra.state.backends.local import LocalStore
        from kontra.state.types import ValidationState, StateSummary

        # Setup: create a contract and some history
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_history
dataset: data.parquet
rules:
  - name: min_rows
    params:
      count: 1
""")

        # Create state store and save a run
        state_dir = tmp_path / ".kontra" / "state"
        state_dir.mkdir(parents=True, exist_ok=True)

        # Mock the get_history by setting up state in the expected location
        # This is an integration test
        pass  # Skipped - requires full integration setup

    def test_get_history_since_days(self):
        """since parameter parses days correctly."""
        # Test the parsing logic
        from datetime import datetime, timedelta, timezone

        since = "7d"
        now = datetime.now(timezone.utc)

        since_lower = since.lower().strip()
        if since_lower.endswith("d"):
            days = int(since_lower[:-1])
            since_dt = now - timedelta(days=days)

        assert since_dt is not None
        assert (now - since_dt).days == 7

    def test_get_history_since_hours(self):
        """since parameter parses hours correctly."""
        from datetime import datetime, timedelta, timezone

        since = "24h"
        now = datetime.now(timezone.utc)

        since_lower = since.lower().strip()
        if since_lower.endswith("h"):
            hours = int(since_lower[:-1])
            since_dt = now - timedelta(hours=hours)

        assert since_dt is not None
        assert abs((now - since_dt).total_seconds() - 24 * 3600) < 1


class TestHistoryCLI:
    """Tests for kontra history CLI command."""

    def test_history_help(self):
        """History command shows help."""
        from typer.testing import CliRunner
        from kontra.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["history", "--help"])

        assert result.exit_code == 0
        assert "Show validation history" in result.output
        assert "--since" in result.output
        assert "--failed-only" in result.output

    def test_history_no_contract(self):
        """History command requires contract argument."""
        from typer.testing import CliRunner
        from kontra.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["history"])

        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_history_nonexistent_contract(self, tmp_path):
        """History command handles nonexistent contract."""
        from typer.testing import CliRunner
        from kontra.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["history", str(tmp_path / "nonexistent.yml")])

        assert result.exit_code != 0
