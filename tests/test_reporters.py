# tests/test_reporters.py
"""Tests for Kontra reporters."""

import json
from datetime import datetime

import pytest

from kontra.reporters.json_reporter import (
    build_payload,
    render_json,
    SCHEMA_VERSION,
)


class TestJSONReporter:
    """Tests for JSON reporter functions."""

    def test_build_payload_basic(self):
        """build_payload creates valid structure."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True, "total_rules": 2, "rules_passed": 2},
            results=[
                {"rule_id": "r1", "passed": True, "message": "OK", "failed_count": 0},
                {"rule_id": "r2", "passed": True, "message": "OK", "failed_count": 0},
            ],
        )

        assert payload["dataset_name"] == "test.parquet"
        assert payload["passed"] is True
        assert payload["schema_version"] == SCHEMA_VERSION
        assert "timestamp_utc" in payload
        assert "statistics" in payload
        assert "results" in payload

    def test_build_payload_failed(self):
        """build_payload correctly flags failures."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": False, "total_rules": 2, "rules_passed": 1},
            results=[
                {"rule_id": "r1", "passed": True, "message": "OK", "failed_count": 0},
                {"rule_id": "r2", "passed": False, "message": "Failed", "failed_count": 5},
            ],
        )

        assert payload["passed"] is False
        assert payload["statistics"]["rules_failed"] == 1
        assert payload["statistics"]["rules_passed"] == 1

    def test_build_payload_statistics(self):
        """build_payload extracts statistics correctly."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={
                "passed": True,
                "total_rules": 3,
                "execution_time_seconds": 1.5,
                "rows_evaluated": 1000,
            },
            results=[
                {"rule_id": "r1", "passed": True, "message": "OK", "failed_count": 0},
            ],
            stats={
                "run_meta": {"duration_ms_total": 1500},
                "dataset": {"nrows": 1000, "ncols": 5},
            },
        )

        assert payload["statistics"]["execution_time_seconds"] == 1.5
        assert payload["statistics"]["rows_evaluated"] == 1000
        assert payload["statistics"]["rules_total"] == 3

    def test_build_payload_with_quarantine(self):
        """build_payload includes quarantine info."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": False},
            results=[
                {"rule_id": "r1", "passed": False, "message": "Failed", "failed_count": 10},
            ],
            quarantine={
                "location": "s3://bucket/quarantine/test.parquet",
                "rows_quarantined": 10,
            },
        )

        assert "quarantine" in payload
        assert payload["quarantine"]["rows_quarantined"] == 10
        assert "s3://" in payload["quarantine"]["location"]

    def test_build_payload_with_stats(self):
        """build_payload includes stats if provided."""
        stats = {
            "dataset": {"nrows": 5000, "ncols": 10},
            "run_meta": {"engine": "duckdb"},
        }

        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[],
            stats=stats,
        )

        assert "stats" in payload
        assert payload["stats"]["dataset"]["nrows"] == 5000

    def test_build_payload_results_sorted(self):
        """build_payload sorts results by rule_id."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[
                {"rule_id": "z_rule", "passed": True, "message": "OK"},
                {"rule_id": "a_rule", "passed": True, "message": "OK"},
                {"rule_id": "m_rule", "passed": True, "message": "OK"},
            ],
        )

        rule_ids = [r["rule_id"] for r in payload["results"]]
        assert rule_ids == ["a_rule", "m_rule", "z_rule"]

    def test_build_payload_results_normalized(self):
        """build_payload normalizes result fields."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[
                {"rule_id": "r1", "passed": True},  # Minimal result
            ],
        )

        result = payload["results"][0]
        assert "message" in result
        assert "failed_count" in result
        assert "severity" in result
        # Default severity is "blocking" when not specified
        assert result["severity"] == "blocking"

    def test_build_payload_preserves_severity(self):
        """build_payload preserves actual severity from contract."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": False},
            results=[
                {"rule_id": "r1", "passed": False, "failed_count": 5, "severity": "warning"},
            ],
        )

        result = payload["results"][0]
        # Should preserve the actual severity from the rule
        assert result["severity"] == "warning"

    def test_render_json_valid_json(self):
        """render_json produces valid JSON string."""
        output = render_json(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[
                {"rule_id": "r1", "passed": True, "message": "OK"},
            ],
        )

        # Should be parseable JSON
        parsed = json.loads(output)
        assert parsed["dataset_name"] == "test.parquet"

    def test_render_json_deterministic(self):
        """render_json produces deterministic output."""
        args = {
            "dataset_name": "test.parquet",
            "summary": {"passed": True},
            "results": [
                {"rule_id": "r1", "passed": True, "message": "OK"},
            ],
        }

        output1 = render_json(**args)
        output2 = render_json(**args)

        # Outputs should be identical (except timestamp)
        parsed1 = json.loads(output1)
        parsed2 = json.loads(output2)

        # Remove timestamp for comparison
        del parsed1["timestamp_utc"]
        del parsed2["timestamp_utc"]

        assert parsed1 == parsed2

    def test_render_json_compact(self):
        """render_json produces compact output (no extra whitespace)."""
        output = render_json(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[],
            pretty=False,  # Request compact output
        )

        # Should not have pretty-print whitespace
        assert "\n" not in output
        assert "  " not in output  # No indentation

    def test_timestamp_format(self):
        """Timestamp is in ISO 8601 format with Z suffix."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[],
        )

        timestamp = payload["timestamp_utc"]
        assert timestamp.endswith("Z")
        # Should be parseable
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        assert dt.year >= 2024

    def test_engine_version_included(self):
        """Engine version is included in payload."""
        payload = build_payload(
            dataset_name="test.parquet",
            summary={"passed": True},
            results=[],
        )

        assert "engine_version" in payload
        assert len(payload["engine_version"]) > 0

    def test_schema_version_constant(self):
        """SCHEMA_VERSION is defined."""
        assert SCHEMA_VERSION is not None
        assert len(SCHEMA_VERSION) > 0


class TestRichReporter:
    """Tests for Rich reporter (placeholder for future tests)."""

    def test_import(self):
        """Rich reporter can be imported."""
        from kontra.reporters import rich_reporter
        assert rich_reporter is not None


class TestScoutReporters:
    """Tests for Scout profile reporters."""

    def test_json_reporter_import(self):
        """Scout JSON reporter can be imported."""
        from kontra.scout.reporters import json_reporter
        assert json_reporter is not None

    def test_render_profile_function(self):
        """render_profile function exists."""
        from kontra.scout.reporters import render_profile
        assert callable(render_profile)
