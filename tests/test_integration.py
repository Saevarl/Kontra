# tests/test_integration.py
"""Integration tests for end-to-end Kontra workflows."""

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml

from kontra.engine.engine import ValidationEngine
from kontra.scout.profiler import ScoutProfiler
from kontra.config.settings import (
    resolve_effective_config,
    resolve_datasource,
    KontraConfig,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve",
                 "Frank", "Grace", "Henry", "Ivy", "Jack"],
        "age": [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com",
                  "f@x.com", "g@x.com", "h@x.com", "i@x.com", "j@x.com"],
        "status": ["active", "active", "inactive", "active", "pending",
                   "active", "inactive", "active", "pending", "active"],
        "score": [85.5, 90.2, 78.9, 92.1, 88.3, 75.6, 93.4, 81.2, 86.7, 79.5],
    })


@pytest.fixture
def parquet_file(sample_data, tmp_path):
    """Create a parquet file with sample data."""
    path = tmp_path / "data.parquet"
    sample_data.write_parquet(path)
    return path


@pytest.fixture
def csv_file(sample_data, tmp_path):
    """Create a CSV file with sample data."""
    path = tmp_path / "data.csv"
    sample_data.write_csv(path)
    return path


# =============================================================================
# Validation Engine Integration
# =============================================================================


class TestValidationEngineIntegration:
    """End-to-end tests for ValidationEngine."""

    def test_parquet_validation_passing(self, parquet_file, tmp_path):
        """Full validation flow with passing parquet file."""
        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: test_contract
datasource: {parquet_file}

rules:
  - name: not_null
    params:
      column: id

  - name: min_rows
    params:
      threshold: 5

  - name: unique
    params:
      column: id
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is True
        assert result["summary"]["total_rules"] == 3
        assert result["summary"]["rules_passed"] == 3

    def test_parquet_validation_failing(self, tmp_path):
        """Full validation flow with failing rules."""
        # Create data with issues
        df = pl.DataFrame({
            "id": [1, 2, 3, None, 5],  # Has NULL
            "name": ["a", "b", "c", "d", "e"],
        })
        parquet = tmp_path / "bad_data.parquet"
        df.write_parquet(parquet)

        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: failing_contract
datasource: {parquet}

rules:
  - name: not_null
    params:
      column: id
    severity: blocking
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is False
        assert result["summary"]["rules_failed"] == 1

    def test_csv_validation(self, csv_file, tmp_path):
        """Validation works with CSV files."""
        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: csv_contract
datasource: {csv_file}

rules:
  - name: min_rows
    params:
      threshold: 5
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
            csv_mode="auto",
        )
        result = engine.run()

        assert result["summary"]["passed"] is True

    def test_data_path_override(self, parquet_file, tmp_path):
        """Data path can override contract dataset."""
        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: override_contract
datasource: nonexistent.parquet

rules:
  - name: min_rows
    params:
      threshold: 1
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            data_path=str(parquet_file),  # Override
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is True

    def test_stats_mode_summary(self, parquet_file, tmp_path):
        """Stats mode returns statistics."""
        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: stats_contract
datasource: {parquet_file}

rules:
  - name: min_rows
    params:
      threshold: 1
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
            stats_mode="summary",
        )
        result = engine.run()

        assert "stats" in result
        assert "dataset" in result["stats"]

    def test_preplan_disabled(self, parquet_file, tmp_path):
        """Validation works with preplan disabled."""
        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: no_preplan_contract
datasource: {parquet_file}

rules:
  - name: min_rows
    params:
      threshold: 1
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
            preplan="off",
        )
        result = engine.run()

        assert result["summary"]["passed"] is True

    def test_pushdown_disabled(self, parquet_file, tmp_path):
        """Validation works with pushdown disabled."""
        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: no_pushdown_contract
datasource: {parquet_file}

rules:
  - name: range
    params:
      column: age
      min: 0
      max: 100
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
            pushdown="off",
        )
        result = engine.run()

        assert result["summary"]["passed"] is True


# =============================================================================
# Scout Profiler Integration
# =============================================================================


class TestScoutProfilerIntegration:
    """End-to-end tests for ScoutProfiler."""

    def test_profile_parquet(self, parquet_file):
        """Profile a parquet file."""
        profiler = ScoutProfiler(str(parquet_file), preset="standard")
        profile = profiler.profile()

        assert profile.row_count == 10
        assert profile.column_count == 6
        assert len(profile.columns) == 6

    def test_profile_csv(self, csv_file):
        """Profile a CSV file."""
        profiler = ScoutProfiler(str(csv_file), preset="lite")
        profile = profiler.profile()

        assert profile.row_count == 10
        assert profile.column_count == 6

    def test_profile_with_sample(self, parquet_file):
        """Profile with sampling."""
        profiler = ScoutProfiler(
            str(parquet_file),
            preset="standard",
            sample_size=5,
        )
        profile = profiler.profile()

        # Should have fewer or equal rows due to sampling
        assert profile.row_count <= 10

    def test_profile_columns_filter(self, parquet_file):
        """Profile specific columns only."""
        profiler = ScoutProfiler(
            str(parquet_file),
            preset="standard",
            columns=["id", "name"],
        )
        profile = profiler.profile()

        # Should only profile specified columns
        column_names = [c.name for c in profile.columns]
        assert "id" in column_names
        assert "name" in column_names

    def test_profile_presets(self, parquet_file):
        """Different presets produce results."""
        for preset in ["lite", "standard", "deep"]:
            profiler = ScoutProfiler(str(parquet_file), preset=preset)
            profile = profiler.profile()
            assert profile.row_count == 10


# =============================================================================
# Configuration Integration
# =============================================================================


class TestConfigIntegration:
    """End-to-end tests for configuration system."""

    def test_config_with_env_vars(self, tmp_path, monkeypatch):
        """Config resolves environment variables."""
        import os
        os.environ["TEST_HOST"] = "example.com"
        os.environ["TEST_DB"] = "testdb"

        try:
            monkeypatch.chdir(tmp_path)
            (tmp_path / ".kontra").mkdir()
            (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  test_ds:
    type: postgres
    host: ${TEST_HOST}
    database: ${TEST_DB}
    user: user
    password: pass
    tables:
      users: public.users
""")

            uri = resolve_datasource("test_ds.users")
            assert "example.com" in uri
            assert "testdb" in uri
        finally:
            del os.environ["TEST_HOST"]
            del os.environ["TEST_DB"]

    def test_config_environment_overlay(self, tmp_path, monkeypatch):
        """Environment overlay overrides defaults."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "off"
environments:
  production:
    preplan: "on"
    pushdown: "on"
""")

        # Without env
        config = resolve_effective_config()
        assert config.preplan == "off"

        # With env
        config = resolve_effective_config(env_name="production")
        assert config.preplan == "on"
        assert config.pushdown == "on"

    def test_config_cli_overrides(self, tmp_path, monkeypatch):
        """CLI overrides take precedence."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "on"
  output_format: "rich"
""")

        config = resolve_effective_config(
            cli_overrides={"preplan": "off", "output_format": "json"}
        )

        assert config.preplan == "off"  # CLI wins
        assert config.output_format == "json"  # CLI wins


# =============================================================================
# Rule Execution Integration
# =============================================================================


class TestRuleExecutionIntegration:
    """Tests for rule execution paths."""

    def test_all_rule_types(self, tmp_path):
        """Test all built-in rule types execute."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
            "status": ["active", "active", "inactive", "pending", "active"],
            "age": [25, 30, 35, 40, 45],
            "updated_at": pl.Series([
                "2024-01-15T10:00:00",
                "2024-01-15T11:00:00",
                "2024-01-15T12:00:00",
                "2024-01-15T13:00:00",
                "2024-01-15T14:00:00",
            ]).str.to_datetime(),
        })
        parquet = tmp_path / "all_rules.parquet"
        df.write_parquet(parquet)

        contract = tmp_path / "all_rules_contract.yml"
        contract.write_text(f"""
name: all_rules_test
datasource: {parquet}

rules:
  - name: not_null
    params:
      column: id

  - name: unique
    params:
      column: id

  - name: min_rows
    params:
      threshold: 1

  - name: max_rows
    params:
      threshold: 100

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: range
    params:
      column: age
      min: 0
      max: 100

  - name: dtype
    params:
      column: age
      type: int64

  - name: regex
    params:
      column: email
      pattern: "@"
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        # All rules should pass
        assert result["summary"]["passed"] is True
        assert result["summary"]["total_rules"] == 8
        assert result["summary"]["rules_passed"] == 8

    def test_severity_levels(self, tmp_path):
        """Test severity levels affect pass/fail."""
        df = pl.DataFrame({
            "id": [1, 2, 3, None, 5],  # Has NULL
            "name": ["a", "b", "c", "d", "e"],
        })
        parquet = tmp_path / "severity.parquet"
        df.write_parquet(parquet)

        # Warning severity shouldn't fail validation
        contract = tmp_path / "warning_contract.yml"
        contract.write_text(f"""
name: warning_test
datasource: {parquet}

rules:
  - name: not_null
    params:
      column: id
    severity: warning

  - name: min_rows
    params:
      threshold: 1
    severity: blocking
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        # Should still pass because not_null is only warning
        assert result["summary"]["passed"] is True


# =============================================================================
# DataFrame Mode Integration
# =============================================================================


class TestDataFrameModeIntegration:
    """Tests for DataFrame input mode."""

    def test_polars_dataframe_validation(self, sample_data, tmp_path):
        """Validate a Polars DataFrame directly."""
        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: df_test
datasource: placeholder

rules:
  - name: not_null
    params:
      column: id

  - name: min_rows
    params:
      threshold: 5
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            dataframe=sample_data,
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is True
        assert result["summary"]["total_rules"] == 2

    def test_pandas_dataframe_validation(self, sample_data, tmp_path):
        """Validate a pandas DataFrame (auto-converted to Polars)."""
        pytest.importorskip("pandas")
        # Convert polars to pandas
        pdf = sample_data.to_pandas()

        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: pandas_test
datasource: placeholder

rules:
  - name: not_null
    params:
      column: id

  - name: unique
    params:
      column: id
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            dataframe=pdf,
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is True
        assert result["summary"]["total_rules"] == 2

    def test_dataframe_validation_failing(self, tmp_path):
        """DataFrame validation correctly detects failures."""
        df = pl.DataFrame({
            "id": [1, 2, None, 4, 5],  # Has NULL
            "name": ["a", "b", "c", "d", "e"],
        })

        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: failing_df_test
datasource: placeholder

rules:
  - name: not_null
    params:
      column: id
    severity: blocking
""")

        engine = ValidationEngine(
            contract_path=str(contract),
            dataframe=df,
            emit_report=False,
            save_state=False,
        )
        result = engine.run()

        assert result["summary"]["passed"] is False
        assert result["summary"]["rules_failed"] == 1


class TestPublicAPIIntegration:
    """Tests for the public kontra.validate() API."""

    def test_validate_function_with_dataframe(self, sample_data, tmp_path):
        """kontra.validate() works with DataFrame."""
        from kontra import validate

        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: api_test
datasource: placeholder

rules:
  - name: min_rows
    params:
      threshold: 5
""")

        result = validate(sample_data, str(contract), save=False)

        assert result.passed is True
        assert result.total_rules == 1

    def test_validate_function_with_path(self, parquet_file, tmp_path):
        """kontra.validate() works with file path."""
        from kontra import validate

        contract = tmp_path / "contract.yml"
        contract.write_text(f"""
name: path_api_test
datasource: {parquet_file}

rules:
  - name: min_rows
    params:
      threshold: 5
""")

        result = validate(str(parquet_file), str(contract), save=False)

        assert result.passed is True
        assert result.total_rules == 1
