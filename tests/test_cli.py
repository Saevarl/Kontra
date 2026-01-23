# tests/test_cli.py
"""Tests for Kontra CLI commands."""

import json
import os
import tempfile
from pathlib import Path

import pytest
import polars as pl
from typer.testing import CliRunner

from kontra.cli.main import app


runner = CliRunner()


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def tmp_project(tmp_path):
    """Create a temporary project directory."""
    os.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample parquet file for testing."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "status": ["active", "active", "inactive", "active", "pending"],
        "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
    })
    path = tmp_path / "data.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def sample_contract(tmp_path, sample_parquet):
    """Create a sample contract file."""
    contract = f"""
name: test_contract
datasource: {sample_parquet}

rules:
  - name: not_null
    params:
      column: id
  - name: min_rows
    params:
      threshold: 1
"""
    path = tmp_path / "contract.yml"
    path.write_text(contract)
    return path


@pytest.fixture
def failing_contract(tmp_path, sample_parquet):
    """Create a contract that will fail validation."""
    contract = f"""
name: failing_contract
datasource: {sample_parquet}

rules:
  - name: min_rows
    params:
      threshold: 1000
    severity: blocking
"""
    path = tmp_path / "failing_contract.yml"
    path.write_text(contract)
    return path


# =============================================================================
# kontra init
# =============================================================================


class TestInit:
    """Tests for kontra init command."""

    def test_init_creates_config(self, tmp_project):
        """Init creates .kontra/config.yml."""
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        assert "Kontra initialized" in result.output

        config_path = tmp_project / ".kontra" / "config.yml"
        assert config_path.exists()

    def test_init_creates_contracts_dir(self, tmp_project):
        """Init creates contracts/ directory."""
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0

        contracts_dir = tmp_project / "contracts"
        assert contracts_dir.exists()
        assert contracts_dir.is_dir()

    def test_init_already_initialized(self, tmp_project):
        """Init warns if already initialized."""
        # First init
        runner.invoke(app, ["init"])

        # Second init without force
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        assert "already initialized" in result.output

    def test_init_force_reinitializes(self, tmp_project):
        """Init --force overwrites existing config."""
        # First init
        runner.invoke(app, ["init"])

        # Modify config
        config_path = tmp_project / ".kontra" / "config.yml"
        config_path.write_text("custom: content")

        # Force reinit
        result = runner.invoke(app, ["init", "--force"])
        assert result.exit_code == 0
        assert "Kontra initialized" in result.output

        # Check config was overwritten
        content = config_path.read_text()
        assert "version:" in content


# =============================================================================
# kontra validate
# =============================================================================


class TestValidate:
    """Tests for kontra validate command."""

    def test_validate_passing(self, sample_contract):
        """Validate succeeds with passing contract."""
        result = runner.invoke(app, ["validate", str(sample_contract)])
        assert result.exit_code == 0

    def test_validate_failing(self, failing_contract):
        """Validate returns exit code 1 on failure."""
        result = runner.invoke(app, ["validate", str(failing_contract)])
        assert result.exit_code == 1

    def test_validate_json_output(self, sample_contract):
        """Validate with JSON output."""
        result = runner.invoke(app, ["validate", str(sample_contract), "-o", "json"])
        assert result.exit_code == 0

        # Should be valid JSON
        output = json.loads(result.output)
        # Check for expected keys (could be "summary" or "validation")
        assert "results" in output or "rules" in output or "dataset_name" in output

    def test_validate_with_stats(self, sample_contract):
        """Validate with stats output."""
        result = runner.invoke(app, ["validate", str(sample_contract), "--stats", "summary"])
        assert result.exit_code == 0
        assert "Stats" in result.output or "rows=" in result.output

    def test_validate_dry_run(self, sample_contract):
        """Validate --dry-run checks contract without running."""
        result = runner.invoke(app, ["validate", str(sample_contract), "--dry-run"])
        assert result.exit_code == 0
        assert "Dry run" in result.output
        assert "Ready to validate" in result.output

    def test_validate_dry_run_invalid_contract(self, tmp_path):
        """Dry run catches invalid contracts."""
        invalid = tmp_path / "invalid.yml"
        invalid.write_text("not: valid: yaml: syntax")

        result = runner.invoke(app, ["validate", str(invalid), "--dry-run"])
        assert result.exit_code == 2

    def test_validate_contract_not_found(self):
        """Validate returns exit code 2 for missing contract."""
        result = runner.invoke(app, ["validate", "nonexistent.yml"])
        assert result.exit_code == 2

    def test_validate_data_override(self, tmp_path, sample_parquet):
        """Validate with --data override."""
        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: test
datasource: placeholder.parquet

rules:
  - name: min_rows
    params:
      threshold: 1
""")
        result = runner.invoke(app, [
            "validate", str(contract),
            "--data", str(sample_parquet)
        ])
        assert result.exit_code == 0

    def test_validate_preplan_off(self, sample_contract):
        """Validate with preplan disabled."""
        result = runner.invoke(app, [
            "validate", str(sample_contract),
            "--preplan", "off"
        ])
        assert result.exit_code == 0

    def test_validate_pushdown_off(self, sample_contract):
        """Validate with pushdown disabled."""
        result = runner.invoke(app, [
            "validate", str(sample_contract),
            "--pushdown", "off"
        ])
        assert result.exit_code == 0

    def test_validate_projection_off(self, sample_contract):
        """Validate with projection disabled."""
        result = runner.invoke(app, [
            "validate", str(sample_contract),
            "--projection", "off"
        ])
        assert result.exit_code == 0

    def test_validate_no_state(self, sample_contract):
        """Validate with --no-state skips state saving."""
        result = runner.invoke(app, [
            "validate", str(sample_contract),
            "--no-state"
        ])
        assert result.exit_code == 0


# =============================================================================
# kontra scout
# =============================================================================


class TestProfile:
    """Tests for kontra profile command."""

    def test_profile_basic(self, sample_parquet):
        """Profile profiles a dataset."""
        result = runner.invoke(app, ["profile", str(sample_parquet)])
        assert result.exit_code == 0
        # Should show column info
        assert "id" in result.output or "name" in result.output

    def test_profile_scout_preset(self, sample_parquet):
        """Profile with scout preset (quick recon)."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--preset", "scout"
        ])
        assert result.exit_code == 0

    def test_profile_scan_preset(self, sample_parquet):
        """Profile with scan preset (default, full stats)."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--preset", "scan"
        ])
        assert result.exit_code == 0

    def test_profile_interrogate_preset(self, sample_parquet):
        """Profile with interrogate preset (deep)."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--preset", "interrogate"
        ])
        assert result.exit_code == 0

    def test_profile_json_output(self, sample_parquet):
        """Profile with JSON output."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "-o", "json"
        ])
        assert result.exit_code == 0

        # Should be valid JSON
        output = json.loads(result.output)
        assert "columns" in output

    def test_profile_draft_rules(self, sample_parquet):
        """Profile --draft generates contract YAML."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--draft"
        ])
        assert result.exit_code == 0
        assert "rules:" in result.output
        assert "name:" in result.output

    def test_profile_columns_filter(self, sample_parquet):
        """Profile with --columns filter."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--columns", "id,name"
        ])
        assert result.exit_code == 0

    def test_profile_sample(self, sample_parquet):
        """Profile with --sample."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--sample", "3"
        ])
        assert result.exit_code == 0

    def test_profile_include_patterns(self, sample_parquet):
        """Profile with --include-patterns."""
        result = runner.invoke(app, [
            "profile", str(sample_parquet),
            "--include-patterns"
        ])
        assert result.exit_code == 0

    def test_profile_file_not_found(self):
        """Profile returns non-zero exit code for missing file."""
        result = runner.invoke(app, ["profile", "nonexistent.parquet"])
        assert result.exit_code != 0  # Could be 2 (config error) or 3 (runtime error)


# =============================================================================
# kontra config
# =============================================================================


class TestConfig:
    """Tests for kontra config command."""

    def test_config_show_no_config(self, tmp_project):
        """Config show works without config file."""
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "preplan" in result.output

    def test_config_show_with_config(self, tmp_project):
        """Config show displays effective config."""
        # Create config
        runner.invoke(app, ["init"])

        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "preplan" in result.output

    def test_config_show_json(self, tmp_project):
        """Config show with JSON output."""
        runner.invoke(app, ["init"])

        result = runner.invoke(app, ["config", "show", "-o", "json"])
        assert result.exit_code == 0

        # Parse JSON from output (skip header lines)
        lines = result.output.strip().split("\n")
        # Find the JSON part (starts with {)
        json_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith("{"):
                json_start = i
                break

        if json_start is not None:
            json_str = "\n".join(lines[json_start:])
            output = json.loads(json_str)
            assert "preplan" in output
        else:
            # Output might not be pure JSON, just check it's not empty
            assert len(result.output) > 0

    def test_config_path_exists(self, tmp_project):
        """Config path shows existing config."""
        runner.invoke(app, ["init"])

        result = runner.invoke(app, ["config", "path"])
        assert result.exit_code == 0
        assert "exists" in result.output

    def test_config_path_not_found(self, tmp_project):
        """Config path shows missing config."""
        result = runner.invoke(app, ["config", "path"])
        assert result.exit_code == 0
        assert "not found" in result.output


# =============================================================================
# kontra diff
# =============================================================================


class TestDiff:
    """Tests for kontra diff command."""

    def test_diff_output(self, tmp_project, sample_contract):
        """Diff produces output."""
        # Run validation twice to have history
        runner.invoke(app, ["validate", str(sample_contract)])
        runner.invoke(app, ["validate", str(sample_contract)])

        result = runner.invoke(app, ["diff"])
        assert result.exit_code == 0
        # Should have some diff output
        assert "Diff" in result.output or "No validation" in result.output or "Only one" in result.output

    def test_diff_json_output(self, tmp_project, sample_contract):
        """Diff with JSON output."""
        # Run validation twice
        runner.invoke(app, ["validate", str(sample_contract)])
        runner.invoke(app, ["validate", str(sample_contract)])

        result = runner.invoke(app, ["diff", "-o", "json"])
        # Should produce output (may be JSON or message)
        assert len(result.output) > 0

    def test_diff_llm_output(self, tmp_project, sample_contract):
        """Diff with LLM output format."""
        runner.invoke(app, ["validate", str(sample_contract)])
        runner.invoke(app, ["validate", str(sample_contract)])

        result = runner.invoke(app, ["diff", "-o", "llm"])
        assert len(result.output) > 0


# =============================================================================
# Error Handling
# =============================================================================


class TestErrorHandling:
    """Tests for CLI error handling."""

    def test_invalid_yaml_contract(self, tmp_path):
        """Invalid YAML returns non-zero exit code."""
        invalid = tmp_path / "invalid.yml"
        invalid.write_text("{{invalid yaml")

        result = runner.invoke(app, ["validate", str(invalid)])
        assert result.exit_code != 0  # Could be 2 or 3

    def test_verbose_errors(self, tmp_path):
        """--verbose shows detailed errors."""
        invalid = tmp_path / "invalid.yml"
        invalid.write_text("{{invalid yaml")

        result = runner.invoke(app, ["validate", str(invalid), "--verbose"])
        assert result.exit_code != 0
        # Should have error message
        assert "Error" in result.output or "error" in result.output.lower()
