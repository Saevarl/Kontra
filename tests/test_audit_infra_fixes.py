"""Regression tests for audited infrastructure fixes.

Covers:
1. get_default_store() rebuilds when the cwd changes (pitfall #6).
2. ValidationState.from_dict schema_version default matches the dataclass.
3. Azure service-principal branch escapes single quotes in account_name.
4. @validate decorator rejects invalid on_fail modes at decoration time.
5. `kontra diff` on a missing contract exits with the config-error code (2).
6. format_error_for_cli does not double the "File not found:" prefix.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


# -----------------------------------------------------------------------------
# 1. get_default_store() cwd invalidation
# -----------------------------------------------------------------------------


class TestGetDefaultStoreCwd:
    def test_rebuilds_when_cwd_changes(self, tmp_path, monkeypatch):
        """A cwd change must retarget the default store's base_path."""
        from kontra.state.backends import get_default_store

        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()

        monkeypatch.chdir(dir_a)
        store_a = get_default_store()
        assert store_a.base_path == dir_a / ".kontra" / "state"

        monkeypatch.chdir(dir_b)
        store_b = get_default_store()
        assert store_b.base_path == dir_b / ".kontra" / "state"
        assert store_b is not store_a

    def test_same_cwd_reuses_instance(self, tmp_path, monkeypatch):
        """Repeated calls in the same cwd should be cheap (same object)."""
        from kontra.state.backends import get_default_store

        monkeypatch.chdir(tmp_path)
        first = get_default_store()
        second = get_default_store()
        assert first is second


# -----------------------------------------------------------------------------
# 2. schema_version default alignment
# -----------------------------------------------------------------------------


class TestSchemaVersionDefault:
    def test_from_dict_default_matches_dataclass(self):
        from dataclasses import fields

        from kontra.state.types import (
            StateSummary,
            ValidationState,
        )

        # The dataclass default for schema_version
        dataclass_default = next(
            f.default for f in fields(ValidationState) if f.name == "schema_version"
        )

        d = {
            "contract_fingerprint": "abc123",
            "dataset_fingerprint": None,
            "contract_name": "c",
            "dataset_uri": "data.parquet",
            "run_at": "2024-01-01T00:00:00+00:00",
            "summary": StateSummary(
                passed=True, total_rules=0, passed_rules=0, failed_rules=0
            ).to_dict(),
            "rules": [],
            # schema_version intentionally absent
        }
        state = ValidationState.from_dict(d)
        assert state.schema_version == dataclass_default == "2.0"


# -----------------------------------------------------------------------------
# 3. Azure service-principal quote escaping
# -----------------------------------------------------------------------------


class _RecordingConn:
    """Minimal DuckDB connection stand-in that records executed SQL."""

    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        return self


class TestAzureServicePrincipalEscaping:
    def test_account_name_single_quote_is_doubled(self):
        from kontra.engine.backends import duckdb_session

        con = _RecordingConn()
        fs_opts = {
            "azure_account_name": "acc'ount",
            "azure_tenant_id": "tenant",
            "azure_client_id": "client",
            "azure_client_secret": "secret",
        }

        duckdb_session._configure_azure(con, fs_opts)

        create_secret_sql = [s for s in con.executed if "CREATE SECRET azure_sp" in s]
        assert len(create_secret_sql) == 1
        sql = create_secret_sql[0]
        # The single quote must be doubled inside the SQL literal.
        assert "acc''ount" in sql
        # And the raw un-escaped form must NOT appear as a lone literal.
        assert "'acc'ount'" not in sql


# -----------------------------------------------------------------------------
# 4. on_fail validation at decoration time
# -----------------------------------------------------------------------------


class TestOnFailValidation:
    def test_invalid_mode_raises_at_decoration(self):
        from kontra.api.decorators import validate_decorator

        with pytest.raises(ValueError, match="Invalid on_fail mode"):
            validate_decorator(
                rules=[{"name": "not_null", "params": {"column": "id"}}],
                on_fail="ignore",
            )

    def test_case_sensitive_typo_raises(self):
        from kontra.api.decorators import validate_decorator

        with pytest.raises(ValueError, match="Invalid on_fail mode"):
            validate_decorator(
                rules=[{"name": "not_null", "params": {"column": "id"}}],
                on_fail="Raise",
            )

    @pytest.mark.parametrize("mode", ["raise", "warn", "return_result"])
    def test_valid_modes_decorate(self, mode):
        from kontra.api.decorators import validate_decorator

        @validate_decorator(
            rules=[{"name": "not_null", "params": {"column": "id"}}],
            on_fail=mode,
        )
        def f():
            return [{"id": 1}]

        # Decoration must not raise; callable is returned.
        assert callable(f)

    def test_callable_handler_is_valid(self):
        from kontra.api.decorators import validate_decorator

        def handler(result, data):
            return data

        @validate_decorator(
            rules=[{"name": "not_null", "params": {"column": "id"}}],
            on_fail=handler,
        )
        def f():
            return [{"id": 1}]

        assert callable(f)

    def test_warn_mode_returns_data_on_failure(self, recwarn):
        from kontra.api.decorators import validate_decorator

        @validate_decorator(
            rules=[{"name": "not_null", "params": {"column": "id"}}],
            on_fail="warn",
        )
        def load():
            return [{"id": None}, {"id": 2}]

        data = load()
        # warn mode returns the data even though validation failed
        assert data == [{"id": None}, {"id": 2}]


# -----------------------------------------------------------------------------
# 5. diff missing-contract exit code
# -----------------------------------------------------------------------------


class TestDiffMissingContractExitCode:
    def test_missing_contract_exits_config_error(self, tmp_path):
        repo_root = Path(__file__).resolve().parent.parent
        env = dict(os.environ)
        # Ensure src is importable when running as a module.
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "kontra.cli.main",
                "diff",
                str(tmp_path / "no" / "such" / "contract.yml"),
            ],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 2, (
            f"expected EXIT_CONFIG_ERROR (2), got {proc.returncode}\n"
            f"stdout={proc.stdout}\nstderr={proc.stderr}"
        )


# -----------------------------------------------------------------------------
# 6. No doubled "File not found:" prefix
# -----------------------------------------------------------------------------


class TestFileNotFoundPrefix:
    def test_prefix_not_doubled(self):
        from kontra.errors import format_error_for_cli

        err = FileNotFoundError("File not found: /some/path.yml")
        msg = format_error_for_cli(err)
        assert msg.count("File not found:") == 1

    def test_prefix_added_for_plain_error(self):
        from kontra.errors import format_error_for_cli

        err = FileNotFoundError("/some/path.yml")
        msg = format_error_for_cli(err)
        assert msg.startswith("File not found:")
        assert "/some/path.yml" in msg
