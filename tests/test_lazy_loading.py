# tests/test_lazy_loading.py
"""
Tests to guard the lazy loading architectural invariant.

IMPORTANT: These tests protect a critical performance optimization.
If any test here fails, it means someone added a top-level import that
breaks lazy loading, causing cold start to go from ~125ms to ~500ms+.

See CLAUDE.md "Architectural Invariants" section for details.

DO NOT skip or delete these tests. Fix the import that broke them.
"""

import subprocess
import sys

import pytest

# Mark entire module
pytestmark = pytest.mark.lazy_loading


@pytest.mark.lazy_loading
class TestLazyLoadingInvariant:
    """
    Guard tests for the lazy loading architectural invariant.

    These tests run in subprocesses to ensure clean import state.
    """

    def test_import_kontra_no_polars(self):
        """
        INVARIANT: `import kontra` must NOT load polars.

        If this fails, someone added a top-level `import polars` in a module
        that's imported when the kontra package loads. Find it and make it lazy.
        """
        code = """
import sys
import kontra
if 'polars' in sys.modules:
    print('FAIL: polars was loaded by import kontra')
    sys.exit(1)
print('OK: polars not loaded')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"polars loaded on import kontra:\n{result.stdout}\n{result.stderr}"

    def test_import_kontra_no_duckdb(self):
        """
        INVARIANT: `import kontra` must NOT load duckdb.

        If this fails, someone added a top-level `import duckdb` in a module
        that's imported when the kontra package loads. Find it and make it lazy.
        """
        code = """
import sys
import kontra
if 'duckdb' in sys.modules:
    print('FAIL: duckdb was loaded by import kontra')
    sys.exit(1)
print('OK: duckdb not loaded')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"duckdb loaded on import kontra:\n{result.stdout}\n{result.stderr}"

    def test_import_kontra_no_psycopg(self):
        """
        INVARIANT: `import kontra` must NOT load psycopg.

        If this fails, someone added a top-level `import psycopg` in a module
        that's imported when the kontra package loads. Find it and make it lazy.
        """
        code = """
import sys
import kontra
if 'psycopg' in sys.modules:
    print('FAIL: psycopg was loaded by import kontra')
    sys.exit(1)
print('OK: psycopg not loaded')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"psycopg loaded on import kontra:\n{result.stdout}\n{result.stderr}"

    def test_import_kontra_no_pymssql(self):
        """
        INVARIANT: `import kontra` must NOT load pymssql.

        If this fails, someone added a top-level `import pymssql` in a module
        that's imported when the kontra package loads. Find it and make it lazy.
        """
        code = """
import sys
import kontra
if 'pymssql' in sys.modules:
    print('FAIL: pymssql was loaded by import kontra')
    sys.exit(1)
print('OK: pymssql not loaded')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"pymssql loaded on import kontra:\n{result.stdout}\n{result.stderr}"

    def test_database_path_engine_no_polars_duckdb(self):
        """
        INVARIANT: Creating a ValidationEngine for database path must NOT load polars or duckdb.

        The database path uses SQL pushdown. polars is only needed if there are
        residual rules that can't be pushed to SQL (loaded at run time, not init).
        duckdb is never needed for database paths.

        If this fails, check:
        - engine.py: builtin rule imports should be in _ensure_builtin_rules_registered()
        - materializers/registry.py: register_materializers_for_path() logic
        - executors/registry.py: register_executors_for_path() logic
        """
        code = """
import sys
import kontra
from kontra.engine.engine import ValidationEngine

engine = ValidationEngine(
    inline_rules=[{'name': 'not_null', 'params': {'column': 'id'}}],
    data_path='postgres://localhost/db/users',
    emit_report=False,
    execution_path='database',
)

failed = []
if 'polars' in sys.modules:
    failed.append('polars')
if 'duckdb' in sys.modules:
    failed.append('duckdb')

if failed:
    print(f'FAIL: {", ".join(failed)} loaded for database path engine')
    sys.exit(1)
print('OK: no polars/duckdb for database path')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Heavy deps loaded for database path:\n{result.stdout}\n{result.stderr}"

    def test_file_path_engine_no_database_drivers(self):
        """
        INVARIANT: Creating a ValidationEngine for file path must NOT load database drivers.

        File paths use polars and duckdb (expected), but should never load
        psycopg or pymssql.

        If this fails, check:
        - materializers/registry.py: register_materializers_for_path() for file path
        - executors/registry.py: register_executors_for_path() for file path
        """
        code = """
import sys
import kontra
from kontra.engine.engine import ValidationEngine

engine = ValidationEngine(
    inline_rules=[{'name': 'not_null', 'params': {'column': 'id'}}],
    data_path='data.parquet',
    emit_report=False,
    execution_path='file',
)

failed = []
if 'psycopg' in sys.modules:
    failed.append('psycopg')
if 'pymssql' in sys.modules:
    failed.append('pymssql')

if failed:
    print(f'FAIL: {", ".join(failed)} loaded for file path engine')
    sys.exit(1)
print('OK: no database drivers for file path')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Database drivers loaded for file path:\n{result.stdout}\n{result.stderr}"

    def test_import_time_under_threshold(self):
        """
        INVARIANT: `import kontra` must complete in <200ms.

        This is a soft check (3 attempts, median) to catch severe regressions.
        If this consistently fails, check what new imports were added to:
        - kontra/__init__.py
        - kontra/api/*.py
        - kontra/config/*.py
        - Any module imported at package load time
        """
        code = """
import time
import sys

# Warm up Python's import machinery
import importlib

times = []
for _ in range(3):
    # Clear kontra modules
    to_remove = [m for m in sys.modules if m.startswith('kontra')]
    for m in to_remove:
        del sys.modules[m]

    start = time.perf_counter()
    import kontra
    elapsed_ms = (time.perf_counter() - start) * 1000
    times.append(elapsed_ms)

median = sorted(times)[1]
print(f'Import times: {[f"{t:.1f}ms" for t in times]}')
print(f'Median: {median:.1f}ms')

if median > 300:  # 300ms threshold (generous for CI variance)
    print(f'FAIL: import too slow ({median:.1f}ms > 300ms)')
    sys.exit(1)
print('OK: import time acceptable')
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Import too slow:\n{result.stdout}\n{result.stderr}"


@pytest.mark.lazy_loading
class TestLazyLoadingHelpers:
    """Test that lazy loading helper functions work correctly."""

    def test_get_polars_returns_module(self):
        """_get_polars() should return the polars module."""
        from kontra.engine.engine import _get_polars

        pl = _get_polars()
        assert pl is not None
        assert hasattr(pl, "DataFrame")
        assert hasattr(pl, "col")

    def test_get_polars_backend_returns_class(self):
        """_get_polars_backend() should return the PolarsBackend class."""
        from kontra.engine.engine import _get_polars_backend

        PolarsBackend = _get_polars_backend()
        assert PolarsBackend is not None
        assert hasattr(PolarsBackend, "execute")

    def test_ensure_builtin_rules_registered(self):
        """_ensure_builtin_rules_registered() should register all builtin rules."""
        from kontra.engine.engine import _ensure_builtin_rules_registered
        from kontra.rule_defs.registry import get_all_rule_names

        _ensure_builtin_rules_registered()

        rule_names = get_all_rule_names()
        # Should have at least the core rules
        assert "not_null" in rule_names
        assert "unique" in rule_names
        assert "range" in rule_names
        assert "allowed_values" in rule_names
        assert len(rule_names) >= 18  # We have 18 builtin rules
