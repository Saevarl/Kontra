"""
Tests for estimate-aware database profiling.

Covers provenance flags, the consistency guard (clamp+label vs exact-vs-exact),
serialization backward-compat, reporter markers, and same-moment COUNT(*) usage
in scanning presets. No live databases required (mocked cursors/backends).
"""

from unittest.mock import MagicMock, patch

import pytest

from kontra.scout.types import (
    ColumnProfile,
    DatasetProfile,
    ProfileState,
    enforce_profile_invariants,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_profile(row_count, row_est, columns, preset="scan"):
    return DatasetProfile(
        source_uri="mssql:///dbo.orders",
        source_format="sqlserver",
        profiled_at="2026-01-01T00:00:00Z",
        engine_version="0.1.0",
        preset=preset,
        row_count=row_count,
        row_count_estimated=row_est,
        column_count=len(columns),
        columns=columns,
    )


# ---------------------------------------------------------------------------
# Consistency guard: clamp AND label, never contradict
# ---------------------------------------------------------------------------


class TestConsistencyGuard:
    def test_clamp_and_flag_nulls_exceeding_rows(self):
        """An estimated null_count > row_count is clamped and flagged."""
        col = ColumnProfile(
            name="c", dtype="string", dtype_raw="varchar",
            row_count=100, null_count=150, null_count_estimated=True,
        )
        profile = _make_profile(100, False, [col])
        warnings = enforce_profile_invariants(profile)

        assert col.null_count == 100
        assert col.null_count_estimated is True
        assert col.null_rate == 1.0
        assert warnings == []  # clamped silently (it involved an estimate)

    def test_clamp_and_flag_identity_distinct_exceeding_rows(self):
        """An estimated distinct_count > row_count (identity col) is clamped."""
        col = ColumnProfile(
            name="id", dtype="int", dtype_raw="bigint",
            row_count=137000, null_count=0,
            distinct_count=150000, distinct_count_estimated=True,
        )
        profile = _make_profile(137000, True, [col])
        warnings = enforce_profile_invariants(profile)

        assert col.distinct_count == 137000
        assert col.distinct_count_estimated is True
        # uniqueness recomputed against non-null count
        assert col.uniqueness_ratio == pytest.approx(1.0)
        assert warnings == []

    def test_exact_metric_vs_estimated_row_count_not_clamped(self):
        """An EXACT metric exceeding an ESTIMATED row_count must not be
        corrupted: the estimate is the soft value (an exact count is a hard
        bound). Leave the exact value, warn that the row estimate is low."""
        col = ColumnProfile(
            name="c", dtype="int", dtype_raw="int",
            row_count=100, distinct_count=120, distinct_count_estimated=False,
        )
        profile = _make_profile(100, True, [col])  # row_count estimated
        warnings = enforce_profile_invariants(profile)

        assert col.distinct_count == 120  # exact value preserved
        assert col.distinct_count_estimated is False  # not relabeled
        assert len(warnings) == 1
        assert "row estimate is low" in warnings[0]

    def test_exact_vs_exact_not_clamped_but_warns(self):
        """Two exact values that contradict are left intact and warned."""
        col = ColumnProfile(
            name="c", dtype="string", dtype_raw="varchar",
            row_count=100, null_count=150, null_count_estimated=False,
        )
        profile = _make_profile(100, False, [col])  # row_count exact
        warnings = enforce_profile_invariants(profile)

        assert col.null_count == 150  # NOT clamped
        assert len(warnings) == 1
        assert "not clamped" in warnings[0]
        assert "null_count" in warnings[0]

    def test_no_violation_leaves_profile_untouched(self):
        col = ColumnProfile(
            name="c", dtype="int", dtype_raw="int",
            row_count=100, null_count=5, distinct_count=90,
        )
        profile = _make_profile(100, False, [col])
        warnings = enforce_profile_invariants(profile)
        assert warnings == []
        assert col.null_count == 5
        assert col.distinct_count == 90

    def test_guard_wired_into_profiler_output(self, tmp_path):
        """enforce_profile_invariants runs during profile() and appends warnings."""
        import polars as pl
        from kontra.scout.profiler import ScoutProfiler

        df = pl.DataFrame({"id": list(range(50))})
        parquet = tmp_path / "d.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="interrogate")
        profile = profiler.profile()
        # Exact DuckDB scan: no impossible facts, no clamp warnings.
        assert profile.row_count == 50
        assert profile.row_count_estimated is False
        for c in profile.columns:
            assert c.distinct_count <= profile.row_count
            assert c.null_count <= profile.row_count


# ---------------------------------------------------------------------------
# Serialization round-trip & backward compat
# ---------------------------------------------------------------------------


class TestSerializationCompat:
    def test_flags_roundtrip(self):
        col = ColumnProfile(
            name="id", dtype="int", dtype_raw="bigint",
            row_count=100, null_count=2, null_count_estimated=True,
            distinct_count=95, distinct_count_estimated=True,
        )
        profile = _make_profile(100, True, [col])
        restored = DatasetProfile.from_dict(profile.to_dict())

        assert restored.row_count_estimated is True
        rc = restored.columns[0]
        assert rc.null_count_estimated is True
        assert rc.distinct_count_estimated is True

    def test_old_dict_without_flags_defaults_false(self):
        """Profiles saved before flags existed load with False defaults."""
        old = {
            "schema_version": "1.0",
            "source_uri": "postgres:///public.users",
            "source_format": "postgres",
            "profiled_at": "2024-01-01T00:00:00Z",
            "engine_version": "0.4.0",
            "preset": "scan",
            "dataset": {
                "row_count": 1000,
                "column_count": 1,
                # NOTE: no row_count_estimated key
            },
            "columns": [
                {
                    "name": "id",
                    "dtype": "int",
                    "dtype_raw": "integer",
                    "counts": {
                        "rows": 1000,
                        "nulls": 0,
                        "null_rate": 0.0,
                        "distinct": 1000,
                        "uniqueness_ratio": 1.0,
                        # NOTE: no *_estimated keys
                    },
                }
            ],
        }
        profile = DatasetProfile.from_dict(old)
        assert profile.row_count_estimated is False
        assert profile.columns[0].null_count_estimated is False
        assert profile.columns[0].distinct_count_estimated is False

    def test_old_profile_state_still_loads(self):
        """A ProfileState from an old dict (no flags) round-trips for diff."""
        old_state = {
            "schema_version": "1.0",
            "engine_version": "0.4.0",
            "source_fingerprint": "abc123",
            "source_uri": "postgres:///public.users",
            "profiled_at": "2024-01-01T00:00:00Z",
            "profile": {
                "source_uri": "postgres:///public.users",
                "source_format": "postgres",
                "profiled_at": "2024-01-01T00:00:00Z",
                "engine_version": "0.4.0",
                "preset": "scan",
                "dataset": {"row_count": 10, "column_count": 0},
                "columns": [],
            },
        }
        state = ProfileState.from_dict(old_state)
        assert state.profile.row_count == 10
        assert state.profile.row_count_estimated is False

    def test_json_emits_flags(self):
        from kontra.scout.reporters.json_reporter import render_json
        import json

        col = ColumnProfile(
            name="id", dtype="int", dtype_raw="bigint",
            row_count=100, distinct_count=95, distinct_count_estimated=True,
        )
        profile = _make_profile(100, True, [col])
        parsed = json.loads(render_json(profile))
        assert parsed["dataset"]["row_count_estimated"] is True
        assert parsed["columns"][0]["counts"]["distinct_estimated"] is True
        assert parsed["columns"][0]["counts"]["nulls_estimated"] is False


# ---------------------------------------------------------------------------
# Reporter markers (~) and to_llm text
# ---------------------------------------------------------------------------


class TestReporterMarkers:
    def _profile_estimated(self):
        col = ColumnProfile(
            name="id", dtype="int", dtype_raw="bigint",
            row_count=137000, null_count=100, null_rate=100 / 137000,
            null_count_estimated=True,
            distinct_count=126375, distinct_count_estimated=True,
        )
        return _make_profile(137000, True, [col])

    def test_rich_marks_estimates(self):
        from kontra.scout.reporters.rich_reporter import render_rich

        out = render_rich(self._profile_estimated())
        assert "~137,000" in out
        assert "~126,375" in out

    def test_markdown_marks_estimates(self):
        from kontra.scout.reporters.markdown_reporter import render_markdown

        out = render_markdown(self._profile_estimated())
        assert "~137,000" in out
        assert "~126,375" in out

    def test_exact_values_not_marked(self):
        from kontra.scout.reporters.markdown_reporter import render_markdown

        col = ColumnProfile(
            name="id", dtype="int", dtype_raw="bigint",
            row_count=100, distinct_count=100, null_rate=0.0,
        )
        profile = _make_profile(100, False, [col])
        out = render_markdown(profile)
        assert "~" not in out

    def test_to_llm_shows_estimated_text(self):
        profile = self._profile_estimated()
        out = profile.to_llm()
        assert "~126,375 (estimated)" in out
        assert "rows=~137,000 (estimated)" in out


# ---------------------------------------------------------------------------
# Same-moment COUNT(*) in scanning presets
# ---------------------------------------------------------------------------


class TestSameMomentRowCount:
    def _profiler_with_mock_backend(self, tmp_path, exec_return):
        import polars as pl
        from kontra.scout.profiler import ScoutProfiler

        df = pl.DataFrame({"id": [1, 2, 3]})
        parquet = tmp_path / "d.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="interrogate")

        backend = MagicMock()
        backend.esc_ident.side_effect = lambda n: '"' + n + '"'
        backend.source_format = "duckdb"
        captured = {}

        def fake_exec(exprs):
            captured["exprs"] = exprs
            return exec_return

        backend.execute_stats_query.side_effect = fake_exec
        # No sampled/top-value data
        backend.fetch_top_values.return_value = []
        backend.fetch_distinct_values.return_value = []
        profiler.backend = backend
        profiler._effective_row_count = 999999  # stale estimate
        profiler._row_count_estimated = True
        return profiler, captured

    def test_scanning_path_emits_count_star(self, tmp_path):
        """The exact-scan path adds COUNT(*) to the aggregate query."""
        exec_return = {
            "__total_rows__": 137000,
            "__null__id": 0,
            "__distinct__id": 100,
        }
        profiler, captured = self._profiler_with_mock_backend(tmp_path, exec_return)
        profiler._profile_columns([("id", "INTEGER")], row_count=999999)

        # A dedicated same-moment COUNT(*) expr (aliased __total_rows__) is added
        assert any("__total_rows__" in e and "COUNT(*)" in e for e in captured["exprs"])

    def test_scanned_count_overrides_estimate(self, tmp_path):
        """The same-moment COUNT(*) replaces the stale estimate."""
        exec_return = {
            "__total_rows__": 137000,
            "__null__id": 0,
            "__distinct__id": 100,
        }
        profiler, _ = self._profiler_with_mock_backend(tmp_path, exec_return)
        profiler._profile_columns([("id", "INTEGER")], row_count=999999)

        assert profiler._effective_row_count == 137000
        assert profiler._row_count_estimated is False

    def test_sampled_path_does_not_override_and_flags(self, tmp_path):
        """When sampling, COUNT(*) is not added and aggregates are flagged."""
        import polars as pl
        from kontra.scout.profiler import ScoutProfiler

        df = pl.DataFrame({"id": [1, 2, 3]})
        parquet = tmp_path / "d.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="interrogate", sample_size=2)
        backend = MagicMock()
        backend.esc_ident.side_effect = lambda n: '"' + n + '"'
        backend.source_format = "duckdb"
        captured = {}

        def fake_exec(exprs):
            captured["exprs"] = exprs
            return {"__null__id": 0, "__distinct__id": 2}

        backend.execute_stats_query.side_effect = fake_exec
        backend.fetch_top_values.return_value = []
        backend.fetch_distinct_values.return_value = []
        profiler.backend = backend
        profiler._effective_row_count = 3
        profiler._row_count_estimated = False

        profiles = profiler._profile_columns([("id", "INTEGER")], row_count=3)
        assert not any("__total_rows__" in e for e in captured["exprs"])
        assert profiles[0].null_count_estimated is True
        assert profiles[0].distinct_count_estimated is True

    def test_adopt_exact_row_count_helper(self, tmp_path):
        import polars as pl
        from kontra.scout.profiler import ScoutProfiler

        df = pl.DataFrame({"id": [1, 2, 3]})
        parquet = tmp_path / "d.parquet"
        df.write_parquet(parquet)
        profiler = ScoutProfiler(str(parquet), preset="scan")
        profiler._effective_row_count = 999
        profiler._row_count_estimated = True

        metadata = {"id": {"null_count": 0, "exact_row_count": 137000}}
        result = profiler._adopt_exact_row_count(metadata, 999)
        assert result == 137000
        assert profiler._effective_row_count == 137000
        assert profiler._row_count_estimated is False


# ---------------------------------------------------------------------------
# PostgreSQL: negative n_distinct + reltuples handling
# ---------------------------------------------------------------------------


class TestPostgresEstimates:
    @pytest.fixture
    def pg_backend(self):
        from kontra.scout.backends.postgres_backend import PostgreSQLBackend

        handle = MagicMock()
        handle.db_params = MagicMock()
        handle.db_params.schema = "public"
        handle.db_params.table = "users"
        backend = PostgreSQLBackend(handle)
        backend._conn = MagicMock()
        return backend

    def test_negative_n_distinct_ratio_conversion(self, pg_backend):
        """Negative n_distinct is a ratio: -1 => all distinct, -0.5 => half."""
        pg_backend._pg_stats = {
            "all_unique": {"null_frac": 0.0, "n_distinct": -1.0},
            "half_unique": {"null_frac": 0.0, "n_distinct": -0.5},
            "exact": {"null_frac": 0.0, "n_distinct": 42},
        }
        schema = [("all_unique", "int"), ("half_unique", "int"), ("exact", "int")]
        result = pg_backend.profile_metadata_only(schema, row_count=1000)

        assert result["all_unique"]["distinct_count"] == 1000  # -1.0 * 1000
        assert result["half_unique"]["distinct_count"] == 500  # -0.5 * 1000
        assert result["exact"]["distinct_count"] == 42
        # pg_stats-derived metrics are flagged estimated per-metric
        for col in result.values():
            assert col["null_count_estimated"] is True
            assert col["distinct_count_estimated"] is True

    def test_reltuples_negative_one_falls_back_to_count(self, pg_backend):
        """reltuples = -1 (never analyzed, PG14+) triggers exact COUNT(*)."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(-1,), (12345,)]
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = pg_backend.get_row_count()
        assert count == 12345
        assert pg_backend.row_count_estimated is False

    def test_reltuples_positive_is_estimate(self, pg_backend):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (500000,)
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = pg_backend.get_row_count()
        assert count == 500000
        assert pg_backend.row_count_estimated is True


# ---------------------------------------------------------------------------
# SQL Server: same-batch COUNT(*) and per-metric flags in metadata path
# ---------------------------------------------------------------------------


class TestSqlServerMetadata:
    @pytest.fixture
    def ss_backend(self):
        from kontra.scout.backends.sqlserver_backend import SqlServerBackend

        handle = MagicMock()
        handle.db_params = MagicMock()
        handle.db_params.schema = "dbo"
        handle.db_params.table = "orders"
        backend = SqlServerBackend(handle)
        backend._conn = MagicMock()
        return backend

    def test_metadata_captures_exact_row_count_and_flags(self, ss_backend):
        """Small-table exact null/distinct scan captures a same-moment COUNT(*)."""
        cur = MagicMock()
        ss_backend._conn.cursor.return_value = cur

        schema = [("id", "bigint"), ("name", "varchar")]

        # fetchone sequence:
        #  1. _get_object_id -> (object_id,)
        #  2. COUNT(DISTINCT ...) + COUNT(*) -> (10, 5, 500)
        #  3. null counts + COUNT(*) -> (0, 3, 500)
        cur.fetchone.side_effect = [(123,), (10, 5, 500), (0, 3, 500)]
        # fetchall sequence: stats properties (empty), histogram (empty)
        cur.fetchall.side_effect = [[], []]

        result = ss_backend.profile_metadata_only(schema, row_count=1000)

        # exact_row_count surfaced on every column
        assert result["id"]["exact_row_count"] == 500
        assert result["name"]["exact_row_count"] == 500
        # exact distinct + exact null => not estimated
        assert result["id"]["distinct_count_estimated"] is False
        assert result["id"]["null_count_estimated"] is False
        assert result["id"]["distinct_count"] == 10
        assert result["name"]["distinct_count"] == 5

        # Assert emitted SQL for both exact scans contains COUNT(*)
        executed = [c.args[0] for c in cur.execute.call_args_list]
        assert any("COUNT(*)" in sql for sql in executed)

    def test_row_count_estimate_flag(self, ss_backend):
        cur = MagicMock()
        cur.fetchone.return_value = (250000,)
        ss_backend._conn.cursor.return_value = cur

        count = ss_backend.get_row_count()
        assert count == 250000
        assert ss_backend.row_count_estimated is True
