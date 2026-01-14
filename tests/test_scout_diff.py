# tests/test_scout_diff.py
"""Tests for Scout profile diff functionality."""

from datetime import datetime, timezone
import tempfile
from pathlib import Path
import pytest

from kontra.scout.types import (
    DatasetProfile,
    ColumnProfile,
    ProfileState,
    ProfileDiff,
    ColumnDiff,
    TopValue,
)
from kontra.scout.store import (
    fingerprint_source,
    LocalProfileStore,
    create_profile_state,
)


class TestFingerprintSource:
    """Tests for source fingerprinting."""

    def test_fingerprint_stable(self):
        """Same URI produces same fingerprint."""
        uri = "data/users.parquet"
        fp1 = fingerprint_source(uri)
        fp2 = fingerprint_source(uri)
        assert fp1 == fp2

    def test_fingerprint_length(self):
        """Fingerprint is 16 hex characters."""
        fp = fingerprint_source("s3://bucket/data.parquet")
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_different_uris_different_fingerprints(self):
        """Different URIs produce different fingerprints."""
        fp1 = fingerprint_source("data/file1.parquet")
        fp2 = fingerprint_source("data/file2.parquet")
        assert fp1 != fp2


class TestProfileState:
    """Tests for ProfileState."""

    def test_to_dict_and_from_dict(self):
        """ProfileState round-trips through dict."""
        profile = DatasetProfile(
            source_uri="data.parquet",
            source_format="parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            engine_version="0.1.0",
            row_count=1000,
            column_count=5,
            columns=[
                ColumnProfile(
                    name="id",
                    dtype="int",
                    dtype_raw="INTEGER",
                    row_count=1000,
                    null_count=0,
                    null_rate=0.0,
                    distinct_count=1000,
                    uniqueness_ratio=1.0,
                )
            ],
        )

        state = ProfileState(
            source_fingerprint="abc123def456gh12",
            source_uri="data.parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            profile=profile,
            engine_version="0.1.0",
        )

        d = state.to_dict()
        restored = ProfileState.from_dict(d)

        assert restored.source_fingerprint == state.source_fingerprint
        assert restored.source_uri == state.source_uri
        assert restored.profile.row_count == 1000

    def test_to_json_and_from_json(self):
        """ProfileState round-trips through JSON."""
        profile = DatasetProfile(
            source_uri="data.parquet",
            source_format="parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            engine_version="0.1.0",
            row_count=500,
            column_count=3,
            columns=[],
        )

        state = ProfileState(
            source_fingerprint="0123456789abcdef",
            source_uri="data.parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            profile=profile,
        )

        json_str = state.to_json()
        restored = ProfileState.from_json(json_str)

        assert restored.source_fingerprint == state.source_fingerprint
        assert restored.profile.row_count == 500


class TestLocalProfileStore:
    """Tests for LocalProfileStore."""

    @pytest.fixture
    def store(self, tmp_path):
        """Create a temporary store."""
        return LocalProfileStore(base_path=str(tmp_path / "profiles"))

    @pytest.fixture
    def sample_state(self):
        """Create a sample ProfileState."""
        profile = DatasetProfile(
            source_uri="test.parquet",
            source_format="parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            engine_version="0.1.0",
            row_count=100,
            column_count=3,
            columns=[],
        )
        return ProfileState(
            source_fingerprint="abc123def456gh12",
            source_uri="test.parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            profile=profile,
        )

    def test_save_and_get_latest(self, store, sample_state):
        """Can save and retrieve a profile."""
        store.save(sample_state)

        retrieved = store.get_latest(sample_state.source_fingerprint)
        assert retrieved is not None
        assert retrieved.source_uri == sample_state.source_uri
        assert retrieved.profile.row_count == 100

    def test_get_history(self, store):
        """get_history returns profiles newest first."""
        # Save multiple profiles with different timestamps
        for i in range(3):
            profile = DatasetProfile(
                source_uri="test.parquet",
                source_format="parquet",
                profiled_at=f"2026-01-1{3-i}T10:00:00+00:00",
                engine_version="0.1.0",
                row_count=100 + i,
                column_count=3,
                columns=[],
            )
            state = ProfileState(
                source_fingerprint="abc123def456gh12",
                source_uri="test.parquet",
                profiled_at=profile.profiled_at,
                profile=profile,
            )
            store.save(state)

        history = store.get_history("abc123def456gh12", limit=10)
        assert len(history) == 3
        # Should be newest first (2026-01-13, 2026-01-12, 2026-01-11)
        assert history[0].profiled_at > history[1].profiled_at

    def test_list_sources(self, store, sample_state):
        """list_sources returns all fingerprints."""
        store.save(sample_state)

        # Save another source
        profile2 = DatasetProfile(
            source_uri="other.parquet",
            source_format="parquet",
            profiled_at="2026-01-13T11:00:00+00:00",
            engine_version="0.1.0",
            row_count=200,
            column_count=5,
            columns=[],
        )
        state2 = ProfileState(
            source_fingerprint="0123456789abcdef",
            source_uri="other.parquet",
            profiled_at=profile2.profiled_at,
            profile=profile2,
        )
        store.save(state2)

        sources = store.list_sources()
        assert len(sources) == 2
        assert "abc123def456gh12" in sources
        assert "0123456789abcdef" in sources

    def test_clear_specific_source(self, store, sample_state):
        """clear() can delete specific source's profiles."""
        store.save(sample_state)

        deleted = store.clear(sample_state.source_fingerprint)
        assert deleted == 1

        retrieved = store.get_latest(sample_state.source_fingerprint)
        assert retrieved is None


class TestProfileDiff:
    """Tests for ProfileDiff computation."""

    def _make_state(
        self,
        row_count: int,
        columns: list,
        profiled_at: str = "2026-01-13T10:00:00+00:00",
    ) -> ProfileState:
        """Helper to create ProfileState."""
        profile = DatasetProfile(
            source_uri="test.parquet",
            source_format="parquet",
            profiled_at=profiled_at,
            engine_version="0.1.0",
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
        )
        return ProfileState(
            source_fingerprint="abc123def456gh12",
            source_uri="test.parquet",
            profiled_at=profiled_at,
            profile=profile,
        )

    def test_no_changes(self):
        """Diff with identical profiles shows no changes."""
        cols = [
            ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100)
        ]
        before = self._make_state(100, cols, "2026-01-12T10:00:00+00:00")
        after = self._make_state(100, cols, "2026-01-13T10:00:00+00:00")

        diff = ProfileDiff.compute(before, after)

        assert diff.row_count_delta == 0
        assert not diff.columns_added
        assert not diff.columns_removed
        assert not diff.has_changes

    def test_row_count_change(self):
        """Diff detects row count changes."""
        cols = [
            ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100)
        ]
        before = self._make_state(100, cols, "2026-01-12T10:00:00+00:00")

        cols_after = [
            ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=150)
        ]
        after = self._make_state(150, cols_after, "2026-01-13T10:00:00+00:00")

        diff = ProfileDiff.compute(before, after)

        assert diff.row_count_before == 100
        assert diff.row_count_after == 150
        assert diff.row_count_delta == 50
        assert diff.row_count_pct_change == 50.0
        assert diff.has_changes

    def test_column_added(self):
        """Diff detects added columns."""
        before = self._make_state(
            100,
            [ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100)],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            100,
            [
                ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100),
                ColumnProfile(name="new_col", dtype="string", dtype_raw="VARCHAR", row_count=100),
            ],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)

        assert diff.columns_added == ["new_col"]
        assert not diff.columns_removed
        assert diff.has_changes
        assert diff.has_schema_changes

    def test_column_removed(self):
        """Diff detects removed columns."""
        before = self._make_state(
            100,
            [
                ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100),
                ColumnProfile(name="old_col", dtype="string", dtype_raw="VARCHAR", row_count=100),
            ],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            100,
            [ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100)],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)

        assert diff.columns_removed == ["old_col"]
        assert not diff.columns_added
        assert diff.has_changes
        assert diff.has_schema_changes

    def test_null_rate_increase(self):
        """Diff detects null rate increases."""
        before = self._make_state(
            100,
            [ColumnProfile(name="email", dtype="string", dtype_raw="VARCHAR", row_count=100, null_rate=0.01)],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            100,
            [ColumnProfile(name="email", dtype="string", dtype_raw="VARCHAR", row_count=100, null_rate=0.15)],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)

        assert len(diff.null_rate_increases) == 1
        assert diff.null_rate_increases[0].column_name == "email"
        assert diff.columns_changed
        assert diff.has_changes

    def test_dtype_change(self):
        """Diff detects dtype changes."""
        before = self._make_state(
            100,
            [ColumnProfile(name="value", dtype="int", dtype_raw="INTEGER", row_count=100)],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            100,
            [ColumnProfile(name="value", dtype="string", dtype_raw="VARCHAR", row_count=100)],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)

        assert len(diff.dtype_changes) == 1
        assert diff.dtype_changes[0].column_name == "value"
        assert diff.dtype_changes[0].dtype_before == "int"
        assert diff.dtype_changes[0].dtype_after == "string"
        assert diff.has_schema_changes

    def test_cardinality_change(self):
        """Diff detects significant cardinality changes."""
        before = self._make_state(
            100,
            [ColumnProfile(name="status", dtype="string", dtype_raw="VARCHAR", row_count=100, distinct_count=5)],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            100,
            [ColumnProfile(name="status", dtype="string", dtype_raw="VARCHAR", row_count=100, distinct_count=10)],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)

        assert len(diff.cardinality_changes) == 1
        assert diff.cardinality_changes[0].column_name == "status"
        assert diff.cardinality_changes[0].distinct_count_delta == 5

    def test_to_llm(self):
        """Diff produces LLM-friendly output."""
        before = self._make_state(
            100,
            [
                ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=100),
                ColumnProfile(name="email", dtype="string", dtype_raw="VARCHAR", row_count=100, null_rate=0.01),
            ],
            "2026-01-12T10:00:00+00:00",
        )
        after = self._make_state(
            150,
            [
                ColumnProfile(name="id", dtype="int", dtype_raw="INTEGER", row_count=150),
                ColumnProfile(name="email", dtype="string", dtype_raw="VARCHAR", row_count=150, null_rate=0.10),
                ColumnProfile(name="new_col", dtype="string", dtype_raw="VARCHAR", row_count=150),
            ],
            "2026-01-13T10:00:00+00:00",
        )

        diff = ProfileDiff.compute(before, after)
        llm_output = diff.to_llm()

        assert "Profile Diff" in llm_output
        assert "100" in llm_output  # before row count
        assert "150" in llm_output  # after row count
        assert "new_col" in llm_output  # added column
        assert "email" in llm_output  # null rate change


class TestCreateProfileState:
    """Tests for create_profile_state helper."""

    def test_creates_state_with_fingerprint(self):
        """create_profile_state generates correct fingerprint."""
        profile = DatasetProfile(
            source_uri="s3://bucket/data.parquet",
            source_format="parquet",
            profiled_at="2026-01-13T10:00:00+00:00",
            engine_version="0.1.0",
            row_count=1000,
            column_count=5,
            columns=[],
        )

        state = create_profile_state(profile)

        assert state.source_uri == "s3://bucket/data.parquet"
        assert state.source_fingerprint == fingerprint_source("s3://bucket/data.parquet")
        assert len(state.source_fingerprint) == 16
        assert state.profile is profile
