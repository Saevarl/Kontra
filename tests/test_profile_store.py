"""Tests for the scout profile store.

Covers:
1. get_default_profile_store() rebuilds when the cwd changes (bug-9 class).
2. get_profile_store() backend dispatch (local / unknown / postgres import).
3. LocalProfileStore public interface (save / get_latest / get_history /
   list_sources / clear).
"""

from __future__ import annotations

import pytest

from kontra.scout.store import (
    LocalProfileStore,
    create_profile_state,
    fingerprint_source,
    get_default_profile_store,
    get_profile_store,
)
from kontra.scout.types import (
    ColumnProfile,
    DatasetProfile,
    ProfileState,
)


def _make_profile(source_uri: str = "mem://demo", rows: int = 10) -> DatasetProfile:
    """Build a minimal but complete DatasetProfile for round-tripping."""
    return DatasetProfile(
        source_uri=source_uri,
        source_format="parquet",
        profiled_at="2026-07-08T12:00:00Z",
        engine_version="test",
        preset="scan",
        row_count=rows,
        column_count=1,
        columns=[
            ColumnProfile(
                name="id",
                dtype="int",
                dtype_raw="INTEGER",
                row_count=rows,
                null_count=0,
                distinct_count=rows,
            )
        ],
    )


# -----------------------------------------------------------------------------
# 1. get_default_profile_store() cwd invalidation (bug-9 class)
# -----------------------------------------------------------------------------


class TestGetDefaultProfileStoreCwd:
    def test_rebuilds_when_cwd_changes(self, tmp_path, monkeypatch):
        """A cwd change must retarget the default profile store's base_path."""
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()

        monkeypatch.chdir(dir_a)
        store_a = get_default_profile_store()
        assert store_a.base_path == dir_a / ".kontra" / "profiles"

        monkeypatch.chdir(dir_b)
        store_b = get_default_profile_store()
        assert store_b.base_path == dir_b / ".kontra" / "profiles"
        assert store_b is not store_a

    def test_same_cwd_reuses_instance(self, tmp_path, monkeypatch):
        """Repeated calls in the same cwd should be cheap (same object)."""
        monkeypatch.chdir(tmp_path)
        first = get_default_profile_store()
        second = get_default_profile_store()
        assert first is second


# -----------------------------------------------------------------------------
# 2. get_profile_store() backend dispatch
# -----------------------------------------------------------------------------


class TestGetProfileStoreDispatch:
    def test_local_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert isinstance(get_profile_store(), LocalProfileStore)
        assert isinstance(get_profile_store("local"), LocalProfileStore)
        assert isinstance(get_profile_store(""), LocalProfileStore)

    def test_unknown_backend_raises(self):
        with pytest.raises(ValueError, match="Unknown profile store backend"):
            get_profile_store("redis")

    def test_postgres_backend_constructs_without_connecting(self):
        """Requesting postgres returns a PostgresProfileStore; it must not
        connect (or require a live DB) until a method is called."""
        from kontra.scout.postgres_store import PostgresProfileStore

        store = get_profile_store(
            "postgres://user:pass@localhost:5999/db"
        )
        assert isinstance(store, PostgresProfileStore)
        # No connection established at construction.
        assert store._conn is None


# -----------------------------------------------------------------------------
# 3. LocalProfileStore public interface
# -----------------------------------------------------------------------------


class TestLocalProfileStoreInterface:
    def test_save_and_get_latest_roundtrip(self, tmp_path):
        store = LocalProfileStore(base_path=str(tmp_path / "profiles"))
        profile = _make_profile()
        state = create_profile_state(profile)

        store.save(state)

        loaded = store.get_latest(state.source_fingerprint)
        assert loaded is not None
        assert loaded.source_uri == state.source_uri
        assert loaded.profile.row_count == profile.row_count
        assert loaded.profile.columns[0].name == "id"

    def test_get_history_grows(self, tmp_path):
        store = LocalProfileStore(base_path=str(tmp_path / "profiles"))
        fp = fingerprint_source("mem://demo")

        s1 = ProfileState(
            source_fingerprint=fp,
            source_uri="mem://demo",
            profiled_at="2026-07-08T10:00:00Z",
            profile=_make_profile(rows=10),
        )
        s2 = ProfileState(
            source_fingerprint=fp,
            source_uri="mem://demo",
            profiled_at="2026-07-08T11:00:00Z",
            profile=_make_profile(rows=20),
        )
        store.save(s1)
        assert len(store.get_history(fp)) == 1
        store.save(s2)
        history = store.get_history(fp)
        assert len(history) == 2
        # Newest first
        assert history[0].profiled_at == "2026-07-08T11:00:00Z"
        assert history[0].profile.row_count == 20

    def test_list_sources_and_clear(self, tmp_path):
        store = LocalProfileStore(base_path=str(tmp_path / "profiles"))
        state = create_profile_state(_make_profile())
        store.save(state)

        assert state.source_fingerprint in store.list_sources()

        deleted = store.clear(state.source_fingerprint)
        assert deleted >= 1
        assert store.get_latest(state.source_fingerprint) is None
