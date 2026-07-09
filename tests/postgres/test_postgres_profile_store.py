"""Live PostgreSQL round-trip tests for PostgresProfileStore.

Requires the PostgreSQL container from tests/postgres/docker-compose.yml.
Tests skip automatically if the database is unreachable (mirrors the skip
pattern used by the rest of tests/postgres/).
"""

from __future__ import annotations

import uuid

import pytest


def _pg_available(host: str = "localhost", port: int = 5433) -> bool:
    try:
        import psycopg

        with psycopg.connect(
            host=host,
            port=port,
            user="kontra",
            password="kontra_test",
            dbname="kontra_test",
            connect_timeout=5,
        ):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_available(),
    reason="PostgreSQL container not reachable on localhost:5433",
)

PG_URI = "postgres://kontra:kontra_test@localhost:5433/kontra_test"


@pytest.fixture
def pg_profile_store():
    """A PostgresProfileStore against the live container, cleaned up after."""
    from kontra.scout.store import get_profile_store

    store = get_profile_store(PG_URI)
    yield store
    store.close()


def _real_profile_state():
    """Build a genuine ProfileState from kontra.profile() over a DataFrame."""
    import kontra
    from kontra.scout.store import create_profile_state

    df = {
        "id": [1, 2, 3, 4, 5],
        "name": ["a", "b", "c", "d", "e"],
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
    }
    profile = kontra.profile(df)
    # Make the source unique per run so history assertions are isolated.
    unique = f"mem://profile-store-test/{uuid.uuid4().hex}"
    profile.source_uri = unique
    return create_profile_state(profile)


class TestPostgresProfileStoreRoundTrip:
    def test_save_and_load_latest(self, pg_profile_store):
        state = _real_profile_state()
        fp = state.source_fingerprint

        try:
            pg_profile_store.save(state)

            loaded = pg_profile_store.get_latest(fp)
            assert loaded is not None
            assert loaded.source_uri == state.source_uri
            assert loaded.source_fingerprint == fp
            # Profile round-trips faithfully.
            assert loaded.profile.row_count == state.profile.row_count
            assert loaded.profile.column_count == state.profile.column_count
            loaded_cols = {c.name for c in loaded.profile.columns}
            orig_cols = {c.name for c in state.profile.columns}
            assert loaded_cols == orig_cols
            # Equal via serialized form (lossless round-trip).
            assert loaded.to_dict() == state.to_dict()
        finally:
            pg_profile_store.clear(fp)

    def test_history_grows_on_resave(self, pg_profile_store):
        state1 = _real_profile_state()
        fp = state1.source_fingerprint

        # Second state: same source, different timestamp => distinct history row.
        state2 = _real_profile_state()
        state2.source_fingerprint = fp
        state2.source_uri = state1.source_uri
        state2.profiled_at = "2099-01-01T00:00:00Z"
        state2.profile.source_uri = state1.source_uri
        state2.profile.profiled_at = "2099-01-01T00:00:00Z"

        try:
            pg_profile_store.save(state1)
            assert len(pg_profile_store.get_history(fp)) == 1

            pg_profile_store.save(state2)
            history = pg_profile_store.get_history(fp)
            assert len(history) == 2
            # Newest (2099) first.
            assert history[0].profiled_at == "2099-01-01T00:00:00Z"

            # get_latest returns the newest.
            latest = pg_profile_store.get_latest(fp)
            assert latest.profiled_at == "2099-01-01T00:00:00Z"

            # Source appears in list_sources.
            assert fp in pg_profile_store.list_sources()
        finally:
            pg_profile_store.clear(fp)

    def test_resave_same_timestamp_upserts(self, pg_profile_store):
        """Re-saving the same profiled_at updates in place (no duplicate row)."""
        state = _real_profile_state()
        fp = state.source_fingerprint

        try:
            pg_profile_store.save(state)
            # Mutate and re-save with the same profiled_at.
            state.source_uri = state.source_uri + "?v=2"
            pg_profile_store.save(state)

            history = pg_profile_store.get_history(fp)
            assert len(history) == 1
            assert history[0].source_uri.endswith("?v=2")
        finally:
            pg_profile_store.clear(fp)
