"""Live-database tests for the source-agnostic compare()/profile_relationship()
probes against real PostgreSQL. Requires the postgres test container."""

from __future__ import annotations

import polars as pl
import pytest

import kontra


class TestPostgresCompare:
    def test_table_vs_itself_no_changes(self, postgres_uri):
        r = kontra.compare(postgres_uri, postgres_uri, key="user_id")
        assert r.dropped == 0
        assert r.added == 0
        assert r.preserved == r.before_rows == r.after_rows

    def test_table_vs_modified_dataframe(self, postgres_uri, postgres_connection):
        cur = postgres_connection.cursor()
        cur.execute("SELECT user_id FROM public.users ORDER BY user_id")
        ids = [row[0] for row in cur.fetchall()]
        cur.close()
        # drop the last real user, add one synthetic key
        after = pl.DataFrame({"user_id": ids[:-1] + [9_999_999]})
        r = kontra.compare(postgres_uri, after, key="user_id")
        assert r.dropped == 1
        assert r.added == 1
        assert r.preserved == len(ids) - 1

    def test_file_vs_table(self, postgres_uri, postgres_connection, tmp_path):
        cur = postgres_connection.cursor()
        cur.execute("SELECT user_id FROM public.users ORDER BY user_id")
        ids = [row[0] for row in cur.fetchall()]
        cur.close()
        f = str(tmp_path / "subset.parquet")
        pl.DataFrame({"user_id": ids[2:]}).write_parquet(f)  # file missing first 2
        r = kontra.compare(f, postgres_uri, key="user_id")
        assert r.added == 2  # 2 users in the table but not the file
        assert r.dropped == 0

    def test_byoc_connection_with_table(self, postgres_connection):
        df = kontra.compare(
            postgres_connection,
            postgres_connection,
            key="user_id",
            before_table="public.users",
            after_table="public.users",
        )
        assert df.dropped == 0 and df.added == 0 and df.preserved > 0

    def test_relationship_table_vs_file(self, postgres_uri, postgres_connection, tmp_path):
        cur = postgres_connection.cursor()
        cur.execute("SELECT user_id FROM public.users ORDER BY user_id")
        ids = [row[0] for row in cur.fetchall()]
        cur.close()
        f = str(tmp_path / "keys.parquet")
        pl.DataFrame({"user_id": ids}).write_parquet(f)
        rel = kontra.profile_relationship(postgres_uri, f, on="user_id")
        assert rel is not None
