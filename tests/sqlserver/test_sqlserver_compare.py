"""Live-database tests for the source-agnostic compare() probe against real
SQL Server (and one cross-engine SQL Server vs PostgreSQL case). Requires the
sqlserver test container."""

from __future__ import annotations

import polars as pl
import pytest

import kontra


def _user_ids(uri: str) -> list:
    import pymssql

    conn = pymssql.connect(
        server="localhost", port=1433, user="sa",
        password="Kontra_Test123!", database="kontra_test",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM dbo.users ORDER BY user_id")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


class TestSqlServerCompare:
    def test_table_vs_itself_no_changes(self, sqlserver_uri):
        r = kontra.compare(sqlserver_uri, sqlserver_uri, key="user_id")
        assert r.dropped == 0 and r.added == 0
        assert r.preserved == r.before_rows == r.after_rows

    def test_table_vs_modified_dataframe(self, sqlserver_uri):
        ids = _user_ids(sqlserver_uri)
        after = pl.DataFrame({"user_id": ids[:-3] + [7_000_001, 7_000_002, 7_000_003]})
        r = kontra.compare(sqlserver_uri, after, key="user_id")
        assert r.dropped == 3 and r.added == 3
        assert r.preserved == len(ids) - 3

    def test_file_vs_table(self, sqlserver_uri, tmp_path):
        ids = _user_ids(sqlserver_uri)
        f = str(tmp_path / "subset.parquet")
        pl.DataFrame({"user_id": ids[1:]}).write_parquet(f)  # file missing first user
        r = kontra.compare(f, sqlserver_uri, key="user_id")
        assert r.added == 1 and r.dropped == 0

    def test_cross_engine_sqlserver_vs_postgres(self, sqlserver_uri):
        """compare() a SQL Server table directly against a PostgreSQL table."""
        pg_uri = "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.users"
        try:
            import psycopg

            with psycopg.connect(
                host="localhost", port=5433, user="kontra",
                password="kontra_test", dbname="kontra_test", connect_timeout=3,
            ):
                pass
        except Exception:
            pytest.skip("PostgreSQL container not available for cross-engine test")

        r = kontra.compare(sqlserver_uri, pg_uri, key="user_id")
        # both are seeded from the same fixture data -> identical key sets
        assert r.dropped == 0 and r.added == 0
        assert r.preserved == r.before_rows == r.after_rows
