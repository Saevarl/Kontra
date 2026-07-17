# tests/sqlserver/test_query_source.py
"""Query source (read-only SELECT) integration tests for SQL Server."""

import uuid

import pytest

import kontra
from kontra import Query


@pytest.fixture
def cross_database_users_uri():
    """Clone users into tempdb so mixed-pair planning must use the query DB."""
    import pymssql

    table = "kontra_query_compare_" + uuid.uuid4().hex[:8]
    conn = pymssql.connect(
        server="localhost", port=1433, user="sa",
        password="Kontra_Test123!", database="kontra_test",
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * INTO tempdb.dbo.{table} FROM dbo.users")
    conn.commit()
    try:
        yield f"mssql://sa:Kontra_Test123!@localhost:1433/tempdb/dbo.{table}"
    finally:
        cur.execute(f"DROP TABLE tempdb.dbo.{table}")
        conn.commit()
        conn.close()


@pytest.mark.integration
class TestQuerySourceSqlServer:
    @pytest.mark.parametrize("query_first", [True, False], ids=["query-table", "table-query"])
    def test_compare_query_and_table_uses_sql_pushdown(
        self, sqlserver_uri, cross_database_users_uri, query_first,
    ):
        query = Query("SELECT * FROM dbo.users", source=sqlserver_uri)
        before, after = (
            (query, cross_database_users_uri)
            if query_first else (cross_database_users_uri, query)
        )

        r = kontra.compare(before, after, key="user_id")

        assert r.execution_tier == "sql"
        assert r.dropped == 0 and r.added == 0 and r.changed_rows == 0
        assert r.preserved == r.before_rows == r.after_rows

    def test_compare_query_vs_query(self, sqlserver_uri):
        before = Query(
            "SELECT 1 AS id, 100 AS amt UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 300",
            source=sqlserver_uri,
        )
        after = Query(
            "SELECT 2 AS id, 200 AS amt UNION ALL SELECT 3, 999 UNION ALL SELECT 4, 400",
            source=sqlserver_uri,
        )

        r = kontra.compare(before, after, key="id")

        assert r.before_rows == 3 and r.after_rows == 3
        assert r.dropped == 1  # id 1
        assert r.added == 1  # id 4
        assert r.preserved == 2  # ids 2, 3
        assert r.changed_rows == 1  # id 3: 300 -> 999
        # Both query sources share one SQL Server engine -> set-based SQL (Mode A).
        assert r.execution_tier == "sql"
        assert r.samples_dropped_keys == []  # aggregate-only by default

    def test_relationship_query_vs_query(self, sqlserver_uri):
        left = Query("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3", source=sqlserver_uri)
        right = Query("SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 2", source=sqlserver_uri)

        r = kontra.profile_relationship(left, right, on="id")

        assert r.right_duplicate_keys == 1
        assert r.left_keys_without_match == 1
