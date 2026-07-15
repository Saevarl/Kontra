# tests/sqlserver/test_query_source.py
"""Query source (read-only SELECT) integration tests for SQL Server."""

import pytest

import kontra
from kontra import Query


@pytest.mark.integration
class TestQuerySourceSqlServer:
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
        assert r.execution_tier == "polars"
        assert r.samples_dropped_keys == []  # aggregate-only by default

    def test_relationship_query_vs_query(self, sqlserver_uri):
        left = Query("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3", source=sqlserver_uri)
        right = Query("SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 2", source=sqlserver_uri)

        r = kontra.profile_relationship(left, right, on="id")

        assert r.right_duplicate_keys == 1
        assert r.left_keys_without_match == 1
