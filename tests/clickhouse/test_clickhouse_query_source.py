# tests/clickhouse/test_query_source.py
"""Query source (read-only SELECT) integration tests for ClickHouse."""

import pytest

import kontra
from kontra import Query


@pytest.mark.integration
class TestQuerySourceClickHouse:
    def test_compare_query_vs_query(self, clickhouse_uri):
        before = Query(
            "SELECT toInt32(1) AS id, toInt32(100) AS amt "
            "UNION ALL SELECT 2, 200 UNION ALL SELECT 3, 300",
            source=clickhouse_uri,
        )
        after = Query(
            "SELECT toInt32(2) AS id, toInt32(200) AS amt "
            "UNION ALL SELECT 3, 999 UNION ALL SELECT 4, 400",
            source=clickhouse_uri,
        )

        r = kontra.compare(before, after, key="id")

        assert r.before_rows == 3 and r.after_rows == 3
        assert r.dropped == 1  # id 1
        assert r.added == 1  # id 4
        assert r.preserved == 2  # ids 2, 3
        assert r.changed_rows == 1  # id 3: 300 -> 999
        assert r.execution_tier == "polars"
        assert r.samples_dropped_keys == []
