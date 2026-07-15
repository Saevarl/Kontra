# tests/postgres/test_query_source.py
"""
Integration tests for the Query source (a read-only SELECT usable as a source).

Requires the PostgreSQL container:
    cd tests/postgres && docker compose up -d

The queries use inline VALUES so the tests are self-contained (no table schema
dependency) while still exercising the real engine + materializer path.
"""

import polars as pl
import pytest

import kontra
from kontra import Query


@pytest.mark.integration
class TestQuerySourceCompare:
    def test_compare_query_vs_query(self, postgres_connection):
        conn = postgres_connection
        before = Query(
            "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(id, val)", source=conn
        )
        after = Query(
            "SELECT * FROM (VALUES (2,'b'),(3,'X'),(4,'d')) AS t(id, val)", source=conn
        )

        r = kontra.compare(before, after, key="id")

        assert r.before_rows == 3 and r.after_rows == 3
        assert r.dropped == 1  # id 1 gone
        assert r.added == 1  # id 4 new
        assert r.preserved == 2  # ids 2, 3
        assert r.changed_rows == 1  # id 3: 'c' -> 'X'
        # Both query sources share one engine -> set-based SQL (Mode A).
        assert r.execution_tier == "sql"
        # Samples default to zero (no row-level values leak).
        assert r.samples_dropped_keys == []
        assert r.samples_changed_rows == []

    def test_compare_query_vs_dataframe(self, postgres_connection):
        conn = postgres_connection
        q = Query("SELECT * FROM (VALUES (1,10),(2,20)) AS t(id, amt)", source=conn)
        df = pl.DataFrame({"id": [1, 2], "amt": [10, 999]})

        r = kontra.compare(q, df, key="id", sample_limit=5)

        assert r.before_rows == 2 and r.after_rows == 2
        assert r.changed_rows == 1  # id 2: 20 -> 999
        assert r.samples_changed_rows  # opted in explicitly

    def test_compare_query_asymmetric_key(self, postgres_connection):
        conn = postgres_connection
        # right side carries a column named like the left key -> alias path still holds
        before = Query(
            "SELECT * FROM (VALUES (1),(2),(3)) AS t(organization_id)", source=conn
        )
        after = Query(
            "SELECT * FROM (VALUES (1,9),(2,9)) AS t(__id, organization_id)", source=conn
        )

        r = kontra.compare(
            before, after, before_key="organization_id", after_key="__id"
        )

        assert r.preserved == 2 and r.dropped == 1


@pytest.mark.integration
class TestQuerySourceFromUri:
    def test_compare_query_uri_source(self, postgres_uri):
        # source is a DB URI (its table is ignored; only the connection is used)
        before = Query(
            "SELECT * FROM (VALUES (1,10),(2,20),(3,30)) AS t(id, amt)", source=postgres_uri
        )
        after = Query(
            "SELECT * FROM (VALUES (2,20),(3,99),(4,40)) AS t(id, amt)", source=postgres_uri
        )

        r = kontra.compare(before, after, key="id")

        assert r.dropped == 1 and r.added == 1 and r.changed_rows == 1
        # Same engine on both sides -> set-based SQL (Mode A).
        assert r.execution_tier == "sql"


@pytest.mark.integration
class TestQuerySourceRelationship:
    def test_relationship_query_vs_query(self, postgres_connection):
        conn = postgres_connection
        left = Query("SELECT * FROM (VALUES (1),(2),(3)) AS t(id)", source=conn)
        right = Query("SELECT * FROM (VALUES (1),(1),(2)) AS t(id)", source=conn)

        r = kontra.profile_relationship(left, right, on="id")

        assert r.right_duplicate_keys == 1  # id 1 appears twice
        assert r.left_keys_without_match == 1  # id 3 has no match
        assert r.samples_right_duplicates == []  # aggregate-only by default


@pytest.mark.integration
class TestQuerySourceGuards:
    def test_rejects_non_select(self, postgres_connection):
        with pytest.raises(ValueError):
            Query("DELETE FROM users", source=postgres_connection)

    def test_rejects_multi_statement(self, postgres_connection):
        with pytest.raises(ValueError):
            Query("SELECT 1; DROP TABLE users", source=postgres_connection)

    def test_requires_source(self):
        with pytest.raises(ValueError):
            Query("SELECT 1")
