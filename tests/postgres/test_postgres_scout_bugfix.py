"""Regression tests (live PostgreSQL) for two Scout profiler bugs:

Bug 8: constant (distinct==1) columns must surface their single value.
Bug 7: estimated distinct on identifier columns must not imply fake duplicates.

Requires the PostgreSQL container (see tests/postgres/conftest.py).
"""

from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

from kontra.scout.profiler import ScoutProfiler

_TABLE = "scout_bugfix_regression"


@pytest.fixture
def constant_id_table(postgres_container):
    """Create a table with a constant column and a truly-unique id whose
    pg_stats n_distinct is forced to under-count (simulating stale/imperfect
    statistics). Dropped after the test."""
    dsn = dict(
        host=postgres_container["host"], port=postgres_container["port"],
        user=postgres_container["user"], password=postgres_container["password"],
        dbname=postgres_container["database"],
    )
    with psycopg.connect(**dsn) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{_TABLE}")
        cur.execute(
            f"CREATE TABLE public.{_TABLE} "
            f"(id bigint PRIMARY KEY, status text NOT NULL, amount double precision)"
        )
        cur.executemany(
            f"INSERT INTO public.{_TABLE} VALUES (%s,%s,%s)",
            [(i, "ACTIVE", float(i % 97)) for i in range(1, 501)],
        )
        # Force pg_stats to under-report the unique id (would be uniq_ratio 0.8).
        cur.execute(
            f"ALTER TABLE public.{_TABLE} ALTER COLUMN id SET (n_distinct = 400)"
        )
        cur.execute(f"ANALYZE public.{_TABLE}")
        conn.commit()

    uri = (
        f"postgres://{postgres_container['user']}:{postgres_container['password']}"
        f"@{postgres_container['host']}:{postgres_container['port']}"
        f"/{postgres_container['database']}/public.{_TABLE}"
    )
    yield uri

    with psycopg.connect(**dsn) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{_TABLE}")
        conn.commit()


@pytest.mark.integration
class TestPostgresScoutBugfix:
    def test_bug8_constant_value_surfaced_scan(self, constant_id_table):
        profile = ScoutProfiler(constant_id_table, preset="scan").profile()
        col = profile.get_column("status")
        assert col.distinct_count == 1
        assert col.top_values, "constant column must surface top_values"
        assert col.top_values[0].value == "ACTIVE"
        assert col.values == ["ACTIVE"]

    def test_bug8_constant_value_surfaced_scout(self, constant_id_table):
        # Metadata-only preset: value comes from pg_stats MCV, synthesized free.
        profile = ScoutProfiler(constant_id_table, preset="scout").profile()
        col = profile.get_column("status")
        assert col.distinct_count == 1
        assert col.top_values, "constant column must surface top_values (scout)"
        assert col.top_values[0].value == "ACTIVE"

    def test_bug7_identifier_no_fake_duplicates(self, constant_id_table):
        # pg_stats says 400 distinct, but the id is truly unique (500 distinct).
        profile = ScoutProfiler(constant_id_table, preset="scan").profile()
        idc = profile.get_column("id")
        assert idc.distinct_count == 500
        assert idc.uniqueness_ratio == pytest.approx(1.0)
        assert idc.distinct_count_estimated is False

    def test_bug7_scout_keeps_estimate_flagged(self, constant_id_table):
        # scout is scan-free: the estimate must remain, flagged as estimated.
        profile = ScoutProfiler(constant_id_table, preset="scout").profile()
        idc = profile.get_column("id")
        assert idc.distinct_count_estimated is True
