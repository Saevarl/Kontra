# tests/trino/test_trino_profile.py
"""
Live Trino profiling and Query source tests.

Profiles a dedicated table in the throwaway container and checks the result
against the same data profiled through the DuckDB backend (a Polars
DataFrame): row, null and distinct counts, numeric stats including exact
percentiles, string lengths and empty counts, and date ranges.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import polars as pl
import pytest

import kontra
from kontra import Query

pytestmark = [pytest.mark.integration, pytest.mark.pushdown]

BASE = "trino://kontra@localhost:8095/memory/kontra"
URI_PROFILE = f"{BASE}.profile_values"
URI_EMPTY = f"{BASE}.profile_empty"

_IDS = [1, 2, 3, 4, 5, 6, 7]
_AMOUNTS = ["10.50", "0.00", None, "-5.25", "99.99", "1.68", "1.68"]
_SCORES = [1.5, 2.0, None, 3.0, 0.5, 10.0, 2.0]
_NAMES = ["alpha", "", None, "beta", "alpha", "Ünïcødé", "gamma"]
_DAYS = ["2020-01-01", "2020-01-02", None, "2021-06-30", "2020-01-01", "2019-12-31", None]
_UUIDS = [str(uuid.UUID(int=i)) for i in range(1, 8)]


def _sql_value(value, kind: str) -> str:
    if value is None:
        return "NULL"
    if kind == "str":
        return "'" + value.replace("'", "''") + "'"
    if kind == "date":
        return f"DATE '{value}'"
    if kind == "uuid":
        return f"UUID '{value}'"
    return str(value)


def _run(trino_connect, *statements: str) -> None:
    with trino_connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


@pytest.fixture(scope="module")
def profile_uri(trino_connect) -> str:
    rows = []
    for i in range(len(_IDS)):
        doc = "NULL" if i == 2 else f"JSON '{{\"k\": {i}}}'"
        tags = "NULL" if i == 2 else f"MAP(ARRAY['k'], ARRAY[{i % 3}])"
        rows.append(
            "("
            + ", ".join(
                [
                    _sql_value(_IDS[i], "num"),
                    _sql_value(_AMOUNTS[i], "num"),
                    _sql_value(_SCORES[i], "num"),
                    _sql_value(_NAMES[i], "str"),
                    _sql_value(_DAYS[i], "date"),
                    _sql_value(_UUIDS[i], "uuid"),
                    doc,
                    tags,
                    "true" if i % 2 else "false",
                ]
            )
            + ")"
        )
    _run(
        trino_connect,
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        "DROP TABLE IF EXISTS memory.kontra.profile_values",
        "CREATE TABLE memory.kontra.profile_values (id bigint NOT NULL, "
        "amount decimal(10, 2), score double, name varchar, day date, uid uuid, "
        "doc json, tags map(varchar, integer), active boolean)",
        "INSERT INTO memory.kontra.profile_values VALUES " + ", ".join(rows),
        "DROP TABLE IF EXISTS memory.kontra.profile_empty",
        "CREATE TABLE memory.kontra.profile_empty (id bigint, amount decimal(10, 2))",
    )
    yield URI_PROFILE
    _run(
        trino_connect,
        "DROP TABLE IF EXISTS memory.kontra.profile_values",
        "DROP TABLE IF EXISTS memory.kontra.profile_empty",
    )


def _reference_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": _IDS,
            "amount": [Decimal(a) if a is not None else None for a in _AMOUNTS],
            "score": _SCORES,
            "name": _NAMES,
            "day": [dt.date.fromisoformat(d) if d else None for d in _DAYS],
        },
        schema={
            "id": pl.Int64,
            "amount": pl.Decimal(10, 2),
            "score": pl.Float64,
            "name": pl.Utf8,
            "day": pl.Date,
        },
    )


def _percentile_cont(values: list[float], fraction: float) -> float:
    """Linear interpolation between closest ranks, as SQL PERCENTILE_CONT."""
    pos = fraction * (len(values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (pos - lo) * (values[hi] - values[lo])


def _by_name(profile):
    return {c.name: c for c in profile.columns}


@pytest.mark.parametrize("preset", ["scan", "interrogate"])
def test_profile_matches_duckdb_backend(profile_uri, preset):
    columns = ["id", "amount", "score", "name", "day"]
    trino = kontra.profile(profile_uri, preset=preset, columns=columns, save=False)
    duck = kontra.profile(_reference_frame(), preset=preset, save=False)

    assert trino.source_format == "trino"
    assert trino.row_count == duck.row_count == 7
    assert trino.row_count_estimated is False

    t, d = _by_name(trino), _by_name(duck)
    for name in columns:
        assert t[name].dtype == d[name].dtype, name
        assert t[name].null_count == d[name].null_count, name
        assert t[name].distinct_count == d[name].distinct_count, name
        assert t[name].null_count_estimated is False
        assert t[name].distinct_count_estimated is False

    for name in ("id", "amount", "score"):
        tn, dn = t[name].numeric, d[name].numeric
        for stat in ("min", "max", "mean", "median", "std"):
            assert getattr(tn, stat) == pytest.approx(getattr(dn, stat)), (name, stat)
        assert tn.percentiles.keys() == dn.percentiles.keys()
        # Against PERCENTILE_CONT itself: DuckDB rounds DECIMAL quantiles to
        # the column's scale (amount p75 is 8.295, DuckDB reports 8.29).
        values = sorted(float(v) for v in _reference_frame()[name].drop_nulls())
        for key in dn.percentiles:
            expected = _percentile_cont(values, int(key[1:]) / 100)
            assert tn.percentiles[key] == pytest.approx(expected), (name, key)

    ts, ds = t["name"].string, d["name"].string
    assert (ts.min_length, ts.max_length, ts.empty_count) == (
        ds.min_length,
        ds.max_length,
        ds.empty_count,
    )
    assert ts.avg_length == pytest.approx(ds.avg_length)
    assert t["day"].temporal.date_min == d["day"].temporal.date_min == "2019-12-31"
    assert t["day"].temporal.date_max == d["day"].temporal.date_max == "2021-06-30"

    top = {v.value: v.count for v in t["name"].top_values}
    assert top["alpha"] == 2


def test_char_profile_matches_materialized_values(trino_connect):
    # Trino compares CHAR(n) with padding, so a blank value equals ''. The
    # driver returns it as n spaces, so the profile must not count it empty.
    _run(
        trino_connect,
        "DROP TABLE IF EXISTS memory.kontra.profile_char",
        "CREATE TABLE memory.kontra.profile_char (c char(3))",
        "INSERT INTO memory.kontra.profile_char "
        "SELECT CAST(v AS char(3)) FROM (VALUES '', 'a', NULL, 'abc') AS t(v)",
    )
    try:
        with trino_connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT c FROM memory.kontra.profile_char")
            values = [r[0] for r in cur.fetchall()]
        assert sorted(v for v in values if v is not None) == ["   ", "a  ", "abc"]

        trino = kontra.profile(f"{BASE}.profile_char", preset="interrogate", save=False)
        duck = kontra.profile(pl.DataFrame({"c": values}), preset="interrogate", save=False)
        t, d = _by_name(trino)["c"], _by_name(duck)["c"]
        assert (t.null_count, t.distinct_count) == (d.null_count, d.distinct_count) == (1, 3)
        ts, ds = t.string, d.string
        assert (ts.min_length, ts.max_length, ts.empty_count) == (
            ds.min_length,
            ds.max_length,
            ds.empty_count,
        )
        assert ts.empty_count == 0
    finally:
        _run(trino_connect, "DROP TABLE IF EXISTS memory.kontra.profile_char")


def test_percentiles_are_exact(profile_uri):
    cols = _by_name(kontra.profile(profile_uri, preset="interrogate", save=False))
    # Non-null scores, sorted: 0.5, 1.5, 2.0, 2.0, 3.0, 10.0 (linear interpolation).
    score = cols["score"].numeric
    assert score.median == 2.0
    assert score.percentiles == {"p25": 1.625, "p75": 2.75, "p99": pytest.approx(9.65)}
    # Decimal mean is not rounded to the column's scale.
    assert cols["amount"].numeric.mean == pytest.approx(108.6 / 6)


def test_profile_other_types_and_presets(profile_uri):
    cols = _by_name(kontra.profile(profile_uri, preset="interrogate", save=False))
    assert cols["doc"].dtype == "string"
    assert (cols["doc"].null_count, cols["doc"].distinct_count) == (1, 6)
    assert cols["doc"].string.min_length == len('{"k":0}')
    assert cols["uid"].dtype == "string"
    assert cols["uid"].distinct_count == 7
    assert cols["uid"].string.min_length == 36
    assert cols["tags"].dtype == "unknown"
    assert (cols["tags"].null_count, cols["tags"].distinct_count) == (1, 3)
    assert cols["active"].dtype == "bool"
    assert cols["active"].distinct_count == 2

    scout = _by_name(kontra.profile(profile_uri, preset="scout", save=False))
    assert scout["amount"].null_count == 1 and scout["amount"].numeric is None

    sampled = kontra.profile(profile_uri, preset="scan", sample=3, save=False)
    assert sampled.sampled is True
    assert all(c.null_count_estimated for c in sampled.columns)


def test_profile_empty_table(profile_uri):
    cols = _by_name(kontra.profile(URI_EMPTY, preset="interrogate", save=False))
    assert cols["amount"].null_count == 0
    assert cols["amount"].numeric.median is None
    assert cols["amount"].numeric.percentiles == {}


def test_profile_missing_table(trino_container):
    missing = f"{BASE}.no_such_table"
    with pytest.raises(ValueError, match="Trino table not found"):
        kontra.profile(missing, save=False)


class TestQuerySource:
    def test_compare_query_vs_query(self, trino_container):
        source = f"{BASE}.unused"
        before = Query(
            "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(id, amt)",
            source=source,
        )
        after = Query(
            "SELECT * FROM (VALUES (2, 200), (3, 999), (4, 400)) AS t(id, amt)",
            source=source,
        )

        r = kontra.compare(before, after, key="id")

        assert r.before_rows == 3 and r.after_rows == 3
        assert (r.dropped, r.added, r.preserved, r.changed_rows) == (1, 1, 2, 1)
        assert r.execution_tier == "polars"
        assert r.samples_dropped_keys == []

    def test_query_on_caller_connection(self, trino_connect):
        conn = trino_connect()
        try:
            q = Query("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, amt)", source=conn)
            df = pl.DataFrame({"id": [1, 2], "amt": [10, 999]})
            r = kontra.compare(q, df, key="id")
        finally:
            conn.close()
        assert r.before_rows == 2 and r.changed_rows == 1
