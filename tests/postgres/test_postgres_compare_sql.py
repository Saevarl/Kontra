# tests/postgres/test_postgres_compare_sql.py
"""Tier-equivalence suite for set-based compare (Mode A) on PostgreSQL.

For each fixture we compute the CompareResult twice: once in Polars (the
reference) from DataFrames, once via set-based SQL from real PostgreSQL tables.
Every count must be identical. Fixtures outside Mode A's safe envelope must fall
back to Polars (execution_tier == "polars").
"""

import polars as pl
import pytest

import kontra

pytestmark = pytest.mark.integration

_BASE = "postgres://kontra:kontra_test@localhost:5433/kontra_test/public."

_PG_TYPE = {
    pl.Int64: "bigint",
    pl.Int32: "integer",
    pl.Float64: "double precision",
    pl.Float32: "real",
    pl.Utf8: "text",
    pl.String: "text",
    pl.Boolean: "boolean",
    pl.Datetime: "timestamp",
    pl.Date: "date",
}


def _load(conn, table, df):
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cols = ", ".join(f'"{n}" {_PG_TYPE.get(type(dt), "text")}' for n, dt in df.schema.items())
    cur.execute(f"CREATE TABLE {table} ({cols})")
    if df.height:
        ph = ", ".join(["%s"] * df.width)
        cur.executemany(f"INSERT INTO {table} VALUES ({ph})", df.rows())
    conn.commit()


_COUNT_FIELDS = [
    "before_rows", "after_rows", "row_delta", "unique_before", "unique_after",
    "preserved", "dropped", "added", "duplicated_after", "unchanged_rows",
    "changed_rows", "columns_added", "columns_removed", "columns_modified",
]


def _assert_equivalent(sql_r, pol_r):
    for f in _COUNT_FIELDS:
        assert getattr(sql_r, f) == getattr(pol_r, f), (
            f"{f}: sql={getattr(sql_r, f)!r} polars={getattr(pol_r, f)!r}"
        )
    assert sql_r.modified_fraction.keys() == pol_r.modified_fraction.keys()
    for k, v in pol_r.modified_fraction.items():
        assert sql_r.modified_fraction[k] == pytest.approx(v), f"modified_fraction[{k}]"
    assert sql_r.nullability_delta.keys() == pol_r.nullability_delta.keys()
    for k, v in pol_r.nullability_delta.items():
        for side in ("before", "after"):
            assert sql_r.nullability_delta[k][side] == pytest.approx(v[side]) or (
                sql_r.nullability_delta[k][side] == v[side]
            ), f"nullability_delta[{k}][{side}]"
    if pol_r.before_rows == 0:
        assert sql_r.row_ratio == pol_r.row_ratio  # both inf/1.0 by same rule
    else:
        assert sql_r.row_ratio == pytest.approx(pol_r.row_ratio)


# (name, before, after, key) — Mode A should fire and equal Polars.
_SAFE = [
    (
        "basic",
        pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}),
        pl.DataFrame({"id": [2, 3, 4], "val": ["b", "X", "d"]}),
        "id",
    ),
    (
        "composite_key",
        pl.DataFrame({"c": ["US", "US", "IS"], "a": [1, 2, 1], "v": [10, 20, 30]}),
        pl.DataFrame({"c": ["US", "IS", "IS"], "a": [1, 1, 2], "v": [10, 99, 40]}),
        ["c", "a"],
    ),
    (
        "duplicate_keys_cross_multiply",  # id=1: 2 before x 3 after -> M contribution 6
        pl.DataFrame({"id": [1, 1, 2], "v": [10, 11, 20]}),
        pl.DataFrame({"id": [1, 1, 1, 2], "v": [10, 11, 12, 20]}),
        "id",
    ),
    (
        "no_overlap",
        pl.DataFrame({"id": [1, 2], "v": [1, 2]}),
        pl.DataFrame({"id": [3, 4], "v": [3, 4]}),
        "id",
    ),
    (
        "no_common_nonkey_cols",
        pl.DataFrame({"id": [1, 2, 3], "x": [1, 2, 3]}),
        pl.DataFrame({"id": [2, 3, 4], "y": [2, 3, 4]}),
        "id",
    ),
    (
        "null_values_in_nonkey",  # NULL/NULL same, NULL/value changed, value/value
        pl.DataFrame({"id": [1, 2, 3], "v": [None, 5, 7]}, schema={"id": pl.Int64, "v": pl.Int64}),
        pl.DataFrame({"id": [1, 2, 3], "v": [None, None, 8]}, schema={"id": pl.Int64, "v": pl.Int64}),
        "id",
    ),
    (
        "case_sensitive_text_change",  # postgres default collation is case-sensitive
        pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}),
        pl.DataFrame({"id": [1, 2], "name": ["alice", "Bob"]}),
        "id",
    ),
    (
        "duplicated_after_key_count",
        pl.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]}),
        pl.DataFrame({"id": [1, 1, 1, 2, 3], "v": [1, 1, 1, 2, 3]}),
        "id",
    ),
    (
        "added_and_removed_columns",
        pl.DataFrame({"id": [1, 2], "keep": [1, 2], "gone": [9, 9]}),
        pl.DataFrame({"id": [1, 2], "keep": [1, 3], "fresh": [7, 7]}),
        "id",
    ),
]


@pytest.mark.parametrize("name,before,after,key", _SAFE, ids=[c[0] for c in _SAFE])
def test_mode_a_matches_polars(postgres_connection, name, before, after, key):
    _load(postgres_connection, "eq_before", before)
    _load(postgres_connection, "eq_after", after)

    pol_r = kontra.compare(before, after, key=key)
    sql_r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key=key)

    assert sql_r.execution_tier == "sql", f"{name}: expected Mode A to fire"
    assert pol_r.execution_tier == "polars"
    _assert_equivalent(sql_r, pol_r)


def test_empty_after_falls_or_matches(postgres_connection):
    before = pl.DataFrame({"id": [1, 2], "v": [1, 2]})
    after = pl.DataFrame({"id": [], "v": []}, schema={"id": pl.Int64, "v": pl.Int64})
    _load(postgres_connection, "eq_before", before)
    _load(postgres_connection, "eq_after", after)

    pol_r = kontra.compare(before, after, key="id")
    sql_r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id")
    _assert_equivalent(sql_r, pol_r)


class TestFallsBackToPolars:
    def test_null_key_falls_back(self, postgres_connection):
        before = pl.DataFrame({"id": [1, None], "v": [1, 2]}, schema={"id": pl.Int64, "v": pl.Int64})
        after = pl.DataFrame({"id": [1, None], "v": [1, 3]}, schema={"id": pl.Int64, "v": pl.Int64})
        _load(postgres_connection, "eq_before", before)
        _load(postgres_connection, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id")
        assert r.execution_tier == "polars"  # NULL keys -> fallback

    def test_cross_type_column_falls_back(self, postgres_connection):
        before = pl.DataFrame({"id": [1, 2], "code": ["01", "02"]})  # text
        after = pl.DataFrame({"id": [1, 2], "code": [1, 2]})  # int
        _load(postgres_connection, "eq_before", before)
        _load(postgres_connection, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id")
        assert r.execution_tier == "polars"  # cross-type common column -> fallback

    def test_samples_requested_falls_back(self, postgres_connection):
        before = pl.DataFrame({"id": [1, 2], "v": [1, 2]})
        after = pl.DataFrame({"id": [1, 3], "v": [1, 2]})
        _load(postgres_connection, "eq_before", before)
        _load(postgres_connection, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id", sample_limit=5)
        assert r.execution_tier == "polars"  # samples come from Polars in v1
        assert r.samples_dropped_keys  # and they are populated
