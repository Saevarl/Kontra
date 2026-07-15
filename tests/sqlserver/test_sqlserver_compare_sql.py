# tests/sqlserver/test_sqlserver_compare_sql.py
"""Tier-equivalence suite for set-based compare (Mode A) on SQL Server.

Each fixture is compared twice: Polars (reference, from DataFrames) and set-based
SQL (from real SQL Server tables). Counts must be identical. The case-sensitive
text fixture specifically proves the binary-collation path (SQL Server's default
collation is case-insensitive; Polars is case-sensitive).
"""

import polars as pl
import pytest

import kontra

pytestmark = pytest.mark.integration

_BASE = "mssql://sa:Kontra_Test123!@localhost:1433/kontra_test/dbo."

_MS_TYPE = {
    pl.Int64: "bigint",
    pl.Int32: "int",
    pl.Float64: "float",
    pl.Float32: "real",
    pl.Utf8: "varchar(255)",
    pl.String: "varchar(255)",
    pl.Boolean: "bit",
    pl.Datetime: "datetime2",
    pl.Date: "date",
}


@pytest.fixture
def mssql_conn():
    import pymssql

    conn = pymssql.connect(
        server="localhost", port=1433, user="sa",
        password="Kontra_Test123!", database="kontra_test",
    )
    yield conn
    conn.close()


def _load(conn, table, df):
    cur = conn.cursor()
    cur.execute(f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DROP TABLE dbo.{table}")
    cols = ", ".join(f"[{n}] {_MS_TYPE.get(type(dt), 'varchar(255)')}" for n, dt in df.schema.items())
    cur.execute(f"CREATE TABLE dbo.{table} ({cols})")
    if df.height:
        ph = ", ".join(["%s"] * df.width)
        cur.executemany(f"INSERT INTO dbo.{table} VALUES ({ph})", df.rows())
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
        assert sql_r.modified_fraction[k] == pytest.approx(v)


_SAFE = [
    ("basic",
     pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}),
     pl.DataFrame({"id": [2, 3, 4], "val": ["b", "X", "d"]}), "id"),
    ("composite_int_key",
     pl.DataFrame({"r": [1, 1, 2], "c": [1, 2, 1], "v": [10, 20, 30]}),
     pl.DataFrame({"r": [1, 2, 2], "c": [1, 1, 2], "v": [10, 99, 40]}), ["r", "c"]),
    ("duplicate_keys_cross_multiply",
     pl.DataFrame({"id": [1, 1, 2], "v": [10, 11, 20]}),
     pl.DataFrame({"id": [1, 1, 1, 2], "v": [10, 11, 12, 20]}), "id"),
    ("no_overlap",
     pl.DataFrame({"id": [1, 2], "v": [1, 2]}),
     pl.DataFrame({"id": [3, 4], "v": [3, 4]}), "id"),
    ("null_values_in_nonkey",
     pl.DataFrame({"id": [1, 2, 3], "v": [None, 5, 7]}, schema={"id": pl.Int64, "v": pl.Int64}),
     pl.DataFrame({"id": [1, 2, 3], "v": [None, None, 8]}, schema={"id": pl.Int64, "v": pl.Int64}), "id"),
    ("case_sensitive_text_change",  # binary collation must see Alice != alice
     pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}),
     pl.DataFrame({"id": [1, 2], "name": ["alice", "Bob"]}), "id"),
    ("trailing_space_change",  # binary collation must see 'x' != 'x '
     pl.DataFrame({"id": [1], "name": ["x"]}),
     pl.DataFrame({"id": [1], "name": ["x "]}), "id"),
    ("duplicated_after_key_count",
     pl.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]}),
     pl.DataFrame({"id": [1, 1, 1, 2, 3], "v": [1, 1, 1, 2, 3]}), "id"),
    ("added_and_removed_columns",
     pl.DataFrame({"id": [1, 2], "keep": [1, 2], "gone": [9, 9]}),
     pl.DataFrame({"id": [1, 2], "keep": [1, 3], "fresh": [7, 7]}), "id"),
]


@pytest.mark.parametrize("name,before,after,key", _SAFE, ids=[c[0] for c in _SAFE])
def test_mode_a_matches_polars(mssql_conn, name, before, after, key):
    _load(mssql_conn, "eq_before", before)
    _load(mssql_conn, "eq_after", after)

    pol_r = kontra.compare(before, after, key=key)
    sql_r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key=key)

    assert sql_r.execution_tier == "sql", f"{name}: expected Mode A to fire"
    _assert_equivalent(sql_r, pol_r)


class TestFallsBackToPolars:
    def test_null_key_falls_back(self, mssql_conn):
        before = pl.DataFrame({"id": [1, None], "v": [1, 2]}, schema={"id": pl.Int64, "v": pl.Int64})
        after = pl.DataFrame({"id": [1, None], "v": [1, 3]}, schema={"id": pl.Int64, "v": pl.Int64})
        _load(mssql_conn, "eq_before", before)
        _load(mssql_conn, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id")
        assert r.execution_tier == "polars"

    def test_samples_requested_falls_back(self, mssql_conn):
        before = pl.DataFrame({"id": [1, 2], "v": [1, 2]})
        after = pl.DataFrame({"id": [1, 3], "v": [1, 2]})
        _load(mssql_conn, "eq_before", before)
        _load(mssql_conn, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="id", sample_limit=5)
        assert r.execution_tier == "polars"

    def test_char_key_falls_back(self, mssql_conn):
        # SQL Server char-key equality is deferred to Polars (collation/padding).
        before = pl.DataFrame({"k": ["US", "IS"], "v": [1, 2]})
        after = pl.DataFrame({"k": ["US", "IS"], "v": [1, 3]})
        _load(mssql_conn, "eq_before", before)
        _load(mssql_conn, "eq_after", after)
        r = kontra.compare(_BASE + "eq_before", _BASE + "eq_after", key="k")
        assert r.execution_tier == "polars"
