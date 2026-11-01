# tests/postgres/test_postgres_materializer_types.py
"""
The Postgres materializer types columns by their declared type when the values
can't: an empty table, an all-NULL column, a column NULL for its first 100 rows.

Before, an empty table loaded as all Utf8, an all-NULL column as Polars Null
(dtype rules failed, range raised NotImplementedError), and a NULL prefix
longer than Polars' 100-row inference window crashed the load (ComputeError).

Requires the PostgreSQL container:
    cd tests/postgres && docker compose up -d
"""

from __future__ import annotations

import uuid

import polars as pl
import pytest

import kontra
from kontra import Query, rules

_URI = "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.{}"
_DDL = "(i bigint, f double precision, s text, b boolean, t timestamp(6), d date)"
_VALUES = "(5, 1.5, 'x', true, '2024-01-02 03:04:05', '2024-01-02')"
_NULLS = "(NULL, NULL, NULL, NULL, NULL, NULL)"


def _rules():
    return [
        rules.dtype("i", "int64"),
        rules.dtype("f", "float64"),
        rules.dtype("s", "string"),
        rules.dtype("b", "bool"),
        rules.dtype("t", "datetime"),
        rules.dtype("d", "date"),
        rules.range("i", min=0),
    ]


@pytest.fixture
def make_table(postgres_connection):
    """Create a uniquely named table from DDL and rows; drop it afterwards."""
    conn = postgres_connection
    conn.autocommit = True
    created = []

    def make(ddl: str, rows: list[str]) -> str:
        name = f"kontra_mat_types_{uuid.uuid4().hex[:10]}"
        conn.execute(f"CREATE TABLE public.{name} {ddl}")
        if rows:
            conn.execute(f"INSERT INTO public.{name} VALUES {', '.join(rows)}")
        created.append(name)
        return name

    yield make
    for name in created:
        conn.execute(f"DROP TABLE IF EXISTS public.{name}")


# scenario -> (rows, range failures): every scenario's NULLs are range failures
_SCENARIOS = {
    "empty": ([], 0),
    "all_null": ([_NULLS, _NULLS], 2),
    "null_prefix": ([_NULLS] * 149 + [_VALUES], 149),
}


_DECLARED = {
    "i": pl.Int64,
    "f": pl.Float64,
    "s": pl.Utf8,
    "b": pl.Boolean,
    "t": pl.Datetime("us"),
    "d": pl.Date,
}


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["uri", "byoc"])
@pytest.mark.parametrize("scenario", list(_SCENARIOS))
def test_validate_without_pushdown(make_table, postgres_connection, kind, scenario):
    rows, range_failures = _SCENARIOS[scenario]
    name = make_table(_DDL, rows)
    if kind == "uri":
        data, table = _URI.format(name), None
    else:
        data, table = postgres_connection, f"public.{name}"

    result = kontra.validate(
        data, table=table, rules=_rules(), save=False, preplan="off", pushdown="off"
    )

    by_id = {r.rule_id: r for r in result.rules}
    for col in _DECLARED:
        r = by_id[f"COL:{col}:dtype"]
        assert r.passed, f"{col}: {r.message}"
    rng = by_id["COL:i:range"]
    assert rng.failed_count == range_failures
    assert rng.passed is (range_failures == 0)
    assert "Rule execution failed" not in rng.message


@pytest.mark.integration
@pytest.mark.parametrize("scenario", list(_SCENARIOS))
def test_query_source_frame(make_table, postgres_connection, scenario):
    """A Query source (as probes load it) is typed by its result columns too."""
    from kontra.probes.utils import load_data

    rows, _ = _SCENARIOS[scenario]
    name = make_table(_DDL, rows)
    q = Query(f"SELECT i, f, s, b, t, d FROM public.{name}", source=postgres_connection)

    df = load_data(q)

    assert dict(df.schema) == _DECLARED
    assert df.height == len(rows)


# column -> (declared type, a value, dtype a populated column loads as)
_TYPES = {
    "c_int2": ("smallint", "1", pl.Int64),
    "c_int4": ("integer", "1", pl.Int64),
    "c_int8": ("bigint", "1", pl.Int64),
    "c_f4": ("real", "1.5", pl.Float64),
    "c_f8": ("double precision", "1.5", pl.Float64),
    "c_num": ("numeric(10,2)", "1.25", pl.Decimal(38, 2)),
    "c_num_neg": ("numeric(5,-2)", "1200", pl.Decimal(38, 0)),
    "c_text": ("text", "'a'", pl.Utf8),
    "c_vc": ("varchar(20)", "'a'", pl.Utf8),
    "c_char": ("char(5)", "'ab'", pl.Utf8),
    "c_bool": ("boolean", "true", pl.Boolean),
    "c_date": ("date", "'2024-01-02'", pl.Date),
    "c_ts": ("timestamp", "'2024-01-02 03:04:05.123456'", pl.Datetime("us")),
    "c_tstz": ("timestamptz", "'2024-01-02 03:04:05+02'", pl.Datetime("us", "UTC")),
    "c_time": ("time", "'03:04:05'", pl.Time),
    "c_interval": ("interval", "'1 day'", pl.Duration("us")),
    "c_bytea": ("bytea", "'\\x0102'", pl.Binary),
}


@pytest.mark.integration
def test_empty_and_all_null_load_as_populated(make_table):
    """Same dtypes for a populated, an all-NULL and an empty table, per type."""
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.materializers.postgres import PostgresMaterializer

    ddl = "(" + ", ".join(f"{c} {t}" for c, (t, _, _) in _TYPES.items()) + ")"
    values = "(" + ", ".join(v for _, v, _ in _TYPES.values()) + ")"
    nulls = "(" + ", ".join("NULL" for _ in _TYPES) + ")"
    expected = {c: dtype for c, (_, _, dtype) in _TYPES.items()}

    for rows in ([values], [nulls], []):
        name = make_table(ddl, rows)
        df = PostgresMaterializer(DatasetHandle.from_uri(_URI.format(name))).to_polars(None)
        assert dict(df.schema) == expected, rows
        assert df.height == len(rows)

    # A populated row's values are unchanged, char padding included.
    name = make_table(ddl, [values, nulls])
    df = PostgresMaterializer(DatasetHandle.from_uri(_URI.format(name))).to_polars(None)
    assert df["c_char"][0] == "ab   "
    assert str(df["c_num"][0]) == "1.25"
    assert df["c_tstz"][0].isoformat() == "2024-01-02T01:04:05+00:00"


@pytest.mark.integration
def test_timestamptz_follows_session_time_zone(make_table, postgres_container):
    """An all-NULL timestamptz takes the zone a populated one is inferred in."""
    import psycopg

    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.materializers.postgres import PostgresMaterializer

    populated = make_table("(t timestamptz)", ["('2024-01-02 03:04:05+00')"])
    all_null = make_table("(t timestamptz)", ["(NULL)"])
    with psycopg.connect(
        host="localhost",
        port=5433,
        user="kontra",
        password="kontra_test",
        dbname="kontra_test",
        autocommit=True,
    ) as conn:
        conn.execute("SET TIME ZONE 'America/New_York'")
        dtypes = [
            PostgresMaterializer(DatasetHandle.from_connection(conn, f"public.{n}"))
            .to_polars(None)
            .schema["t"]
            for n in (populated, all_null)
        ]
    assert dtypes == [pl.Datetime("us", "America/New_York")] * 2
