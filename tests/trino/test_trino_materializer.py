# tests/trino/test_trino_materializer.py
"""
The Trino materializer's frame, decoded column by column from raw values,
equals the frame built from the client's Python objects, for every Trino type.

The reference is the previous decoder: a default cursor's ``fetchall`` and a
row-wise DataFrame cast to the declared types. Each table is read in one
chunk and in chunks smaller than the table, so values of one column arrive
both together and apart.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest
import trino
from polars.testing import assert_frame_equal

from kontra.connectors.handle import DatasetHandle
from kontra.engine.materializers.trino import TrinoMaterializer

pytestmark = pytest.mark.integration

# (type, values): each table also gets a NULL row. Edge values sit where a
# fast decode could go wrong: rounding, extremes, NaN and the infinities,
# padding, empty values and zones other than UTC.
_COMMON = {
    "tinyint": ["TINYINT '-128'", "TINYINT '127'"],
    "smallint": ["SMALLINT '32767'"],
    "integer": ["-2147483648", "7"],
    "bigint": ["BIGINT '9223372036854775807'", "BIGINT '-9223372036854775808'"],
    "boolean": ["TRUE", "FALSE"],
    "real": ["REAL '0.1'", "CAST(nan() AS real)", "REAL '3.4028235e38'", "REAL '1.4e-45'"],
    "double": [
        "nan()",
        "infinity()",
        "-infinity()",
        "DOUBLE '-0.0'",
        "DOUBLE '1.7976931348623157e308'",
        "DOUBLE '4.9e-324'",
        "DOUBLE '0.1'",
        "DOUBLE '3'",
    ],
    "decimal(38,10)": [
        "DECIMAL '9999999999999999999999999999.9999999999'",
        "DECIMAL '-0.0000000001'",
        "DECIMAL '0'",
    ],
    "decimal(38,0)": ["DECIMAL '-99999999999999999999999999999999999999'"],
    "decimal(5,2)": ["DECIMAL '-123.45'", "DECIMAL '0.10'"],
    "varchar": ["'Ünï ' || chr(10)", "''", "'NaN'", "'2026-01-01'"],
    "varchar(30)": ["'x'"],
    "date": ["DATE '2026-01-01'", "DATE '0001-01-01'", "DATE '9999-12-31'"],
    "timestamp(6)": [
        "TIMESTAMP '2026-01-01 00:00:00.000001'",
        "TIMESTAMP '0001-01-01 00:00:00'",
        "TIMESTAMP '9999-12-31 23:59:59.999999'",
    ],
    "timestamp(6) with time zone": [
        "TIMESTAMP '2026-01-01 00:00:00.123456 UTC'",
        "TIMESTAMP '2026-06-01 12:00:00 UTC'",
    ],
    "time(6)": ["TIME '23:59:59.999999'", "TIME '00:00:00'"],
    "varbinary": ["X''", "X'00FF'"],
    "uuid": ["UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"],
    "array(integer)": ["ARRAY[1, NULL]", "ARRAY[]"],
    "map(varchar, double)": ["MAP(ARRAY['a'], ARRAY[nan()])"],
}
# Types the Iceberg connector doesn't store, and zones other than UTC (one per
# column: a column holding two zones has its own test).
_MEMORY_ONLY = {
    "char(3)": ["CAST('a' AS char(3))", "CAST('' AS char(3))"],
    "json": ["JSON '{\"a\": [1, 2.5, null]}'", "JSON 'null'"],
    "timestamp(0)": ["TIMESTAMP '2026-01-01 00:00:00'"],
    "timestamp(1)": ["TIMESTAMP '2026-01-01 00:00:00.5'"],
    "timestamp(3)": ["TIMESTAMP '2026-01-01 00:00:00.123'", "TIMESTAMP '9999-12-31 23:59:59.999'"],
    # Finer than microseconds: the client rounds half to even.
    "timestamp(9)": [
        "TIMESTAMP '2026-01-01 23:59:59.9999995'",
        "TIMESTAMP '2026-01-01 00:00:00.0000005'",
        "TIMESTAMP '2026-01-01 00:00:00.0000015'",
    ],
    "timestamp(12)": ["TIMESTAMP '2026-01-01 00:00:00.123456789012'"],
    "timestamp(0) with time zone": ["TIMESTAMP '2026-03-08 02:30:00 +05:30'"],
    "timestamp(3) with time zone": ["TIMESTAMP '2026-01-01 00:00:00.5 America/Los_Angeles'"],
    "timestamp(9) with time zone": ["TIMESTAMP '2026-01-01 00:00:00.123456789 UTC'"],
    "time(0)": ["TIME '23:59:59'"],
    "time(3)": ["TIME '01:02:03.004'"],
    "time(9)": ["TIME '23:59:59.9999995'"],
    "time(3) with time zone": ["TIME '01:02:03.004 +05:30'"],
    "interval year to month": ["INTERVAL '3' MONTH"],
    "interval day to second": ["INTERVAL '2' DAY"],
    "ipaddress": ["IPADDRESS '10.0.0.1'"],
    "array(decimal(5,2))": ["ARRAY[DECIMAL '1.10']"],
}
# Decoded straight from raw values; every other column takes the client's mapper.
_FAST = {
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "boolean",
    "real",
    "double",
    "decimal(38,10)",
    "decimal(38,0)",
    "decimal(5,2)",
    "varchar",
    "varchar(30)",
    "char(3)",
    "json",
    "date",
    "timestamp(0)",
    "timestamp(1)",
    "timestamp(3)",
    "timestamp(6)",
    "timestamp(0) with time zone",
    "timestamp(3) with time zone",
    "timestamp(6) with time zone",
    "time(0)",
    "time(3)",
    "time(6)",
    "varbinary",
}


def _connect(**kwargs):
    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", **kwargs)


def _run(*statements: str) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


def _cases(catalog: str) -> list[tuple[str, str, list[str]]]:
    types = dict(_COMMON)
    if catalog == "memory":
        types.update(_MEMORY_ONLY)
    return [
        (f"c{i}", data_type, values) for i, (data_type, values) in enumerate(sorted(types.items()))
    ]


@pytest.fixture(scope="module", params=["memory", "iceberg"])
def typed(request, trino_container):
    """One table per catalog: an id, then one column per type, each value in its own row."""
    catalog = request.param
    cases = _cases(catalog)
    table = f"{catalog}.kontra.frames"
    depth = max(len(values) for _, _, values in cases) + 1  # one NULL row at least
    rows = []
    for r in range(depth):
        cells = [str(r)]
        for _, _, values in cases:
            cells.append(values[r] if r < len(values) else "NULL")
        rows.append(f"({', '.join(cells)})")
    ddl = ", ".join(f"{name} {data_type}" for name, data_type, _ in cases)
    _run(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.kontra",
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id integer, {ddl})",
        f"INSERT INTO {table} VALUES {', '.join(rows)}",
    )
    yield catalog, table, cases
    _run(f"DROP TABLE IF EXISTS {table}")


def _reference(conn, table: str, names: list[str]) -> pl.DataFrame:
    """The previous decoder: a default cursor's Python objects, row-wise."""
    cur = conn.cursor(legacy_primitive_types=False)
    cur.execute(f"SELECT {', '.join(names)} FROM {table}")
    return TrinoMaterializer._rows_frame(cur.fetchall(), list(cur.description), {})


def _assert_same(expected: pl.DataFrame, actual: pl.DataFrame, label) -> None:
    expected, actual = expected.sort("id"), actual.sort("id")
    assert expected.schema == actual.schema, label
    for name, dtype in expected.schema.items():
        if dtype == pl.Object:
            # Object columns compare by identity in assert_frame_equal.
            assert expected[name].to_list() == actual[name].to_list(), (label, name)
        else:
            assert_frame_equal(expected.select(name), actual.select(name), check_exact=True)


def _materialize(conn, table: str, names: list[str], chunk_rows: int, monkeypatch):
    monkeypatch.setattr(TrinoMaterializer, "chunk_rows", chunk_rows)
    monkeypatch.setenv("KONTRA_IO_DEBUG", "1")
    mat = TrinoMaterializer(DatasetHandle.from_connection(conn, table))
    frame = mat.to_polars(names)
    # Never the row-wise path, which would compare the reference to itself.
    assert mat.io_debug()["decode"] == "columns"
    return frame


@pytest.mark.parametrize("chunk_rows", [100_000, 2, 1])
def test_frame_equals_the_python_object_frame(typed, chunk_rows, monkeypatch):
    """Every type, NULLs and edge values: the same frame, in one chunk or several."""
    catalog, table, cases = typed
    names = ["id"] + [name for name, _, _ in cases]
    with _connect() as conn:
        expected = _reference(conn, table, names)
        actual = _materialize(conn, table, names, chunk_rows, monkeypatch)
    for name, data_type, _ in cases:
        _assert_same(
            expected.select("id", name),
            actual.select("id", name),
            (catalog, data_type, chunk_rows),
        )


def test_fast_decode_covers_the_mapped_types(typed):
    """The common types skip the client's Python objects; the rest use its mapper."""
    from kontra.engine.materializers.trino_decode import _fast_decoder

    _, table, cases = typed
    with _connect() as conn:
        cur = conn.cursor(legacy_primitive_types=True)
        cur.execute(f"SELECT {', '.join(name for name, _, _ in cases)} FROM {table} LIMIT 0")
        cur.fetchall()
        described = {d[0]: d[1] for d in cur.description}
    from kontra.connectors.trino_types import polars_dtype

    for name, data_type, _ in cases:
        cursor_type = described[name]
        fast = _fast_decoder(cursor_type, polars_dtype(cursor_type)) is not None
        assert fast == (data_type in _FAST), (data_type, cursor_type)


def test_timestamps_in_several_zones_load_as_their_utc_instants(trino_container, monkeypatch):
    """The intended difference: the previous decoder raised on a column holding two zones.

    Read in chunks, whether two zones met in one chunk would decide whether the
    fetch raised. Every chunking now gives the same instants, in UTC.
    """
    table = "memory.kontra.zones"
    _run(
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id integer, t timestamp(3) with time zone)",
        f"INSERT INTO {table} VALUES (0, TIMESTAMP '2026-01-01 00:00:00.123 UTC'), "
        "(1, TIMESTAMP '2026-01-01 00:00:00.5 America/Los_Angeles'), "
        "(2, TIMESTAMP '2026-03-08 02:30:00 +05:30'), (3, NULL)",
    )
    utc = dt.timezone.utc
    expected = [
        dt.datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=utc),
        dt.datetime(2026, 1, 1, 8, 0, 0, 500000, tzinfo=utc),
        dt.datetime(2026, 3, 7, 21, 0, 0, tzinfo=utc),
        None,
    ]
    try:
        with _connect() as conn:
            with pytest.raises(Exception, match="supertype"):
                _reference(conn, table, ["id", "t"])
            for chunk_rows in (100_000, 2, 1):
                frame = _materialize(conn, table, ["id", "t"], chunk_rows, monkeypatch).sort("id")
                assert frame.schema["t"] == pl.Datetime("us", "UTC")
                assert frame["t"].to_list() == expected, chunk_rows
    finally:
        _run(f"DROP TABLE IF EXISTS {table}")


def test_a_callers_raw_values_connection_gets_the_same_frame(typed, monkeypatch):
    """A caller's connection set to raw values gives the same frame, and keeps its setting."""
    _, table, cases = typed
    names = ["id"] + [name for name, _, _ in cases]
    with _connect() as conn, _connect(legacy_primitive_types=True) as raw:
        expected = _reference(conn, table, names)
        actual = _materialize(raw, table, names, 100_000, monkeypatch)
        assert raw.legacy_primitive_types is True
    _assert_same(expected, actual, "legacy_primitive_types=True connection")


@pytest.mark.parametrize("catalog", ["memory", "iceberg"])
def test_row_columns_raise_as_before(catalog, trino_container, monkeypatch):
    """A row column fails to load with the same error as before, in any chunking.

    Polars can't build a frame from the client's row tuples (``NamedRowTuple``
    answers every unknown attribute with None). That predates this decoder and
    is kept, not fixed, here.
    """
    table = f"{catalog}.kontra.rows"
    _run(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.kontra",
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id integer, r row(a integer, b varchar))",
        f"INSERT INTO {table} VALUES (0, CAST(ROW(1, 'x') AS row(a integer, b varchar)))",
    )
    try:
        with _connect() as conn:
            with pytest.raises(TypeError) as before:
                _reference(conn, table, ["id", "r"])
            for chunk_rows in (100_000, 1):
                with pytest.raises(TypeError) as after:
                    _materialize(conn, table, ["id", "r"], chunk_rows, monkeypatch)
                assert str(after.value) == str(before.value)
    finally:
        _run(f"DROP TABLE IF EXISTS {table}")


def test_values_python_cant_hold_raise_as_before(trino_container, monkeypatch):
    """A date before year 1 fails in the client's mapper, with the client's message."""
    from trino.exceptions import TrinoDataError

    table = "memory.kontra.old_dates"
    _run(
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id integer, d date)",
        f"INSERT INTO {table} VALUES (0, DATE '2026-01-01'), (1, DATE '-0001-01-01')",
    )
    try:
        with _connect() as conn:
            with pytest.raises(TrinoDataError) as before:
                _reference(conn, table, ["id", "d"])
            with pytest.raises(TrinoDataError) as after:
                _materialize(conn, table, ["id", "d"], 100_000, monkeypatch)
        assert str(after.value) == str(before.value)
    finally:
        _run(f"DROP TABLE IF EXISTS {table}")


def test_empty_result_keeps_the_declared_schema(typed, monkeypatch):
    _, table, cases = typed
    names = ["id"] + [name for name, _, _ in cases]
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(names)} FROM {table} WHERE false")
        expected = TrinoMaterializer._rows_frame(cur.fetchall(), list(cur.description), {})
        handle = DatasetHandle.from_connection(conn, table)
        mat = TrinoMaterializer(handle)
        monkeypatch.setattr(
            mat, "_qualified_table", f"(SELECT * FROM {table} WHERE false) AS empty_result"
        )
        actual = mat.to_polars(names)
    assert actual.height == 0
    assert actual.schema == expected.schema
