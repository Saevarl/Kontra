# tests/sqlserver/test_sqlserver_materializer_types.py
"""
The SQL Server materializer types columns by their declared type when the
values can't: an empty table, an all-NULL column, a column NULL for its first
100 rows. pymssql's description is too coarse, so it looks the types up;
pyodbc's description carries them.

Before, an empty table loaded as all Utf8 and an all-NULL column as Polars
Null: dtype rules failed and range raised NotImplementedError.

Requires the SQL Server container:
    cd tests/sqlserver && docker compose up -d
The pyodbc cases also need the Microsoft ODBC Driver for SQL Server (msodbcsql18).
"""

from __future__ import annotations

import uuid

import polars as pl
import pytest

import kontra
from kontra import Query, rules

_HOST = "127.0.0.1"
_PORT = 1433
_USER = "sa"
_PASSWORD = "Kontra_Test123!"
_DATABASE = "kontra_test"
_URI = f"mssql://{_USER}:{_PASSWORD}@localhost:{_PORT}/{_DATABASE}/dbo.{{}}"

_DDL = "(i BIGINT, f FLOAT, s NVARCHAR(100), b BIT, t DATETIME2(6), d DATE)"
_VALUES = "(5, 1.5, N'x', 1, '2024-01-02 03:04:05', '2024-01-02')"
_NULLS = "(NULL, NULL, NULL, NULL, NULL, NULL)"

_DECLARED = {
    "i": pl.Int64,
    "f": pl.Float64,
    "s": pl.Utf8,
    "b": pl.Boolean,
    "t": pl.Datetime("us"),
    "d": pl.Date,
}

# scenario -> (rows, range failures): every scenario's NULLs are range failures
_SCENARIOS = {
    "empty": ([], 0),
    "all_null": ([_NULLS, _NULLS], 2),
    "null_prefix": ([_NULLS] * 149 + [_VALUES], 149),
}


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


def _pymssql_connect():
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(_HOST, _USER, _PASSWORD, _DATABASE, port=_PORT, autocommit=True)
    except pymssql.Error as e:
        pytest.skip(f"SQL Server container unreachable: {e}")


def _pyodbc_connect():
    pyodbc = pytest.importorskip("pyodbc")
    driver = next((d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d), None)
    if driver is None:
        pytest.skip("Microsoft ODBC Driver for SQL Server (msodbcsql18) is not installed")
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={_HOST},{_PORT};DATABASE={_DATABASE};"
        f"UID={_USER};PWD={_PASSWORD};Encrypt=no;TrustServerCertificate=yes",
        timeout=5,
    )


@pytest.fixture
def make_table():
    """Create a uniquely named dbo table from DDL and rows; drop it afterwards."""
    conn = _pymssql_connect()
    created = []

    def make(ddl: str, rows: list[str]) -> str:
        name = f"kontra_mat_types_{uuid.uuid4().hex[:10]}"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE dbo.{name} {ddl}")
        # 1000 rows per INSERT ... VALUES at most
        for start in range(0, len(rows), 1000):
            cur.execute(f"INSERT INTO dbo.{name} VALUES {', '.join(rows[start : start + 1000])}")
        created.append(name)
        return name

    yield make
    cur = conn.cursor()
    for name in created:
        cur.execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")
    conn.close()


@pytest.fixture(params=["pymssql", "pyodbc"])
def byoc_conn(request):
    conn = _pymssql_connect() if request.param == "pymssql" else _pyodbc_connect()
    yield conn
    conn.close()


def _assert_results(result, range_failures: int) -> None:
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
def test_validate_uri_without_pushdown(make_table, scenario):
    rows, range_failures = _SCENARIOS[scenario]
    name = make_table(_DDL, rows)

    result = kontra.validate(
        _URI.format(name), rules=_rules(), save=False, preplan="off", pushdown="off"
    )

    _assert_results(result, range_failures)


@pytest.mark.integration
@pytest.mark.parametrize("scenario", list(_SCENARIOS))
def test_validate_byoc_without_pushdown(make_table, byoc_conn, scenario):
    rows, range_failures = _SCENARIOS[scenario]
    name = make_table(_DDL, rows)

    result = kontra.validate(
        byoc_conn,
        table=f"dbo.{name}",
        rules=_rules(),
        save=False,
        preplan="off",
        pushdown="off",
    )

    _assert_results(result, range_failures)


@pytest.mark.integration
@pytest.mark.parametrize("scenario", list(_SCENARIOS))
def test_query_source_frame(make_table, byoc_conn, scenario):
    """A Query source (as probes load it) is typed by its result columns too."""
    from kontra.probes.utils import load_data

    rows, _ = _SCENARIOS[scenario]
    name = make_table(_DDL, rows)

    df = load_data(Query(f"SELECT i, f, s, b, t, d FROM dbo.{name}", source=byoc_conn))

    assert dict(df.schema) == _DECLARED
    assert df.height == len(rows)


# column -> (declared type, a value, dtype a populated column loads as)
_TYPES = {
    "c_tiny": ("TINYINT", "1", pl.Int64),
    "c_small": ("SMALLINT", "1", pl.Int64),
    "c_int": ("INT", "1", pl.Int64),
    "c_big": ("BIGINT", "1", pl.Int64),
    "c_bit": ("BIT", "1", pl.Boolean),
    "c_real": ("REAL", "1.5", pl.Float32),
    "c_float": ("FLOAT", "1.5", pl.Float64),
    "c_dec": ("DECIMAL(10,2)", "1.25", pl.Decimal(38, 2)),
    "c_num0": ("NUMERIC(18,0)", "12", pl.Decimal(38, 0)),
    "c_money": ("MONEY", "1.5", pl.Decimal(38, 4)),
    "c_char": ("CHAR(5)", "'ab'", pl.Utf8),
    "c_vc": ("VARCHAR(20)", "'a'", pl.Utf8),
    "c_nvc": ("NVARCHAR(100)", "N'a'", pl.Utf8),
    "c_vcmax": ("VARCHAR(MAX)", "'a'", pl.Utf8),
    "c_date": ("DATE", "'2024-01-02'", pl.Date),
    "c_dt": ("DATETIME", "'2024-01-02 03:04:05.123'", pl.Datetime("us")),
    "c_dt2": ("DATETIME2(6)", "'2024-01-02 03:04:05.123456'", pl.Datetime("us")),
    "c_sdt": ("SMALLDATETIME", "'2024-01-02 03:04:00'", pl.Datetime("us")),
    "c_time": ("TIME", "'03:04:05'", pl.Time),
    "c_vbin": ("VARBINARY(10)", "0x0102", pl.Binary),
}


@pytest.mark.integration
@pytest.mark.parametrize("driver", ["pymssql", "pyodbc"])
def test_empty_and_all_null_load_as_populated(make_table, driver):
    """Same dtypes for a populated, an all-NULL and an empty table, per type."""
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.materializers.sqlserver import SqlServerMaterializer

    types = dict(_TYPES)
    if driver == "pymssql":
        # pyodbc can't fetch datetimeoffset (ODBC SQL type -155).
        types["c_dto"] = (
            "DATETIMEOFFSET",
            "'2024-01-02 03:04:05 +02:00'",
            pl.Datetime("us", "UTC"),
        )
    ddl = "(" + ", ".join(f"{c} {t}" for c, (t, _, _) in types.items()) + ")"
    values = "(" + ", ".join(v for _, v, _ in types.values()) + ")"
    nulls = "(" + ", ".join("NULL" for _ in types) + ")"
    expected = {c: dtype for c, (_, _, dtype) in types.items()}

    conn = _pymssql_connect() if driver == "pymssql" else _pyodbc_connect()
    try:
        for rows in ([values], [nulls], [], [values, nulls]):
            name = make_table(ddl, rows)
            df = SqlServerMaterializer(
                DatasetHandle.from_connection(conn, f"dbo.{name}")
            ).to_polars(None)
            assert dict(df.schema) == expected, rows
            assert df.height == len(rows)
        # A populated row's values are unchanged, char padding included.
        assert df["c_char"][0] == "ab   "
        assert str(df["c_dec"][0]) == "1.25"
        assert df["c_bit"][0] is True
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.parametrize("preplan,pushdown", [("on", "on"), ("on", "off"), ("off", "on"), ("off", "off")])
def test_real_is_float32_on_every_route(make_table, preplan, pushdown):
    """A REAL column is a float32, not a float64, from metadata and in Polars,
    and its 0.1 is in [0.1] as SQL Server compares it."""
    name = make_table("(h REAL)", ["(0.1)"])

    result = kontra.validate(
        _URI.format(name),
        rules=[
            rules.dtype("h", "float32", id="f32"),
            rules.dtype("h", "float64", id="f64"),
            rules.allowed_values("h", [0.1], id="in"),
        ],
        save=False,
        preplan=preplan,
        pushdown=pushdown,
    )

    assert {r.rule_id: r.passed for r in result.rules} == {"f32": True, "f64": False, "in": True}
