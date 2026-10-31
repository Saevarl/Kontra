# tests/sqlserver/test_sqlserver_scout_percentiles.py
"""Median and percentiles in SQL Server profiles, against known data.

SQL Server has PERCENTILE_CONT only as a window function, so the backend
computes percentiles in a separate query. These tests check its values against
the PostgreSQL definition (linear interpolation over the sorted non-null
values) and against SQL Server's own PERCENTILE_CONT, for every numeric type,
NULLs, all-NULL and empty columns, values that overflow a DECIMAL or MONEY
interpolation, and a sampled profile.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

from decimal import Decimal
from math import ceil, floor

import pytest

SERVER = "127.0.0.1"
PORT = 1433
USER = "sa"
PASSWORD = "Kontra_Test123!"
DATABASE = "kontra_test"
TABLE = "scout_percentiles"
EMPTY = "scout_percentiles_empty"
SAMPLED = "scout_percentiles_sampled"


def _uri(table):
    return f"mssql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}/dbo.{table}"


COLUMNS = {
    "i": "INT NULL",
    "big": "BIGINT NULL",
    "d": "DECIMAL(38,4) NULL",
    "m": "MONEY NULL",
    "f": "FLOAT NULL",
    "r": "REAL NULL",
    "s": "SMALLINT NULL",
    "t": "TINYINT NULL",
    "flag": "BIT NULL",
    "nothing": "INT NULL",
}
N = 11
_INTS = [5, None, 1, 9, 3, None, 7, 2, 8, 4, 6]
ROWS = [
    (
        v,
        None if v is None else 9_000_000_000_000_000_000 + v * 10**16,
        None if v is None else Decimal(v) * 10**33 + Decimal("0.5"),
        None if v is None else Decimal("900000000000000.1234") + v,
        None if v is None else v / 3,
        None if v is None else v * 0.25,
        None if v is None else -v * 1000,
        None if v is None else v * 25,
        None if v is None else v % 2,
        None,
    )
    for v in _INTS
]
NUMERIC = ["i", "big", "d", "m", "f", "r", "s", "t"]
LEVELS = [25, 50, 75, 99]


def _expected(values, fraction):
    """PostgreSQL's percentile_cont over the non-null values."""
    xs = sorted(float(v) for v in values if v is not None)
    if not xs:
        return None
    p = fraction * (len(xs) - 1)
    lo, hi = xs[floor(p)], xs[ceil(p)]
    return lo + (p - floor(p)) * (hi - lo)


def _connect():
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(
            server=SERVER, port=PORT, user=USER, password=PASSWORD,
            database=DATABASE, timeout=30, login_timeout=5, autocommit=True,
        )
    except pymssql.Error as e:
        pytest.skip(f"SQL Server container not reachable: {e}")


def _run(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None
    finally:
        cur.close()


def _create(conn, table):
    _run(conn, f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DROP TABLE dbo.{table}")
    cols = ", ".join(f"[{n}] {t}" for n, t in COLUMNS.items())
    _run(conn, f"CREATE TABLE dbo.{table} (id INT NOT NULL, {cols})")


@pytest.fixture(scope="module")
def conn():
    c = _connect()
    try:
        yield c
    finally:
        for table in (TABLE, EMPTY, SAMPLED):
            _run(c, f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DROP TABLE dbo.{table}")
        c.close()


@pytest.fixture(scope="module")
def known_table(conn):
    _create(conn, TABLE)
    cur = conn.cursor()
    cur.executemany(
        f"INSERT INTO dbo.{TABLE} VALUES (%s, {', '.join(['%s'] * len(COLUMNS))})",
        [(k, *row) for k, row in enumerate(ROWS)],
    )
    cur.close()
    return _uri(TABLE)


@pytest.mark.integration
class TestInterrogatePercentiles:
    def test_values_match_postgres_definition(self, known_table):
        import kontra

        profile = kontra.profile(known_table, preset="interrogate", save=False)
        for k, name in enumerate(NUMERIC):
            values = [row[k] for row in ROWS]
            numeric = profile.get_column(name).numeric
            assert numeric.median == pytest.approx(_expected(values, 0.5), rel=1e-12), name
            assert numeric.percentiles == pytest.approx(
                {f"p{p}": _expected(values, p / 100) for p in LEVELS if p != 50}, rel=1e-12
            ), name
            # The median lies within the same profile's range.
            assert numeric.min <= numeric.median <= numeric.max

    def test_values_match_native_percentile_cont(self, conn, known_table):
        import kontra

        profile = kontra.profile(known_table, preset="interrogate", save=False)
        for name in ("i", "f", "s", "t"):
            native = _run(
                conn,
                "SELECT TOP 1 "
                + ", ".join(
                    f"PERCENTILE_CONT({p / 100}) WITHIN GROUP (ORDER BY [{name}]) OVER ()"
                    for p in LEVELS
                )
                + f" FROM dbo.{TABLE}",
            )[0]
            numeric = profile.get_column(name).numeric
            got = [numeric.percentiles["p25"], numeric.median,
                   numeric.percentiles["p75"], numeric.percentiles["p99"]]
            assert got == pytest.approx(list(native), rel=1e-12), name

    def test_custom_levels_match(self, known_table):
        import kontra

        levels = [0, 100, 10, 90, 50]
        profile = kontra.profile(
            known_table, preset="interrogate", percentiles=levels, columns=["i"], save=False
        )
        numeric = profile.columns[0].numeric
        values = [row[0] for row in ROWS]
        assert numeric.median == _expected(values, 0.5) == 5.0
        assert numeric.percentiles == pytest.approx(
            {f"p{p}": _expected(values, p / 100) for p in levels if p != 50}, rel=1e-12
        )
        assert (numeric.percentiles["p0"], numeric.percentiles["p100"]) == (1.0, 9.0)

    def test_all_null_column_has_no_median(self, known_table):
        import kontra

        profile = kontra.profile(known_table, preset="interrogate", save=False)
        nothing = profile.get_column("nothing")
        assert nothing.null_count == N
        assert nothing.numeric.median is None
        assert nothing.numeric.percentiles == {}

    def test_bit_column_has_no_numeric_stats(self, known_table):
        import kontra

        profile = kontra.profile(known_table, preset="interrogate", save=False)
        assert profile.get_column("flag").numeric is None

    def test_empty_table(self, conn):
        import kontra

        _create(conn, EMPTY)
        profile = kontra.profile(_uri(EMPTY), preset="interrogate", save=False)
        assert profile.row_count == 0
        for name in NUMERIC:
            numeric = profile.get_column(name).numeric
            assert numeric.median is None and numeric.percentiles == {}, name

    def test_scan_keeps_skipping_median(self, known_table):
        # scan reads numeric stats from a block sample of the table without a
        # median, on SQL Server as on PostgreSQL.
        import kontra

        profile = kontra.profile(known_table, preset="scan", save=False)
        assert profile.get_column("i").numeric.median is None


@pytest.mark.integration
def test_sampled_median_comes_from_the_same_sample(conn):
    from kontra.scout.profiler import ScoutProfiler

    _run(conn, f"IF OBJECT_ID('dbo.{SAMPLED}','U') IS NOT NULL DROP TABLE dbo.{SAMPLED}")
    _run(
        conn,
        f"SELECT TOP (50000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS x "
        f"INTO dbo.{SAMPLED} FROM sys.all_objects a CROSS JOIN sys.all_objects b",
    )
    profiler = ScoutProfiler(_uri(SAMPLED), preset="interrogate", sample_size=5000)
    profile = profiler.profile()
    numeric = profile.columns[0].numeric
    seed = profiler.backend._sample_seed
    sample = f"dbo.{SAMPLED} TABLESAMPLE (5000 ROWS) REPEATABLE ({seed})"
    low, high, median = _run(
        conn,
        f"SELECT TOP 1 MIN(x) OVER (), MAX(x) OVER (), "
        f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER () FROM {sample}",
    )[0]
    assert (numeric.min, numeric.max) == (low, high)
    assert numeric.median == pytest.approx(median, rel=1e-12)


@pytest.mark.integration
def test_pyodbc_connection(known_table):
    """The Entra ID path connects with pyodbc; its rows are not tuples."""
    pyodbc = pytest.importorskip("pyodbc", exc_type=ImportError)
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
    if not drivers:
        pytest.skip("No Microsoft ODBC Driver for SQL Server installed")
    from kontra.connectors.handle import DatasetHandle
    from kontra.scout.backends.sqlserver_backend import SqlServerBackend

    backend = SqlServerBackend(DatasetHandle.from_uri(known_table))
    backend._conn = pyodbc.connect(
        f"DRIVER={{{drivers[0]}}};SERVER={SERVER},{PORT};DATABASE={DATABASE};"
        f"UID={USER};PWD={PASSWORD};TrustServerCertificate=yes",
        timeout=5,
    )
    try:
        result = backend.execute_stats_query([
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [m]) AS [q_m]",
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [nothing]) AS [q_nothing]",
        ])
    finally:
        backend.close()
    values = [row[3] for row in ROWS]
    assert result["q_m"] == pytest.approx(
        [_expected(values, 0.5), _expected(values, 0.25)], rel=1e-12
    )
    assert result["q_nothing"] is None
