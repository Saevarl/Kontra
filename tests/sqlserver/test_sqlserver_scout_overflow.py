# tests/sqlserver/test_sqlserver_scout_overflow.py
"""
Regression test for BUG 6: `kontra profile` with the `scan` preset failed on
wide-value-range integer columns with

    Arithmetic overflow error converting expression to data type int

SQL Server accumulates AVG's (and SUM's) internal sum in the column's native
`int` type, which overflows 2^31-1 on wide integer columns. The scout backend
now widens integer aggregate arguments (AVG/STDEV -> FLOAT, SUM -> BIGINT) so
accumulation is overflow-safe. The widened values are unchanged.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

import pytest

SERVER = "localhost"
PORT = 1433
USER = "sa"
PASSWORD = "Kontra_Test123!"
DATABASE = "kontra_test"
TABLE = "bug6_scan_overflow"
URI = f"mssql://sa:Kontra_Test123!@localhost:1433/kontra_test/dbo.{TABLE}"

# Rows whose SUM(big_int_col) exceeds 2^31-1 (2,147,483,647), forcing the
# int accumulator inside AVG(int) to overflow before the fix.
N_ROWS = 30
BASE = 200_000_000
ROWS = [(i, BASE + i, i % 5, f"lbl{i % 3}") for i in range(N_ROWS)]

EXPECTED_MIN = float(BASE)
EXPECTED_MAX = float(BASE + N_ROWS - 1)
EXPECTED_SUM = sum(BASE + i for i in range(N_ROWS))
EXPECTED_MEAN = EXPECTED_SUM / N_ROWS


def _connect(database=DATABASE):
    """Connect via pymssql, skipping the test if unreachable."""
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(
            server=SERVER, port=PORT, user=USER,
            password=PASSWORD, database=database, timeout=5, login_timeout=5,
        )
    except pymssql.Error as e:  # OperationalError / InterfaceError, etc.
        pytest.skip(f"SQL Server container not reachable: {e}")


@pytest.fixture(scope="module")
def overflow_table():
    """Create a table whose int column SUM overflows int (2^31-1)."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"IF OBJECT_ID('dbo.{TABLE}','U') IS NOT NULL DROP TABLE dbo.{TABLE}"
        )
        cur.execute(
            f"""
            CREATE TABLE dbo.{TABLE} (
                id INT NOT NULL,
                big_int_col INT NOT NULL,
                small_col INT NULL,
                label VARCHAR(20) NULL
            )
            """
        )
        cur.executemany(
            f"INSERT INTO dbo.{TABLE} (id, big_int_col, small_col, label) "
            f"VALUES (%s, %s, %s, %s)",
            ROWS,
        )
        conn.commit()

        # Sanity: the raw SUM(int) really does overflow (this is the bug).
        cur.execute(f"SELECT SUM(CAST(big_int_col AS BIGINT)) FROM dbo.{TABLE}")
        assert int(cur.fetchone()[0]) == EXPECTED_SUM
        assert EXPECTED_SUM > 2**31 - 1

        yield URI
    finally:
        cur.execute(
            f"IF OBJECT_ID('dbo.{TABLE}','U') IS NOT NULL DROP TABLE dbo.{TABLE}"
        )
        conn.commit()
        cur.close()
        conn.close()


@pytest.mark.integration
class TestSqlServerScanOverflow:
    def test_scan_preset_succeeds_on_wide_int_column(self, overflow_table):
        """scan preset must not raise arithmetic-overflow and must be correct."""
        import kontra

        profile = kontra.profile(overflow_table, preset="scan")

        col = next(c for c in profile.columns if c.name == "big_int_col")
        assert col.numeric is not None
        # Overflow-sensitive stats must be exact.
        assert col.numeric.min == EXPECTED_MIN
        assert col.numeric.max == EXPECTED_MAX
        assert col.numeric.mean == pytest.approx(EXPECTED_MEAN, abs=1e-6)
        # STDEV is computed (widened to FLOAT); just ensure it is populated.
        assert col.numeric.std is not None and col.numeric.std >= 0

    def test_scan_matches_duckdb_parquet(self, overflow_table):
        """Cross-backend: mean/min/max agree with a DuckDB/parquet copy."""
        import kontra
        pl = pytest.importorskip("polars")

        df = pl.DataFrame(
            {
                "id": [r[0] for r in ROWS],
                "big_int_col": [r[1] for r in ROWS],
                "small_col": [r[2] for r in ROWS],
                "label": [r[3] for r in ROWS],
            }
        )
        pq = "/private/tmp/bug6_scan_overflow.parquet"
        df.write_parquet(pq)

        ss = kontra.profile(overflow_table, preset="scan")
        duck = kontra.profile(pq, preset="scan")

        s = next(c for c in ss.columns if c.name == "big_int_col").numeric
        d = next(c for c in duck.columns if c.name == "big_int_col").numeric

        assert s.min == d.min == EXPECTED_MIN
        assert s.max == d.max == EXPECTED_MAX
        assert s.mean == pytest.approx(d.mean, abs=1e-6)
        assert s.mean == pytest.approx(EXPECTED_MEAN, abs=1e-6)
