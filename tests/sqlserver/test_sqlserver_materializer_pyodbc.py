# tests/sqlserver/test_sqlserver_materializer_pyodbc.py
"""
Regression tests for SqlServerMaterializer.to_polars() against the live
SQL Server container, exercising the pyodbc driver path.

These reproduce two confirmed bugs:

  BUG 4 — pyodbc rows -> ShapeError:
    cursor.fetchall() returns pyodbc.Row objects on the pyodbc/Entra path.
    pl.DataFrame(rows, orient="row") does not unpack pyodbc.Row, raising
    ShapeError. pymssql returns plain tuples, so the pymssql tests never
    caught it. Fix: coerce every row to a tuple before constructing the frame.

  BUG 5 — dtype inference truncated at 100 rows:
    Polars' default infer_schema_length=100 infers the wrong dtype for a
    column that is NULL for its first 100 rows, then dies at the first real
    value (e.g. a datetime column NULL for the first 150 rows). Fix: pass
    infer_schema_length=None so all rows are scanned.

Requires the SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d

Tests skip (not fail) if the container / pyodbc driver is unreachable.
"""

import datetime

import pytest

# Connection parameters mirror tests/sqlserver/conftest.py (the mssql:// URI).
_HOST = "localhost"
_PORT = 1433
_DATABASE = "kontra_test"
_USER = "sa"
_PASSWORD = "Kontra_Test123!"

_TABLE = "dbo.pyodbc_null_dt_regression"
_NULL_ROWS = 150
_TOTAL_ROWS = 300
_BASE_TS = datetime.datetime(2020, 4, 29, 12, 0, 0)


def _pyodbc_connect():
    """
    Open a pyodbc connection to the live container, or skip.

    Uses SQL auth over the ODBC driver — this routes the same DBAPI code path
    as the Entra ID auth mode (both are pyodbc), which is what the bug needs.
    """
    pyodbc = pytest.importorskip("pyodbc")

    driver = None
    for name in pyodbc.drivers():
        if "ODBC Driver" in name and "SQL Server" in name:
            driver = name
            break
    if driver is None:
        pytest.skip("No Microsoft ODBC Driver for SQL Server installed")

    conn_str = (
        f"DRIVER={{{driver}}};SERVER={_HOST},{_PORT};DATABASE={_DATABASE};"
        f"UID={_USER};PWD={_PASSWORD};Encrypt=no;TrustServerCertificate=yes"
    )
    try:
        return pyodbc.connect(conn_str, timeout=5)
    except pyodbc.Error as e:  # noqa: F841 - reason surfaced via skip
        pytest.skip(f"SQL Server container unreachable via pyodbc: {e}")


@pytest.fixture()
def pyodbc_null_datetime_table():
    """
    Create a table whose datetime column is NULL for the first 150 rows then
    holds real datetimes, plus a plain string column. Yields (conn, table).

    The connection is a live pyodbc connection so the materializer exercises
    the pyodbc.Row + truncated-inference code path.
    """
    conn = _pyodbc_connect()
    cur = conn.cursor()
    cur.execute(f"IF OBJECT_ID('{_TABLE}','U') IS NOT NULL DROP TABLE {_TABLE}")
    cur.execute(f"CREATE TABLE {_TABLE} (id INT, ts DATETIME, name NVARCHAR(50))")

    rows = []
    for i in range(_TOTAL_ROWS):
        ts = None if i < _NULL_ROWS else _BASE_TS + datetime.timedelta(hours=i)
        rows.append((i, ts, f"n{i}"))
    cur.executemany(
        f"INSERT INTO {_TABLE} (id, ts, name) VALUES (?, ?, ?)", rows
    )
    conn.commit()

    try:
        yield conn, _TABLE
    finally:
        cur.execute(f"IF OBJECT_ID('{_TABLE}','U') IS NOT NULL DROP TABLE {_TABLE}")
        conn.commit()
        conn.close()


@pytest.mark.integration
class TestSqlServerMaterializerPyodbc:
    """Regression coverage for BUG 4 (ShapeError) and BUG 5 (dtype inference)."""

    def test_pyodbc_null_datetime_materializes(self, pyodbc_null_datetime_table):
        """
        BUG 4 + BUG 5: materialize a pyodbc-backed table whose datetime column
        is NULL for the first 150 rows.

        Asserts:
          (a) no ShapeError (BUG 4 — pyodbc.Row unpacking)
          (b) the datetime column is dtype Datetime, not str/null (BUG 5)
          (c) post-row-150 datetime values are present and correct (BUG 5)
        """
        import polars as pl

        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.sqlserver import SqlServerMaterializer

        conn, table = pyodbc_null_datetime_table
        handle = DatasetHandle.from_connection(conn, table)
        mat = SqlServerMaterializer(handle)

        # (a) Must not raise ShapeError.
        df = mat.to_polars(None)

        assert df.height == _TOTAL_ROWS
        assert set(df.columns) == {"id", "ts", "name"}

        # (b) datetime column keeps its real dtype despite the NULL prefix.
        assert df.schema["ts"] == pl.Datetime, (
            f"expected Datetime, got {df.schema['ts']}"
        )

        # First 150 rows are NULL, the rest are populated.
        assert df["ts"].null_count() == _NULL_ROWS

        # (c) a specific post-NULL value round-trips correctly.
        row200 = df.filter(pl.col("id") == 200)
        assert row200.height == 1
        expected = _BASE_TS + datetime.timedelta(hours=200)
        assert row200["ts"][0] == expected

        # A NULL-region row is genuinely null.
        row10 = df.filter(pl.col("id") == 10)
        assert row10["ts"][0] is None

    def test_pyodbc_projection_subset(self, pyodbc_null_datetime_table):
        """Column projection also works through the pyodbc path (no ShapeError)."""
        import polars as pl

        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.sqlserver import SqlServerMaterializer

        conn, table = pyodbc_null_datetime_table
        handle = DatasetHandle.from_connection(conn, table)
        mat = SqlServerMaterializer(handle)

        df = mat.to_polars(["id", "ts"])
        assert list(df.columns) == ["id", "ts"]
        assert df.schema["ts"] == pl.Datetime
        assert df.height == _TOTAL_ROWS


@pytest.mark.integration
class TestSqlServerMaterializerPymssqlRegression:
    """The pymssql (mssql:// URI) path must keep working unchanged."""

    def test_pymssql_materializer_still_loads(self, sqlserver_uri):
        """Materialize dbo.users via pymssql — no regression from the fix."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.registry import (
            pick_materializer,
            register_default_materializers,
        )

        register_default_materializers()
        handle = DatasetHandle.from_uri(sqlserver_uri)
        mat = pick_materializer(handle)
        assert mat.materializer_name == "sqlserver"

        df = mat.to_polars(["user_id", "email", "status"])
        assert len(df) == 1002
        assert list(df.columns) == ["user_id", "email", "status"]
