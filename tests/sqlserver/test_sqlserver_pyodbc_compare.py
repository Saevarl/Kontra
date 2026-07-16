# tests/sqlserver/test_sqlserver_pyodbc_compare.py
"""Set-based compare (Mode A) over the pyodbc driver — the Entra ID path.

Entra ID auth (Azure SQL / Managed Instance) connects via pyodbc, whose
cursor.description reports Python types, not pymssql DBAPIType objects. These
tests drive Mode A through a real pyodbc connection to the local SQL Server
container (SQL auth exercises the identical driver behaviour Entra uses) to
verify type classification and firing. They skip when pyodbc / an ODBC driver /
the container is unavailable.
"""

import polars as pl
import pytest

import kontra
from kontra import Query

pytestmark = pytest.mark.integration


@pytest.fixture
def pyodbc_conn():
    pyodbc = pytest.importorskip("pyodbc")
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        pytest.skip("no SQL Server ODBC driver installed")
    conn_str = (
        f"DRIVER={{{drivers[-1]}}};SERVER=localhost,1433;DATABASE=kontra_test;"
        "UID=sa;PWD=Kontra_Test123!;TrustServerCertificate=yes;Encrypt=no"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=False, timeout=5)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"SQL Server not reachable via pyodbc: {e}")
    yield conn
    conn.close()


def _q(conn, sql):
    return Query(sql, source=conn)


class TestModeAViaPyodbc:
    def test_mode_a_fires_and_char_change_is_byte_exact(self, pyodbc_conn):
        # varchar value, changed only by case -> byte-exact VARBINARY must catch
        # it (SQL Server's default collation is case-insensitive). int key.
        before = _q(pyodbc_conn, "SELECT 1 AS id, CAST('Alice' AS varchar(20)) AS name "
                                 "UNION ALL SELECT 2, 'Bob'")
        after = _q(pyodbc_conn, "SELECT 1 AS id, CAST('alice' AS varchar(20)) AS name "
                                "UNION ALL SELECT 2, 'Bob'")
        r = kontra.compare(before, after, key="id")
        assert r.execution_tier == "sql"  # Mode A fired through pyodbc
        assert r.changed_rows == 1
        assert r.columns_modified == ["name"]
        pyodbc_conn.rollback()

    def test_matches_polars_trailing_space_and_value(self, pyodbc_conn):
        before = _q(pyodbc_conn, "SELECT 1 AS id, CAST('a ' AS varchar(20)) AS s "
                                 "UNION ALL SELECT 2, 'x'")
        after = _q(pyodbc_conn, "SELECT 1 AS id, CAST('a' AS varchar(20)) AS s "
                                "UNION ALL SELECT 2, 'y'")
        r = kontra.compare(before, after, key="id")

        pb = pl.DataFrame({"id": [1, 2], "s": ["a ", "x"]})
        pa = pl.DataFrame({"id": [1, 2], "s": ["a", "y"]})
        rp = kontra.compare(pb, pa, key="id")

        assert r.execution_tier == "sql" and rp.execution_tier == "polars"
        assert r.changed_rows == rp.changed_rows == 2  # trailing space + value
        pyodbc_conn.rollback()

    def test_char_key_defers_via_pyodbc(self, pyodbc_conn):
        # is_char must classify a varchar KEY through pyodbc -> defer to Polars.
        before = _q(pyodbc_conn, "SELECT CAST('US' AS varchar(2)) AS k, 1 AS v "
                                 "UNION ALL SELECT 'IS', 2")
        after = _q(pyodbc_conn, "SELECT CAST('US' AS varchar(2)) AS k, 1 AS v "
                                "UNION ALL SELECT 'IS', 3")
        r = kontra.compare(before, after, key="k")
        assert r.execution_tier == "polars"
        pyodbc_conn.rollback()

    def test_int_key_int_value_matches_polars(self, pyodbc_conn):
        before = _q(pyodbc_conn, "SELECT 1 AS id, 100 AS amt UNION ALL SELECT 2, 200 "
                                 "UNION ALL SELECT 3, 300")
        after = _q(pyodbc_conn, "SELECT 2 AS id, 200 AS amt UNION ALL SELECT 3, 999 "
                                "UNION ALL SELECT 4, 400")
        r = kontra.compare(before, after, key="id")
        assert r.execution_tier == "sql"
        assert (r.dropped, r.added, r.preserved, r.changed_rows) == (1, 1, 2, 1)
        pyodbc_conn.rollback()
