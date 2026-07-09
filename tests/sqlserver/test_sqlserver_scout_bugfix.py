"""Regression tests (live SQL Server) for two Scout profiler bugs:

Bug 8: constant (distinct==1) columns must surface their single value.
Bug 7: estimated distinct on identifier columns is annotated / corrected.

Requires the SQL Server container (see tests/sqlserver/conftest.py).
"""

from __future__ import annotations

import pytest

from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection
from kontra.scout.profiler import ScoutProfiler

_TABLE = "scout_bugfix_regression"


def _params():
    return SqlServerConnectionParams(
        host="localhost", port=1433, user="sa", password="Kontra_Test123!",
        database="kontra_test", schema="dbo", table=_TABLE,
    )


def _reachable() -> bool:
    try:
        conn = get_connection(_params())
        conn.close()
        return True
    except Exception:  # noqa: BLE001 - reachability probe only
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(), reason="SQL Server not reachable on localhost:1433"
)


@pytest.fixture
def constant_id_table():
    params = _params()
    conn = get_connection(params)
    cur = conn.cursor()
    cur.execute(f"IF OBJECT_ID('dbo.{_TABLE}','U') IS NOT NULL DROP TABLE dbo.{_TABLE}")
    cur.execute(
        f"CREATE TABLE dbo.{_TABLE} "
        f"(id bigint PRIMARY KEY, status nvarchar(50) NOT NULL, amount float)"
    )
    cur.executemany(
        f"INSERT INTO dbo.{_TABLE} (id, status, amount) VALUES (%s,%s,%s)",
        [(i, "ACTIVE", float(i % 97)) for i in range(1, 501)],
    )
    cur.execute(f"UPDATE STATISTICS dbo.{_TABLE}")
    conn.commit()
    cur.close()
    conn.close()

    uri = f"mssql://sa:Kontra_Test123!@localhost:1433/kontra_test/dbo.{_TABLE}"
    yield uri

    conn = get_connection(params)
    cur = conn.cursor()
    cur.execute(f"IF OBJECT_ID('dbo.{_TABLE}','U') IS NOT NULL DROP TABLE dbo.{_TABLE}")
    conn.commit()
    cur.close()
    conn.close()


@pytest.mark.integration
class TestSqlServerScoutBugfix:
    def test_bug8_constant_value_surfaced_scan(self, constant_id_table):
        profile = ScoutProfiler(constant_id_table, preset="scan").profile()
        col = profile.get_column("status")
        assert col.distinct_count == 1
        assert col.top_values, "constant column must surface top_values"
        assert col.top_values[0].value == "ACTIVE"

    def test_bug8_constant_value_surfaced_scout(self, constant_id_table):
        # SQL Server has no MCV metadata; the value is surfaced via a targeted
        # single-group fetch.
        profile = ScoutProfiler(constant_id_table, preset="scout").profile()
        col = profile.get_column("status")
        assert col.distinct_count == 1
        assert col.top_values, "constant column must surface top_values (scout)"
        assert col.top_values[0].value == "ACTIVE"
        assert col.values == ["ACTIVE"]

    def test_bug7_identifier_distinct_exact_scan(self, constant_id_table):
        # scan preset replaces the histogram estimate with an exact count.
        profile = ScoutProfiler(constant_id_table, preset="scan").profile()
        idc = profile.get_column("id")
        assert idc.distinct_count == 500
        assert idc.uniqueness_ratio == pytest.approx(1.0)
        assert idc.distinct_count_estimated is False
