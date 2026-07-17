"""Unit coverage for Mode-A engine identity and connection selection."""

from kontra import Query
from kontra.probes.compare_sql import plan_compare


MSSQL_VH = "mssql://user:pass@warehouse:1433/vh/dbo.anchor"
MSSQL_SANDBOX = "mssql://user:pass@warehouse:1433/vhsandbox/dbo.anchor"
PG_MAIN = "postgresql://user:pass@warehouse:5432/main/public.anchor"
PG_OTHER = "postgresql://user:pass@warehouse:5432/other/public.anchor"


def _plan(before, after):
    return plan_compare(before, after, ["id"], ["id"], None, None, 0)


def test_sqlserver_query_before_table_uses_query_connection():
    query = Query("SELECT 1 AS id", source=MSSQL_SANDBOX)

    plan = _plan(query, MSSQL_VH.replace("anchor", "fact"))

    assert plan is not None
    assert plan["connection"] is plan["before"]
    assert plan["connection"].sql == query.sql


def test_sqlserver_table_before_query_uses_query_connection():
    query = Query("SELECT 1 AS id", source=MSSQL_SANDBOX)

    plan = _plan(MSSQL_VH.replace("anchor", "fact"), query)

    assert plan is not None
    assert plan["connection"] is plan["after"]
    assert plan["connection"].sql == query.sql


def test_sqlserver_cross_database_query_pair_still_falls_back():
    before = Query("SELECT 1 AS id", source=MSSQL_SANDBOX)
    after = Query("SELECT 1 AS id", source=MSSQL_VH)

    assert _plan(before, after) is None


def test_sqlserver_same_database_query_pair_remains_eligible():
    before = Query("SELECT 1 AS id", source=MSSQL_SANDBOX)
    after = Query("SELECT 2 AS id", source=MSSQL_SANDBOX)

    assert _plan(before, after) is not None


def test_sqlserver_different_server_or_user_is_not_eligible():
    query = Query("SELECT 1 AS id", source=MSSQL_SANDBOX)
    other_host = MSSQL_VH.replace("warehouse", "other-host")
    other_user = MSSQL_VH.replace("user:pass", "other:pass")

    assert _plan(query, other_host) is None
    assert _plan(query, other_user) is None


def test_postgres_still_requires_database_equality():
    query = Query("SELECT 1 AS id", source=PG_MAIN)

    assert _plan(query, PG_MAIN.replace("anchor", "fact")) is not None
    assert _plan(query, PG_OTHER.replace("anchor", "fact")) is None
