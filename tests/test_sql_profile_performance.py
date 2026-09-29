"""Live SQL profiling equivalence, native values, fallback, and call lifetime."""

from datetime import date
from decimal import Decimal
from importlib import import_module

import pytest

import kontra


@pytest.fixture(params=["postgres", "sqlserver", "clickhouse"])
def sql_profile_table(request):
    family = request.param
    table = "sqlprof_regression"
    name = "odd'\"name]"
    if family == "postgres":
        driver = pytest.importorskip("psycopg")
        try:
            conn = driver.connect(
                "postgresql://kontra:kontra_test@127.0.0.1:5433/kontra_test", autocommit=True
            )
        except driver.Error as e:
            pytest.skip(f"PostgreSQL unavailable: {e}")
        uri = f"postgresql://kontra:kontra_test@127.0.0.1:5433/kontra_test/public.{table}"
        cls = import_module("kontra.scout.backends.postgres_backend").PostgreSQLBackend
        execute = lambda sql: conn.execute(sql)
        types = ["DOUBLE PRECISION", "VARCHAR(30)", "DECIMAL(12,2)", "DATE", "BOOLEAN", "BIGINT"]
    elif family == "sqlserver":
        driver = pytest.importorskip("pymssql")
        try:
            conn = driver.connect(
                "127.0.0.1", "sa", "Kontra_Test123!", database="kontra_test", autocommit=True
            )
        except driver.Error as e:
            pytest.skip(f"SQL Server unavailable: {e}")
        uri = f"mssql://sa:Kontra_Test123!@127.0.0.1:1433/kontra_test/dbo.{table}"
        cls = import_module("kontra.scout.backends.sqlserver_backend").SqlServerBackend

        def execute(sql):
            cur = conn.cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()

        types = ["FLOAT", "NVARCHAR(30)", "DECIMAL(12,2)", "DATE", "BIT", "BIGINT"]
    else:
        driver = pytest.importorskip("clickhouse_connect")
        from clickhouse_connect.driver.exceptions import ClickHouseError, Error

        try:
            conn = driver.get_client(
                host="127.0.0.1", username="kontra", password="kontra_test", database="kontra_test"
            )
        except (ClickHouseError, Error) as e:
            pytest.skip(f"ClickHouse unavailable: {e}")
        uri = f"clickhouse://kontra:kontra_test@127.0.0.1:8123/kontra_test/{table}"
        cls = import_module("kontra.scout.backends.clickhouse_backend").ClickHouseBackend
        execute = conn.command
        types = [
            "Nullable(Float64)",
            "Nullable(String)",
            "Nullable(Decimal(12,2))",
            "Nullable(Date)",
            "Nullable(Bool)",
            "Nullable(Int64)",
        ]
    from kontra.connectors.handle import DatasetHandle

    backend = cls(DatasetHandle.from_uri(uri))
    names = ["x", name, "price", "day", "enabled", "nothing"]
    drop = f"DROP TABLE IF EXISTS {table}"
    execute(drop)
    columns = ", ".join(f"{backend.esc_ident(n)} {t}" for n, t in zip(names, types))
    execute(
        f"CREATE TABLE {table} ({columns})"
        + (" ENGINE=MergeTree ORDER BY tuple()" if family == "clickhouse" else "")
    )
    true, false = ("1", "0") if family == "sqlserver" else ("true", "false")
    execute(
        f"INSERT INTO {table} VALUES (-5,'z',1.25,'2020-01-01',{true},NULL), (1,'a',2.50,'2020-01-02',{false},NULL), (1,'a',2.50,'2020-01-02',{false},NULL), (2,NULL,NULL,NULL,NULL,NULL), (8,'z',4.75,'2020-01-03',{true},NULL), (NULL,NULL,NULL,NULL,NULL,NULL)"
    )
    try:
        yield family, uri, cls, execute, name
    finally:
        execute(drop)
        conn.close()


def test_native_distributions_projection_and_new_calls(sql_profile_table):
    _family, uri, _cls, execute, name = sql_profile_table
    p = kontra.profile(uri, preset="interrogate", save=False)
    assert p.row_count == 6
    x = p.get_column("x")
    assert (x.null_count, x.distinct_count) == (1, 4)
    assert x.values == [-5, 1, 2, 8]
    assert {v.value: v.count for v in x.top_values} == {-5: 1, 1: 2, 2: 1, 8: 1}
    assert p.get_column(name).values == ["a", "z"]
    assert p.get_column("price").values == [Decimal("1.25"), Decimal("2.50"), Decimal("4.75")]
    day = p.get_column("day").values[0]
    assert (day.date() if hasattr(day, "date") else day) == date(2020, 1, 1)
    assert p.get_column("enabled").values == [False, True]
    assert p.get_column("nothing").values == []
    execute("INSERT INTO sqlprof_regression (x) VALUES (9)")
    q = kontra.profile(uri, preset="interrogate", columns=["x"], save=False)
    assert q.row_count == 7
    assert [c.name for c in q.columns] == ["x"]
    assert q.columns[0].values == [-5, 1, 2, 8, 9]


@pytest.mark.parametrize("levels", [[99, 25, 50, 75], [50], [0, 100, 25, 25]])
def test_quantiles_keep_backend_exact_semantics(sql_profile_table, levels):
    family, uri, cls, _execute, _name = sql_profile_table
    p = kontra.profile(uri, preset="interrogate", columns=["x"], percentiles=levels, save=False)
    x = p.columns[0]
    if family == "sqlserver":
        assert x.numeric.median is None and x.numeric.percentiles == {}
        return
    from kontra.connectors.handle import DatasetHandle

    backend = cls(DatasetHandle.from_uri(uri))
    backend.connect()
    try:
        # Scalar SQL is the old algorithm, independent of the new array decoder.
        expected = backend.execute_stats_query(
            [
                f"PERCENTILE_CONT({level / 100}) WITHIN GROUP (ORDER BY x) AS {backend.esc_ident('q' + str(i))}"
                for i, level in enumerate([50] + [v for v in levels if v != 50])
            ]
        )
    finally:
        backend.close()
    assert x.numeric.median == expected["q0"]
    assert x.numeric.percentiles == {
        f"p{v}": expected[f"q{i}"] for i, v in enumerate([v for v in levels if v != 50], 1)
    }


def test_combined_value_failure_uses_original_queries(sql_profile_table, monkeypatch):
    _family, uri, cls, _execute, _name = sql_profile_table
    monkeypatch.setattr(cls, "fetch_value_counts", lambda *a: None)
    p = kontra.profile(uri, preset="interrogate", columns=["x"], save=False)
    assert p.columns[0].values == [-5, 1, 2, 8]
    assert sorted((v.value, v.count) for v in p.columns[0].top_values) == [
        (-5, 1),
        (1, 2),
        (2, 1),
        (8, 1),
    ]


def test_connection_closed_after_query_failure(sql_profile_table, monkeypatch):
    _family, uri, cls, _execute, _name = sql_profile_table
    seen = []
    original = cls.close

    def close(backend):
        original(backend)
        seen.append(backend._conn)

    def fail(*a, **kw):
        raise RuntimeError("injected stats failure")

    monkeypatch.setattr(cls, "close", close)
    monkeypatch.setattr(cls, "execute_stats_query", fail)
    with pytest.raises(RuntimeError, match="injected"):
        kontra.profile(uri, preset="interrogate", save=False)
    assert seen == [None]


def test_unique_frequency_proof_is_exact_and_sampling_does_not_reuse_it(
    sql_profile_table, monkeypatch
):
    family, uri, cls, execute, _name = sql_profile_table
    # Low thresholds force a genuinely unique non-null column down the bounded
    # top-value path, but duplicates in x must retain their exact frequencies.
    execute(
        "DELETE FROM sqlprof_regression"
        if family != "clickhouse"
        else "TRUNCATE TABLE sqlprof_regression"
    )
    execute(
        "INSERT INTO sqlprof_regression (x, price) VALUES (1,1),(2,1),(3,2),(4,2),(5,3),(NULL,NULL)"
    )
    grouped = []
    original = cls.fetch_top_values

    def top(backend, column, limit):
        grouped.append(column)
        return original(backend, column, limit)

    monkeypatch.setattr(cls, "fetch_top_values", top)
    p = kontra.profile(
        uri,
        preset="interrogate",
        columns=["x", "price"],
        list_values_threshold=1,
        top_n=2,
        save=False,
    )
    assert [(v.count, v.pct) for v in p.get_column("x").top_values] == [
        (1, pytest.approx(100 / 6))
    ] * 2
    assert [v.count for v in p.get_column("price").top_values] == [2, 2]
    assert "x" not in grouped
    # Directly verify the proof guard, independently of the database's random
    # sampling (a small SQL Server page sample can legitimately be empty).
    from kontra.connectors.handle import DatasetHandle
    from kontra.scout.profiler import ScoutProfiler

    profiler = ScoutProfiler(uri, preset="interrogate", list_values_threshold=1)
    profiler.backend = cls(DatasetHandle.from_uri(uri))
    c = p.get_column("x")
    assert profiler._has_exact_value_frequency(c, p.row_count)
    c.distinct_count_estimated = True
    assert not profiler._has_exact_value_frequency(c, p.row_count)
    c.distinct_count_estimated = False
    c.null_count_estimated = True
    assert not profiler._has_exact_value_frequency(c, p.row_count)


def test_driver_failure_in_combined_query_falls_back(sql_profile_table, monkeypatch):
    family, uri, cls, _execute, _name = sql_profile_table
    from kontra.connectors.handle import DatasetHandle

    backend = cls(DatasetHandle.from_uri(uri))
    backend.connect()
    try:
        # The new query is optional: a database error must leave the connection
        # usable for the original per-column path (PostgreSQL needs rollback).
        assert backend.fetch_value_counts("missing_column") is None
        assert backend.fetch_top_values("x", 1) == [(1, 2)]
        if family == "clickhouse":
            backend.prefetch_value_counts([("missing_column", 5), ("x", 5)])
            assert backend.fetch_top_values("x", 1) == [(1, 2)]
    finally:
        backend.close()


def test_nonfinite_and_all_null_quantiles_match_scalar_sql(sql_profile_table):
    family, uri, cls, execute, _name = sql_profile_table
    execute(
        "DELETE FROM sqlprof_regression"
        if family != "clickhouse"
        else "TRUNCATE TABLE sqlprof_regression"
    )
    if family == "postgres":
        values = "('NaN'),('Infinity'),('-Infinity'),(1),(3),(3),(NULL)"
    elif family == "clickhouse":
        values = "(nan),(inf),(-inf),(1),(3),(3),(NULL)"
    else:
        # SQL Server FLOAT cannot represent IEEE non-finite values. Its null
        # behavior and intentionally absent percentiles still need preservation.
        values = "(NULL),(NULL),(NULL)"
    execute("INSERT INTO sqlprof_regression (x) VALUES " + values)
    p = kontra.profile(uri, preset="interrogate", columns=["x", "nothing"], save=False)
    from math import isnan

    from kontra.connectors.handle import DatasetHandle

    backend = cls(DatasetHandle.from_uri(uri))
    backend.connect()
    try:
        for name in ("x", "nothing"):
            c = p.get_column(name)
            if family == "sqlserver":
                assert c.numeric.median is None and c.numeric.percentiles == {}
                assert c.null_count == p.row_count
                continue
            expected = backend.execute_stats_query(
                [
                    f"PERCENTILE_CONT({level / 100}) WITHIN GROUP (ORDER BY {backend.esc_ident(name)}) AS {backend.esc_ident('q' + str(level))}"
                    for level in (25, 50, 75, 99)
                ]
            )
            actual = {
                50: c.numeric.median,
                **{i: c.numeric.percentiles.get(f"p{i}") for i in (25, 75, 99)},
            }
            for level, value in actual.items():
                ref = expected[f"q{level}"]
                assert value == ref or (
                    value is not None and ref is not None and isnan(value) and isnan(ref)
                )
    finally:
        backend.close()
