"""Equivalence, projection, sampling and lifetime of per-call profile reuse."""

import dataclasses
from datetime import date
from decimal import Decimal
from importlib import import_module

import duckdb
import polars as pl
import pytest

import kontra

Backend = import_module("kontra.scout.backends.duckdb_backend").DuckDBBackend


def normalized(profile):
    d = dataclasses.asdict(profile)
    for k in ("profiled_at", "profile_duration_ms", "source_uri"):
        d.pop(k, None)
    for c in d["columns"]:
        # Top-N representatives with the same frequency have no tie-order contract.
        c["top_values"] = sorted((v["count"], v["pct"]) for v in c["top_values"])
    return d


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("preset", ["scout", "scan", "interrogate"])
def test_reused_distributions_match_original_queries(tmp_path, monkeypatch, suffix, preset):
    path = tmp_path / f"values.{suffix}"
    df = pl.DataFrame(
        {
            "id": list(range(120)),
            "constant": ["x"] * 120,
            "nullable": [None, 2, 1, 1] * 30,
            'odd"name': ["z", "a", None, "a"] * 30,
            "price": [Decimal("1.25"), Decimal("2.50"), None, Decimal("1.25")] * 30,
            "day": [date(2020, 1, 1), date(2020, 1, 2), None, date(2020, 1, 1)] * 30,
            "flag": [True, False, None, False] * 30,
        }
    )
    getattr(df, "write_" + suffix)(path)
    kwargs = {"preset": preset, "include_patterns": True, "save": False}
    fast = kontra.profile(str(path), **kwargs)
    monkeypatch.setattr(Backend, "exact_value_frequency", False)
    monkeypatch.setattr(Backend, "prefetch_value_counts", lambda *a: None)
    monkeypatch.setattr(Backend, "fetch_value_counts", None)
    monkeypatch.setattr(Backend, "prepare_repeated_reads", lambda *a: None)
    slow = kontra.profile(str(path), **kwargs)
    assert normalized(fast) == normalized(slow)
    if preset != "scout":
        assert fast.get_column("nullable").values == [1, 2]
        assert [(v.value, v.count) for v in fast.get_column("nullable").top_values] == [
            (1, 60),
            (2, 30),
        ]


def test_csv_materializes_only_selected_columns_and_rebinds(tmp_path, monkeypatch):
    path = tmp_path / "projected.csv"
    columns = ["a", "b", "c", 'odd"name']
    df = pl.DataFrame({c: [1, 2, 3] for c in columns + ["unused"]})
    df.write_csv(path)
    original = Backend.prepare_repeated_reads
    tables = []

    def observe(self, selected):
        original(self, selected)
        tables.append((self._view_name, self.get_schema()))

    monkeypatch.setattr(Backend, "prepare_repeated_reads", observe)
    kwargs = {"columns": columns, "preset": "interrogate", "save": False}
    first = kontra.profile(str(path), **kwargs)
    assert first.row_count == 3
    pl.DataFrame({c: ["new"] * 2 for c in columns + ["unused"]}).write_csv(path)
    second = kontra.profile(str(path), **kwargs)
    assert second.row_count == 2
    assert all(c.values == ["new"] for c in second.columns)
    assert all(name == "_scout_materialized" for name, _ in tables)
    assert all([name for name, _ in schema] == columns for _, schema in tables)


@pytest.mark.parametrize("reason", ["sample", "budget", "narrow", "scout", "top-zero"])
def test_csv_materialization_is_selective(tmp_path, monkeypatch, reason):
    path = tmp_path / "source.csv"
    pl.DataFrame({f"col{i}": [7] * 100 for i in range(5)}).write_csv(path)
    kwargs = {"preset": "interrogate", "save": False}
    if reason == "sample":
        kwargs["sample"] = 10
    if reason == "budget":
        monkeypatch.setattr(Backend, "_MAX_MATERIALIZED_CSV_BYTES", 0)
    if reason == "narrow":
        kwargs["columns"] = ["col0"]
    if reason == "scout":
        kwargs["preset"] = "scout"
    if reason == "top-zero":
        kwargs["top_n"] = 0
    names = []
    original = Backend.execute_stats_query

    def observe(self, exprs):
        names.append(self._view_name)
        return original(self, exprs)

    monkeypatch.setattr(Backend, "execute_stats_query", observe)
    p = kontra.profile(str(path), **kwargs)
    assert names == ["_scout"]
    assert p.row_count == (10 if reason == "sample" else 100)
    if reason == "sample":
        assert all(c.distinct_count_estimated for c in p.columns)


@pytest.mark.parametrize("failure", ["distribution", "materialize", "stats"])
def test_repeated_query_fallback_and_cleanup(tmp_path, monkeypatch, failure):
    module = import_module("kontra.scout.backends.duckdb_backend")
    path = tmp_path / "source.csv"
    pl.DataFrame({f"col{i}": [1, 1, 2, None] for i in range(4)}).write_csv(path)
    connections = []
    real = module.create_duckdb_connection

    class Connection:
        def __init__(self, con):
            self.con = con

        def __getattr__(self, k):
            return getattr(self.con, k)

        def execute(self, sql, *a, **kw):
            if (failure == "materialize" and "CREATE TEMP TABLE" in sql) or (
                failure == "distribution"
                and ("HISTOGRAM(" in sql or ("GROUP BY" in sql and 'ORDER BY "' in sql))
            ):
                raise duckdb.InvalidInputException("injected optional query failure")
            return self.con.execute(sql, *a, **kw)

    def connect(h):
        con = real(h)
        connections.append(con)
        return Connection(con)

    monkeypatch.setattr(module, "create_duckdb_connection", connect)
    if failure == "stats":

        def fail(*a):
            raise RuntimeError("injected stats failure")

        monkeypatch.setattr(Backend, "execute_stats_query", fail)
        with pytest.raises(RuntimeError, match="injected"):
            kontra.profile(str(path), preset="interrogate", save=False)
    else:
        p = kontra.profile(str(path), preset="interrogate", save=False)
        assert all(c.values == [1, 2] for c in p.columns)
        assert all(
            [(v.value, v.count) for v in c.top_values] == [(1, 2), (2, 1)] for c in p.columns
        )
    assert len(connections) == 1
    with pytest.raises(duckdb.ConnectionException):
        connections[0].execute("SELECT 1")


def test_batched_medium_cardinality_preserves_public_list_and_top_limits(tmp_path):
    path = tmp_path / "medium.parquet"
    pl.DataFrame(
        {"x": [i % 30 for i in range(1000)], "y": [i % 40 for i in range(1000)]}
    ).write_parquet(path)
    p = kontra.profile(str(path), preset="scan", top_n=3, list_values_threshold=5, save=False)
    assert all(c.values is None for c in p.columns)
    assert all(len(c.top_values) == 3 for c in p.columns)
    assert [v.count for v in p.get_column("x").top_values] == [34] * 3
    assert [v.count for v in p.get_column("y").top_values] == [25] * 3


def test_batched_distributions_keep_nan_separate(tmp_path):
    path = tmp_path / "nan.parquet"
    nan = float("nan")
    pl.DataFrame({"x": [nan, 1.0, nan, 1.0], "y": [False, True, False, True]}).write_parquet(path)
    batched = kontra.profile(str(path), preset="interrogate")
    alone = kontra.profile(str(path), preset="interrogate", columns=["x"])
    x, x_alone = (next(c for c in p.columns if c.name == "x") for p in (batched, alone))
    assert sorted(t.count for t in x.top_values) == [2, 2]
    assert [t.count for t in x.top_values] == [t.count for t in x_alone.top_values]
    assert len(x.values) == 2


@pytest.mark.parametrize(
    "frame",
    [
        pl.DataFrame({"tags": [["a"], ["b"], ["a"]], "k": [1, 2, 1]}),
        pl.DataFrame({
            "t": pl.Series([1, 2, 3] * 10, dtype=pl.Int64).cast(pl.Datetime("ns")),
            "k": [1, 2] * 15,
        }),
    ],
    ids=["nested", "timestamp_ns"],
)
def test_batched_distributions_skip_unsafe_key_types(tmp_path, frame):
    path = tmp_path / "keys.parquet"
    frame.write_parquet(path)
    assert_batched_matches_single_column(str(path), frame.columns[0], frame.height)


def test_batched_distributions_keep_infinite_dates_separate(tmp_path):
    path = tmp_path / "dates.parquet"
    duckdb.sql(
        "SELECT * FROM (VALUES (DATE 'infinity', 1), (DATE '9999-12-31', 2),"
        " (DATE 'infinity', 1), (DATE '9999-12-31', 2)) t(d, k)"
    ).write_parquet(str(path))
    assert_batched_matches_single_column(str(path), "d", 4)


def assert_batched_matches_single_column(path, name, height):
    batched = kontra.profile(path, preset="interrogate")
    alone = kontra.profile(path, preset="interrogate", columns=[name])
    col, col_alone = (next(c for c in p.columns if c.name == name) for p in (batched, alone))
    assert sorted(t.count for t in col.top_values) == sorted(t.count for t in col_alone.top_values)
    assert sum(t.count for t in col.top_values) == height
