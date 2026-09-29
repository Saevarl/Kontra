"""Public profiling semantics and resource lifetime across optimized file paths."""

from importlib import import_module

import duckdb
import polars as pl
import pytest

import kontra


@pytest.mark.parametrize("preset", ["scout", "scan", "interrogate"])
def test_csv_quoted_names_multiline_nulls_and_rebinding(tmp_path, preset):
    path = tmp_path / "a'b.csv"
    df = pl.DataFrame({'odd"name': [1, 1, None, 3], "text": ["line\nbreak", "x", "x", None]})
    df.write_csv(path)
    p = kontra.profile(str(path), preset=preset, include_patterns=True)
    assert p.row_count == 4
    c = p.get_column('odd"name')
    assert (c.null_count, c.distinct_count) == (1, 2)
    assert c.values == [1, 3]
    if preset != "scout":
        assert (c.numeric.min, c.numeric.max, c.numeric.mean) == (1, 3, pytest.approx(5 / 3))
        assert [(v.value, v.count) for v in c.top_values] == [(1, 2), (3, 1)]
    # The same pathname now has different data AND a different inferred type.
    pl.DataFrame({'odd"name': ["new", "new"], "text": ["different", "different"]}).write_csv(path)
    q = kontra.profile(str(path), preset=preset)
    assert q.row_count == 2
    assert q.get_column('odd"name').dtype == "string"
    assert q.get_column('odd"name').values == ["new"]


@pytest.mark.parametrize("preset", ["scout", "scan", "interrogate"])
def test_sampled_csv_counts_projection_and_flags(tmp_path, preset):
    path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [7] * 1000, "unused": range(1000)}).write_csv(path)
    p = kontra.profile(str(path), preset=preset, sample=50, columns=["x"])
    assert p.sampled and p.sample_size == 50
    assert p.row_count == 50  # Preserve existing local-file sampling semantics.
    assert [c.name for c in p.columns] == ["x"]
    c = p.columns[0]
    assert (c.null_count, c.distinct_count) == (0, 1)
    assert c.null_count_estimated and c.distinct_count_estimated
    assert c.values == [7]


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("rows", [0, 5])
def test_empty_and_all_null_columns(tmp_path, suffix, rows):
    path = tmp_path / ("empty." + suffix)
    df = pl.DataFrame({"x": pl.Series([None] * rows, dtype=pl.Int64)})
    getattr(df, "write_" + suffix)(path)
    p = kontra.profile(str(path), preset="interrogate")
    assert p.row_count == rows
    assert p.columns[0].null_count == rows
    assert p.columns[0].distinct_count == 0
    assert p.columns[0].top_values == []


def test_nonfinite_values_keep_counts_and_finite_statistics(tmp_path):
    path = tmp_path / "nonfinite.parquet"
    pl.DataFrame(
        {"x": [float("nan"), float("inf"), float("-inf"), None, 1.0, 3.0, 3.0]}
    ).write_parquet(path)
    p = kontra.profile(str(path), preset="interrogate")
    c = p.columns[0]
    assert (c.null_count, c.distinct_count) == (1, 5)
    assert (c.numeric.min, c.numeric.max, c.numeric.mean) == (1.0, 3.0, pytest.approx(7 / 3))
    assert c.numeric.median == 3
    assert c.numeric.percentiles == {"p25": 2.0, "p75": 3.0, "p99": 3.0}


@pytest.mark.parametrize("failure", [None, "bind", "query"])
def test_owned_connection_closed_on_success_and_failure(tmp_path, monkeypatch, failure):
    module = import_module("kontra.scout.backends.duckdb_backend")
    original = module.create_duckdb_connection
    connections = []

    def connect(handle):
        con = original(handle)
        connections.append(con)
        return con

    monkeypatch.setattr(module, "create_duckdb_connection", connect)
    path = tmp_path / "input.csv"
    pl.DataFrame({"x": [1, 2, 3]}).write_csv(path)
    if failure:

        def fail(*args, **kwargs):
            raise RuntimeError("injected profiling failure")

        monkeypatch.setattr(
            module.DuckDBBackend,
            "_create_source_view" if failure == "bind" else "execute_stats_query",
            fail,
        )
        with pytest.raises(RuntimeError, match="injected"):
            kontra.profile(str(path))
    else:
        assert kontra.profile(str(path)).row_count == 3
    assert len(connections) == 1
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].execute("SELECT 1")


@pytest.mark.parametrize("percentiles", [[99, 25, 50, 75], [50], [0, 100, 25, 25]])
def test_exact_quantile_batch_matches_scalar_sql(tmp_path, percentiles):
    path = tmp_path / "quantiles.parquet"
    pl.DataFrame({"x": [-5.0, 1.0, 1.0, 2.0, 8.0, None, float("nan"), float("inf")]}).write_parquet(
        path
    )
    c = kontra.profile(str(path), preset="interrogate", percentiles=percentiles).columns[0]
    with duckdb.connect() as con:
        expected = con.execute(
            "SELECT "
            + ", ".join(
                f"PERCENTILE_CONT({p / 100}) WITHIN GROUP (ORDER BY CASE WHEN ISFINITE(x) THEN x END)"
                for p in [50] + [p for p in percentiles if p != 50]
            )
            + " FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    assert c.numeric.median == expected[0]
    assert c.numeric.percentiles == {
        f"p{p}": v for p, v in zip((p for p in percentiles if p != 50), expected[1:])
    }
