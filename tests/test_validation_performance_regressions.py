"""Correctness and ownership guards for validation performance changes."""

import polars as pl
import pytest

import kontra
from kontra import rules


@pytest.mark.parametrize("values", [[-1, 20, 30], [20, 30, 150], [-5, -3, -1]])
@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_range_requires_both_metadata_bounds(tmp_path, values, pushdown):
    path = tmp_path / "range.parquet"
    pl.DataFrame({"age": values}).write_parquet(path, row_group_size=1)
    result = kontra.validate(
        str(path), rules=[rules.range("age", min=0, max=120)], pushdown=pushdown, save=False
    )
    assert not result.passed
    assert result.rules[0].failed_count >= 1


@pytest.mark.parametrize("values", [[1, 2, None], [1, 1, 1, None, None]])
def test_unique_preserves_duplicate_count_and_failure_samples(values):
    from kontra.rule_defs.builtin.unique import UniqueRule

    rule = UniqueRule("unique", {"column": "id"})
    df = pl.DataFrame({"id": values})
    result = rule.validate(df)
    expected = 0 if len(values) == 3 else 2
    assert result["failed_count"] == expected
    if expected:
        assert df.filter(result["_failure_mask"])["id"].to_list() == [1, 1, 1]
    else:
        assert "_failure_mask" not in result


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("values", [[], [1, 1, 2, None, None]])
def test_batched_sql_counts_include_empty_and_null_inputs(tmp_path, suffix, values):
    df = pl.DataFrame({"id": values}, schema={"id": pl.Int64})
    path = tmp_path / f"counts.{suffix}"
    if suffix == "csv":
        df.write_csv(path)
    else:
        df.write_parquet(path)
    spec = [rules.unique("id", tally=True), rules.not_null("id", tally=True), rules.min_rows(1)]
    actual = kontra.validate(str(path), rules=spec, preplan="off", save=False)
    expected = kontra.validate(df, rules=spec, save=False)
    assert sorted((r.rule_id, r.passed, r.failed_count) for r in actual.rules) == sorted(
        (r.rule_id, r.passed, r.failed_count) for r in expected.rules
    )


@pytest.mark.parametrize("operation", ["execute", "introspect"])
@pytest.mark.parametrize("raises", [False, True])
def test_duckdb_executor_closes_owned_connection(tmp_path, monkeypatch, operation, raises):
    import duckdb

    import kontra.engine.executors.duckdb_sql as module
    from kontra.connectors.handle import DatasetHandle

    path = tmp_path / "owned.parquet"
    pl.DataFrame({"id": [1, 2]}).write_parquet(path)
    handle = DatasetHandle.from_uri(str(path))
    con = duckdb.connect()
    monkeypatch.setattr(module, "create_duckdb_connection", lambda handle: con)
    if raises:

        def fail(*args, **kwargs):
            raise duckdb.IOException("injected source failure")

        monkeypatch.setattr(module, "_create_source_view", fail)
    executor = module.DuckDBSqlExecutor()

    def invoke():
        if operation == "introspect":
            return executor.introspect(handle)
        plan = executor.compile([{"kind": "not_null", "column": "id", "rule_id": "id"}])
        return executor.execute(handle, plan)

    if raises:
        with pytest.raises(duckdb.IOException):
            invoke()
    else:
        invoke()
    with pytest.raises(duckdb.ConnectionException):
        con.execute("SELECT 1")
