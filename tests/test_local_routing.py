"""Public API equivalence and conservative routing/ownership regressions."""

import polars as pl
import pytest

import kontra
from kontra import rules


def signature(result):
    return sorted((r.rule_id, r.passed, r.failed_count, r.severity) for r in result.rules)


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize(
    "values", [[], [None, None], [1.0, 1.0, None, float("nan"), float("nan"), -1.0, float("inf")]]
)
def test_local_route_matches_sql_null_nan_and_duplicate_counts(tmp_path, suffix, values):
    path = tmp_path / f"counts.{suffix}"
    df = pl.DataFrame({"id": values}, schema={"id": pl.Float64})
    getattr(df, f"write_{suffix}")(path)
    spec = [rules.not_null("id"), rules.unique("id"), rules.range("id", min=0, max=2)]
    kwargs = {"rules": spec, "tally": True, "save": False, "sample": 0}
    actual = kontra.validate(str(path), **kwargs)
    sql = kontra.validate(str(path), csv_mode="duckdb", **kwargs)
    assert signature(actual) == signature(sql)


def test_mixed_numeric_allowed_values_matches_sql(tmp_path):
    path = tmp_path / "mixed.parquet"
    pl.DataFrame({"x": [1.0, 2.5]}).write_parquet(path)
    kwargs = {"rules": [rules.allowed_values("x", [1, 2.5])], "tally": True, "save": False}
    actual = kontra.validate(str(path), **kwargs)
    sql = kontra.validate(str(path), preplan="off", **kwargs)
    assert signature(actual) == signature(sql)
    assert actual.rules[0].passed


@pytest.mark.parametrize("bounds", [{"min": 0}, {"max": 2}, {"min": 0, "max": 2}])
def test_float_range_nan_matches_sql(tmp_path, bounds):
    path = tmp_path / "nan.parquet"
    pl.DataFrame({"x": [1.0, float("nan"), 1.5]}).write_parquet(path)
    kwargs = {"rules": [rules.range("x", **bounds)], "tally": True, "save": False, "sample": 0}
    actual = kontra.validate(str(path), **kwargs)
    sql = kontra.validate(str(path), preplan="off", **kwargs)
    assert signature(actual) == signature(sql)
    assert actual.rules[0].source == sql.rules[0].source == "sql"


@pytest.mark.parametrize(
    "text",
    [
        'value;label\n1;""\n2;hello\n;world\n',
        'value,label\n001,""\n002,"two\nlines"\n,hello\n',
        'value,label\ntrue,"a,b"\nfalse,""\n,hello\n',
        "value,label\n2024-01-01,a\n2024-02-01,b\n,c\n",
    ],
)
def test_csv_keeps_duckdb_dialect_and_inference(tmp_path, text):
    path = tmp_path / "reader's.csv"
    path.write_text(text)
    kwargs = {
        "rules": [rules.not_null("value"), rules.not_null("label"), rules.unique("value")],
        "tally": True,
        "save": False,
    }
    assert signature(kontra.validate(str(path), **kwargs)) == signature(
        kontra.validate(str(path), csv_mode="duckdb", **kwargs)
    )


@pytest.mark.parametrize(
    "reason", ["fast", "mixed", "forced", "budget", "custom", "coercion", "preplan-off"]
)
def test_ineligible_plans_keep_sql(tmp_path, monkeypatch, reason):
    import kontra.engine.phases.local_route as route

    path = tmp_path / "source.parquet"
    pl.DataFrame({"id": [1, 2, 3]}).write_parquet(path)
    spec = [rules.not_null("id")]
    kwargs = {"tally": True}
    if reason == "fast":
        kwargs["tally"] = False
    if reason == "mixed":
        spec.append(rules.unique("id", tally=False))
    if reason == "forced":
        kwargs["csv_mode"] = "duckdb"
    if reason == "budget":
        monkeypatch.setattr(route, "_MAX_PARQUET_BYTES", 0)
    if reason == "custom":
        spec.append(rules.min_rows(1))
    if reason == "coercion":
        spec = [rules.allowed_values("id", ["1", "2", "3"])]
    result = kontra.validate(
        str(path),
        rules=spec,
        preplan="off" if reason == "preplan-off" else "on",
        save=False,
        **kwargs,
    )
    assert all(r.source in ("sql", "metadata") for r in result.rules)


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
def test_routing_projection_samples_severity_and_fresh_reads(tmp_path, suffix):
    path = tmp_path / f"source.{suffix}"
    df = pl.DataFrame({"id": [1, 1, 2], "age": [-1, 30, 150], "unused": ["a", "b", "c"]})
    getattr(df, f"write_{suffix}")(path)
    spec = [rules.unique("id", severity="warning"), rules.range("age", min=0, max=120)]
    kwargs = {"rules": spec, "tally": True, "sample": 2, "save": False}
    result = kontra.validate(str(path), **kwargs)
    assert all(r.source == ("polars" if suffix == "parquet" else "sql") for r in result.rules)
    assert signature(result) == signature(kontra.validate(str(path), csv_mode="duckdb", **kwargs))
    assert next(r for r in result.rules if r.rule_id.endswith(":range")).failed_count == 2
    assert all(r.samples is not None for r in result.rules)
    # Same URI, completely different data: no cross-call materialization reuse.
    getattr(pl.DataFrame({"id": [1, 2], "age": [20, 30], "unused": ["x", "y"]}), f"write_{suffix}")(
        path
    )
    assert kontra.validate(str(path), **kwargs).passed


def test_parquet_native_reader_failure_falls_back_to_sql(tmp_path, monkeypatch):
    from kontra.engine.materializers.polars_connector import PolarsConnectorMaterializer

    path = tmp_path / "source.parquet"
    pl.DataFrame({"id": [1, None, 3]}).write_parquet(path)

    def fail(*a, **kw):
        raise pl.exceptions.ComputeError("injected reader failure")

    monkeypatch.setattr(PolarsConnectorMaterializer, "to_polars", fail)
    result = kontra.validate(str(path), rules=[rules.not_null("id")], tally=True, save=False)
    assert result.rules[0].source == "sql"
    assert result.rules[0].failed_count == 1


@pytest.mark.parametrize("projection", [True, False])
def test_exact_rules_read_all_row_groups_and_preserve_samples(tmp_path, projection):
    path = tmp_path / "groups.parquet"
    pl.DataFrame({"id": [1, 2, 1, None], "unused": ["a", "b", "c", "d"]}).write_parquet(
        path, row_group_size=2
    )
    result = kontra.validate(
        str(path),
        rules=[rules.not_null("id"), rules.unique("id")],
        tally=True,
        sample=5,
        save=False,
        projection=projection,
    )
    assert all(r.failed_count == 1 for r in result.rules)
    assert result.total_rows == 4
    for r in result.rules:
        assert r.samples
        assert all("unused" in row for row in r.samples)


@pytest.mark.parametrize("kind", ["range", "allowed_values"])
@pytest.mark.parametrize(
    "dtype,values,bound",
    [
        (pl.Int64, [2**53 - 1, 2**53, 2**53 + 1, 2**53 + 2], float(2**53)),
        (pl.Int64, [1, 2, 3], 10**100),
        (pl.UInt64, [2**64 - 3, 2**64 - 2, 2**64 - 1], 2**64 - 2),
        (pl.Float32, [0.1, 0.2, 0.3], 0.1),
    ],
)
def test_numeric_coercion_and_oversized_literals_retain_sql(tmp_path, kind, dtype, values, bound):
    path = tmp_path / "numeric.parquet"
    pl.DataFrame({"x": values}, schema={"x": dtype}).write_parquet(path)
    spec = rules.range("x", max=bound) if kind == "range" else rules.allowed_values("x", [bound])
    kwargs = {"rules": [spec], "tally": True, "save": False}
    actual = kontra.validate(str(path), **kwargs)
    expected = kontra.validate(str(path), csv_mode="duckdb", **kwargs)
    assert signature(actual) == signature(expected)
    assert actual.rules[0].source == "sql"


def test_local_route_does_not_import_arrow_or_numpy(tmp_path):
    """File-only routing must not pay for an unused native metadata reader."""
    import subprocess
    import sys

    path = tmp_path / "local.parquet"
    pl.DataFrame({"id": [1, 1, None]}).write_parquet(path, row_group_size=1)
    code = """
import sys
import kontra
from kontra import rules
r = kontra.validate(sys.argv[1], rules=[rules.unique('id'), rules.not_null('id')],
                    tally=True, sample=0, save=False)
assert all(x.source == 'polars' and x.failed_count == 1 for x in r.rules)
assert not any(m.split('.')[0] in ('pyarrow', 'numpy') for m in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(path)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("size", [None, -1, 64 * 1024 * 1024 + 1])
def test_unknown_or_excessive_uncompressed_size_keeps_sql(tmp_path, monkeypatch, size):
    from dataclasses import replace

    from kontra.preplan import planner

    path = tmp_path / "budget.parquet"
    pl.DataFrame({"id": [1, None, 3]}).write_parquet(path)
    original = planner.read_parquet_meta
    monkeypatch.setattr(
        planner, "read_parquet_meta", lambda p: replace(original(p), total_byte_size=size)
    )
    result = kontra.validate(str(path), rules=[rules.not_null("id")], tally=True, save=False)
    assert result.rules[0].source == "sql"
    assert result.rules[0].failed_count == 1


def test_local_route_reuses_preplan_budget_and_schema_failure_falls_back(tmp_path, monkeypatch):
    from kontra.preplan import planner

    path = tmp_path / "source.parquet"
    pl.DataFrame({"id": [1, None, 3]}).write_parquet(path)
    original = planner.read_parquet_meta
    reads = []

    def read(p):
        reads.append(p)
        return original(p)

    monkeypatch.setattr(planner, "read_parquet_meta", read)
    kwargs = {"rules": [rules.not_null("id")], "tally": True, "save": False}
    assert kontra.validate(str(path), **kwargs).rules[0].source == "polars"
    assert reads == [str(path)]

    def fail(*a, **kw):
        raise pl.exceptions.ComputeError("injected schema failure")

    monkeypatch.setattr(pl, "read_parquet_schema", fail)
    result = kontra.validate(str(path), **kwargs)
    assert result.rules[0].source == "sql"
    assert result.rules[0].failed_count == 1
