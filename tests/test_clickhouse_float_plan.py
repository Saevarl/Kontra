"""ClickHouse float plan rewrite, without a server (live checks: tests/clickhouse)."""

from __future__ import annotations

from kontra.connectors.handle import DatasetHandle
from kontra.engine.executors.clickhouse_sql import ClickHouseSqlExecutor

HANDLE = DatasetHandle.from_uri("clickhouse://u:p@localhost:8123/db/t")

SPECS = [
    {"kind": "unique", "rule_id": "u", "column": "x", "tally": True},
    {"kind": "compare", "rule_id": "c", "left": "x", "right": "y", "op": "<", "tally": False},
    {
        "kind": "conditional_not_null",
        "rule_id": "w",
        "column": "s",
        "when_column": "x",
        "when_op": ">",
        "when_value": 0,
        "tally": True,
    },
    {"kind": "range", "rule_id": "r", "column": "x", "min": 0, "tally": True},
    {"kind": "not_null", "rule_id": "n", "column": "s", "tally": True},
]


def _plan(families):
    executor = ClickHouseSqlExecutor()
    compiled = executor.compile(SPECS)
    return compiled, executor._float_plan(HANDLE, compiled, families)


def test_non_float_columns_keep_compiled_plan():
    compiled, (plan, probes) = _plan({"x": "int64", "y": "uint32", "s": "string"})
    assert plan is compiled
    assert probes == []


def test_float_columns_get_typed_sql():
    compiled, (plan, probes) = _plan({"x": "float64", "y": "int64", "s": "string"})
    selects = dict(zip([s["rule_id"] for s in plan["aggregate_specs"]], plan["aggregate_selects"]))
    assert selects["u"] == (
        "(COUNT(`x`) - COUNT(DISTINCT multiIf(isNaN(`x`), nan, `x` = 0, 0, `x`))) AS `u`"
    )
    assert "isNaN(`x`) OR `x` > 0.0" in selects["w"]
    assert "isNaN(`x`)" in selects["r"]
    # Rules on other columns keep compile()'s SQL.
    original = dict(zip([s["rule_id"] for s in compiled["aggregate_specs"]], compiled["aggregate_selects"]))
    assert selects["n"] == original["n"]
    # The early-stop compare runs as a typed probe, beside the plan.
    assert [s["rule_id"] for s in plan["exists_specs"]] == []
    assert [spec["rule_id"] for spec, _ in probes] == ["c"]
    assert "toFloat64(`y`)" in probes[0][1] and "isNaN(`x`)" in probes[0][1]


def test_float32_literals_cast_to_float32():
    _, (plan, _) = _plan({"x": "float32", "y": "float32", "s": "string"})
    selects = dict(zip([s["rule_id"] for s in plan["aggregate_specs"]], plan["aggregate_selects"]))
    assert "`x` > toFloat32(0.0)" in selects["w"]
    assert "`x` < toFloat32(0.0)" in selects["r"]


def test_unexact_specs_left_to_polars():
    executor = ClickHouseSqlExecutor()
    specs = [
        {"kind": "compare", "rule_id": "dec", "left": "x", "right": "d", "op": "<", "tally": True},
        {
            "kind": "conditional_not_null",
            "rule_id": "nan",
            "column": "s",
            "when_column": "x",
            "when_op": "==",
            "when_value": float("nan"),
            "tally": True,
        },
        {"kind": "range", "rule_id": "big", "column": "x", "max": 2**60, "tally": True},
        {"kind": "not_null", "rule_id": "n", "column": "s", "tally": True},
    ]
    plan, probes = executor._float_plan(
        HANDLE, executor.compile(specs), {"x": "float64", "d": "decimal", "s": "string"}
    )
    assert [s["rule_id"] for s in plan["aggregate_specs"]] == ["n"]
    assert probes == []
