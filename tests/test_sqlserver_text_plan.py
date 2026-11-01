"""SQL Server exact-text plan rewrite, without a server (live checks: tests/sqlserver)."""

from __future__ import annotations

import pytest

from kontra.connectors.handle import DatasetHandle
from kontra.engine import sql_utils
from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

HANDLE = DatasetHandle.from_uri("mssql://u:p@localhost:1433/db/dbo.t")
KEY = "(CAST(CAST([s] AS NVARCHAR(MAX)) AS VARBINARY(MAX)) + 0x01)"

SPECS = [
    {"kind": "unique", "rule_id": "u", "column": "s", "tally": True},
    {"kind": "unique", "rule_id": "uf", "column": "s", "tally": False},
    {"kind": "allowed_values", "rule_id": "a", "column": "s", "values": ["abc", "x "], "tally": True},
    {"kind": "length", "rule_id": "l", "column": "s", "min": 1, "max": 3, "tally": True},
    {"kind": "starts_with", "rule_id": "p", "column": "s", "prefix": "a'%", "tally": True},
    {"kind": "compare", "rule_id": "c", "left": "s", "right": "s2", "op": "==", "tally": True},
    {
        "kind": "conditional_not_null",
        "rule_id": "w",
        "column": "i",
        "when_column": "s",
        "when_op": "!=",
        "when_value": "x",
        "tally": True,
    },
    {"kind": "metric", "rule_id": "m", "metric": "distinct_count", "column": "s", "tally": True},
    {"kind": "not_null", "rule_id": "n", "column": "s", "tally": True},
]


def _plan(texts, specs=SPECS):
    executor = SqlServerSqlExecutor()
    compiled = executor.compile(specs)
    return compiled, executor._text_plan(HANDLE, compiled, texts)


def _selects(plan):
    return dict(zip([s["rule_id"] for s in plan["aggregate_specs"]], plan["aggregate_selects"]))


def test_non_text_columns_keep_compiled_plan():
    compiled, (plan, probes) = _plan(set())
    assert plan is compiled
    assert probes == []


def test_text_columns_compare_exact_text():
    compiled, (plan, probes) = _plan({"s", "s2"})
    selects = _selects(plan)
    assert selects["u"] == f"(COUNT([s]) - COUNT(DISTINCT {KEY})) AS [u]"
    # 'abc' and 'x ' as UTF-16LE bytes plus the 0x01 end byte.
    assert f"{KEY} NOT IN (0x61006200630001, 0x7800200001)" in selects["a"]
    assert "(LEN((CAST([s] AS NVARCHAR(MAX)) + N'.') COLLATE Latin1_General_100_CI_AS_SC) - 1) < 1" in selects["l"]
    assert (
        "CAST([s] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 NOT LIKE N'a''\\%%' ESCAPE '\\'"
        in selects["p"]
    )
    assert f"NOT ({KEY} = {KEY.replace('[s]', '[s2]')})" in selects["c"]
    assert f"({KEY} <> 0x780001) AND [i] IS NULL" in selects["w"]
    assert selects["m"] == f"COUNT_BIG(DISTINCT {KEY}) AS [m]"
    # Rules that compare no text keep compile()'s SQL.
    assert selects["n"] == _selects(compiled)["n"]
    # The early-stop unique runs as a typed probe, beside the plan.
    assert plan["exists_specs"] == []
    assert [spec["rule_id"] for spec, _ in probes] == ["uf"]
    assert f"GROUP BY {KEY} HAVING COUNT(*) > 1" in probes[0][1]


def test_text_ordering_left_to_polars():
    specs = [
        {"kind": "compare", "rule_id": "lt", "left": "s", "right": "s2", "op": "<", "tally": True},
        {
            "kind": "conditional_not_null",
            "rule_id": "gt",
            "column": "i",
            "when_column": "s",
            "when_op": ">",
            "when_value": "a",
            "tally": True,
        },
        {"kind": "range", "rule_id": "r", "column": "s", "min": "2024-01-01", "tally": True},
        {"kind": "allowed_values", "rule_id": "num", "column": "s", "values": [1, 2], "tally": True},
        # Not text: a number bound, and a text column against a number.
        {"kind": "range", "rule_id": "ri", "column": "i", "min": 0, "tally": True},
        {"kind": "compare", "rule_id": "si", "left": "s", "right": "i", "op": "<", "tally": True},
    ]
    compiled, (plan, probes) = _plan({"s", "s2"}, specs)
    assert [s["rule_id"] for s in plan["aggregate_specs"]] == ["ri", "si"]
    assert plan["aggregate_selects"] == compiled["aggregate_selects"][-2:]
    assert probes == []


# What the builders render for the other dialects on main. The SQL Server
# rewrite must leave them byte for byte as they were.
OTHER_DIALECTS = {
    ('clickhouse', 'allowed_values'): "SUM(CASE WHEN `s` IS NULL THEN 1 WHEN toString(`s`) NOT IN ('abc', 'x ') THEN 1 ELSE 0 END) AS `r`",
    ('clickhouse', 'compare'): 'SUM(CASE WHEN `s` IS NULL OR `s2` IS NULL OR NOT (`s` = `s2`) THEN 1 ELSE 0 END) AS `r`',
    ('clickhouse', 'contains'): "EXISTS (SELECT 1 FROM t WHERE `s` IS NULL OR `s` NOT LIKE '%😀%' LIMIT 1) AS `r`",
    ('clickhouse', 'disallowed_values'): "EXISTS (SELECT 1 FROM t WHERE `s` IS NOT NULL AND toString(`s`) IN ('ABC') LIMIT 1) AS `r`",
    ('clickhouse', 'distinct_count'): 'COUNT(DISTINCT `s`) AS `r`',
    ('clickhouse', 'length'): 'SUM(CASE WHEN `s` IS NULL OR lengthUTF8(`s`) < 1 OR lengthUTF8(`s`) > 3 THEN 1 ELSE 0 END) AS `r`',
    ('clickhouse', 'starts_with'): "SUM(CASE WHEN `s` IS NULL OR `s` NOT LIKE 'a%' THEN 1 ELSE 0 END) AS `r`",
    ('clickhouse', 'unique'): '(COUNT(`s`) - COUNT(DISTINCT `s`)) AS `r`',
    ('clickhouse', 'when'): "SUM(CASE WHEN (`s` = 'x') AND `t` IS NULL THEN 1 ELSE 0 END) AS `r`",
    ('duckdb', 'allowed_values'): 'SUM(CASE WHEN "s" IS NULL THEN 1 WHEN "s" NOT IN (\'abc\', \'x \') THEN 1 ELSE 0 END) AS "r"',
    ('duckdb', 'compare'): 'SUM(CASE WHEN "s" IS NULL OR "s2" IS NULL OR NOT ("s" = "s2") THEN 1 ELSE 0 END) AS "r"',
    ('duckdb', 'contains'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NULL OR "s" NOT LIKE \'%😀%\' ESCAPE \'\\\' LIMIT 1) AS "r"',
    ('duckdb', 'disallowed_values'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NOT NULL AND "s" IN (\'ABC\') LIMIT 1) AS "r"',
    ('duckdb', 'distinct_count'): 'COUNT(DISTINCT "s") AS "r"',
    ('duckdb', 'length'): 'SUM(CASE WHEN "s" IS NULL OR LENGTH("s") < 1 OR LENGTH("s") > 3 THEN 1 ELSE 0 END) AS "r"',
    ('duckdb', 'starts_with'): 'SUM(CASE WHEN "s" IS NULL OR "s" NOT LIKE \'a%\' ESCAPE \'\\\' THEN 1 ELSE 0 END) AS "r"',
    ('duckdb', 'unique'): '(COUNT("s") - COUNT(DISTINCT "s")) AS "r"',
    ('duckdb', 'when'): 'SUM(CASE WHEN ("s" = \'x\') AND "t" IS NULL THEN 1 ELSE 0 END) AS "r"',
    ('postgres', 'allowed_values'): 'SUM(CASE WHEN "s" IS NULL THEN 1 WHEN "s"::text NOT IN (\'abc\', \'x \') THEN 1 ELSE 0 END) AS "r"',
    ('postgres', 'compare'): 'SUM(CASE WHEN "s" IS NULL OR "s2" IS NULL OR NOT ("s" = "s2") THEN 1 ELSE 0 END) AS "r"',
    ('postgres', 'contains'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NULL OR "s" NOT LIKE \'%😀%\' ESCAPE \'\\\' LIMIT 1) AS "r"',
    ('postgres', 'disallowed_values'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NOT NULL AND "s"::text IN (\'ABC\') LIMIT 1) AS "r"',
    ('postgres', 'distinct_count'): 'COUNT(DISTINCT "s") AS "r"',
    ('postgres', 'length'): 'SUM(CASE WHEN "s" IS NULL OR LENGTH("s") < 1 OR LENGTH("s") > 3 THEN 1 ELSE 0 END) AS "r"',
    ('postgres', 'starts_with'): 'SUM(CASE WHEN "s" IS NULL OR "s" NOT LIKE \'a%\' ESCAPE \'\\\' THEN 1 ELSE 0 END) AS "r"',
    ('postgres', 'unique'): '(COUNT("s") - COUNT(DISTINCT "s")) AS "r"',
    ('postgres', 'when'): 'SUM(CASE WHEN ("s" = \'x\') AND "t" IS NULL THEN 1 ELSE 0 END) AS "r"',
    ('trino', 'allowed_values'): 'count_if(("s" IS NULL) OR ("s" NOT IN (\'abc\', \'x \'))) AS "r"',
    ('trino', 'compare'): 'count_if(("s" IS NULL OR "s2" IS NULL OR NOT ("s" = "s2"))) AS "r"',
    ('trino', 'contains'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NULL OR "s" NOT LIKE \'%😀%\' ESCAPE \'\\\' LIMIT 1) AS "r"',
    ('trino', 'disallowed_values'): 'EXISTS (SELECT 1 FROM t WHERE "s" IS NOT NULL AND "s" IN (\'ABC\') LIMIT 1) AS "r"',
    ('trino', 'distinct_count'): 'COUNT(DISTINCT "s") AS "r"',
    ('trino', 'length'): 'count_if(("s" IS NULL OR LENGTH("s") < 1 OR LENGTH("s") > 3)) AS "r"',
    ('trino', 'starts_with'): 'count_if(("s" IS NULL OR "s" NOT LIKE \'a%\' ESCAPE \'\\\')) AS "r"',
    ('trino', 'unique'): '(COUNT("s") - COUNT(DISTINCT "s")) AS "r"',
    ('trino', 'when'): 'count_if((("s" = \'x\') AND "t" IS NULL)) AS "r"',
}

_CALLS = {
    "unique": lambda d: sql_utils.agg_unique("s", "r", d),
    "allowed_values": lambda d: sql_utils.agg_allowed_values("s", ["abc", "x "], "r", d),
    "disallowed_values": lambda d: sql_utils.exists_disallowed_values("s", ["ABC"], "t", "r", d),
    "length": lambda d: sql_utils.agg_length("s", 1, 3, "r", d),
    "starts_with": lambda d: sql_utils.agg_starts_with("s", "a", "r", d),
    "contains": lambda d: sql_utils.exists_contains("s", "😀", "t", "r", d),
    "compare": lambda d: sql_utils.agg_compare("s", "s2", "==", "r", d),
    "when": lambda d: sql_utils.agg_conditional_not_null("t", "s", "==", "x", "r", d),
    "distinct_count": lambda d: sql_utils.agg_metric("distinct_count", "s", "r", d),
}


@pytest.mark.parametrize("dialect, rule", sorted(OTHER_DIALECTS))
def test_other_dialects_render_unchanged(dialect, rule):
    assert _CALLS[rule](dialect) == OTHER_DIALECTS[(dialect, rule)]
