# tests/test_missing_column_routes.py
"""
A rule naming a column the data lacks fails with failure_mode "config_error"
on every route: Parquet metadata (preplan), DuckDB pushdown and Polars, for
Parquet, CSV and in-memory input, fail-fast or exact. It never passes, and
it never escapes as a raw engine error.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

import kontra
from kontra import rules
from kontra.config.models import RuleSpec
from kontra.rule_defs.factory import RuleFactory
from kontra.rule_defs.static_predicates import extract_static_predicates

MISSING = "zz"

# Every built-in rule that reads a column, once per column it reads.
RULES = {
    "not_null": lambda: rules.not_null(MISSING),
    "unique": lambda: rules.unique(MISSING),
    "dtype": lambda: rules.dtype(MISSING, "int64"),
    "range": lambda: rules.range(MISSING, min=0),
    "allowed_values": lambda: rules.allowed_values(MISSING, [1, 2, 3]),
    "disallowed_values": lambda: rules.disallowed_values(MISSING, [9]),
    "regex": lambda: rules.regex(MISSING, "^x"),
    "length": lambda: rules.length(MISSING, min=0, max=10),
    "contains": lambda: rules.contains(MISSING, "x"),
    "starts_with": lambda: rules.starts_with(MISSING, "x"),
    "ends_with": lambda: rules.ends_with(MISSING, "a"),
    "freshness": lambda: rules.freshness(MISSING, "1d"),
    "compare:left": lambda: rules.compare(MISSING, "b", ">="),
    "compare:right": lambda: rules.compare("a", MISSING, ">="),
    "conditional_not_null:column": lambda: rules.conditional_not_null(MISSING, "a == 1"),
    "conditional_not_null:when": lambda: rules.conditional_not_null("a", f"{MISSING} == 1"),
    "conditional_range:column": lambda: rules.conditional_range(MISSING, "a == 1", min=0),
    "conditional_range:when": lambda: rules.conditional_range("a", f"{MISSING} == 1", min=0),
}

ROUTES = list(itertools.product(["on", "off"], ["on", "off"], [False, True]))


def _frame() -> pl.DataFrame:
    # No nulls and in-range values, so any metadata proof on the present
    # columns would PASS: only the missing column can fail these rules.
    now = datetime.now(timezone.utc)
    return pl.DataFrame(
        {"a": [1, 2, 3], "b": [1, 2, 3], "s": ["xa", "xb", "xc"], "ts": [now] * 3}
    )


@pytest.fixture(scope="module")
def sources(tmp_path_factory):
    root = tmp_path_factory.mktemp("missing_column")
    df = _frame()
    df.write_parquet(root / "data.parquet")
    df.write_csv(root / "data.csv")
    return {
        "parquet": str(root / "data.parquet"),
        "csv": str(root / "data.csv"),
        "dataframe": df,
    }


@pytest.mark.parametrize("source", ["parquet", "csv", "dataframe"])
@pytest.mark.parametrize("preplan,pushdown,tally", ROUTES)
@pytest.mark.parametrize("rule", list(RULES))
def test_missing_column_is_a_config_error_on_every_route(
    sources, rule, source, preplan, pushdown, tally
):
    result = kontra.validate(
        sources[source],
        rules=[RULES[rule]()],
        save=False,
        preplan=preplan,
        pushdown=pushdown,
        tally=tally,
    )

    (outcome,) = result.rules
    assert outcome.passed is False
    assert outcome.failure_mode == "config_error"
    assert outcome.details["missing_columns"] == [MISSING]
    assert outcome.failed_count == 3


@pytest.mark.parametrize("source", ["parquet", "csv"])
@pytest.mark.parametrize("preplan,pushdown", [("on", "on"), ("on", "off"), ("off", "on"), ("off", "off")])
def test_present_columns_still_validate_beside_a_missing_one(sources, source, preplan, pushdown):
    result = kontra.validate(
        sources[source],
        rules=[
            rules.not_null("a", id="nn"),
            rules.range("b", min=2, id="range"),
            rules.conditional_not_null("a", f"{MISSING} == 1", id="cnn"),
        ],
        save=False,
        preplan=preplan,
        pushdown=pushdown,
    )

    by_id = {r.rule_id: r for r in result.rules}
    assert by_id["nn"].passed is True
    assert by_id["range"].passed is False
    assert by_id["range"].failure_mode != "config_error"
    assert by_id["cnn"].passed is False
    assert by_id["cnn"].failure_mode == "config_error"


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_duckdb_materializer_leaves_missing_columns_out(sources, fmt):
    """Remote files load through DuckDB; a missing column must not fail the load."""
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.materializers.duckdb import DuckDBMaterializer

    mat = DuckDBMaterializer(DatasetHandle.from_uri(sources[fmt]))

    assert mat.to_polars(["b", MISSING, "a"]).columns == ["b", "a"]
    # With nothing to keep, one column still carries the row count.
    assert mat.to_polars([MISSING]).height == 3


# --------------------------------------------------------------------------- #
# Metadata planners: conditional_not_null's proof ignores its condition, so a
# planner may settle it only when the condition's column exists.
# --------------------------------------------------------------------------- #


def _cnn_predicates(when: str):
    # Registers the rule; otherwise these tests depend on another test having
    # imported it first.
    import kontra.rule_defs.builtin.conditional_not_null  # noqa: F401

    spec = RuleSpec(name="conditional_not_null", id="cnn", params={"column": "a", "when": when})
    return extract_static_predicates(rules=RuleFactory([spec]).build_rules())


def test_static_predicate_names_the_condition_column():
    assert _cnn_predicates(f"{MISSING} == 1") == [("cnn", "a", "not_null_when", MISSING)]


@pytest.mark.parametrize("when,expected", [("b == 1", "pass_meta"), (f"{MISSING} == 1", "unknown")])
def test_parquet_planner(sources, when, expected):
    from kontra.preplan.planner import preplan_single_parquet

    pre = preplan_single_parquet(sources["parquet"], ["a"], _cnn_predicates(when))

    assert pre.rule_decisions == {"cnn": expected}


# "__table__" is the planners' key for table stats, not a column.
@pytest.mark.parametrize(
    "when,expected",
    [("b == 1", "pass_meta"), (f"{MISSING} == 1", "unknown"), ("__table__ == 1", "unknown")],
)
def test_postgres_planner(monkeypatch, when, expected):
    import kontra.preplan.postgres as postgres_preplan

    stats = {"__table__": {"row_estimate": 3}, "a": {"null_frac": 0.0}, "b": {"null_frac": 0.0}}
    monkeypatch.setattr(postgres_preplan, "fetch_pg_stats_for_preplan", lambda *a, **k: stats)

    pre = postgres_preplan.preplan_postgres(
        SimpleNamespace(db_params=object()), ["a"], _cnn_predicates(when)
    )

    assert pre.rule_decisions == {"cnn": expected}


# "__table__" is the planners' key for table stats, not a column.
@pytest.mark.parametrize(
    "when,expected",
    [("b == 1", "pass_meta"), (f"{MISSING} == 1", "unknown"), ("__table__ == 1", "unknown")],
)
def test_sqlserver_planner(monkeypatch, when, expected):
    import kontra.preplan.sqlserver as sqlserver_preplan

    column = {"is_nullable": False, "is_identity": False, "has_unique_constraint": False}
    metadata = {"__table__": {"row_estimate": 3}, "a": column, "b": column}
    monkeypatch.setattr(sqlserver_preplan, "fetch_sqlserver_metadata", lambda params: metadata)

    pre = sqlserver_preplan.preplan_sqlserver(
        SimpleNamespace(db_params=object()), ["a"], _cnn_predicates(when)
    )

    assert pre.rule_decisions == {"cnn": expected}


@pytest.mark.parametrize("when,expected", [("b == 1", "pass_meta"), (f"{MISSING} == 1", "unknown")])
def test_clickhouse_planner(monkeypatch, when, expected):
    import kontra.preplan.clickhouse as clickhouse_preplan

    monkeypatch.setattr(clickhouse_preplan, "_db_and_table", lambda handle: ("db", "t"))
    monkeypatch.setattr(
        clickhouse_preplan, "_fetch_column_types", lambda *a: {"a": "Int64", "b": "Int64"}
    )
    monkeypatch.setattr(clickhouse_preplan, "_fetch_row_count", lambda *a: 3)

    pre = clickhouse_preplan.preplan_clickhouse(object(), ["a"], _cnn_predicates(when))

    assert pre.rule_decisions == {"cnn": expected}
