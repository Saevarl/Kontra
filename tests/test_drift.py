"""drift rule: parameters, verdicts, measurement routes and history backends."""

from __future__ import annotations

import json
import math
import warnings
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

import kontra
from kontra import rules
from kontra.config.models import RuleSpec
from kontra.engine.phases.compilation import _ensure_builtin_rules_registered
from kontra.rule_defs.factory import RuleFactory
from kontra.state.backends.local import LocalStore
from kontra.state.types import RuleState, StateSummary, ValidationState

_ensure_builtin_rules_registered()

from kontra.rule_defs.builtin.drift import DriftRule  # noqa: E402  (after registration)


def build(params, *, id=None):
    """A drift rule built the way a contract builds it."""
    return RuleFactory([RuleSpec(name="drift", params=params, id=id)]).build_rules()[0]


def judged(params, history, value):
    rule = build(params)
    rule.set_history(history)
    return rule.evaluate(value)


@pytest.fixture()
def workdir(tmp_path, monkeypatch):
    """A cwd of its own, so the default local store starts empty."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def by_name(result):
    return {r.rule_id: r for r in result.rules}


def status(rule_result):
    return rule_result.details["status"]


# --------------------------------------------------------------------------- #
# Parameters, ids and builders
# --------------------------------------------------------------------------- #


class TestParameters:
    @pytest.mark.parametrize(
        "params, bad",
        [
            ({"metric": "median", "column": "a", "max_change": 1}, "metric"),
            ({"metric": "null_rate", "max_change": 1}, "column"),
            ({"metric": "null_rate", "column": " ", "max_change": 1}, "column"),
            ({"metric": "row_count", "column": "a", "max_change": 1}, "column"),
            ({"metric": "row_count"}, "max_change"),
            ({"metric": "row_count", "max_change": -1}, "max_change"),
            ({"metric": "row_count", "max_change": float("nan")}, "max_change"),
            ({"metric": "row_count", "max_change": True}, "max_change"),
            ({"metric": "row_count", "max_change_pct": "5"}, "max_change_pct"),
            ({"metric": "row_count", "max_change": 1, "baseline": "mode"}, "baseline"),
            ({"metric": "row_count", "max_change": 1, "window": 3}, "window"),
            ({"metric": "row_count", "max_change": 1, "baseline": "median", "window": 0}, "window"),
            ({"metric": "row_count", "max_change": 1, "on_no_baseline": "warn"}, "on_no_baseline"),
            ({"metric": "row_count", "max_chnage": 1}, "max_chnage"),
        ],
    )
    def test_invalid_params_name_the_parameter(self, params, bad):
        from kontra.errors import RuleParameterError

        with pytest.raises(RuleParameterError, match=f"'{bad}'"):
            build(params)

    def test_defaults(self):
        rule = build({"metric": "row_count", "max_change_pct": 10})
        assert (rule.baseline, rule.window, rule.on_no_baseline) == ("previous", 1, "pass")
        assert rule.supports_tally is False
        assert rule.rule_scope == "dataset"
        median = build({"metric": "mean", "column": "x", "max_change": 1, "baseline": "median"})
        assert median.window == 7
        assert median.rule_scope == "column"
        assert median.history_needed == 7

    def test_rule_ids_carry_the_metric(self):
        built = RuleFactory([
            RuleSpec(name="drift", params={"metric": "null_rate", "column": "email", "max_increase": 0.05}),
            RuleSpec(name="drift", params={"metric": "distinct_count", "column": "email", "max_change_pct": 5}),
            RuleSpec(name="drift", params={"metric": "row_count", "max_decrease_pct": 20}),
            RuleSpec(name="drift", params={"metric": "row_count", "max_increase_pct": 50}, id="growth"),
            RuleSpec(name="not_null", params={"column": "email"}),
        ]).build_rules()
        assert [r.rule_id for r in built] == [
            "COL:email:drift:null_rate",
            "COL:email:drift:distinct_count",
            "DATASET:drift:row_count",
            "growth",
            "COL:email:not_null",
        ]

    def test_same_metric_twice_needs_an_id(self):
        from kontra.errors import DuplicateRuleIdError

        with pytest.raises(DuplicateRuleIdError):
            RuleFactory([
                RuleSpec(name="drift", params={"metric": "row_count", "max_decrease_pct": 20}),
                RuleSpec(name="drift", params={"metric": "row_count", "max_increase_pct": 50}),
            ]).build_rules()

    def test_tally_is_unsupported(self):
        with pytest.warns(UserWarning, match="does not support tally"):
            built = RuleFactory(
                [RuleSpec(name="drift", params={"metric": "row_count", "max_change": 1}, tally=True)]
            ).build_rules()
        assert built[0].tally is None

    def test_builder_matches_contract_form(self):
        rule = rules.drift(
            "row_count", baseline="median", window=5,
            max_decrease_pct=20, on_no_baseline="fail", severity="warning", context={"owner": "x"},
        )
        assert rule == {
            "name": "drift",
            "params": {
                "metric": "row_count",
                "max_decrease_pct": 20,
                "baseline": "median",
                "window": 5,
                "on_no_baseline": "fail",
            },
            "severity": "warning",
            "context": {"owner": "x"},
        }
        assert rules.drift("null_rate", "email", max_increase=0.05)["params"] == {
            "metric": "null_rate", "column": "email", "max_increase": 0.05,
        }

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            ({"metric": "null_rate"}, "column"),
            ({"metric": "row_count"}, "at least one limit"),
            ({"metric": "row_count", "max_change": 1, "severity": "fatal"}, "severity"),
        ],
    )
    def test_builder_rejects_bad_input(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            rules.drift(**kwargs)

    def test_yaml_contract(self, workdir):
        pl.DataFrame({"a": [1, 2, None]}).write_parquet("d.parquet")
        (workdir / "c.yml").write_text(
            "name: drift_yaml\n"
            "datasource: d.parquet\n"
            "rules:\n"
            "  - name: drift\n"
            "    params: { metric: null_count, column: a, max_increase: 0 }\n"
            "    severity: warning\n"
            "    context: { owner: data }\n",
            encoding="utf-8",
        )
        result = kontra.validate(contract="c.yml")
        rule = result.rules[0]
        assert rule.rule_id == "COL:a:drift:null_count"
        assert rule.name == "drift"
        assert rule.severity == "warning"
        assert rule.context == {"owner": "data"}
        assert rule.details["value"] == 1
        assert status(rule) == "no_baseline"

    def test_dry_run_accepts_and_rejects(self):
        ok = kontra.validate(rules=[rules.drift("row_count", max_change=1)], dry_run=True)
        assert ok.valid
        bad = kontra.validate(rules=[{"name": "drift", "params": {"metric": "row_count"}}], dry_run=True)
        assert not bad.valid


# --------------------------------------------------------------------------- #
# Verdicts against a given history
# --------------------------------------------------------------------------- #


class TestVerdict:
    def test_no_history_passes_but_says_so(self):
        result = judged({"metric": "null_rate", "column": "email", "max_increase": 0.05}, [], 0.1)
        assert result["passed"] is True
        assert result["failed_count"] == 0
        assert result["message"].startswith("No baseline: null_rate of 'email' is 0.1")
        assert "found 0" in result["message"]
        assert result["details"]["status"] == "no_baseline"
        assert result["details"]["value"] == 0.1
        assert result["details"]["baseline"] is None
        assert "failure_mode" not in result

    def test_history_never_attached_is_no_baseline(self):
        rule = build({"metric": "row_count", "max_change": 1})
        assert rule.evaluate(10)["details"]["status"] == "no_baseline"

    def test_no_history_fails_when_configured(self):
        result = judged({"metric": "row_count", "max_change": 1, "on_no_baseline": "fail"}, [], 10)
        assert result["passed"] is False
        assert result["failed_count"] == 1
        assert result["failure_mode"] == "no_baseline"
        assert result["details"]["status"] == "no_baseline"

    def test_history_read_error_is_reported(self):
        rule = build({"metric": "row_count", "max_change": 1})
        rule.set_history(None, error="OperationalError: down")
        result = rule.evaluate(5)
        assert result["passed"] is True
        assert result["details"]["status"] == "no_baseline"
        assert result["details"]["history_error"] == "OperationalError: down"
        assert "history could not be read (OperationalError: down)" in result["message"]

    def test_previous_baseline_uses_newest_run(self):
        result = judged({"metric": "row_count", "max_change": 5}, [100, 10, 10], 103)
        assert result["passed"] is True
        assert result["details"]["baseline"] == 100
        assert result["details"]["change"] == 3
        assert result["details"]["history_runs"] == 1
        assert result["details"]["status"] == "ok"
        assert "against 100 in the previous run (+3, +3.0%), within limits" in result["message"]

    def test_median_and_mean_over_window(self):
        history = [100, 90, 200, 80, 10_000]  # newest first; the last is outside window=4
        median = judged(
            {"metric": "row_count", "max_decrease_pct": 20, "baseline": "median", "window": 4}, history, 76
        )
        assert median["details"]["baseline"] == 95
        assert median["details"]["history_runs"] == 4
        assert median["passed"] is True  # -20%
        mean = judged(
            {"metric": "row_count", "max_decrease_pct": 20, "baseline": "mean", "window": 4}, history, 93
        )
        assert mean["details"]["baseline"] == 117.5
        assert mean["passed"] is False
        assert mean["details"]["exceeded"] == ["max_decrease_pct"]
        assert "the mean of the last 4 runs" in mean["message"]

    def test_short_history_uses_what_exists(self):
        result = judged(
            {"metric": "row_count", "max_change": 1, "baseline": "median", "window": 7}, [10, 20], 15
        )
        assert result["details"]["baseline"] == 15
        assert result["details"]["history_runs"] == 2

    @pytest.mark.parametrize(
        "limit, value, passed",
        [
            ({"max_increase": 5}, 105, True),
            ({"max_increase": 5}, 106, False),
            ({"max_increase": 5}, 0, True),  # a fall is not an increase
            ({"max_decrease": 5}, 95, True),
            ({"max_decrease": 5}, 94, False),
            ({"max_decrease": 5}, 1000, True),
            ({"max_change": 5}, 94, False),
            ({"max_change": 5}, 106, False),
            ({"max_change": 5}, 96, True),
            ({"max_increase_pct": 10}, 110, True),
            ({"max_increase_pct": 10}, 111, False),
            ({"max_decrease_pct": 10}, 90, True),
            ({"max_decrease_pct": 10}, 89, False),
            ({"max_change_pct": 10}, 89, False),
            ({"max_increase": 50, "max_decrease_pct": 1}, 98, False),
            ({"max_increase": 50, "max_decrease_pct": 1}, 140, True),
        ],
    )
    def test_limit_edges(self, limit, value, passed):
        result = judged({"metric": "row_count", **limit}, [100], value)
        assert result["passed"] is passed
        assert result["details"]["status"] == ("ok" if passed else "drifted")
        if not passed:
            assert result["failure_mode"] == "metric_drift"
            assert result["failed_count"] == 1

    @pytest.mark.parametrize("before, after", [(0.02, 0.07), (0.25, 0.30), (0.1, 0.15), (0.35, 0.30)])
    def test_a_change_equal_to_the_limit_passes_despite_float_error(self, before, after):
        result = judged(
            {"metric": "null_rate", "column": "c", "max_change": 0.05}, [before], after
        )
        assert result["passed"] is True

    def test_percent_from_a_zero_baseline(self):
        params = {"metric": "null_count", "column": "c", "max_increase_pct": 1000}
        rise = judged(params, [0], 1)
        assert rise["passed"] is False
        assert rise["details"]["change_pct"] is None
        assert "rose from 0 to 1 (+1)" in rise["message"]
        assert judged(params, [0], 0)["passed"] is True
        # A fall from zero can't break an increase limit, but does break a decrease one.
        neg = {"metric": "mean", "column": "c", "max_decrease_pct": 50, "max_increase_pct": 50}
        assert judged(neg, [0.0], -2.0)["details"]["exceeded"] == ["max_decrease_pct"]

    def test_negative_baseline_percent_uses_magnitude(self):
        result = judged({"metric": "mean", "column": "c", "max_increase_pct": 10}, [-100.0], -95.0)
        assert result["details"]["change_pct"] == pytest.approx(5.0)
        assert result["passed"] is True

    def test_undefined_value_with_a_baseline_fails(self):
        result = judged({"metric": "mean", "column": "c", "max_change": 1}, [5.0], None)
        assert result["passed"] is False
        assert result["details"]["status"] == "no_value"
        assert result["details"]["value"] is None
        assert "undefined (no non-null values)" in result["message"]
        empty = judged({"metric": "null_rate", "column": "c", "max_change": 1}, [0.1], float("nan"))
        assert "the dataset is empty" in empty["message"]

    def test_undefined_value_without_history_is_no_baseline(self):
        result = judged({"metric": "mean", "column": "c", "max_change": 1}, [], None)
        assert result["passed"] is True
        assert result["details"]["status"] == "no_baseline"
        assert "is undefined" in result["message"]

    def test_values_are_plain_json_numbers(self):
        from decimal import Decimal

        result = judged({"metric": "mean", "column": "c", "max_change": 1}, [1.5], Decimal("2.25"))
        assert result["details"]["value"] == 2.25 and type(result["details"]["value"]) is float
        count = judged({"metric": "row_count", "max_change": 1}, [10], 10.0)
        assert type(count["details"]["value"]) is int
        json.dumps(result)  # NaN/inf/Decimal never reach stored state

    def test_integers_beyond_float_precision_compare_exactly(self):
        import numpy as np

        params = {"metric": "max", "column": "x", "max_change": 0}
        result = judged(params, [2**53], np.int64(2**53 + 1))
        assert result["passed"] is False
        assert result["details"]["value"] == 2**53 + 1 and type(result["details"]["value"]) is int
        assert result["details"]["change"] == 1
        assert judged(params, [2**53], 2**53)["passed"] is True
        assert judged({**params, "max_change": 1}, [2**62], 2**62 + 2)["passed"] is False

    @pytest.mark.parametrize("baseline, history", [
        ("mean", [2**53]),
        ("median", [2**53, 2**53]),
        ("median", [2**53, 2**53 + 2]),  # midpoint 2**53 + 1, a whole number
    ])
    def test_aggregated_integer_baselines_stay_exact(self, baseline, history):
        params = {"metric": "max", "column": "x", "max_change": 0, "baseline": baseline,
                  "window": 2}
        assert judged(params, history, 2**53 + 1)["passed"] is (baseline == "median"
                                                               and history[1] == 2**53 + 2)
        assert judged(params, history, 2**53 + 3)["passed"] is False

    def test_fractional_integer_baseline(self):
        params = {"metric": "max", "column": "x", "max_change": 0, "baseline": "median",
                  "window": 2}
        result = judged(params, [2**53, 2**53 + 1], 2**53 + 1)
        assert result["passed"] is False
        assert result["details"]["change"] == 0.5

    def test_sql_non_numeric_measurement_is_config_error(self):
        rule = build({"metric": "max", "column": "name", "max_change": 1})
        rule.set_history([1.0])
        result = rule.evaluate_measurement("zebra")
        assert result["passed"] is False
        assert result["failure_mode"] == "config_error"
        assert "needs a numeric column" in result["message"]

    def test_stored_measurement_must_match_metric_and_column(self):
        rule = build({"metric": "null_rate", "column": "a", "max_change": 1}, id="watch")

        def state(name, details):
            return RuleState(rule_id="watch", rule_name=name, passed=True, failed_count=0,
                             execution_source="sql", details=details)

        assert rule.stored_measurement(state("drift", {"metric": "null_rate", "column": "a", "value": 0.5})) == 0.5
        assert rule.stored_measurement(state("drift", {"metric": "null_count", "column": "a", "value": 3})) is None
        assert rule.stored_measurement(state("drift", {"metric": "null_rate", "column": "b", "value": 0.5})) is None
        assert rule.stored_measurement(state("not_null", {"metric": "null_rate", "column": "a", "value": 0.5})) is None
        assert rule.stored_measurement(state("drift", {"metric": "null_rate", "column": "a", "value": None})) is None
        assert rule.stored_measurement(None) is None


# --------------------------------------------------------------------------- #
# Polars measurement
# --------------------------------------------------------------------------- #


FRAME = pl.DataFrame({
    "n": [1, 2, 2, None, 5],
    "f": [0.5, None, 1.5, 2.5, None],
    "s": ["a", "b", None, "a", None],
})


class TestPolarsMeasure:
    @pytest.mark.parametrize(
        "metric, column, expected",
        [
            ("row_count", None, 5),
            ("null_count", "n", 1),
            ("null_rate", "f", 0.4),
            ("distinct_count", "n", 3),  # NULL is not a distinct value
            ("distinct_count", "s", 2),
            ("mean", "n", 2.5),
            ("min", "f", 0.5),
            ("max", "n", 5),
        ],
    )
    def test_metrics(self, metric, column, expected):
        params = {"metric": metric, "max_change": 1}
        if column:
            params["column"] = column
        rule = build(params)
        rule.set_history([])
        result = rule.validate(FRAME)
        assert result["details"]["value"] == pytest.approx(expected)

    def test_empty_frame(self):
        empty = FRAME.head(0)
        for metric, expected in [("row_count", 0), ("null_count", 0), ("distinct_count", 0)]:
            params = {"metric": metric, "max_change": 1}
            if metric != "row_count":
                params["column"] = "n"
            assert build(params).validate(empty)["details"]["value"] == expected
        for metric in ("null_rate", "mean", "min", "max"):
            assert build({"metric": metric, "column": "f", "max_change": 1}).validate(empty)["details"]["value"] is None

    def test_missing_column_is_config_error(self):
        result = build({"metric": "null_rate", "column": "nope", "max_change": 1}).validate(FRAME)
        assert result["passed"] is False
        assert result["failure_mode"] == "config_error"
        assert "details" not in result or "value" not in result["details"]

    def test_numeric_metric_on_text_is_config_error(self):
        result = build({"metric": "mean", "column": "s", "max_change": 1}).validate(FRAME)
        assert result["failure_mode"] == "config_error"
        assert "needs a numeric column" in result["message"]


# --------------------------------------------------------------------------- #
# End to end with the local state store
# --------------------------------------------------------------------------- #


def users(n_rows: int, null_every: int = 0, age_shift: float = 0.0) -> pl.DataFrame:
    return pl.DataFrame({
        "id": list(range(n_rows)),
        "email": [None if null_every and i % null_every == 0 else f"u{i}@x" for i in range(n_rows)],
        "age": [20.0 + (i % 10) + age_shift for i in range(n_rows)],
    })


RULES = [
    rules.drift("null_rate", "email", max_increase=0.05),
    rules.drift("row_count", baseline="median", window=3, max_decrease_pct=20),
    rules.drift("mean", "age", max_change_pct=10),
]


class TestEndToEnd:
    def test_runs_build_and_use_history(self, workdir):
        users(100, null_every=10).write_parquet("u.parquet")
        first = by_name(kontra.validate("u.parquet", rules=RULES))
        assert all(r.passed and status(r) == "no_baseline" for r in first.values())

        second = kontra.validate("u.parquet", rules=RULES)
        assert second.passed
        assert {status(r) for r in second.rules} == {"ok"}

        users(70, null_every=4, age_shift=5).write_parquet("u.parquet")
        third = by_name(kontra.validate("u.parquet", rules=RULES))
        null_rate = third["COL:email:drift:null_rate"]
        assert not null_rate.passed
        assert null_rate.failure_mode == "metric_drift"
        assert null_rate.details["baseline"] == pytest.approx(0.1)
        assert null_rate.details["value"] == pytest.approx(18 / 70)
        rows = third["DATASET:drift:row_count"]
        assert not rows.passed and rows.details["change_pct"] == pytest.approx(-30)
        assert not third["COL:age:drift:mean"].passed

    def test_measurements_are_saved_for_every_outcome(self, workdir):
        from kontra.state.backends import get_default_store

        users(10).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=RULES)
        store = get_default_store()
        (fingerprint,) = store.list_contracts()
        (state,) = store.get_history(fingerprint)
        saved = {r.rule_id: r for r in state.rules}
        assert saved["DATASET:drift:row_count"].rule_name == "drift"
        assert saved["DATASET:drift:row_count"].details["value"] == 10
        assert saved["COL:email:drift:null_rate"].details == {
            "metric": "null_rate", "column": "email", "value": 0.0, "baseline": None,
            "baseline_method": "previous", "history_runs": 0, "limits": {"max_increase": 0.05},
            "status": "no_baseline",
        }

    def test_median_over_n_runs(self, workdir):
        rule = [rules.drift("row_count", baseline="median", window=3, max_change_pct=50)]
        for n in (100, 110, 90):
            users(n).write_parquet("u.parquet")
            kontra.validate("u.parquet", rules=rule)
        users(1000).write_parquet("u.parquet")
        result = kontra.validate("u.parquet", rules=rule).rules[0]
        assert result.details["baseline"] == 100
        assert result.details["history_runs"] == 3
        assert status(result) == "drifted"
        users(105).write_parquet("u.parquet")
        # Median of (1000, 90, 110): a drifted run still counts as history.
        assert kontra.validate("u.parquet", rules=rule).rules[0].details["baseline"] == 110

    def test_each_dataset_has_its_own_history(self, workdir):
        rule = [rules.drift("row_count", max_change=0)]
        users(10).write_parquet("a.parquet")
        users(500).write_parquet("b.parquet")
        kontra.validate("a.parquet", rules=rule)
        b_first = kontra.validate("b.parquet", rules=rule).rules[0]
        assert status(b_first) == "no_baseline"
        assert status(kontra.validate("a.parquet", rules=rule).rules[0]) == "ok"
        assert status(kontra.validate("b.parquet", rules=rule).rules[0]) == "ok"

    def test_changing_the_contract_starts_a_new_history(self, workdir):
        users(10).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=[rules.drift("row_count", max_change=0)])
        widened = [rules.drift("row_count", max_change=0), rules.not_null("id")]
        result = kontra.validate("u.parquet", rules=widened)
        assert status(by_name(result)["DATASET:drift:row_count"]) == "no_baseline"

    def test_unsaved_runs_read_history_but_add_none(self, workdir):
        rule = [rules.drift("row_count", max_change=0)]
        users(10).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=rule)
        users(11).write_parquet("u.parquet")
        dry = kontra.validate("u.parquet", rules=rule, save=False).rules[0]
        assert status(dry) == "drifted" and dry.details["baseline"] == 10
        users(10).write_parquet("u.parquet")
        assert status(kontra.validate("u.parquet", rules=rule).rules[0]) == "ok"

    def test_filtered_run_is_judged_but_not_saved(self, workdir):
        rule = [rules.drift("row_count", max_change=0), rules.not_null("id")]
        users(10).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=rule)
        users(20).write_parquet("u.parquet")
        with pytest.warns(UserWarning, match="Skipping state save"):
            filtered = kontra.validate("u.parquet", rules=rule, only=["drift"])
        assert status(filtered.rules[0]) == "drifted"
        assert by_name(kontra.validate("u.parquet", rules=rule))["DATASET:drift:row_count"].details["baseline"] == 10

    def test_dataframe_input(self, workdir):
        rule = [rules.drift("distinct_count", "id", max_decrease=0)]
        kontra.validate(users(10), rules=rule)
        result = kontra.validate(users(8), rules=rule).rules[0]
        assert result.source == "polars"
        assert status(result) == "drifted"
        assert result.message == (
            "distinct_count of 'id' fell from 10 to 8 (-2, -20.0%) against the previous run; "
            "exceeds max_decrease 0"
        )

    def test_runs_within_one_second_stay_in_order(self, workdir):
        rule = [rules.drift("row_count", max_change=0)]
        for n in (5, 6, 7, 8):
            users(n).write_parquet("u.parquet")
            kontra.validate("u.parquet", rules=rule)
        users(8).write_parquet("u.parquet")
        assert kontra.validate("u.parquet", rules=rule).rules[0].details["baseline"] == 8

    def test_store_failure_reports_no_baseline(self, workdir):
        class Broken(LocalStore):
            def get_history(self, *args, **kwargs):
                raise OSError("disk gone")

        users(5).write_parquet("u.parquet")
        result = kontra.validate(
            "u.parquet", rules=[rules.drift("row_count", max_change=0)],
            state_store=Broken(str(workdir / "state")),
        )
        rule = result.rules[0]
        assert result.passed and status(rule) == "no_baseline"
        assert rule.details["history_error"] == "OSError: disk gone"

    def test_explicit_store_is_read_not_the_default(self, workdir):
        store = LocalStore(str(workdir / "elsewhere"))
        rule = [rules.drift("row_count", max_change=0)]
        users(5).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=rule, state_store=store)
        users(6).write_parquet("u.parquet")
        assert status(kontra.validate("u.parquet", rules=rule).rules[0]) == "no_baseline"
        assert status(kontra.validate("u.parquet", rules=rule, state_store=store).rules[0]) == "drifted"


class TestHistoryRead:
    """attach_history against a real LocalStore."""

    @staticmethod
    def save(store, run_at, uri, value):
        store.save(ValidationState(
            contract_fingerprint="fp", dataset_fingerprint=None, contract_name="c",
            dataset_uri=uri, run_at=run_at,
            summary=StateSummary(passed=True, total_rules=1, passed_rules=1, failed_rules=0),
            rules=[RuleState(rule_id="rows", rule_name="drift", passed=True, failed_count=0,
                             execution_source="sql", details={"metric": "row_count", "value": value})],
        ))

    @staticmethod
    def rule():
        rule = build({"metric": "row_count", "max_change": 0}, id="rows")
        return rule

    def test_reads_past_runs_of_other_datasets(self, tmp_path):
        from kontra.engine.phases.drift import attach_history

        store = LocalStore(str(tmp_path))
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.save(store, start, "a.parquet", 100)
        for i in range(40):
            self.save(store, start + timedelta(seconds=i + 1), "b.parquet", 7)
        rule = self.rule()
        attach_history([rule], store, "fp", "a.parquet")
        result = rule.evaluate(200)
        assert result["details"]["baseline"] == 100
        assert result["details"]["status"] == "drifted"

    def test_latest_run_is_the_baseline_when_a_read_cuts_into_one_second(self, tmp_path, monkeypatch):
        from kontra.engine.phases.drift import attach_history

        store = LocalStore(str(tmp_path))
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # Twelve runs in one second whose file names sort oldest-first, so a
        # read of the first eleven names misses the newest run.
        for i in range(12):
            monkeypatch.setattr(
                store, "_generate_run_id",
                lambda run_at, i=i: run_at.strftime("%Y-%m-%dT%H-%M-%S") + f"_{11 - i:06d}",
            )
            self.save(store, start + timedelta(microseconds=i), "a.parquet", 200 if i == 11 else 100)
        rule = self.rule()
        attach_history([rule], store, "fp", "a.parquet")
        assert rule.evaluate(200)["details"]["baseline"] == 200

    def test_a_full_read_at_the_cap_keeps_its_runs(self, tmp_path):
        """Exactly the cap's worth of runs, all in one second: the read is
        complete, so the latest run is still the baseline."""
        from kontra.engine.phases import drift as phase

        class OneSecond:
            def get_history(self, fingerprint, limit):
                start = datetime(2026, 1, 1, tzinfo=timezone.utc)
                runs = [
                    ValidationState(
                        contract_fingerprint="fp", dataset_fingerprint=None, contract_name="c",
                        dataset_uri="a.parquet", run_at=start + timedelta(microseconds=i),
                        summary=StateSummary(passed=True, total_rules=0, passed_rules=0,
                                             failed_rules=0),
                        rules=[RuleState(rule_id="rows", rule_name="drift", passed=True,
                                         failed_count=0, execution_source="sql",
                                         details={"metric": "row_count", "value": i})],
                    )
                    for i in range(phase._HISTORY_SCAN_MAX)
                ]
                return sorted(runs, key=lambda r: r.run_at, reverse=True)[:limit]

        rule = self.rule()
        phase.attach_history([rule], OneSecond(), "fp", "a.parquet")
        assert rule.evaluate(2000)["details"]["baseline"] == phase._HISTORY_SCAN_MAX - 1

    def test_read_is_bounded(self):
        from kontra.engine.phases import drift as phase

        class Endless:
            """A history of other datasets that never ends."""

            def __init__(self):
                self.limits = []

            def get_history(self, fingerprint, limit):
                self.limits.append(limit)
                start = datetime(2026, 1, 1, tzinfo=timezone.utc)
                return [
                    ValidationState(
                        contract_fingerprint="fp", dataset_fingerprint=None, contract_name="c",
                        dataset_uri="other", run_at=start - timedelta(seconds=i),
                        summary=StateSummary(passed=True, total_rules=0, passed_rules=0, failed_rules=0),
                        rules=[],
                    )
                    for i in range(limit)
                ]

        store = Endless()
        rule = self.rule()
        phase.attach_history([rule], store, "fp", "a.parquet")
        # One run past the limit tells a full read from a cut-off one.
        assert max(store.limits) == phase._HISTORY_SCAN_MAX + 1
        assert rule.evaluate(5)["details"]["status"] == "no_baseline"


# --------------------------------------------------------------------------- #
# Measurement routes agree
# --------------------------------------------------------------------------- #


ALL_METRICS = [
    ("row_count", None),
    ("null_count", "f"),
    ("null_rate", "f"),
    ("distinct_count", "n"),
    ("mean", "n"),
    ("min", "f"),
    ("max", "n"),
]


def metric_rules():
    return [rules.drift(m, c, max_change=0) for m, c in ALL_METRICS]


class TestRoutes:
    @pytest.fixture()
    def data(self, workdir):
        frame = pl.DataFrame({
            "n": [3, 1, 4, 1, 5, None, 9, 2, 6, 5] * 30,
            "f": [0.5, None, 2.25, None, -1.5, 3.0, None, 8.0, 0.0, 1.0] * 30,
        })
        frame.write_parquet("m.parquet", row_group_size=64)
        frame.write_csv("m.csv")
        return frame

    def values(self, source, **kw):
        result = kontra.validate(source, rules=metric_rules(), save=False, **kw)
        return (
            {r.details["metric"]: r.details["value"] for r in result.rules},
            {r.details["metric"]: r.source for r in result.rules},
        )

    def test_every_route_measures_the_same(self, data):
        expected, _ = self.values(data)
        assert expected["row_count"] == 300 and expected["null_count"] == 90
        routes = {
            "parquet": self.values("m.parquet"),
            "parquet_polars": self.values("m.parquet", preplan="off", pushdown="off"),
            "csv": self.values("m.csv"),
            "csv_polars": self.values("m.csv", pushdown="off"),
        }
        for name, (values, _) in routes.items():
            assert values.keys() == expected.keys(), name
            for metric, value in expected.items():
                assert values[metric] == pytest.approx(value), (name, metric)

    def test_cheapest_route(self, data):
        _, sources = self.values("m.parquet")
        assert set(sources.values()) == {"sql"}
        _, polars = self.values("m.parquet", preplan="off", pushdown="off")
        assert set(polars.values()) == {"polars"}

    @pytest.mark.parametrize("metric, expected", [("min", -2.0), ("max", 1.0), ("mean", None)])
    def test_nan_is_measured_as_polars_does(self, workdir, metric, expected):
        # Polars' min/max skip NaN; its mean is NaN, which is undefined.
        pl.DataFrame({"x": [1.0, float("nan"), -2.0, None]}).write_parquet("nan.parquet")
        rule = [rules.drift(metric, "x", max_change=0)]
        sql = kontra.validate("nan.parquet", rules=rule, save=False).rules[0]
        polars = kontra.validate("nan.parquet", rules=rule, save=False, pushdown="off").rules[0]
        assert (sql.source, polars.source) == ("sql", "polars")
        assert sql.details["value"] == polars.details["value"] == expected

    def test_all_nan_column_is_undefined(self, workdir):
        pl.DataFrame({"x": [float("nan"), None]}).write_parquet("nan.parquet")
        for metric in ("min", "max"):
            rule = [rules.drift(metric, "x", max_change=0)]
            for kw in ({}, {"pushdown": "off"}):
                measured = kontra.validate("nan.parquet", rules=rule, save=False, **kw).rules[0]
                assert measured.details["value"] is None, (metric, kw)

    def test_large_integers_stay_exact(self, workdir):
        rule = [rules.drift("max", "x", max_change=0)]
        for kw in ({}, {"pushdown": "off"}):
            store = LocalStore(str(workdir / f"state{len(kw)}"))
            for x in (2**53, 2**53 + 1):
                pl.DataFrame({"x": [x]}).write_parquet("big.parquet")
                result = kontra.validate("big.parquet", rules=rule, state_store=store, **kw).rules[0]
            assert result.details["value"] == 2**53 + 1, kw
            assert result.details["baseline"] == 2**53, kw
            assert result.details["change"] == 1, kw
            assert status(result) == "drifted", kw

    def test_text_column_mean_leaves_other_rules_in_sql(self, workdir):
        pl.DataFrame({"s": ["a", "b"], "x": [1, None]}).write_parquet("t.parquet")
        result = kontra.validate(
            "t.parquet",
            rules=[rules.drift("mean", "s", max_change=0), rules.not_null("x")],
            preplan="off",
            save=False,
        )
        got = by_name(result)
        assert got["COL:s:drift:mean"].failure_mode == "config_error"
        assert got["COL:x:not_null"].source == "sql"

    def test_missing_column(self, workdir):
        pl.DataFrame({"x": [1]}).write_parquet("t.parquet")
        result = kontra.validate("t.parquet", rules=[rules.drift("null_rate", "nope", max_change=0)], save=False)
        assert result.rules[0].failure_mode == "config_error"
        assert "nope" in result.rules[0].message

    def test_explain_shows_the_tier(self, data):
        plan = kontra.validate("m.parquet", rules=metric_rules(), explain=True)
        tiers = {e.rule_id: e.tier for e in plan.rules}
        assert tiers["DATASET:drift:row_count"] == "sql"
        assert tiers["COL:n:drift:mean"] == "sql"


# --------------------------------------------------------------------------- #
# SQL rendering
# --------------------------------------------------------------------------- #


class TestSqlRendering:
    @pytest.mark.parametrize(
        "dialect, metric, expected",
        [
            ("duckdb", "row_count", 'COUNT(*) AS "r"'),
            ("duckdb", "null_rate", 'CAST(COUNT(*) - COUNT("c") AS DOUBLE) / NULLIF(COUNT(*), 0) AS "r"'),
            ("duckdb", "mean", 'AVG(CAST("c" AS DOUBLE)) AS "r"'),
            ("duckdb", "max", 'MAX(CASE WHEN isnan("c") THEN NULL ELSE "c" END) AS "r"'),
            ("postgres", "mean", 'AVG(CAST("c" AS DOUBLE PRECISION)) AS "r"'),
            ("postgres", "min", 'MIN(CASE WHEN "c"::text = \'NaN\' THEN NULL ELSE "c" END) AS "r"'),
            ("sqlserver", "null_count", '(COUNT_BIG(*) - COUNT_BIG([c])) AS [r]'),
            ("sqlserver", "mean", 'AVG(CAST([c] AS FLOAT)) AS [r]'),
            ("sqlserver", "max", 'MAX([c]) AS [r]'),
            ("sqlserver", "distinct_count", 'COUNT_BIG(DISTINCT [c]) AS [r]'),
            ("clickhouse", "mean", "AVG(toFloat64(`c`)) AS `r`"),
            ("clickhouse", "min", "minOrNull(`c`) AS `r`"),
            ("clickhouse", "max", "maxOrNull(`c`) AS `r`"),
            ("trino", "max", 'MAX("c") AS "r"'),
        ],
    )
    def test_agg_metric(self, dialect, metric, expected):
        from kontra.engine.sql_utils import agg_metric

        assert agg_metric(metric, None if metric == "row_count" else "c", "r", dialect) == expected

    def test_results_from_row_keeps_the_measurement(self):
        from decimal import Decimal

        from kontra.engine.sql_utils import results_from_row

        out = results_from_row(
            ["m", "nn"], (Decimal("2.5"), 3), rule_kinds={"m": "metric", "nn": "not_null"}
        )
        assert out[0]["metric_value"] == Decimal("2.5")
        assert out[0]["passed"] is True
        assert out[1]["failed_count"] == 3 and "metric_value" not in out[1]

    @pytest.mark.parametrize(
        "metric, family, exact",
        [
            ("row_count", None, True),
            ("null_rate", "varchar", True),
            ("null_rate", None, False),
            ("distinct_count", "float", True),
            ("distinct_count", "char", False),
            ("mean", "float", True),
            ("mean", "varchar", False),
            ("max", "decimal", True),
            ("max", "float", False),
        ],
    )
    def test_trino_exactness_gate(self, metric, family, exact):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        assert TrinoSqlExecutor._metric_is_exact(metric, family) is exact


# --------------------------------------------------------------------------- #
# State backends and compatibility
# --------------------------------------------------------------------------- #


def _fake_s3_fs():
    from fsspec.implementations.memory import MemoryFileSystem

    class FakeS3(MemoryFileSystem):
        """In-memory stand-in for s3fs: same s3:// paths, private store."""

        protocol = ("s3",)

        @classmethod
        def _strip_protocol(cls, path):
            if path.startswith("s3://"):
                path = path[len("s3://"):]
            return super()._strip_protocol(path)

    fs = FakeS3()
    fs.store = {}
    fs.pseudo_dirs = [""]
    return fs


class TestBackends:
    def test_s3_store(self, workdir):
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://bucket/kontra")
        store._fs = _fake_s3_fs()
        rule = [rules.drift("null_count", "email", max_increase=0)]
        users(20, null_every=5).write_parquet("u.parquet")
        first = kontra.validate("u.parquet", rules=rule, state_store=store).rules[0]
        assert status(first) == "no_baseline"
        users(20, null_every=2).write_parquet("u.parquet")
        second = kontra.validate("u.parquet", rules=rule, state_store=store).rules[0]
        assert status(second) == "drifted"
        assert (second.details["baseline"], second.details["value"]) == (4, 10)
        # Nothing went to the default local store.
        assert not (workdir / ".kontra").exists()

    def test_old_history_without_measurements_loads(self, workdir):
        """Runs saved before drift existed (and older state files) are no baseline."""
        from kontra.state.fingerprint import fingerprint_contract
        from kontra.config.models import Contract

        users(10).write_parquet("u.parquet")
        rule = rules.drift("row_count", max_change=0)
        contract = Contract(name="u.parquet", datasource="u.parquet",
                            rules=[RuleSpec(name="drift", params=rule["params"])])
        fingerprint = fingerprint_contract(contract)
        store = LocalStore(str(workdir / "state"))
        old = ValidationState.from_dict({
            "schema_version": "1.0",
            "engine_version": "0.9.0",
            "contract_fingerprint": fingerprint,
            "dataset_fingerprint": None,
            "contract_name": "u.parquet",
            "dataset_uri": "u.parquet",
            "run_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            "summary": {"passed": True, "total_rules": 1, "passed_rules": 1, "failed_rules": 0},
            # Same rule id, no stored measurement (e.g. hand-written or legacy).
            "rules": [{"rule_id": "DATASET:drift:row_count", "rule_name": "", "passed": True,
                       "failed_count": 0, "execution_source": "polars"}],
        })
        store.save(old)
        result = kontra.validate("u.parquet", rules=[rule], state_store=store).rules[0]
        assert status(result) == "no_baseline"
        assert status(kontra.validate("u.parquet", rules=[rule], state_store=store).rules[0]) == "ok"

    def test_state_roundtrip_keeps_details(self):
        state = ValidationState(
            contract_fingerprint="fp", dataset_fingerprint=None, contract_name="c",
            dataset_uri="d", run_at=datetime.now(timezone.utc),
            summary=StateSummary(passed=True, total_rules=1, passed_rules=1, failed_rules=0),
            rules=[RuleState(rule_id="DATASET:drift:row_count", rule_name="drift", passed=True,
                             failed_count=0, execution_source="metadata",
                             details={"metric": "row_count", "value": 7})],
        )
        again = ValidationState.from_json(state.to_json())
        assert again.rules[0].details == {"metric": "row_count", "value": 7}
        assert again.rules[0].rule_name == "drift"


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #


class TestReporting:
    def test_to_llm_marks_unchecked_rules(self, workdir):
        users(10).write_parquet("u.parquet")
        first = kontra.validate("u.parquet", rules=RULES)
        text = first.to_llm()
        assert "PASSED: 3 rules" in text
        assert ("NO BASELINE (drift not checked): COL:email:drift:null_rate, "
                "DATASET:drift:row_count, COL:age:drift:mean") in text
        mean = by_name(first)["COL:age:drift:mean"]
        assert mean.to_llm().startswith("COL:age:drift:mean: PASS (No baseline: mean of 'age' is 24.5;")

        users(5).write_parquet("u.parquet")
        second = kontra.validate("u.parquet", rules=RULES)
        text = second.to_llm()
        assert "NO BASELINE" not in text
        assert "BLOCKING: DATASET:drift:row_count (row_count fell from 10 to 5" in text

    def test_to_dict_carries_details(self, workdir):
        users(10).write_parquet("u.parquet")
        payload = kontra.validate("u.parquet", rules=RULES).to_dict()
        rule = next(r for r in payload["rules"] if r["rule_id"] == "DATASET:drift:row_count")
        assert rule["name"] == "drift"
        assert rule["details"]["status"] == "no_baseline"
        json.dumps(payload)

    def test_console_report(self, workdir, capsys):
        users(10).write_parquet("u.parquet")
        rule = [rules.drift("row_count", max_change=0)]
        kontra.validate("u.parquet", rules=rule, emit_report=True)
        first = capsys.readouterr().out
        assert "⏳ DATASET:drift:row_count [sql]: No baseline" in first
        users(12).write_parquet("u.parquet")
        kontra.validate("u.parquet", rules=rule, emit_report=True)
        second = capsys.readouterr().out
        assert "DATASET:drift:row_count [sql]: row_count rose from 10 to 12" in second
        assert "--tally" not in second

    def test_catalog(self):
        detail = kontra.describe_rule("drift")
        names = [p["name"] for p in detail["parameters"]]
        assert names[:2] == ["metric", "column"]
        assert detail["supports_tally"] is False
        assert "no_baseline" in detail["fails_when"]


class TestCli:
    def test_no_state_still_reads_the_configured_backend(self, workdir, monkeypatch):
        from typer.testing import CliRunner

        import kontra.state.backends as backends
        from kontra.cli.main import app

        store = LocalStore(str(workdir / "remote"))
        monkeypatch.setattr(backends, "get_store", lambda uri: store)
        users(10).write_parquet("u.parquet")
        (workdir / "c.yml").write_text(
            "name: cli_drift\ndatasource: u.parquet\nrules:\n"
            "  - name: drift\n    params: { metric: row_count, max_change: 0 }\n",
            encoding="utf-8",
        )
        args = ["validate", "c.yml", "--state-backend", "s3://bucket/kontra", "-o", "json"]
        runner = CliRunner()
        assert runner.invoke(app, args).exit_code == 0
        users(12).write_parquet("u.parquet")
        result = runner.invoke(app, args + ["--no-state"])
        assert result.exit_code == 1
        assert "row_count rose from 10 to 12" in result.output
        # --no-state saved nothing.
        (fingerprint,) = store.list_contracts()
        assert len(store.get_history(fingerprint)) == 1


def test_config_errors_still_name_the_rule(workdir):
    pl.DataFrame({"x": [1]}).write_parquet("t.parquet")
    result = kontra.validate("t.parquet", rules=[rules.drift("null_rate", "nope", max_change=0)], save=False)
    assert result.rules[0].name == "drift"
