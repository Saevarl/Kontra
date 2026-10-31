"""
Drift rule - fails when a dataset statistic moves sharply against earlier runs.

Usage in contract:
  - name: drift
    params:
      metric: null_rate        # row_count | null_count | null_rate | distinct_count | mean | min | max
      column: email            # required for every metric except row_count
      max_increase: 0.05       # null rate may rise by at most 5 percentage points

  - name: drift
    params:
      metric: row_count
      baseline: median         # previous (default) | median | mean
      window: 7                # earlier runs the median is taken over
      max_decrease_pct: 20     # at most a 20% drop against that median

A run measures the metric with a SQL aggregate where the engine pushes one
down, otherwise with Polars, and stores the measurement in the rule's
result details. The baseline comes from those stored measurements: the saved
runs of the same contract (fingerprint) on the same dataset URI, read from the
configured state backend. The engine attaches them before execution
(``kontra.engine.phases.drift``), so the rule itself never touches the store.

Without an earlier measurement there is nothing to compare against. The
rule then passes (or fails, with ``on_no_baseline: fail``) with
``details["status"] == "no_baseline"`` and a message that says so: it never
looks like a checked pass.
"""

from __future__ import annotations

import math
import numbers
import statistics
from fractions import Fraction
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set

if TYPE_CHECKING:
    import polars as pl
    from kontra.state.types import RuleState

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode

RULE_NAME = "drift"

METRICS = ("row_count", "null_count", "null_rate", "distinct_count", "mean", "min", "max")
# Metrics that are whole numbers and are always defined (0 on an empty dataset).
COUNT_METRICS = frozenset({"row_count", "null_count", "distinct_count"})
# Metrics that only make sense on numeric columns.
NUMERIC_METRICS = frozenset({"mean", "min", "max"})

BASELINES = ("previous", "median", "mean")
DEFAULT_WINDOW = 7

ABSOLUTE_LIMITS = ("max_increase", "max_decrease", "max_change")
PERCENT_LIMITS = ("max_increase_pct", "max_decrease_pct", "max_change_pct")
LIMITS = ABSOLUTE_LIMITS + PERCENT_LIMITS

ON_NO_BASELINE = ("pass", "fail")

# Result details["status"] values.
STATUS_OK = "ok"
STATUS_DRIFTED = "drifted"
STATUS_NO_BASELINE = "no_baseline"
STATUS_NO_VALUE = "no_value"

_KNOWN_PARAMS = frozenset(
    {"metric", "column", "baseline", "window", "on_no_baseline", *LIMITS}
)


def _param_error(param: str, issue: str) -> Exception:
    from kontra.errors import RuleParameterError

    return RuleParameterError(RULE_NAME, param, issue)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _positive_int(params: Dict[str, Any], key: str, default: int) -> int:
    value = params.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise _param_error(key, f"must be a positive integer, got {value!r}")
    return value


def parse_drift_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate drift params and return them normalized.

    Shared by the rule and the ``kontra.rules.drift`` builder so a mistake is
    reported the same way wherever the rule is written.
    """
    unknown = sorted(set(params) - _KNOWN_PARAMS)
    if unknown:
        raise _param_error(unknown[0], f"unknown parameter; expected one of {', '.join(sorted(_KNOWN_PARAMS))}")

    metric = params.get("metric")
    if metric not in METRICS:
        raise _param_error("metric", f"must be one of {', '.join(METRICS)}, got {metric!r}")

    column = params.get("column")
    if metric == "row_count":
        if column is not None:
            raise _param_error("column", "row_count measures the whole dataset; remove column")
    elif not isinstance(column, str) or not column.strip():
        raise _param_error("column", f"metric {metric} requires a non-empty column name")

    baseline = params.get("baseline", "previous")
    if baseline not in BASELINES:
        raise _param_error("baseline", f"must be one of {', '.join(BASELINES)}, got {baseline!r}")

    if baseline == "previous":
        if "window" in params and params["window"] != 1:
            raise _param_error("window", "applies to median and mean baselines; previous compares with one run")
        window = 1
    else:
        window = _positive_int(params, "window", DEFAULT_WINDOW)

    limits: Dict[str, float] = {}
    for key in LIMITS:
        if key in params and params[key] is not None:
            value = params[key]
            if not _is_number(value) or not math.isfinite(value) or value < 0:
                raise _param_error(key, f"must be a non-negative number, got {value!r}")
            limits[key] = value
    if not limits:
        raise _param_error("max_change", f"set at least one limit: {', '.join(LIMITS)}")

    on_no_baseline = params.get("on_no_baseline", "pass")
    if on_no_baseline not in ON_NO_BASELINE:
        raise _param_error("on_no_baseline", f"must be pass or fail, got {on_no_baseline!r}")

    return {
        "metric": metric,
        "column": column,
        "baseline": baseline,
        "window": window,
        "limits": limits,
        "on_no_baseline": on_no_baseline,
    }


def clean_measurement(metric: str, value: Any) -> Optional[float]:
    """
    A measurement as a plain JSON number, or None when it is undefined.

    SQL drivers hand back Decimal, numpy and int types; NaN and infinity are
    undefined (and PostgreSQL JSONB state can't store them).
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, numbers.Integral):
        # Integer extrema stay exact: a float has no room for every Int64, so
        # 2**53 -> 2**53 + 1 would compare as no change.
        return int(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if metric in COUNT_METRICS:
        return int(number)
    return number


def _plain(value: Any) -> Any:
    """A fraction from exact integer aggregation as a JSON number."""
    return float(value) if isinstance(value, Fraction) else value


def _fmt(value: Optional[float]) -> str:
    value = _plain(value)
    if value is None:
        return "undefined"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:.6g}"


def _signed(value: float) -> str:
    value = _plain(value)
    if isinstance(value, int):
        return f"{value:+,}"
    return f"{value:+.6g}"


def _exceeds(amount: float, limit: float) -> bool:
    if isinstance(amount, numbers.Rational) and isinstance(limit, numbers.Rational):
        # Exact changes (integers, or fractions of them) need no rounding
        # tolerance, which would let a change of 1 on a 2**53 baseline pass a
        # limit of 0.
        return amount > limit
    # Float subtraction lands a hair either side of an exact limit (0.07 - 0.02
    # is 0.05000000000000001); a change equal to the limit is allowed.
    return amount > limit and not math.isclose(amount, limit, rel_tol=1e-9, abs_tol=1e-12)


@register_rule(RULE_NAME, _builtin=True)
class DriftRule(BaseRule):
    """
    Compares a dataset statistic with its value in earlier runs.

    Parameters:
        metric: row_count | null_count | null_rate | distinct_count | mean | min | max
        column: Column to measure (every metric except row_count)
        baseline: previous (default) | median | mean of the last ``window`` runs
        window: Earlier runs the median/mean is taken over (default 7)
        max_increase / max_decrease / max_change: Absolute limits, in metric units
        max_increase_pct / max_decrease_pct / max_change_pct: Limits in percent
            of the baseline (20 means 20%)
        on_no_baseline: pass (default) | fail when there is no earlier measurement
    """

    rule_scope = "dataset"
    supports_tally = False

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        spec = parse_drift_params(params)
        self.metric: str = spec["metric"]
        self.column: Optional[str] = spec["column"]
        self.baseline: str = spec["baseline"]
        self.window: int = spec["window"]
        self.limits: Dict[str, float] = spec["limits"]
        self.on_no_baseline: str = spec["on_no_baseline"]
        if self.column is not None:
            self.rule_scope = "column"
        # Earlier measurements, newest first. None until the engine attaches
        # history (or when it could not be read).
        self._history: Optional[List[float]] = None
        self._history_error: Optional[str] = None

    # ------------------------------------------------------------------ #
    # History (attached by the engine)
    # ------------------------------------------------------------------ #

    @property
    def history_needed(self) -> int:
        """How many earlier measurements the baseline can use."""
        return self.window

    def set_history(self, values: Optional[Sequence[float]], error: Optional[str] = None) -> None:
        """Attach earlier measurements (newest first), or the error that prevented reading them."""
        self._history = list(values) if values is not None else None
        self._history_error = error

    def stored_measurement(self, rule_state: Optional["RuleState"]) -> Optional[float]:
        """This rule's measurement from an earlier run's saved result, if it has one."""
        if rule_state is None or rule_state.rule_name != RULE_NAME:
            return None
        details = rule_state.details or {}
        # An explicit rule id can outlive a change of metric or column; a
        # measurement of something else is no baseline.
        if details.get("metric") != self.metric or details.get("column") != self.column:
            return None
        return clean_measurement(self.metric, details.get("value"))

    # ------------------------------------------------------------------ #
    # Measurement
    # ------------------------------------------------------------------ #

    def required_columns(self) -> Set[str]:
        return {self.column} if self.column else set()

    def compile_predicate(self):
        return None  # a dataset statistic, not a row-level predicate

    def to_sql_spec(self) -> Dict[str, Any]:
        """Measure through the executors' shared aggregate scan (kind "metric")."""
        return {
            "kind": "metric",
            "rule_id": self.rule_id,
            "metric": self.metric,
            "column": self.column,
        }

    def measure(self, df: "pl.DataFrame") -> Optional[float]:
        """The measurement on a Polars frame (the caller checked the column)."""
        if self.metric == "row_count":
            return df.height
        series = df[self.column]
        if self.metric == "null_count":
            return series.null_count()
        if self.metric == "null_rate":
            return series.null_count() / df.height if df.height else None
        if self.metric == "distinct_count":
            # SQL COUNT(DISTINCT) ignores NULL; n_unique() would count it.
            return series.drop_nulls().n_unique()
        if self.metric == "mean":
            return series.mean()
        if self.metric == "min":
            return series.min()
        return series.max()

    def validate(self, df: "pl.DataFrame") -> Dict[str, Any]:
        if self.column is not None:
            col_check = self._check_columns(df, {self.column})
            if col_check is not None:
                return col_check
            dtype = df[self.column].dtype
            if self.metric in NUMERIC_METRICS and not dtype.is_numeric():
                return {
                    "rule_id": self.rule_id,
                    "passed": False,
                    "failed_count": 1,
                    "message": f"drift metric {self.metric} needs a numeric column; '{self.column}' is {dtype}",
                    "failure_mode": str(FailureMode.CONFIG_ERROR),
                }
        return self.evaluate(self.measure(df))

    # ------------------------------------------------------------------ #
    # Verdict
    # ------------------------------------------------------------------ #

    def evaluate_measurement(self, raw_value: Any) -> Dict[str, Any]:
        """
        Judge a measurement taken by a SQL aggregate.

        SQL answers MIN/MAX of a text or date column with a non-number; that is
        the same configuration error the Polars tier reports.
        """
        if raw_value is not None and (
            isinstance(raw_value, bool) or not isinstance(raw_value, numbers.Number)
        ):
            return {
                "rule_id": self.rule_id,
                "rule_name": RULE_NAME,
                "passed": False,
                "failed_count": 1,
                "message": (
                    f"drift metric {self.metric} needs a numeric column; "
                    f"'{self.column}' holds {type(raw_value).__name__} values"
                ),
                "failure_mode": str(FailureMode.CONFIG_ERROR),
            }
        return self.evaluate(raw_value)

    def _subject(self) -> str:
        return self.metric if self.column is None else f"{self.metric} of '{self.column}'"

    def _baseline_label(self, used: int) -> str:
        if self.baseline == "previous":
            return "the previous run"
        if used == 1:
            return "the only earlier run"
        return f"the {self.baseline} of the last {used} runs"

    def evaluate(self, raw_value: Any) -> Dict[str, Any]:
        """Turn the current measurement into this rule's result, using the attached history."""
        value = clean_measurement(self.metric, raw_value)
        history = self._history
        usable = history[: self.window] if history is not None else []

        details: Dict[str, Any] = {"metric": self.metric}
        if self.column is not None:
            details["column"] = self.column
        details.update({
            # What the next run reads back as its history.
            "value": value,
            "baseline": None,
            "baseline_method": self.baseline,
            "history_runs": len(usable),
            "limits": dict(self.limits),
        })
        result: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "rule_name": RULE_NAME,
            "details": details,
        }

        if not history:
            details["status"] = STATUS_NO_BASELINE
            details["history_runs"] = 0
            reason = "needs 1 earlier run of this contract on this dataset, found 0"
            if self._history_error:
                details["history_error"] = self._history_error
                reason = f"history could not be read ({self._history_error})"
            passed = self.on_no_baseline == "pass"
            outcome = "not checked" if passed else "failing (on_no_baseline: fail)"
            result.update({
                "passed": passed,
                "failed_count": 0 if passed else 1,
                "message": f"No baseline: {self._subject()} is {_fmt(value)}; {reason}. Drift {outcome}",
            })
            if not passed:
                result["failure_mode"] = str(FailureMode.NO_BASELINE)
            return result

        # Integers aggregate as fractions, so a baseline past 2**53 stays exact.
        exact = all(isinstance(v, int) for v in usable)
        pool = [Fraction(v) for v in usable] if exact else usable
        if self.baseline == "median":
            baseline = statistics.median(pool)
        elif self.baseline == "mean":
            baseline = statistics.mean(pool) if exact else statistics.fmean(pool)
        else:
            baseline = pool[0]
        if isinstance(baseline, Fraction) and baseline.denominator == 1:
            baseline = int(baseline)
        if self.metric in COUNT_METRICS and float(baseline).is_integer():
            baseline = int(baseline)
        details["baseline"] = _plain(baseline)
        against = self._baseline_label(len(usable))

        if value is None:
            details["status"] = STATUS_NO_VALUE
            why = "the dataset is empty" if self.metric == "null_rate" else "no non-null values"
            result.update({
                "passed": False,
                "failed_count": 1,
                "message": (
                    f"{self._subject()} is undefined ({why}) "
                    f"against {_fmt(baseline)} in {against}"
                ),
                "failure_mode": str(FailureMode.METRIC_DRIFT),
            })
            return result

        change = value - baseline
        if baseline != 0:
            change_pct: Optional[float] = float(change / abs(baseline) * 100)
        else:
            # Any move away from zero is an unbounded relative change.
            change_pct = 0.0 if change == 0 else math.copysign(math.inf, change)
        details["change"] = _plain(change)
        details["change_pct"] = change_pct if math.isfinite(change_pct) else None

        exceeded = self._exceeded_limits(change, change_pct)
        change_text = _signed(change)
        if math.isfinite(change_pct):
            change_text += f", {change_pct:+.1f}%"
        if exceeded:
            details["status"] = STATUS_DRIFTED
            details["exceeded"] = exceeded
            verb = "rose" if change > 0 else "fell"
            limits = ", ".join(f"{key} {_fmt(self.limits[key])}" for key in exceeded)
            result.update({
                "passed": False,
                "failed_count": 1,
                "message": (
                    f"{self._subject()} {verb} from {_fmt(baseline)} to {_fmt(value)} "
                    f"({change_text}) against {against}; exceeds {limits}"
                ),
                "failure_mode": str(FailureMode.METRIC_DRIFT),
            })
        else:
            details["status"] = STATUS_OK
            result.update({
                "passed": True,
                "failed_count": 0,
                "message": (
                    f"{self._subject()} is {_fmt(value)} against {_fmt(baseline)} "
                    f"in {against} ({change_text}), within limits"
                ),
            })
        return result

    def _exceeded_limits(self, change: float, change_pct: float) -> List[str]:
        increase = max(change, 0)
        decrease = max(-change, 0)
        amounts = {
            "max_increase": increase,
            "max_decrease": decrease,
            "max_change": abs(change),
            "max_increase_pct": max(change_pct, 0),
            "max_decrease_pct": max(-change_pct, 0),
            "max_change_pct": abs(change_pct),
        }
        return [key for key in LIMITS if key in self.limits and _exceeds(amounts[key], self.limits[key])]
