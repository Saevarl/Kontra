# src/kontra/engine/phases/drift.py
"""
Drift rule support around the execution tiers.

Before execution, each drift rule gets its earlier measurements from the state
backend (``attach_history``). The SQL tier only *measures* a drift rule; once
every tier has run, ``finalize_results`` turns those measurements into verdicts.
A drift rule that reached Polars judged itself in ``validate()``.

History is the saved runs of the same contract fingerprint whose dataset URI is
the current run's; each holds the rule's measurement in its result details.
Runs saved before a rule existed, or of another dataset, are no baseline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from kontra.rule_defs.builtin.drift import DriftRule
    from kontra.state.backends.base import StateBackend

from kontra.logging import get_logger

_logger = get_logger(__name__)

# The first read covers the window plus some runs of other datasets sharing
# the contract's history; each further read doubles it, up to a fixed cap so
# a long history of other datasets can't make every run load all of it.
_HISTORY_SCAN_FACTOR = 3
_HISTORY_SCAN_SLACK = 10
_HISTORY_SCAN_MAX = 1000


def drift_rules(rules: List[Any]) -> List["DriftRule"]:
    """The drift rules among compiled rules (builtins already registered)."""
    from kontra.rule_defs.builtin.drift import DriftRule

    return [r for r in rules if isinstance(r, DriftRule)]


def attach_history(
    rules: List["DriftRule"],
    store: Optional["StateBackend"],
    contract_fingerprint: str,
    dataset_uri: str,
) -> None:
    """
    Give each drift rule its earlier measurements, newest first.

    A store that can't be read leaves every rule without a baseline, with the
    error in its result: validation goes on, and the result says why the
    check didn't run.
    """
    if not rules:
        return
    if store is None:
        for rule in rules:
            rule.set_history(None, error="no state backend")
        return

    needed = max(rule.history_needed for rule in rules)
    limit = max(needed * _HISTORY_SCAN_FACTOR, needed + _HISTORY_SCAN_SLACK)
    cap = max(limit, _HISTORY_SCAN_MAX)
    while True:
        try:
            states = store.get_history(contract_fingerprint, limit=limit)
        except Exception as e:  # any backend failure; reported per rule
            _logger.warning("Could not read drift history: %s", e)
            for rule in rules:
                rule.set_history(None, error=f"{type(e).__name__}: {e}")
            return
        complete = len(states) < limit
        if not complete:
            states = _drop_partial_second(states)
        histories = _histories(rules, states, dataset_uri)
        if complete or limit >= cap or all(
            len(values) >= rule.history_needed for rule, values in zip(rules, histories)
        ):
            break
        limit = min(limit * 2, cap)

    for rule, values in zip(rules, histories):
        rule.set_history(values)


def _drop_partial_second(states: List[Any]) -> List[Any]:
    """
    Drop the runs of the oldest second in a truncated read.

    File-based stores order runs by a second-resolution name with a random
    suffix, so a read cut off inside one second holds an arbitrary part of that
    second's runs, and an unread run of it may be newer than the read ones.
    Every later second was read in full.
    """
    seconds = [_aware(s.run_at).replace(microsecond=0) for s in states]
    if not seconds:
        return states
    oldest = min(seconds)
    return [s for s, second in zip(states, seconds) if second != oldest]


def _histories(
    rules: List["DriftRule"], states: List[Any], dataset_uri: str
) -> List[List[float]]:
    """Each rule's measurements in this dataset's runs, newest first."""
    # run_at keeps microseconds, so runs saved within one second still come
    # out in order.
    states = sorted(
        (s for s in states if s.dataset_uri == dataset_uri),
        key=lambda s: _aware(s.run_at),
        reverse=True,
    )
    histories: List[List[float]] = []
    for rule in rules:
        values: List[float] = []
        for state in states:
            value = rule.stored_measurement(state.get_rule(rule.rule_id))
            if value is not None:
                values.append(value)
                if len(values) >= rule.history_needed:
                    break
        histories.append(values)
    return histories


def _aware(moment: datetime) -> datetime:
    # A naive timestamp from an older or hand-written state is UTC.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def finalize_results(results: List[Dict[str, Any]], rules: List["DriftRule"]) -> None:
    """Judge drift measurements taken by the SQL tier, in place."""
    by_id = {rule.rule_id: rule for rule in rules}
    for result in results:
        rule = by_id.get(result.get("rule_id"))
        if rule is None:
            continue
        # Every drift result names its rule, including config errors and
        # crashes, so results and saved runs read as drift, not by id suffix.
        result["rule_name"] = rule.name
        if "metric_value" in result:
            verdict = rule.evaluate_measurement(result.pop("metric_value"))
            # Tier fields (execution_source, severity, tally, context) stay.
            result.pop("failure_mode", None)
            result.update(verdict)
