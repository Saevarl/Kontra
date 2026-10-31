# src/kontra/engine/phases/drift.py
"""
Drift rule support around the execution tiers.

Before execution, each drift rule gets its earlier measurements from the state
backend (``attach_history``). Preplan and SQL only *measure* a drift rule; once
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

# Runs of other datasets share the contract's history, so read past them.
_HISTORY_SCAN_FACTOR = 3
_HISTORY_SCAN_SLACK = 10


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
    try:
        states = store.get_history(contract_fingerprint, limit=limit)
    except Exception as e:  # any backend failure; reported per rule
        _logger.warning("Could not read drift history: %s", e)
        for rule in rules:
            rule.set_history(None, error=f"{type(e).__name__}: {e}")
        return

    # File-based stores order runs by a second-resolution name; run_at keeps
    # microseconds, so runs saved within one second still come out in order.
    states = sorted(
        (s for s in states if s.dataset_uri == dataset_uri),
        key=lambda s: _aware(s.run_at),
        reverse=True,
    )
    for rule in rules:
        values: List[float] = []
        for state in states:
            value = rule.stored_measurement(state.get_rule(rule.rule_id))
            if value is not None:
                values.append(value)
                if len(values) >= rule.history_needed:
                    break
        rule.set_history(values)


def _aware(moment: datetime) -> datetime:
    # A naive timestamp from an older or hand-written state is UTC.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def finalize_results(results: List[Dict[str, Any]], rules: List["DriftRule"]) -> None:
    """Judge drift measurements taken by preplan or SQL, in place."""
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
