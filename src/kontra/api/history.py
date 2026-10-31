"""
Validation history: kontra.get_history(), diff(), get_run(), has_runs()
and the deprecated list_runs().
"""

import json
import os
from typing import Any, Dict, List, Optional

from kontra.api.inputs import _validate_contract_path
from kontra.api.results import Diff, RuleResult, ValidationResult
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


def get_history(
    contract: str,
    *,
    limit: int = 20,
    since: Optional[str] = None,
    failed_only: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get validation history for a contract.

    Args:
        contract: Path to contract YAML file
        limit: Maximum number of runs to return (default: 20)
        since: Only return runs after this date/time. Formats:
            - "24h", "7d" - relative time
            - "2026-01-15" - specific date
        failed_only: Only return failed runs

    Returns:
        List of run summaries, newest first. Each summary contains:
        - run_id: Unique identifier
        - timestamp: When the run occurred (ISO format)
        - passed: Overall pass/fail
        - failed_count: Total failures
        - total_rows: Row count (if available)
        - contract_name: Name of the contract

    Example:
        history = kontra.get_history("contract.yml")
        for run in history:
            print(f"{run['timestamp']}: {'PASS' if run['passed'] else 'FAIL'}")

        # Last 7 days only
        recent = kontra.get_history("contract.yml", since="7d")

        # Only failed runs
        failures = kontra.get_history("contract.yml", failed_only=True)
    """
    from datetime import datetime, timedelta, timezone
    from kontra.config.loader import ContractLoader
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.state.backends import get_default_store

    # Validate that contract is a YAML file, not a data file (BUG-014)
    _validate_contract_path(contract, "get_history")

    # Load contract to get fingerprint
    contract_obj = ContractLoader.from_path(contract)
    fp = fingerprint_contract(contract_obj)

    # Parse since parameter
    since_dt = None
    if since:
        now = datetime.now(timezone.utc)
        since_lower = since.lower().strip()

        if since_lower.endswith("h"):
            hours = int(since_lower[:-1])
            since_dt = now - timedelta(hours=hours)
        elif since_lower.endswith("d"):
            days = int(since_lower[:-1])
            since_dt = now - timedelta(days=days)
        else:
            # Try parsing as date
            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"Invalid since format: {since}. Use '24h', '7d', or 'YYYY-MM-DD'")

    # Get history from store
    store = get_default_store()
    if store is None:
        return []

    summaries = store.get_run_summaries(
        contract_fingerprint=fp,
        limit=limit,
        since=since_dt,
        failed_only=failed_only,
    )

    return [s.to_dict() for s in summaries]


def diff(
    contract: str,
    *,
    since: Optional[str] = None,
    before: Optional[str] = None,
    after: Optional[str] = None,
) -> Optional[Diff]:
    """
    Compare validation runs over time.

    Args:
        contract: Contract name or path
        since: Compare to run from this time ago ("7d", "24h", "2024-01-15")
        before: Specific run ID for before state
        after: Specific run ID for after state (default: latest)

    Returns:
        Diff with .has_changes, .regressed, .new_failures, .to_llm()
        Returns None if no history available

    Example:
        diff = kontra.diff("users_contract", since="7d")
        if diff and diff.regressed:
            print("Quality regressed!")
            for failure in diff.new_failures:
                print(f"  NEW: {failure['rule_id']}")
    """
    from kontra.state.backends import get_default_store
    from kontra.state.types import StateDiff
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader
    from kontra.errors import StateCorruptedError

    store = get_default_store()
    if store is None:
        return Diff.empty("No state store configured")

    # Validate that contract is a YAML file, not a data file (BUG-014)
    if os.path.isfile(contract):
        _validate_contract_path(contract, "diff")

    # Resolve contract to fingerprint
    try:
        # If it's a file path, load contract and compute semantic fingerprint
        if os.path.isfile(contract):
            contract_obj = ContractLoader.from_path(contract)
            contract_fp = fingerprint_contract(contract_obj)
        else:
            # Assume it's a contract name - search stored states
            # Look through all contracts for matching name
            contract_fp = None
            for fp in store.list_contracts():
                history = store.get_history(fp, limit=1)
                if history and history[0].contract_name == contract:
                    contract_fp = fp
                    break

            if contract_fp is None:
                return Diff.empty(f"No history found for contract '{contract}'")

        # Get history for this contract
        states = store.get_history(contract_fp, limit=100)
        if len(states) < 2:
            return Diff.empty("Need at least 2 validation runs to compute a diff")

        # states are newest first, so [0] is latest, [1] is previous
        after_state = states[0]
        before_state = states[1]

        # Compute diff
        state_diff = StateDiff.compute(before_state, after_state)
        return Diff.from_state_diff(state_diff)

    except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
        # These indicate corrupted state data
        raise StateCorruptedError(contract, str(e))
    except FileNotFoundError:
        # No history available - this is normal
        return Diff.empty("No history available")
    except Exception as e:
        # For other exceptions, log and re-raise as state corruption
        # since we've already handled the "no history" case
        log_exception(_logger, "Failed to compute diff", e)
        raise StateCorruptedError(contract, str(e))


def _resolve_contract_fingerprint(contract: str, store: Any, caller: str = "state function") -> Optional[str]:
    """
    Resolve a contract name or path to its fingerprint.

    Args:
        contract: Contract name or file path
        store: State store instance
        caller: Name of the calling function (for error messages)

    Returns:
        Contract fingerprint or None if not found
    """
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    # If it's a file path, load contract and compute semantic fingerprint
    if os.path.isfile(contract):
        # Validate that it's not a data file (BUG-014)
        _validate_contract_path(contract, caller)
        contract_obj = ContractLoader.from_path(contract)
        return fingerprint_contract(contract_obj)

    # Assume it's a contract name - search stored states
    for fp in store.list_contracts():
        history = store.get_history(fp, limit=1)
        if history and history[0].contract_name == contract:
            return fp

    return None


def list_runs(contract: str) -> List[Dict[str, Any]]:
    """
    List past validation runs for a contract.

    .. deprecated::
        Use :func:`get_history` instead, which supports filtering by date,
        limit, and failed-only.

    Args:
        contract: Contract name or path

    Returns:
        List of run summaries (same schema as get_history).
    """
    import warnings
    warnings.warn(
        "kontra.list_runs() is deprecated, use kontra.get_history() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_history(contract, limit=100)


def get_run(
    contract: str,
    run_id: Optional[str] = None,
) -> Optional[ValidationResult]:
    """
    Get a specific validation run.

    Args:
        contract: Contract name or path
        run_id: Specific run ID (default: latest)

    Returns:
        ValidationResult or None if not found
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return None

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_run")
        if contract_fp is None:
            return None

        # Get history and find specific run or latest
        states = store.get_history(contract_fp, limit=100)
        if not states:
            return None

        state = None
        if run_id:
            # Find specific run - try multiple match strategies:
            # 1. Match by state.id (the run_id from get_history)
            # 2. Match by timestamp ISO format
            for s in states:
                state_id = str(s.id) if s.id else None
                if state_id == run_id or s.run_at.isoformat() == run_id:
                    state = s
                    break
        else:
            # Get latest (first in list, newest first)
            state = states[0]

        if state is None:
            if run_id:
                raise ValueError(f"Run not found: {run_id}")
            return None

        # Convert state to ValidationResult
        # Ensure passed is bool, not None (BUG-043)
        passed = state.summary.passed
        if passed is None:
            passed = state.summary.blocking_failures == 0
        return ValidationResult(
            passed=passed,
            dataset=state.dataset_uri,
            total_rows=state.summary.row_count or 0,
            total_rules=state.summary.total_rules,
            passed_count=state.summary.passed_rules,
            failed_count=state.summary.blocking_failures,
            warning_count=state.summary.warning_failures,
            rules=[
                RuleResult(
                    rule_id=r.rule_id,
                    name=r.rule_name,
                    passed=r.passed,
                    failed_count=r.failed_count,
                    message=r.message or "",
                    severity=r.severity,
                    source=r.execution_source,
                    column=r.column,
                )
                for r in state.rules
            ],
        )
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
        log_exception(_logger, "Failed to get run", e)
        return None


def has_runs(contract: str) -> bool:
    """
    Check if any validation history exists for a contract.

    Args:
        contract: Contract name or path

    Returns:
        True if history exists
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return False

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "has_runs")
        if contract_fp is None:
            return False

        states = store.get_history(contract_fp, limit=1)
        return len(states) > 0
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
        log_exception(_logger, "Failed to check runs", e)
        return False
