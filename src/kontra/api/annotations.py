"""
Annotations on validation runs: kontra.annotate(), get_annotations()
and get_run_with_annotations().
"""

import json
from typing import Any, Dict, List, Optional

from kontra.api.history import _resolve_contract_fingerprint
from kontra.api.results import RuleResult, ValidationResult
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


def annotate(
    contract: str,
    *,
    run_id: Optional[str] = None,
    rule_id: Optional[str] = None,
    actor_type: str = "agent",
    actor_id: str,
    annotation_type: str,
    summary: str,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Save an annotation on a validation run or specific rule.

    Annotations provide "memory without authority" - agents and humans can
    record context about runs (resolutions, root causes, acknowledgments)
    without affecting Kontra's validation behavior.

    Invariants:
    - Append-only: annotations are never updated or deleted
    - Uninterpreted: Kontra stores annotation_type but doesn't define vocabulary
    - Never read during validation or diff

    Args:
        contract: Contract name or path
        run_id: Run ID to annotate (default: latest run).
            For file-based backends: string like "2024-01-15T09-30-00_abc123"
            For database backends: integer ID as string
        rule_id: Optional rule ID to annotate a specific rule
        actor_type: Who is creating the annotation ("agent" | "human" | "system")
        actor_id: Identifier for the actor (e.g., "repair-agent-v2", "alice@example.com")
        annotation_type: Type of annotation (e.g., "resolution", "root_cause", "acknowledged")
        summary: Human-readable summary
        payload: Optional structured data (dict)

    Returns:
        Annotation ID (integer)

    Raises:
        ValueError: If contract or run not found, or rule_id not found in run
        RuntimeError: If annotation save fails

    annotation_type is an OPEN vocabulary: any non-empty string is accepted
    (custom, workflow-specific types included). The values below are the
    documented, well-known types surfaced as suggestions - not an enforced set.

    Common annotation_type values (suggested, not enforced):
    - "resolution": I fixed this
    - "root_cause": This failed because...
    - "false_positive": This isn't actually a problem
    - "acknowledged": I saw this, will address later
    - "suppressed": Intentionally ignoring this
    - "note": General comment
    - "diagnosis": First-responder's assessment of a failure
    - "expected": Owner verdict in an adjudication flow

    Example:
        # Annotate the latest run for a contract
        kontra.annotate(
            "users_contract.yml",
            actor_type="agent",
            actor_id="repair-agent-v2",
            annotation_type="resolution",
            summary="Fixed null emails by backfilling from user_profiles table",
        )

        # Annotate a specific rule
        kontra.annotate(
            "users_contract.yml",
            rule_id="COL:email:not_null",
            actor_type="human",
            actor_id="alice@example.com",
            annotation_type="false_positive",
            summary="These are service accounts, nulls are expected",
        )

        # Annotate with structured payload
        kontra.annotate(
            "users_contract.yml",
            actor_type="agent",
            actor_id="analysis-agent",
            annotation_type="root_cause",
            summary="Upstream data source failed validation",
            payload={
                "upstream_source": "crm_export",
                "failure_time": "2024-01-15T08:30:00Z",
                "affected_rows": 1523,
            },
        )
    """
    # Validate annotation inputs (BUG-044, BUG-045).
    #
    # annotation_type is an OPEN vocabulary. Annotations are "memory without
    # authority" - Kontra stores annotation_type but does not interpret it, so
    # there is no reason to hard-reject unknown types. We accept any non-empty
    # string; KNOWN_ANNOTATION_TYPES only drives discoverability/suggestions.
    # A type outside the documented set is accepted and logged at debug level.
    from kontra.state.types import KNOWN_ANNOTATION_TYPES

    if annotation_type is None or not str(annotation_type).strip():
        raise ValueError(
            "annotation_type must be a non-empty string. Known types: "
            f"{', '.join(sorted(KNOWN_ANNOTATION_TYPES))} "
            "(custom types are also accepted)."
        )
    if annotation_type not in KNOWN_ANNOTATION_TYPES:
        _logger.debug(
            "annotation_type %r is outside the documented vocabulary (%s); "
            "accepting as a custom type.",
            annotation_type,
            ", ".join(sorted(KNOWN_ANNOTATION_TYPES)),
        )
    if not summary or not summary.strip():
        raise ValueError("summary must be a non-empty string")

    from kontra.state.backends import get_default_store
    from kontra.state.types import Annotation
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    store = get_default_store()
    if store is None:
        raise RuntimeError("State store not available")

    # Resolve contract to fingerprint
    contract_fp = _resolve_contract_fingerprint(contract, store, "annotate")
    if contract_fp is None:
        raise ValueError(f"Contract not found: {contract}")

    # Get the run state
    if run_id is None:
        # Get latest run
        state = store.get_latest(contract_fp)
        if state is None:
            raise ValueError(f"No runs found for contract: {contract}")
    else:
        # Find specific run
        states = store.get_history(contract_fp, limit=100)
        state = None

        # Try to match run_id as integer (database backends) or string timestamp
        for s in states:
            # Check run_at timestamp match
            if s.run_at.isoformat() == run_id:
                state = s
                break
            # Check ID match (for database backends)
            if s.id is not None and str(s.id) == run_id:
                state = s
                break

        if state is None:
            raise ValueError(f"Run not found: {run_id}")

    # If annotating a specific rule, find the rule_result_id
    rule_result_id = None
    if rule_id is not None:
        found = False
        for rule in state.rules:
            if rule.rule_id == rule_id:
                found = True
                rule_result_id = rule.id  # May be None for file backends
                break

        if not found:
            raise ValueError(f"Rule not found in run: {rule_id}")

    # Create the annotation
    annotation = Annotation(
        run_id=state.id or 0,
        rule_result_id=rule_result_id,
        rule_id=rule_id,  # Store semantic rule ID for cross-run queries
        actor_type=actor_type,
        actor_id=actor_id,
        annotation_type=annotation_type,
        summary=summary,
        payload=payload,
    )

    # Save annotation - method depends on backend type
    try:
        # For database backends, save_annotation works directly
        if hasattr(store, "save_annotation") and not isinstance(store, type):
            try:
                return store.save_annotation(annotation)
            except NotImplementedError:
                pass

        # For file-based backends, need to find the run_id string
        if hasattr(store, "save_annotation_for_run"):
            # Find the run_id string by scanning the runs directory
            run_id_str = _find_run_id_string(store, contract_fp, state)
            if run_id_str is None:
                raise RuntimeError("Could not find run file for annotation")
            return store.save_annotation_for_run(contract_fp, run_id_str, annotation)

        raise RuntimeError("Backend does not support annotations")

    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
        raise RuntimeError(f"Failed to save annotation: {e}") from e


def _find_run_id_string(store: Any, contract_fp: str, state: Any) -> Optional[str]:
    """
    Find the run_id string for a state in file-based backends.

    This is needed because file-based backends use string run IDs but
    ValidationState.id is an integer hash.
    """
    from pathlib import Path

    # LocalStore
    if hasattr(store, "_runs_dir"):
        runs_dir = store._runs_dir(contract_fp)
        if runs_dir.exists():
            for filepath in runs_dir.glob("*.json"):
                if filepath.name.endswith(".ann.jsonl"):
                    continue
                loaded = store._load_state(filepath)
                if loaded and loaded.id == state.id:
                    return filepath.stem
        return None

    # S3Store - similar pattern but via fsspec
    if hasattr(store, "_runs_prefix") and hasattr(store, "_get_fs"):
        fs = store._get_fs()
        prefix = store._runs_prefix(contract_fp)
        try:
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [f for f in all_files if not f.endswith(".ann.jsonl")]
            for filepath in files:
                loaded = store._load_state(filepath)
                if loaded and loaded.id == state.id:
                    return filepath.rsplit("/", 1)[-1].replace(".json", "")
        except (OSError, IOError, ValueError):
            # S3 access failed - can't look up run ID
            pass
        return None

    return None


def get_run_with_annotations(
    contract: str,
    run_id: Optional[str] = None,
) -> Optional[ValidationResult]:
    """
    Get a validation run with its annotations loaded.

    By default, annotations are not loaded (they're opt-in for performance).
    Use this function when you need to see annotations.

    Args:
        contract: Contract name or path
        run_id: Run ID (default: latest run)

    Returns:
        ValidationResult with annotations, or None if not found

    Example:
        result = kontra.get_run_with_annotations("users_contract.yml")
        if result:
            for rule in result.rules:
                print(f"{rule.rule_id}: {rule.annotations}")
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return None

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_run_with_annotations")
        if contract_fp is None:
            return None

        # Convert run_id string to integer if needed
        run_id_int = None
        if run_id is not None:
            try:
                run_id_int = int(run_id)
            except ValueError:
                # It's a timestamp or string ID - need to find the matching state
                states = store.get_history(contract_fp, limit=100)
                for s in states:
                    if s.run_at.isoformat() == run_id:
                        run_id_int = s.id
                        break

        state = store.get_run_with_annotations(contract_fp, run_id_int)
        if state is None:
            return None

        # Convert to ValidationResult
        return ValidationResult(
            passed=state.summary.passed,
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
                    annotations=[a.to_dict() for a in r.annotations] if r.annotations else None,
                )
                for r in state.rules
            ],
            annotations=[a.to_dict() for a in state.annotations] if state.annotations else None,
        )
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
        log_exception(_logger, "Failed to get run with annotations", e)
        return None


def get_annotations(
    contract: str,
    *,
    rule_id: Optional[str] = None,
    annotation_type: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Retrieve annotations across runs for a contract.

    Primary use case: Agent sees a failure, wants to check if past runs
    have hints about this rule. This provides cross-session memory.

    Args:
        contract: Contract name or path
        rule_id: Filter to annotations on this rule (recommended)
        annotation_type: Filter by type (e.g., "resolution", "false_positive")
        limit: Max annotations to return (default 20)

    Returns:
        List of annotation dicts, most recent first. Each dict contains:
        - id: Annotation ID
        - run_id: Which run this was attached to
        - rule_id: Semantic rule ID (e.g., "COL:email:not_null") or None for run-level
        - actor_type: "agent" | "human" | "system"
        - actor_id: Who created it
        - annotation_type: Type (e.g., "resolution", "root_cause")
        - summary: Human-readable summary
        - payload: Optional structured data
        - created_at: When it was created

    Example:
        # Agent sees COL:email:not_null failing, checks for past hints
        hints = kontra.get_annotations(
            "users_contract.yml",
            rule_id="COL:email:not_null",
        )

        for hint in hints:
            print(f"[{hint['annotation_type']}] {hint['summary']}")

        # Get only resolutions
        resolutions = kontra.get_annotations(
            "users_contract.yml",
            rule_id="COL:email:not_null",
            annotation_type="resolution",
        )
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return []

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_annotations")
        if contract_fp is None:
            return []

        annotations = store.get_annotations_for_contract(
            contract_fp,
            rule_id=rule_id,
            annotation_type=annotation_type,
            limit=limit,
        )

        return [a.to_dict() for a in annotations]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
        log_exception(_logger, "Failed to get annotations", e)
        return []
