# src/kontra/state/backends/base.py
"""
StateBackend protocol definition.

All state storage implementations must conform to this protocol.

v0.5 adds:
- Normalized schema (kontra_runs, kontra_rule_results)
- Annotations (kontra_annotations) with append-only semantics
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.state.types import Annotation, RunSummary, ValidationState


class StateBackend(ABC):
    """
    Abstract base class for state storage backends.

    Implementations provide persistence for ValidationState objects,
    enabling history tracking and comparison across runs.

    Design principles:
    - Immutable writes: Each save creates a new record
    - Query by contract: States are indexed by contract fingerprint
    - Time-ordered: History is returned newest-first
    """

    @abstractmethod
    def save(self, state: "ValidationState") -> None:
        """
        Save a validation state.

        The state is immutable once saved. Each call creates a new record
        identified by (contract_fingerprint, run_at).

        Args:
            state: The ValidationState to persist

        Raises:
            IOError: If the save fails
        """
        ...

    @abstractmethod
    def get_latest(self, contract_fingerprint: str) -> Optional["ValidationState"]:
        """
        Get the most recent state for a contract.

        Args:
            contract_fingerprint: The contract's fingerprint hash

        Returns:
            The most recent ValidationState, or None if no history exists
        """
        ...

    @abstractmethod
    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List["ValidationState"]:
        """
        Get recent history for a contract.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            limit: Maximum number of states to return

        Returns:
            List of ValidationState objects, newest first
        """
        ...

    def get_at(
        self,
        contract_fingerprint: str,
        timestamp: datetime,
    ) -> Optional["ValidationState"]:
        """
        Get state at or before a specific timestamp.

        Default implementation uses get_history and filters.
        Backends may override with more efficient queries.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            timestamp: The target timestamp

        Returns:
            The ValidationState at or before timestamp, or None
        """
        history = self.get_history(contract_fingerprint, limit=100)
        for state in history:
            if state.run_at <= timestamp:
                return state
        return None

    def get_previous(
        self,
        contract_fingerprint: str,
        before: datetime,
    ) -> Optional["ValidationState"]:
        """
        Get the state immediately before a timestamp.

        Useful for comparing current run to previous run.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            before: Get state before this timestamp

        Returns:
            The most recent ValidationState before timestamp, or None
        """
        history = self.get_history(contract_fingerprint, limit=100)
        for state in history:
            if state.run_at < before:
                return state
        return None

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """
        Delete old states, keeping the most recent ones.

        Default implementation does nothing. Backends may override
        to implement retention policies.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            keep_count: Number of recent states to keep

        Returns:
            Number of states deleted
        """
        return 0

    def list_contracts(self) -> List[str]:
        """
        List all contract fingerprints with stored state.

        Default implementation returns empty list. Backends may override.

        Returns:
            List of contract fingerprint strings
        """
        return []

    def get_run_summaries(
        self,
        contract_fingerprint: str,
        limit: int = 20,
        since: Optional[datetime] = None,
        failed_only: bool = False,
    ) -> List["RunSummary"]:
        """
        Get lightweight run summaries for history listing.

        More efficient than get_history() as it doesn't load rule details.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            limit: Maximum number of summaries to return
            since: Only return runs after this timestamp
            failed_only: Only return failed runs

        Returns:
            List of RunSummary objects, newest first
        """
        # Default implementation converts from full states
        from kontra.state.types import RunSummary

        states = self.get_history(contract_fingerprint, limit=limit * 2)
        summaries = []

        for i, state in enumerate(states):
            if since and state.run_at < since:
                continue
            if failed_only and state.summary.passed:
                continue

            run_id = str(state.id) if state.id else f"run_{i}"
            summaries.append(RunSummary.from_validation_state(state, run_id))

            if len(summaries) >= limit:
                break

        return summaries

    # -------------------------------------------------------------------------
    # Annotation Methods (v0.5)
    # -------------------------------------------------------------------------
    #
    # Annotations are append-only records that agents/humans can attach to
    # validation runs. Kontra never reads annotations during validation or diff.
    #
    # Default implementations do nothing. Database backends override with
    # actual persistence logic.

    @staticmethod
    def _attach_annotations_to_state(
        state: "ValidationState",
        annotations: List["Annotation"],
    ) -> None:
        """
        Attach annotations to a ValidationState, grouping by rule_result_id.

        Modifies state in-place:
        - Sets state.annotations to run-level annotations (rule_result_id is None)
        - Sets rule.annotations for each rule result

        Args:
            state: The ValidationState to modify
            annotations: List of annotations to attach
        """
        # Group annotations by rule_result_id
        run_annotations: List["Annotation"] = []
        rule_annotations: Dict[int, List["Annotation"]] = {}

        for ann in annotations:
            if ann.rule_result_id is None:
                run_annotations.append(ann)
            else:
                rule_annotations.setdefault(ann.rule_result_id, []).append(ann)

        state.annotations = run_annotations
        for rule in state.rules:
            if rule.id is not None:
                rule.annotations = rule_annotations.get(rule.id, [])
            else:
                rule.annotations = []

    def save_annotation(self, annotation: "Annotation") -> int:
        """
        Save an annotation.

        Annotations are append-only. Each save creates a new record.

        Args:
            annotation: The Annotation to persist

        Returns:
            The database-assigned ID of the new annotation

        Raises:
            IOError: If the save fails
            ValueError: If annotation references non-existent run/rule
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support annotations"
        )

    def get_annotations(
        self,
        run_id: int,
        rule_result_id: Optional[int] = None,
    ) -> List["Annotation"]:
        """
        Get annotations for a run or specific rule result.

        Args:
            run_id: The run ID to get annotations for
            rule_result_id: If provided, filter to annotations on this rule

        Returns:
            List of Annotation objects, newest first
        """
        return []

    def get_annotations_for_contract(
        self,
        contract_fingerprint: str,
        rule_id: Optional[str] = None,
        annotation_type: Optional[str] = None,
        limit: int = 20,
    ) -> List["Annotation"]:
        """
        Get annotations across all runs for a contract.

        This is the cross-run query for agent memory - "what annotations exist
        for this rule across all past runs?"

        Args:
            contract_fingerprint: The contract's fingerprint hash
            rule_id: If provided, filter to annotations on this rule
            annotation_type: If provided, filter by annotation type
            limit: Maximum number of annotations to return

        Returns:
            List of Annotation objects, newest first, with rule_id populated
        """
        return []

    def get_run_with_annotations(
        self,
        contract_fingerprint: str,
        run_id: Optional[int] = None,
    ) -> Optional["ValidationState"]:
        """
        Get a validation state with its annotations loaded.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            run_id: Specific run ID. If None, gets the latest run.

        Returns:
            ValidationState with annotations populated, or None
        """
        # Default: get latest and attach empty annotations
        state = self.get_latest(contract_fingerprint) if run_id is None else None
        if state:
            state.annotations = []
            for rule in state.rules:
                rule.annotations = []
        return state

    def get_history_with_annotations(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List["ValidationState"]:
        """
        Get recent history with annotations loaded.

        Args:
            contract_fingerprint: The contract's fingerprint hash
            limit: Maximum number of states to return

        Returns:
            List of ValidationState objects with annotations, newest first
        """
        states = self.get_history(contract_fingerprint, limit=limit)
        for state in states:
            state.annotations = []
            for rule in state.rules:
                rule.annotations = []
        return states
