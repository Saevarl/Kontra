# src/kontra/state/backends/base.py
"""
StateBackend protocol definition.

All state storage implementations must conform to this protocol.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.state.types import ValidationState


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
