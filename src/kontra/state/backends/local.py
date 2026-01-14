# src/kontra/state/backends/local.py
"""
Local filesystem state storage.

Stores validation states in .kontra/state/ directory structure:

.kontra/
└── state/
    └── <contract_fingerprint>/
        ├── 2024-01-13T10-30-00.json
        ├── 2024-01-12T10-30-00.json
        └── ...
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .base import StateBackend
from kontra.state.types import ValidationState


class LocalStore(StateBackend):
    """
    Filesystem-based state storage.

    Default storage location is .kontra/state/ in the current working
    directory. Can be customized via the base_path parameter.

    State files are JSON with timestamp-based names for easy sorting.
    """

    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize the local store.

        Args:
            base_path: Base directory for state storage.
                      Defaults to .kontra/state/ in cwd.
        """
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path.cwd() / ".kontra" / "state"

    def _contract_dir(self, contract_fingerprint: str) -> Path:
        """Get the directory for a contract's states."""
        return self.base_path / contract_fingerprint

    def _state_filename(self, run_at: datetime) -> str:
        """Generate filename from timestamp."""
        # Use ISO format but replace : with - for filesystem compatibility
        ts = run_at.isoformat().replace(":", "-").replace("+", "_")
        return f"{ts}.json"

    def _parse_filename_timestamp(self, filename: str) -> Optional[datetime]:
        """Parse timestamp from filename."""
        try:
            # Remove .json extension
            ts_str = filename.replace(".json", "")
            # Restore : from - and + from _
            ts_str = ts_str.replace("-", ":", 2)  # Only first two for time
            # Handle the date part separately
            parts = ts_str.split("T")
            if len(parts) != 2:
                return None
            date_part = parts[0]
            time_part = parts[1].replace("-", ":")
            time_part = time_part.replace("_", "+")
            ts_str = f"{date_part}T{time_part}"
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None

    def save(self, state: ValidationState) -> None:
        """Save a validation state to the filesystem."""
        contract_dir = self._contract_dir(state.contract_fingerprint)
        contract_dir.mkdir(parents=True, exist_ok=True)

        filename = self._state_filename(state.run_at)
        filepath = contract_dir / filename

        # Write atomically using temp file
        temp_path = filepath.with_suffix(".tmp")
        try:
            temp_path.write_text(state.to_json(), encoding="utf-8")
            temp_path.rename(filepath)
        except Exception:
            # Clean up temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            raise

    def get_latest(self, contract_fingerprint: str) -> Optional[ValidationState]:
        """Get the most recent state for a contract."""
        history = self.get_history(contract_fingerprint, limit=1)
        return history[0] if history else None

    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history for a contract, newest first."""
        contract_dir = self._contract_dir(contract_fingerprint)

        if not contract_dir.exists():
            return []

        # List all JSON files
        state_files = sorted(
            contract_dir.glob("*.json"),
            key=lambda p: p.name,
            reverse=True,  # Newest first
        )

        states = []
        for filepath in state_files[:limit]:
            try:
                content = filepath.read_text(encoding="utf-8")
                state = ValidationState.from_json(content)
                states.append(state)
            except Exception:
                # Skip corrupted files
                continue

        return states

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """Delete old states, keeping the most recent ones."""
        contract_dir = self._contract_dir(contract_fingerprint)

        if not contract_dir.exists():
            return 0

        # List all JSON files, sorted newest first
        state_files = sorted(
            contract_dir.glob("*.json"),
            key=lambda p: p.name,
            reverse=True,
        )

        # Delete files beyond keep_count
        deleted = 0
        for filepath in state_files[keep_count:]:
            try:
                filepath.unlink()
                deleted += 1
            except Exception:
                continue

        return deleted

    def list_contracts(self) -> List[str]:
        """List all contract fingerprints with stored state."""
        if not self.base_path.exists():
            return []

        contracts = []
        for item in self.base_path.iterdir():
            if item.is_dir() and len(item.name) == 16:  # Fingerprint length
                contracts.append(item.name)

        return sorted(contracts)

    def clear(self, contract_fingerprint: Optional[str] = None) -> int:
        """
        Clear stored states.

        Args:
            contract_fingerprint: If provided, only clear this contract's states.
                                 If None, clear all states.

        Returns:
            Number of state files deleted.
        """
        deleted = 0

        if contract_fingerprint:
            contract_dir = self._contract_dir(contract_fingerprint)
            if contract_dir.exists():
                for filepath in contract_dir.glob("*.json"):
                    filepath.unlink()
                    deleted += 1
                # Remove empty directory
                try:
                    contract_dir.rmdir()
                except OSError:
                    pass
        else:
            # Clear all
            if self.base_path.exists():
                for contract_dir in self.base_path.iterdir():
                    if contract_dir.is_dir():
                        for filepath in contract_dir.glob("*.json"):
                            filepath.unlink()
                            deleted += 1
                        try:
                            contract_dir.rmdir()
                        except OSError:
                            pass

        return deleted

    def __repr__(self) -> str:
        return f"LocalStore(base_path={self.base_path})"
