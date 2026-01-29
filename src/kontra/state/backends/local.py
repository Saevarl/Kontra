# src/kontra/state/backends/local.py
"""
Local filesystem state storage with normalized format (v0.5).

Directory structure:
    .kontra/state/
    └── <contract_fingerprint>/
        └── runs/
            ├── <run_id>.json       # run metadata + rule results
            └── <run_id>.ann.jsonl  # annotations (append-only)
"""

from __future__ import annotations

import json
import logging
import os
import random
import string
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .base import StateBackend
from kontra.state.types import Annotation, ValidationState

_logger = logging.getLogger(__name__)


class LocalStore(StateBackend):
    """
    Filesystem-based state storage with normalized format.

    Default storage location is .kontra/state/ in the current working
    directory. Can be customized via the base_path parameter.

    Run IDs are timestamp-based: YYYY-MM-DDTHH-MM-SS_<random>
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

    def _runs_dir(self, contract_fingerprint: str) -> Path:
        """Get the runs directory for a contract."""
        return self._contract_dir(contract_fingerprint) / "runs"

    def _generate_run_id(self, run_at: datetime) -> str:
        """Generate a unique run ID from timestamp."""
        # Format: YYYY-MM-DDTHH-MM-SS_<random>
        # The timestamp prefix makes them sortable
        ts = run_at.strftime("%Y-%m-%dT%H-%M-%S")
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{ts}_{suffix}"

    def _parse_run_id_timestamp(self, run_id: str) -> Optional[datetime]:
        """Parse timestamp from run ID."""
        try:
            # Split on underscore to get timestamp part
            ts_part = run_id.split("_")[0]
            return datetime.strptime(ts_part, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            # ValueError: invalid timestamp format
            # IndexError: no underscore in run_id
            return None

    def _run_file(self, contract_fingerprint: str, run_id: str) -> Path:
        """Get the path for a run's state file."""
        return self._runs_dir(contract_fingerprint) / f"{run_id}.json"

    def _annotations_file(self, contract_fingerprint: str, run_id: str) -> Path:
        """Get the path for a run's annotations file."""
        return self._runs_dir(contract_fingerprint) / f"{run_id}.ann.jsonl"

    def save(self, state: ValidationState) -> None:
        """Save a validation state to the filesystem."""
        runs_dir = self._runs_dir(state.contract_fingerprint)
        runs_dir.mkdir(parents=True, exist_ok=True)

        # Generate run ID if not set
        run_id = self._generate_run_id(state.run_at)

        # Store run_id in the state dict
        state_dict = state.to_dict()
        state_dict["_run_id"] = run_id

        filepath = self._run_file(state.contract_fingerprint, run_id)

        # Write atomically using temp file
        temp_path = filepath.with_suffix(".tmp")
        try:
            temp_path.write_text(
                json.dumps(state_dict, indent=2, default=str),
                encoding="utf-8",
            )
            temp_path.rename(filepath)
        except (OSError, PermissionError, IOError):
            # Clean up temp file on failure, then re-raise
            if temp_path.exists():
                temp_path.unlink()
            raise

    def _load_state(self, filepath: Path) -> Optional[ValidationState]:
        """Load a state from a file path."""
        try:
            content = filepath.read_text(encoding="utf-8")
            data = json.loads(content)

            # Extract run_id for later use
            run_id = data.pop("_run_id", None)

            state = ValidationState.from_dict(data)

            # Store run_id as a synthetic ID (hash for now)
            if run_id:
                # Use string hash as integer ID for compatibility
                state.id = hash(run_id) & 0x7FFFFFFF  # Positive integer

            return state
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as e:
            _logger.warning(f"Malformed state file {filepath}: {e}")
            return None
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not read state from {filepath}: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            _logger.warning(f"Invalid state data in {filepath}: {e}")
            return None

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
        runs_dir = self._runs_dir(contract_fingerprint)

        if not runs_dir.exists():
            return []

        # List all JSON files (excluding .ann.jsonl)
        state_files = sorted(
            [f for f in runs_dir.glob("*.json") if not f.name.endswith(".ann.jsonl")],
            key=lambda p: p.name,
            reverse=True,  # Newest first (timestamp prefix sorts correctly)
        )

        states = []
        for filepath in state_files[:limit]:
            state = self._load_state(filepath)
            if state:
                states.append(state)

        return states

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """Delete old states, keeping the most recent ones."""
        runs_dir = self._runs_dir(contract_fingerprint)

        if not runs_dir.exists():
            return 0

        # List all JSON files, sorted newest first
        state_files = sorted(
            [f for f in runs_dir.glob("*.json") if not f.name.endswith(".ann.jsonl")],
            key=lambda p: p.name,
            reverse=True,
        )

        # Delete files beyond keep_count
        deleted = 0
        for filepath in state_files[keep_count:]:
            try:
                # Delete state file
                filepath.unlink()
                deleted += 1

                # Also delete corresponding annotations file if exists
                run_id = filepath.stem
                ann_file = self._annotations_file(contract_fingerprint, run_id)
                if ann_file.exists():
                    ann_file.unlink()
            except FileNotFoundError:
                pass  # Already deleted
            except (OSError, PermissionError) as e:
                _logger.debug(f"Could not delete old state {filepath}: {e}")
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
            runs_dir = self._runs_dir(contract_fingerprint)
            if runs_dir.exists():
                for filepath in runs_dir.glob("*.json"):
                    filepath.unlink()
                    deleted += 1
                for filepath in runs_dir.glob("*.jsonl"):
                    filepath.unlink()
                # Remove empty directories
                try:
                    runs_dir.rmdir()
                    self._contract_dir(contract_fingerprint).rmdir()
                except OSError:
                    pass
        else:
            # Clear all
            if self.base_path.exists():
                for contract_dir in self.base_path.iterdir():
                    if contract_dir.is_dir():
                        runs_dir = contract_dir / "runs"
                        if runs_dir.exists():
                            for filepath in runs_dir.glob("*.json"):
                                filepath.unlink()
                                deleted += 1
                            for filepath in runs_dir.glob("*.jsonl"):
                                filepath.unlink()
                            try:
                                runs_dir.rmdir()
                            except OSError:
                                pass
                        try:
                            contract_dir.rmdir()
                        except OSError:
                            pass

        return deleted

    # -------------------------------------------------------------------------
    # Annotation Methods
    # -------------------------------------------------------------------------

    def save_annotation(self, annotation: Annotation) -> int:
        """
        Save an annotation (append-only).

        For file-based backends, we need the run_id string, not the integer ID.
        Annotations are stored in JSONL format alongside the run file.
        """
        # We need to find the run file to get the run_id string
        # This is a limitation of file-based backends - we need the fingerprint

        # For now, raise NotImplementedError - annotations require the contract_fingerprint
        # which isn't stored in the annotation. Callers should use save_annotation_for_run.
        raise NotImplementedError(
            "LocalStore.save_annotation requires contract fingerprint. "
            "Use save_annotation_for_run instead."
        )

    def save_annotation_for_run(
        self,
        contract_fingerprint: str,
        run_id_str: str,
        annotation: Annotation,
    ) -> int:
        """
        Save an annotation for a specific run.

        Args:
            contract_fingerprint: The contract fingerprint
            run_id_str: The string run ID (e.g., "2024-01-15T09-30-00_abc123")
            annotation: The annotation to save

        Returns:
            A synthetic annotation ID (line number)
        """
        ann_file = self._annotations_file(contract_fingerprint, run_id_str)
        ann_file.parent.mkdir(parents=True, exist_ok=True)

        # Generate a synthetic ID based on existing line count
        line_count = 0
        if ann_file.exists():
            with open(ann_file, encoding="utf-8") as f:
                line_count = sum(1 for _ in f)
        annotation.id = line_count + 1

        # Append to JSONL file
        with open(ann_file, "a", encoding="utf-8") as f:
            f.write(annotation.to_json() + "\n")

        return annotation.id

    def get_annotations(
        self,
        run_id: int,
        rule_result_id: Optional[int] = None,
    ) -> List[Annotation]:
        """
        Get annotations for a run.

        Note: For file-based backends, run_id is a hash of the run_id string.
        This method may not work directly. Use get_run_with_annotations instead.
        """
        # File-based backends need the fingerprint to locate annotations
        return []

    def get_annotations_for_contract(
        self,
        contract_fingerprint: str,
        rule_id: Optional[str] = None,
        annotation_type: Optional[str] = None,
        limit: int = 20,
    ) -> List[Annotation]:
        """Get annotations across all runs for a contract."""
        runs_dir = self._runs_dir(contract_fingerprint)
        if not runs_dir.exists():
            return []

        # Collect all annotations from all .ann.jsonl files
        all_annotations: List[Annotation] = []

        for ann_file in runs_dir.glob("*.ann.jsonl"):
            with open(ann_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ann = Annotation.from_json(line)

                        # Filter by rule_id if specified
                        if rule_id is not None and ann.rule_id != rule_id:
                            continue

                        # Filter by annotation_type if specified
                        if annotation_type is not None and ann.annotation_type != annotation_type:
                            continue

                        all_annotations.append(ann)
                    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
                        # Skip malformed annotations but log for debugging
                        _logger.debug(f"Skipping malformed annotation in {ann_file}: {e}")
                        continue

        # Sort by created_at descending (newest first)
        all_annotations.sort(
            key=lambda a: a.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

        # Apply limit
        return all_annotations[:limit]

    def get_run_with_annotations(
        self,
        contract_fingerprint: str,
        run_id: Optional[int] = None,
    ) -> Optional[ValidationState]:
        """Get a validation state with its annotations loaded."""
        # Get the state
        if run_id is None:
            state = self.get_latest(contract_fingerprint)
        else:
            # Search for state with matching ID hash
            states = self.get_history(contract_fingerprint, limit=100)
            state = None
            for s in states:
                if s.id == run_id:
                    state = s
                    break

        if not state:
            return None

        # Load annotations
        runs_dir = self._runs_dir(contract_fingerprint)
        if not runs_dir.exists():
            state.annotations = []
            for rule in state.rules:
                rule.annotations = []
            return state

        # Find the corresponding run file to get run_id string
        run_id_str = None
        for filepath in runs_dir.glob("*.json"):
            if filepath.name.endswith(".ann.jsonl"):
                continue
            loaded = self._load_state(filepath)
            if loaded and loaded.id == state.id:
                run_id_str = filepath.stem
                break

        if not run_id_str:
            state.annotations = []
            for rule in state.rules:
                rule.annotations = []
            return state

        # Load annotations from JSONL
        ann_file = self._annotations_file(contract_fingerprint, run_id_str)
        annotations = []
        if ann_file.exists():
            with open(ann_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        annotations.append(Annotation.from_json(line))

        self._attach_annotations_to_state(state, annotations)
        return state

    def get_history_with_annotations(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history with annotations loaded."""
        states = self.get_history(contract_fingerprint, limit=limit)

        runs_dir = self._runs_dir(contract_fingerprint)
        if not runs_dir.exists():
            for state in states:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
            return states

        # Build ID to run_id_str mapping
        id_to_run_id: Dict[int, str] = {}
        for filepath in runs_dir.glob("*.json"):
            if filepath.name.endswith(".ann.jsonl"):
                continue
            loaded = self._load_state(filepath)
            if loaded and loaded.id:
                id_to_run_id[loaded.id] = filepath.stem

        # Load annotations for each state
        for state in states:
            if state.id is None or state.id not in id_to_run_id:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
                continue

            run_id_str = id_to_run_id[state.id]
            ann_file = self._annotations_file(contract_fingerprint, run_id_str)

            annotations = []
            if ann_file.exists():
                with open(ann_file, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            annotations.append(Annotation.from_json(line))

            self._attach_annotations_to_state(state, annotations)

        return states

    def __repr__(self) -> str:
        return f"LocalStore(base_path={self.base_path})"
