# src/kontra/state/backends/s3.py
"""
S3-compatible state storage with normalized format (v0.5).

Directory structure:
    s3://bucket/prefix/
    └── state/
        └── <contract_fingerprint>/
            └── runs/
                ├── <run_id>.json       # run metadata + rule results
                └── <run_id>.ann.jsonl  # annotations (append-only)

Works with:
- AWS S3
- MinIO
- Any S3-compatible storage
"""

from __future__ import annotations

import json
import logging
import os
import random
import string
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import StateBackend
from kontra.state.types import Annotation, ValidationState

_logger = logging.getLogger(__name__)


class S3Store(StateBackend):
    """
    S3-compatible object storage backend with normalized format.

    Uses fsspec/s3fs for S3 access. Supports AWS S3, MinIO, and other
    S3-compatible storage systems.

    URI format: s3://bucket/prefix
    """

    def __init__(self, uri: str):
        """
        Initialize the S3 store.

        Args:
            uri: S3 URI in format s3://bucket/prefix

        Environment variables:
            AWS_ACCESS_KEY_ID: Access key
            AWS_SECRET_ACCESS_KEY: Secret key
            AWS_ENDPOINT_URL: Custom endpoint (for MinIO)
            AWS_REGION: AWS region
        """
        self.uri = uri
        parsed = urlparse(uri)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        if self.prefix:
            self.prefix = f"{self.prefix}/state"
        else:
            self.prefix = "state"

        self._fs = None  # Lazy initialization

    def _get_fs(self):
        """Get or create the S3 filesystem."""
        if self._fs is not None:
            return self._fs

        try:
            import fsspec
        except ImportError as e:
            raise RuntimeError(
                "S3 state backend requires 's3fs'. Install with: pip install s3fs"
            ) from e

        storage_options = self._storage_options()
        self._fs = fsspec.filesystem("s3", **storage_options)
        return self._fs

    @staticmethod
    def _storage_options() -> Dict[str, Any]:
        """Build fsspec storage options from environment."""
        opts: Dict[str, Any] = {"anon": False}

        key = os.getenv("AWS_ACCESS_KEY_ID")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if key and secret:
            opts["key"] = key
            opts["secret"] = secret

        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            opts["client_kwargs"] = {"endpoint_url": endpoint}
            opts["config_kwargs"] = {"s3": {"addressing_style": "path"}}
            opts["use_ssl"] = endpoint.startswith("https")

        region = os.getenv("AWS_REGION")
        if region:
            opts.setdefault("client_kwargs", {})
            opts["client_kwargs"]["region_name"] = region

        return opts

    def _runs_prefix(self, contract_fingerprint: str) -> str:
        """Get the S3 prefix for a contract's runs."""
        return f"{self.bucket}/{self.prefix}/{contract_fingerprint}/runs"

    def _generate_run_id(self, run_at: datetime) -> str:
        """Generate a unique run ID from timestamp."""
        ts = run_at.strftime("%Y-%m-%dT%H-%M-%S")
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{ts}_{suffix}"

    def _run_key(self, contract_fingerprint: str, run_id: str) -> str:
        """Get the S3 key for a run's state file."""
        return f"{self._runs_prefix(contract_fingerprint)}/{run_id}.json"

    def _annotations_key(self, contract_fingerprint: str, run_id: str) -> str:
        """Get the S3 key prefix for a run's annotations (legacy JSONL)."""
        return f"{self._runs_prefix(contract_fingerprint)}/{run_id}.ann.jsonl"

    def _annotation_key(
        self, contract_fingerprint: str, run_id: str, annotation_id: int
    ) -> str:
        """Get the S3 key for a single annotation file."""
        return f"{self._runs_prefix(contract_fingerprint)}/{run_id}.ann.{annotation_id:06d}.json"

    def _annotations_prefix(self, contract_fingerprint: str, run_id: str) -> str:
        """Get the S3 prefix for a run's annotation files."""
        return f"{self._runs_prefix(contract_fingerprint)}/{run_id}.ann."

    def _load_annotations(
        self, fs, contract_fingerprint: str, run_id_str: str
    ) -> List[Annotation]:
        """
        Load annotations for a run (supports both legacy JSONL and new per-file format).

        Args:
            fs: The fsspec filesystem
            contract_fingerprint: The contract fingerprint
            run_id_str: The string run ID

        Returns:
            List of annotations
        """
        annotations = []

        # Load from legacy JSONL format
        legacy_key = self._annotations_key(contract_fingerprint, run_id_str)
        try:
            with fs.open(f"s3://{legacy_key}", "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        annotations.append(Annotation.from_json(line))
        except FileNotFoundError:
            pass  # No legacy annotations, this is expected
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not read legacy annotations from {legacy_key}: {e}")
        except (json.JSONDecodeError, ValueError) as e:
            _logger.warning(f"Malformed annotation data in {legacy_key}: {e}")

        # Load from new per-file format
        prefix = self._annotations_prefix(contract_fingerprint, run_id_str)
        try:
            ann_files = fs.glob(f"s3://{prefix}*.json")
            for ann_file in sorted(ann_files):
                try:
                    with fs.open(f"s3://{ann_file}", "r") as f:
                        content = f.read().strip()
                        if content:
                            annotations.append(Annotation.from_json(content))
                except FileNotFoundError:
                    pass  # File disappeared between glob and read, ignore
                except (json.JSONDecodeError, ValueError) as e:
                    _logger.warning(f"Malformed annotation in {ann_file}: {e}")
                except (OSError, PermissionError) as e:
                    _logger.debug(f"Could not read annotation {ann_file}: {e}")
        except FileNotFoundError:
            pass  # No annotation directory, expected for runs without annotations
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not list annotations at {prefix}: {e}")

        return annotations

    def save(self, state: ValidationState) -> None:
        """Save a validation state to S3."""
        fs = self._get_fs()

        # Generate run ID
        run_id = self._generate_run_id(state.run_at)

        # Store run_id in the state dict
        state_dict = state.to_dict()
        state_dict["_run_id"] = run_id

        key = self._run_key(state.contract_fingerprint, run_id)

        try:
            with fs.open(f"s3://{key}", "w") as f:
                f.write(json.dumps(state_dict, indent=2, default=str))
        except Exception as e:
            raise IOError(f"Failed to save state to S3: {e}") from e

    def _load_state(self, filepath: str) -> Optional[ValidationState]:
        """Load a state from an S3 path."""
        fs = self._get_fs()
        try:
            with fs.open(f"s3://{filepath}", "r") as f:
                content = f.read()
            data = json.loads(content)

            # Extract run_id for later use
            run_id = data.pop("_run_id", None)

            state = ValidationState.from_dict(data)

            # Store run_id as a synthetic ID (hash)
            if run_id:
                state.id = hash(run_id) & 0x7FFFFFFF

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
        fs = self._get_fs()
        prefix = self._runs_prefix(contract_fingerprint)

        try:
            # List all JSON files (excluding annotation files)
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [
                f for f in all_files
                if not f.endswith(".ann.jsonl") and ".ann." not in f.rsplit("/", 1)[-1]
            ]
        except FileNotFoundError:
            return []  # No state directory for this contract
        except (OSError, PermissionError) as e:
            _logger.warning(f"Could not list state history for {contract_fingerprint}: {e}")
            return []

        if not files:
            return []

        # Sort by filename (timestamp prefix), newest first
        files = sorted(files, reverse=True)

        states = []
        for filepath in files[:limit]:
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
        fs = self._get_fs()
        prefix = self._runs_prefix(contract_fingerprint)

        try:
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [
                f for f in all_files
                if not f.endswith(".ann.jsonl") and ".ann." not in f.rsplit("/", 1)[-1]
            ]
        except FileNotFoundError:
            return 0  # No state directory
        except (OSError, PermissionError) as e:
            _logger.warning(f"Could not list states for cleanup in {contract_fingerprint}: {e}")
            return 0

        if not files:
            return 0

        # Sort newest first
        files = sorted(files, reverse=True)

        # Delete files beyond keep_count
        deleted = 0
        for filepath in files[keep_count:]:
            try:
                # Delete state file
                fs.rm(f"s3://{filepath}")
                deleted += 1

                # Delete corresponding annotations (both legacy JSONL and new per-file)
                run_id = filepath.rsplit("/", 1)[-1].replace(".json", "")

                # Legacy JSONL - ignore if not found
                ann_key = self._annotations_key(contract_fingerprint, run_id)
                try:
                    fs.rm(f"s3://{ann_key}")
                except FileNotFoundError:
                    pass  # No legacy annotations for this run

                # New per-file annotations
                ann_prefix = self._annotations_prefix(contract_fingerprint, run_id)
                try:
                    ann_files = fs.glob(f"s3://{ann_prefix}*.json")
                    for ann_file in ann_files:
                        try:
                            fs.rm(f"s3://{ann_file}")
                        except FileNotFoundError:
                            pass  # Already deleted
                except FileNotFoundError:
                    pass  # No annotations directory
            except (OSError, PermissionError) as e:
                _logger.debug(f"Could not delete old state {filepath}: {e}")
                continue

        return deleted

    def list_contracts(self) -> List[str]:
        """List all contract fingerprints with stored state."""
        fs = self._get_fs()
        prefix = f"{self.bucket}/{self.prefix}"

        try:
            # List directories under the state prefix
            items = fs.ls(f"s3://{prefix}/", detail=False)
        except FileNotFoundError:
            return []  # No state directory yet
        except (OSError, PermissionError) as e:
            _logger.warning(f"Could not list contracts in S3 state store: {e}")
            return []

        contracts = []
        for item in items:
            # Extract the fingerprint (last part of the path)
            parts = item.rstrip("/").split("/")
            if parts:
                name = parts[-1]
                # Fingerprints are 16 hex characters
                if len(name) == 16 and all(c in "0123456789abcdef" for c in name):
                    contracts.append(name)

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
        fs = self._get_fs()
        deleted = 0

        if contract_fingerprint:
            prefix = self._runs_prefix(contract_fingerprint)
            try:
                # Delete all files (json and jsonl)
                for pattern in ["*.json", "*.jsonl"]:
                    try:
                        files = fs.glob(f"s3://{prefix}/{pattern}")
                        for filepath in files:
                            try:
                                fs.rm(f"s3://{filepath}")
                                if filepath.endswith(".json") and not filepath.endswith(".ann.jsonl"):
                                    deleted += 1
                            except FileNotFoundError:
                                pass  # Already deleted
                    except FileNotFoundError:
                        pass  # No files matching pattern
            except (OSError, PermissionError) as e:
                _logger.warning(f"Could not clear state for {contract_fingerprint}: {e}")
        else:
            # Clear all contracts
            for fp in self.list_contracts():
                deleted += self.clear(fp)

        return deleted

    # -------------------------------------------------------------------------
    # Annotation Methods
    # -------------------------------------------------------------------------

    def save_annotation(self, annotation: Annotation) -> int:
        """
        Save an annotation (append-only).

        For S3 backends, we need the contract fingerprint and run_id string.
        """
        raise NotImplementedError(
            "S3Store.save_annotation requires contract fingerprint. "
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

        Each annotation is stored as a separate file to avoid race conditions.
        File pattern: {run_id}.ann.{annotation_id:06d}.json

        Args:
            contract_fingerprint: The contract fingerprint
            run_id_str: The string run ID
            annotation: The annotation to save

        Returns:
            The annotation ID
        """
        fs = self._get_fs()
        prefix = self._annotations_prefix(contract_fingerprint, run_id_str)

        # Count existing annotation files to generate next ID
        existing_count = 0
        try:
            # Glob for annotation files (new format)
            ann_files = fs.glob(f"s3://{prefix}*.json")
            existing_count = len(ann_files)

            # Also check legacy JSONL for backwards compatibility
            legacy_key = self._annotations_key(contract_fingerprint, run_id_str)
            try:
                with fs.open(f"s3://{legacy_key}", "r") as f:
                    existing_count += sum(1 for _ in f)
            except FileNotFoundError:
                pass  # No legacy annotations
        except FileNotFoundError:
            pass  # No annotations yet, starting fresh
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not count existing annotations: {e}")

        annotation.id = existing_count + 1

        # Write annotation as a separate file (atomic, no race condition)
        ann_key = self._annotation_key(
            contract_fingerprint, run_id_str, annotation.id
        )
        try:
            with fs.open(f"s3://{ann_key}", "w") as f:
                f.write(annotation.to_json())
            return annotation.id
        except Exception as e:
            raise IOError(f"Failed to save annotation to S3: {e}") from e

    def get_annotations(
        self,
        run_id: int,
        rule_result_id: Optional[int] = None,
    ) -> List[Annotation]:
        """Get annotations for a run."""
        return []

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
            states = self.get_history(contract_fingerprint, limit=100)
            state = None
            for s in states:
                if s.id == run_id:
                    state = s
                    break

        if not state:
            return None

        fs = self._get_fs()
        prefix = self._runs_prefix(contract_fingerprint)

        # Find the run file to get run_id string
        run_id_str = None
        try:
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [f for f in all_files if not f.endswith(".ann.jsonl") and ".ann." not in f.rsplit("/", 1)[-1]]

            for filepath in files:
                loaded = self._load_state(filepath)
                if loaded and loaded.id == state.id:
                    run_id_str = filepath.rsplit("/", 1)[-1].replace(".json", "")
                    break
        except FileNotFoundError:
            pass  # No state files
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not search for run file: {e}")

        if not run_id_str:
            state.annotations = []
            for rule in state.rules:
                rule.annotations = []
            return state

        # Load annotations (supports both legacy JSONL and new per-file format)
        annotations = self._load_annotations(
            fs, contract_fingerprint, run_id_str
        )

        self._attach_annotations_to_state(state, annotations)
        return state

    def get_history_with_annotations(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history with annotations loaded."""
        states = self.get_history(contract_fingerprint, limit=limit)

        fs = self._get_fs()
        prefix = self._runs_prefix(contract_fingerprint)

        # Build ID to run_id_str mapping
        id_to_run_id: Dict[int, str] = {}
        try:
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [f for f in all_files if not f.endswith(".ann.jsonl") and ".ann." not in f.rsplit("/", 1)[-1]]

            for filepath in files:
                loaded = self._load_state(filepath)
                if loaded and loaded.id:
                    run_id_str = filepath.rsplit("/", 1)[-1].replace(".json", "")
                    id_to_run_id[loaded.id] = run_id_str
        except FileNotFoundError:
            pass  # No state files
        except (OSError, PermissionError) as e:
            _logger.debug(f"Could not build run ID mapping: {e}")

        # Load annotations for each state
        for state in states:
            if state.id is None or state.id not in id_to_run_id:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
                continue

            run_id_str = id_to_run_id[state.id]

            # Load annotations (supports both legacy JSONL and new per-file format)
            annotations = self._load_annotations(
                fs, contract_fingerprint, run_id_str
            )

            self._attach_annotations_to_state(state, annotations)

        return states

    def __repr__(self) -> str:
        return f"S3Store(uri={self.uri})"
