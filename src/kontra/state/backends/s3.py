# src/kontra/state/backends/s3.py
"""
S3-compatible state storage.

Stores validation states in S3 bucket with structure:

s3://bucket/prefix/
└── state/
    └── <contract_fingerprint>/
        ├── 2024-01-13T10-30-00.json
        ├── 2024-01-12T10-30-00.json
        └── ...

Works with:
- AWS S3
- MinIO
- Any S3-compatible storage
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import StateBackend
from kontra.state.types import ValidationState


class S3Store(StateBackend):
    """
    S3-compatible object storage backend.

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

    def _contract_prefix(self, contract_fingerprint: str) -> str:
        """Get the S3 prefix for a contract's states."""
        return f"{self.bucket}/{self.prefix}/{contract_fingerprint}"

    def _state_key(self, contract_fingerprint: str, run_at: datetime) -> str:
        """Generate S3 key for a state."""
        ts = run_at.isoformat().replace(":", "-").replace("+", "_")
        return f"{self._contract_prefix(contract_fingerprint)}/{ts}.json"

    def save(self, state: ValidationState) -> None:
        """Save a validation state to S3."""
        fs = self._get_fs()
        key = self._state_key(state.contract_fingerprint, state.run_at)

        try:
            with fs.open(f"s3://{key}", "w") as f:
                f.write(state.to_json())
        except Exception as e:
            raise IOError(f"Failed to save state to S3: {e}") from e

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
        prefix = self._contract_prefix(contract_fingerprint)

        try:
            # List all JSON files in the contract prefix
            files = fs.glob(f"s3://{prefix}/*.json")
        except Exception:
            return []

        if not files:
            return []

        # Sort by filename (which contains timestamp), newest first
        files = sorted(files, reverse=True)

        states = []
        for filepath in files[:limit]:
            try:
                with fs.open(f"s3://{filepath}", "r") as f:
                    content = f.read()
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
        fs = self._get_fs()
        prefix = self._contract_prefix(contract_fingerprint)

        try:
            files = fs.glob(f"s3://{prefix}/*.json")
        except Exception:
            return 0

        if not files:
            return 0

        # Sort newest first
        files = sorted(files, reverse=True)

        # Delete files beyond keep_count
        deleted = 0
        for filepath in files[keep_count:]:
            try:
                fs.rm(f"s3://{filepath}")
                deleted += 1
            except Exception:
                continue

        return deleted

    def list_contracts(self) -> List[str]:
        """List all contract fingerprints with stored state."""
        fs = self._get_fs()
        prefix = f"{self.bucket}/{self.prefix}"

        try:
            # List directories under the state prefix
            items = fs.ls(f"s3://{prefix}/", detail=False)
        except Exception:
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
            prefix = self._contract_prefix(contract_fingerprint)
            try:
                files = fs.glob(f"s3://{prefix}/*.json")
                for filepath in files:
                    fs.rm(f"s3://{filepath}")
                    deleted += 1
            except Exception:
                pass
        else:
            # Clear all contracts
            for fp in self.list_contracts():
                deleted += self.clear(fp)

        return deleted

    def __repr__(self) -> str:
        return f"S3Store(uri={self.uri})"
