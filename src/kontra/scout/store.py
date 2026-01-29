# src/kontra/scout/store.py
"""
Profile storage for Kontra Scout.

Stores scout profiles using the same backend infrastructure as validation state.
Profiles are stored separately from validation states but can use the same
storage backend (local, S3, PostgreSQL).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from kontra.version import VERSION
from .types import DatasetProfile, ProfileState


def fingerprint_source(source_uri: str) -> str:
    """
    Generate a stable fingerprint for a data source URI.

    Args:
        source_uri: The data source URI

    Returns:
        16-character hex fingerprint
    """
    # Normalize the URI
    normalized = source_uri.strip()

    # Hash it
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


class LocalProfileStore:
    """
    Filesystem-based profile storage.

    Stores profiles in .kontra/profiles/ directory:
        .kontra/profiles/<source_fingerprint>/<timestamp>.json
    """

    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize the local profile store.

        Args:
            base_path: Base directory for profile storage.
                      Defaults to .kontra/profiles/ in cwd.
        """
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path.cwd() / ".kontra" / "profiles"

    def _source_dir(self, source_fingerprint: str) -> Path:
        """Get the directory for a source's profiles."""
        return self.base_path / source_fingerprint

    def _profile_filename(self, profiled_at: str) -> str:
        """Generate filename from timestamp."""
        # Use ISO format but replace : with - for filesystem compatibility
        ts = profiled_at.replace(":", "-").replace("+", "_")
        return f"{ts}.json"

    def save(self, state: ProfileState) -> None:
        """Save a profile state to the filesystem."""
        source_dir = self._source_dir(state.source_fingerprint)
        source_dir.mkdir(parents=True, exist_ok=True)

        filename = self._profile_filename(state.profiled_at)
        filepath = source_dir / filename

        # Write atomically
        temp_path = filepath.with_suffix(".tmp")
        try:
            temp_path.write_text(state.to_json(), encoding="utf-8")
            temp_path.rename(filepath)
        except (OSError, IOError):
            if temp_path.exists():
                temp_path.unlink()
            raise

    def get_latest(self, source_fingerprint: str) -> Optional[ProfileState]:
        """Get the most recent profile for a source."""
        history = self.get_history(source_fingerprint, limit=1)
        return history[0] if history else None

    def get_history(
        self,
        source_fingerprint: str,
        limit: int = 10,
    ) -> List[ProfileState]:
        """Get recent profile history for a source, newest first."""
        source_dir = self._source_dir(source_fingerprint)

        if not source_dir.exists():
            return []

        # List all JSON files
        profile_files = sorted(
            source_dir.glob("*.json"),
            key=lambda p: p.name,
            reverse=True,
        )

        states = []
        for filepath in profile_files[:limit]:
            try:
                content = filepath.read_text(encoding="utf-8")
                state = ProfileState.from_json(content)
                states.append(state)
            except (OSError, IOError, json.JSONDecodeError, ValueError, KeyError):
                # Skip corrupted or unreadable profile files
                continue

        return states

    def list_sources(self) -> List[str]:
        """List all source fingerprints with stored profiles."""
        if not self.base_path.exists():
            return []

        sources = []
        for item in self.base_path.iterdir():
            if item.is_dir() and len(item.name) == 16:
                sources.append(item.name)

        return sorted(sources)

    def clear(self, source_fingerprint: Optional[str] = None) -> int:
        """Clear stored profiles."""
        deleted = 0

        if source_fingerprint:
            source_dir = self._source_dir(source_fingerprint)
            if source_dir.exists():
                for filepath in source_dir.glob("*.json"):
                    filepath.unlink()
                    deleted += 1
                try:
                    source_dir.rmdir()
                except OSError:
                    pass
        else:
            if self.base_path.exists():
                for source_dir in self.base_path.iterdir():
                    if source_dir.is_dir():
                        for filepath in source_dir.glob("*.json"):
                            filepath.unlink()
                            deleted += 1
                        try:
                            source_dir.rmdir()
                        except OSError:
                            pass

        return deleted

    def __repr__(self) -> str:
        return f"LocalProfileStore(base_path={self.base_path})"


def create_profile_state(profile: DatasetProfile) -> ProfileState:
    """
    Create a ProfileState from a DatasetProfile.

    Args:
        profile: The profiled dataset

    Returns:
        ProfileState ready for storage
    """
    return ProfileState(
        source_fingerprint=fingerprint_source(profile.source_uri),
        source_uri=profile.source_uri,
        profiled_at=profile.profiled_at,
        profile=profile,
        engine_version=VERSION,
    )


# Default store
_default_profile_store: Optional[LocalProfileStore] = None


def get_default_profile_store() -> LocalProfileStore:
    """Get the default profile store."""
    global _default_profile_store
    if _default_profile_store is None:
        _default_profile_store = LocalProfileStore()
    return _default_profile_store


def get_profile_store(backend: str = "local") -> LocalProfileStore:
    """
    Get a profile store by backend identifier.

    Currently only supports local storage. Future: S3, PostgreSQL.
    """
    if not backend or backend == "local":
        return get_default_profile_store()

    # For now, all backends use local profile storage
    # Future: implement S3ProfileStore, PostgresProfileStore
    return get_default_profile_store()
