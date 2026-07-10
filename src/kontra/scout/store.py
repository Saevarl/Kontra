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
import os
import threading
from dataclasses import replace
from pathlib import Path
from typing import List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from kontra.connectors.handle import mask_credentials
from kontra.version import VERSION
from .types import DatasetProfile, ProfileState


_SENSITIVE_QUERY_PARAMS = {"password", "client_secret", "sslpassword"}


def fingerprint_source(source_uri: str) -> str:
    """
    Generate a stable fingerprint for a data source URI.

    Args:
        source_uri: The data source URI

    Returns:
        16-character hex fingerprint
    """
    # Preserve existing fingerprints for non-URI sources such as file paths.
    normalized = source_uri.strip()

    if "://" in normalized:
        parsed = urlsplit(normalized)
        # Discard userinfo entirely so credential rotation does not create a
        # new history identity. Split on the last @ to handle @ in passwords.
        netloc = parsed.netloc.rsplit("@", 1)[-1]
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() not in _SENSITIVE_QUERY_PARAMS
            ]
        )
        normalized = urlunsplit(
            (parsed.scheme, netloc, parsed.path, query, parsed.fragment)
        )

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
    masked_source_uri = mask_credentials(profile.source_uri)
    stored_profile = replace(profile, source_uri=masked_source_uri)
    return ProfileState(
        source_fingerprint=fingerprint_source(profile.source_uri),
        source_uri=masked_source_uri,
        profiled_at=profile.profiled_at,
        profile=stored_profile,
        engine_version=VERSION,
    )


# Default store factory.
#
# LocalProfileStore resolves its base_path to ``<cwd>/.kontra/profiles`` at
# construction time. Long-lived processes (services, MCP servers) or tests may
# change the working directory after the first call, so we key the cached
# singleton on the cwd it was built for and rebuild it when the cwd changes.
# Repeated calls from the same directory reuse the cached instance and never
# take the lock. Mirrors ``kontra.state.backends.get_default_store``.
_default_profile_store: Optional[LocalProfileStore] = None
_default_profile_store_cwd: Optional[str] = None
_default_profile_store_lock = threading.Lock()


def get_default_profile_store() -> LocalProfileStore:
    """
    Get the default local profile store.

    Uses .kontra/profiles/ in the current working directory. The store is
    cached per working directory: if the cwd changes between calls (e.g. in a
    long-lived service or after ``kontra.set_config``), the store is rebuilt so
    reads and writes always target the current directory's profile tree.
    """
    global _default_profile_store, _default_profile_store_cwd
    current_cwd = os.getcwd()
    # Fast path: same cwd as the cached store, no locking needed.
    if (
        _default_profile_store is not None
        and _default_profile_store_cwd == current_cwd
    ):
        return _default_profile_store
    with _default_profile_store_lock:
        # Re-check under the lock in case another thread just rebuilt it.
        if (
            _default_profile_store is None
            or _default_profile_store_cwd != current_cwd
        ):
            _default_profile_store = LocalProfileStore()
            _default_profile_store_cwd = current_cwd
        return _default_profile_store


def get_profile_store(backend: str = "local", uri: Optional[str] = None):
    """
    Get a profile store by backend identifier.

    Args:
        backend: Backend identifier. Options:
            - "local" or "": LocalProfileStore (default, cwd-aware singleton)
            - "postgres" / "postgresql": PostgresProfileStore (connection from
              ``uri`` or PGXXX / DATABASE_URL environment variables)
            - "postgres://..." / "postgresql://...": PostgresProfileStore using
              the given connection URI
        uri: Optional connection URI for the postgres backend. If omitted,
            connection details come from environment variables.

    Environment Variables:
        For PostgreSQL:
            PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, DATABASE_URL

    Returns:
        A profile store exposing the same public interface as
        LocalProfileStore (save, get_latest, get_history, list_sources, clear).
    """
    if not backend or backend == "local":
        return get_default_profile_store()

    if backend in ("postgres", "postgresql"):
        # Lazy import keeps psycopg out of the base import path.
        from .postgres_store import PostgresProfileStore

        return PostgresProfileStore(uri or "postgres://")

    if backend.startswith("postgres://") or backend.startswith("postgresql://"):
        from .postgres_store import PostgresProfileStore

        return PostgresProfileStore(uri or backend)

    raise ValueError(f"Unknown profile store backend: {backend}")
