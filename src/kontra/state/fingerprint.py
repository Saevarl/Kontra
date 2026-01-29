# src/kontra/state/fingerprint.py
"""
Fingerprinting utilities for contracts and datasets.

Fingerprints are stable hashes that identify a contract or dataset
across runs, enabling state comparison and history lookup.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING


def _json_default(obj: Any) -> Any:
    """JSON encoder for non-serializable types (dates, etc.)."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

if TYPE_CHECKING:
    from kontra.config.models import Contract
    from kontra.connectors.handle import DatasetHandle


def _stable_hash(data: str) -> str:
    """Generate a stable SHA-256 hash prefix."""
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]


def fingerprint_contract(
    contract: "Contract",
    *,
    include_dataset: bool = False,
) -> str:
    """
    Generate a stable fingerprint for a contract.

    The fingerprint is based on:
    - Contract name
    - Rule definitions (name, params)
    - Optionally: dataset URI

    This allows tracking the same contract across runs even if
    the file path changes.

    Args:
        contract: The Contract object
        include_dataset: If True, include dataset URI in fingerprint

    Returns:
        A 16-character hex string (sha256 prefix)
    """
    # Build canonical representation
    canonical: Dict[str, Any] = {
        "name": contract.name,
        "rules": [],
    }

    # Sort rules for determinism
    for rule in sorted(contract.rules, key=lambda r: (r.name, json.dumps(r.params, sort_keys=True, default=_json_default))):
        canonical["rules"].append({
            "name": rule.name,
            "params": rule.params,
        })

    if include_dataset:
        canonical["datasource"] = contract.datasource

    # Generate stable JSON string
    json_str = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=_json_default)
    return _stable_hash(json_str)


def fingerprint_contract_file(path: str) -> str:
    """
    Generate a fingerprint from a contract file path.

    Uses the file content hash for simplicity. Less stable than
    fingerprint_contract() if formatting changes, but works without
    parsing.

    Args:
        path: Path to the contract YAML file

    Returns:
        A 16-character hex string
    """
    content = Path(path).read_text(encoding="utf-8")
    return _stable_hash(content)


def fingerprint_dataset(
    handle: "DatasetHandle",
    *,
    include_stats: bool = False,
    row_count: Optional[int] = None,
    schema: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Generate a fingerprint for a dataset.

    For files: based on URI (and optionally metadata like row count)
    For databases: based on connection params and table name

    Args:
        handle: The DatasetHandle
        include_stats: If True, include row count and schema in fingerprint
        row_count: Row count (if known)
        schema: List of column names (if known)

    Returns:
        A 16-character hex string, or None if fingerprinting fails
    """
    try:
        canonical: Dict[str, Any] = {
            "uri": handle.uri,
            "scheme": handle.scheme,
        }

        # Add database-specific identifiers
        if handle.db_params:
            db = handle.db_params
            canonical["db"] = {
                "host": getattr(db, "host", None),
                "database": getattr(db, "database", None),
                "schema": getattr(db, "schema", None),
                "table": getattr(db, "table", None),
            }

        if include_stats:
            if row_count is not None:
                canonical["row_count"] = row_count
            if schema is not None:
                canonical["schema"] = sorted(schema)

        json_str = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return _stable_hash(json_str)

    except (TypeError, ValueError, OSError):
        # Don't fail validation if fingerprinting fails
        return None


def fingerprint_from_name_and_uri(name: str, uri: str) -> str:
    """
    Simple fingerprint from contract name and dataset URI.

    Use this when you don't have access to the full Contract object.

    Args:
        name: Contract name
        uri: Dataset URI

    Returns:
        A 16-character hex string
    """
    canonical = json.dumps(
        {"name": name, "uri": uri},
        sort_keys=True,
        separators=(",", ":"),
    )
    return _stable_hash(canonical)
