from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

# --- Version handling (robust to early-boot states) ---------------------------
try:
    from kontra.version import VERSION as _VERSION
except (ImportError, ModuleNotFoundError):  # pragma: no cover
    _VERSION = "0.0.0-dev"

SCHEMA_VERSION = "1.0"

# --- Optional JSON Schema validation (non-fatal if missing) -------------------
try:
    import fastjsonschema  # type: ignore
    _HAVE_VALIDATOR = True
except ImportError:  # pragma: no cover
    _HAVE_VALIDATOR = False

_VALIDATOR = None  # lazy-compiled validator


def _utc_now_iso() -> str:
    """UTC timestamp in stable ISO 8601 format with trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_result(item: Dict[str, Any]) -> Dict[str, Any]:
    passed = bool(item.get("passed", False))
    msg = str(item.get("message", ""))

    # Use actual severity from contract/rule, default to blocking if not specified
    severity = item.get("severity", "blocking")
    # Normalize to lowercase to match contract format
    if isinstance(severity, str):
        severity = severity.lower()
        # Map legacy values
        if severity == "error":
            severity = "blocking"
        elif severity == "info" and passed:
            severity = "info"  # Keep info for passed rules if explicitly set
        elif severity not in ("blocking", "warning", "info"):
            severity = "blocking"  # Default unknown values to blocking

    # Get tally mode to indicate if count is exact or approximate
    tally = item.get("tally", True)

    result = {
        "rule_id": str(item.get("rule_id", "")),
        "passed": passed,
        "message": msg,
        "failed_count": int(item.get("failed_count", 0)),
        "failed_count_exact": tally,  # False = count is a lower bound
        "severity": severity,
        "actions_executed": list(item.get("actions_executed", [])),
    }

    # Include context if present
    context = item.get("context")
    if context:
        result["context"] = context

    return result



def _sorted_results(results: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deterministic ordering by rule_id, then message for tie-breaks."""
    normalized = [_normalize_result(r) for r in results]
    return sorted(normalized, key=lambda r: (r["rule_id"], r["message"]))


def _derive_exec_seconds(summary: Dict[str, Any], stats: Optional[Dict[str, Any]]) -> float:
    # Prefer summary if provided; else fall back to stats.run_meta.duration_ms_total
    val = summary.get("execution_time_seconds")
    if isinstance(val, (int, float)) and val:
        return float(val)
    if stats:
        try:
            ms = stats.get("run_meta", {}).get("duration_ms_total")
            if isinstance(ms, (int, float)):
                return float(ms) / 1000.0
        except (TypeError, AttributeError, KeyError):
            pass  # stats structure unexpected
    return 0.0


def _derive_rows_evaluated(summary: Dict[str, Any], stats: Optional[Dict[str, Any]]) -> int:
    # Prefer summary if provided; else fall back to stats.dataset.nrows
    val = summary.get("rows_evaluated")
    if isinstance(val, int) and val >= 0:
        return int(val)
    if stats:
        try:
            n = stats.get("dataset", {}).get("nrows")
            if isinstance(n, int) and n >= 0:
                return int(n)
        except (TypeError, AttributeError, KeyError):
            pass  # stats structure unexpected
    return 0


def build_payload(
    *,
    dataset_name: str,
    summary: Dict[str, Any],
    results: List[Dict[str, Any]],
    stats: Optional[Dict[str, Any]] = None,
    quarantine: Optional[Dict[str, Any]] = None,
    schema_version: str = SCHEMA_VERSION,
    engine_version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Construct the stable, versioned JSON document for CI/CD and machines.
    This function is side-effect free and ideal for unit tests.
    """
    total = int(summary.get("total_rules", len(results)))
    passed_count = int(summary.get("rules_passed", sum(1 for r in results if r.get("passed"))))
    failed_count = int(summary.get("rules_failed", total - passed_count))

    payload: Dict[str, Any] = {
        "schema_version": str(schema_version),
        "dataset_name": str(dataset_name),
        "timestamp_utc": _utc_now_iso(),
        "engine_version": str(engine_version or _VERSION),
        "passed": bool(summary.get("passed", failed_count == 0)),
        "statistics": {
            "execution_time_seconds": _derive_exec_seconds(summary, stats),
            "rows_evaluated": _derive_rows_evaluated(summary, stats),
            "rules_total": total,
            "rules_passed": passed_count,
            "rules_failed": failed_count,
        },
        "results": _sorted_results(results),
    }

    if quarantine:
        payload["quarantine"] = {
            "location": str(quarantine.get("location", "")),
            "rows_quarantined": int(quarantine.get("rows_quarantined", 0)),
        }

    if stats is not None:
        # Namespaced so the core schema remains stable as stats evolve
        payload["stats"] = stats

    return payload


def render_json(
    *,
    dataset_name: str,
    summary: Dict[str, Any],
    results: List[Dict[str, Any]],
    stats: Optional[Dict[str, Any]] = None,
    quarantine: Optional[Dict[str, Any]] = None,
    validate: bool = False,
    pretty: bool = True,
) -> str:
    """
    Build (+ optionally validate) and dump as JSON.

    Args:
        pretty: If True (default), output indented JSON for readability.
                If False, output compact JSON for machine use.
    """
    payload = build_payload(
        dataset_name=dataset_name,
        summary=summary,
        results=results,
        stats=stats,
        quarantine=quarantine,
    )

    if validate and _HAVE_VALIDATOR:
        _validate_against_local_schema(payload)

    if pretty:
        return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False)
    else:
        # Compact deterministic format for machine use
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# --- Optional local schema validation ----------------------------------------


def _load_local_schema() -> Optional[Dict[str, Any]]:
    """
    Load the local JSON Schema if bundled. Silently returns None if absent.
    """
    try:
        from importlib import resources
        from importlib.resources import files

        schema_pkg = "schemas"  # repository-level schema package
        path = files(schema_pkg).joinpath("validation_output.schema.json")
        with resources.as_file(path) as p:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except (ImportError, ModuleNotFoundError, FileNotFoundError, OSError, json.JSONDecodeError):
        return None  # Schema not available, validation will be skipped


def _validate_against_local_schema(payload: Dict[str, Any]) -> None:
    global _VALIDATOR
    if not _HAVE_VALIDATOR:
        return
    if _VALIDATOR is None:
        schema = _load_local_schema()
        if not schema:
            return  # schema not bundled; skip validation
        _VALIDATOR = fastjsonschema.compile(schema)  # type: ignore
    _VALIDATOR(payload)  # type: ignore


__all__ = ["build_payload", "render_json", "SCHEMA_VERSION"]
