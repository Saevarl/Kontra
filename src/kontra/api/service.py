"""
Configuration and service/agent support: kontra.resolve(), config(),
set_config(), get_config_path(), list_rules(), describe_rule(), health().
"""

import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from kontra.version import VERSION

if TYPE_CHECKING:
    from kontra.config.settings import KontraConfig


# =============================================================================
# Configuration Functions
# =============================================================================


def resolve(name: str) -> str:
    """
    Resolve a datasource name to URI.

    Args:
        name: Datasource name (e.g., "users" or "prod_db.users")

    Returns:
        Resolved URI

    Example:
        uri = kontra.resolve("users")
        uri = kontra.resolve("prod_db.users")
    """
    from kontra.config.settings import resolve_datasource

    return resolve_datasource(name)


def config(env: Optional[str] = None) -> "KontraConfig":
    """
    Get effective configuration.

    Args:
        env: Environment name (default: use KONTRA_ENV or defaults)

    Returns:
        KontraConfig with preplan, pushdown, etc.

    Example:
        cfg = kontra.config()
        cfg = kontra.config(env="production")
        print(cfg.preplan)  # "on"
    """
    from kontra.config.settings import resolve_effective_config

    return resolve_effective_config(env_name=env)


# =============================================================================
# Service/Agent Support Functions
# =============================================================================


# Global config path override for service/agent use
_config_path_override: Optional[str] = None


def set_config(path: Optional[str]) -> None:
    """
    Set config file path for service/agent use.

    By default, Kontra discovers config from cwd (.kontra/config.yml).
    For long-running services or agents, use this to set an explicit path.

    Args:
        path: Path to config.yml (or None to reset to auto-discovery)

    Example:
        kontra.set_config("/etc/kontra/config.yml")
        result = kontra.validate(df, rules=[...])

        # Reset to default behavior
        kontra.set_config(None)
    """
    global _config_path_override
    if path is not None and not os.path.exists(path):
        import warnings
        warnings.warn(
            f"Config path does not exist: {path}. "
            f"Config will be ignored until the file is created.",
            UserWarning,
            stacklevel=2,
        )
    _config_path_override = path
    # Register the override with the config layer so find_config_file() honors
    # it for ALL resolution (validate, resolve_datasource, state, profile) —
    # not just health(). Without this, services that can't control their cwd
    # silently fall back to cwd discovery and miss the config.
    from kontra.config.settings import set_config_path_override
    set_config_path_override(path)


def get_config_path() -> Optional[str]:
    """
    Get the current config path override.

    Returns:
        The overridden config path, or None if using auto-discovery.
    """
    return _config_path_override


def list_rules() -> List[Dict[str, Any]]:
    """
    List all available validation rules.

    For agents and integrations that need to discover what rules exist.

    Returns:
        List of rule info dicts with name, description, params

    Example:
        rules = kontra.list_rules()
        for rule in rules:
            print(f"{rule['name']}: {rule['description']}")
    """
    from kontra.rule_catalog import list_rule_summaries

    return list_rule_summaries()


def describe_rule(name: str) -> Dict[str, Any]:
    """Return exact, version-matched semantics for one built-in rule.

    The result includes parameters, failure and NULL behavior, counting
    semantics, tally support, and a valid contract YAML example.
    """
    from kontra.rule_catalog import describe_rule_spec

    return describe_rule_spec(name)


def health() -> Dict[str, Any]:
    """
    Health check for service/agent use.

    Returns version, config status, and available rules.
    Use this to verify Kontra is properly installed and configured.

    Returns:
        Dict with version, config_found, config_path, rule_count, status

    Example:
        health = kontra.health()
        if health["status"] == "ok":
            print(f"Kontra {health['version']} ready")
        else:
            print(f"Issue: {health['status']}")
    """
    from kontra.rule_defs.registry import RULE_REGISTRY
    from kontra.config.settings import find_config_file
    from kontra.engine.phases.compilation import _ensure_builtin_rules_registered
    from pathlib import Path

    # Ensure builtin rules are registered before checking the registry
    _ensure_builtin_rules_registered()

    result: Dict[str, Any] = {
        "version": VERSION,
        "status": "ok",
    }

    # Check config
    if _config_path_override:
        config_path = Path(_config_path_override)
        result["config_path"] = str(config_path)
        result["config_found"] = config_path.exists()
        if not config_path.exists():
            result["status"] = "config_not_found"
    else:
        found = find_config_file()
        result["config_path"] = str(found) if found else None
        result["config_found"] = found is not None

    # Rule count
    result["rule_count"] = len(RULE_REGISTRY)

    # List available rules
    result["rules"] = sorted(RULE_REGISTRY.keys())

    return result
