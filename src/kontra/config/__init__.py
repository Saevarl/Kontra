# src/kontra/config/__init__.py
"""
Kontra configuration module - Contract and settings handling.

Public API:
    - Contract, RuleSpec: Data models for contracts
    - ContractLoader: Loads contracts from files or S3
    - KontraConfig, EffectiveConfig: Configuration models
    - load_config_file, find_config_file, resolve_effective_config

Submodules load lazily (PEP 562): they pull in pydantic/yaml, which would
otherwise dominate `import kontra` time.
"""

from typing import Any

_LAZY_ATTRS = {
    "Contract": "kontra.config.models",
    "RuleSpec": "kontra.config.models",
    "ContractLoader": "kontra.config.loader",
    "KontraConfig": "kontra.config.settings",
    "EffectiveConfig": "kontra.config.settings",
    "load_config_file": "kontra.config.settings",
    "find_config_file": "kontra.config.settings",
    "resolve_effective_config": "kontra.config.settings",
}

__all__ = list(_LAZY_ATTRS)


def __getattr__(name: str) -> Any:
    try:
        module_name = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module 'kontra.config' has no attribute '{name}'") from None
    import importlib

    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value
