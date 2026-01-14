# src/kontra/config/__init__.py
"""
Kontra configuration module - Contract and settings handling.

Public API:
    - Contract, RuleSpec: Data models for contracts
    - ContractLoader: Loads contracts from files or S3
    - KontraConfig, EffectiveConfig: Configuration models
    - load_config: Load project configuration
"""

from kontra.config.models import Contract, RuleSpec
from kontra.config.loader import ContractLoader
from kontra.config.settings import (
    KontraConfig,
    EffectiveConfig,
    load_config_file,
    resolve_effective_config,
    find_config_file,
)

__all__ = [
    # Contract models
    "Contract",
    "RuleSpec",
    # Loader
    "ContractLoader",
    # Config
    "KontraConfig",
    "EffectiveConfig",
    "load_config_file",
    "find_config_file",
    "resolve_effective_config",
]
