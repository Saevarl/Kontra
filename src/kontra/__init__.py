# src/kontra/__init__.py
"""
Kontra - Developer-first Data Quality Engine

Usage:
    # CLI
    $ kontra validate contract.yml
    $ kontra profile data.parquet

    # Python API - Simple validation
    import kontra
    result = kontra.validate(df, "contract.yml")
    if result.passed:
        print("All rules passed!")

    # Python API - Inline rules
    from kontra import rules
    result = kontra.validate(df, rules=[
        rules.not_null("user_id"),
        rules.unique("email"),
    ])

    # Python API - Profile data
    profile = kontra.profile(df)
    print(profile)

    # Python API - Draft rules from profile
    suggestions = kontra.draft(profile)
    suggestions.save("contracts/users.yml")
"""

from kontra.version import VERSION as __version__

# typing names, json and os stay importable as kontra.<name> (they were
# module globals before the API moved into kontra.api.*).
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING  # noqa: F401

import json  # noqa: F401
import os  # noqa: F401

# Heavy imports are lazy-loaded for faster `import kontra`
# polars, ValidationEngine, ScoutProfiler are imported when first needed
if TYPE_CHECKING:
    from kontra.engine.engine import ValidationEngine
    from kontra.scout.profiler import ScoutProfiler

# Scout types (lightweight - just dataclasses)
from kontra.scout.types import DatasetProfile, ColumnProfile, ProfileDiff

# Logging (lightweight)
from kontra.logging import get_logger, log_exception  # noqa: F401

_logger = get_logger(__name__)


# =============================================================================
# Lazy Loading Support
# =============================================================================

# Attributes resolved on first access via PEP 562 module __getattr__.
# Covers heavy deps (polars via probes) and pydantic (via config.settings),
# keeping `import kontra` fast.
_LAZY_ATTRS: Dict[str, tuple] = {
    "ValidationEngine": ("kontra.engine.engine", "ValidationEngine"),
    "ScoutProfiler": ("kontra.scout.profiler", "ScoutProfiler"),
    "pl": ("polars", None),
    "compare": ("kontra.probes", "compare"),
    "profile_relationship": ("kontra.probes", "profile_relationship"),
    "KontraConfig": ("kontra.config.settings", "KontraConfig"),
    "resolve_datasource": ("kontra.config.settings", "resolve_datasource"),
    "resolve_effective_config": ("kontra.config.settings", "resolve_effective_config"),
    "list_datasources": ("kontra.config.settings", "list_datasources"),
}


def __getattr__(name: str) -> Any:
    """Lazy load heavy dependencies on first attribute access (PEP 562)."""
    if name == "_config_path_override":
        # Live view of the set_config() override (state lives in kontra.api.service)
        from kontra.api import service

        return service._config_path_override
    try:
        module_name, attr = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module 'kontra' has no attribute '{name}'") from None
    import importlib

    module = importlib.import_module(module_name)
    value = module if attr is None else getattr(module, attr)
    globals()[name] = value  # cache so later access bypasses __getattr__
    return value


# API types
from kontra.api.results import (
    ValidationResult,
    RuleResult,
    DryRunResult,
    ExplainResult,
    RuleExplainEntry,
    Diff,
    Suggestions,
    SuggestedRule,
)

# Probe types (lightweight - just dataclasses)
from kontra.api.compare import CompareResult, RelationshipProfile

# Query source (lightweight - a read-only SELECT usable anywhere a source is)
from kontra.connectors.query import Query

# Transformation probes - lazy loaded via __getattr__ (they import polars)
# Users access via: kontra.compare(), kontra.profile_relationship()

# Rules helpers
from kontra.api.rules import rules

# Decorators
from kontra.api.decorators import validate as validate_decorator

# Errors
from kontra.errors import ValidationError, StateCorruptedError, ContractNotFoundError  # noqa: F401

# Configuration symbols (KontraConfig, resolve_datasource, ...) are lazy via
# __getattr__: config.settings pulls in pydantic, which costs ~80ms at import.
#
# The subpackage import below must stay: loading `kontra.config` here lets the
# import machinery bind the `config` attribute NOW, so that importing the
# config() function further down permanently overwrites it (pitfall #14: submodule/attribute
# clash). Without it, the first lazy import of kontra.config.* would clobber
# the config() function with the subpackage module. The package __init__ is
# lazy, so this costs nothing.
import kontra.config  # noqa: F401

if TYPE_CHECKING:
    from kontra.config.settings import KontraConfig


# =============================================================================
# Public API (implemented in kontra.api.*)
# =============================================================================

from kontra.api.inputs import (  # noqa: F401
    _DATA_FILE_EXTENSIONS,
    _is_pandas_dataframe,
    _is_polars_dataframe,
    _normalize_to_dataframe,
    _validate_contract_path,
)
from kontra.api.validation import validate, explain
from kontra.api.profiling import (
    profile,
    draft,
    compare_profiles,
    profile_diff,
    list_profiles,
    get_profile,
    scout,
    suggest_rules,
    scout_diff,
)
from kontra.api.history import get_history, diff, list_runs, get_run, has_runs
from kontra.api.annotations import annotate, get_run_with_annotations, get_annotations
from kontra.api.service import (
    resolve,
    config,
    set_config,
    get_config_path,
    list_rules,
    describe_rule,
    health,
)

# Keep help(), repr and pickling reporting these as kontra.<name>.
for _fn in (
    validate, explain,
    profile, draft, compare_profiles, profile_diff, list_profiles, get_profile,
    scout, suggest_rules, scout_diff,
    get_history, diff, list_runs, get_run, has_runs,
    annotate, get_run_with_annotations, get_annotations,
    resolve, config, set_config, get_config_path, list_rules, describe_rule, health,
):
    _fn.__module__ = __name__
del _fn


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Version
    "__version__",
    # Core functions
    "validate",
    "profile",
    "draft",
    "compare_profiles",
    "explain",
    "diff",
    "profile_diff",
    # Transformation probes
    "compare",
    "profile_relationship",
    # Deprecated aliases (kept for backward compatibility)
    "scout",           # Use profile() instead
    "suggest_rules",   # Use draft() instead
    "scout_diff",      # Use profile_diff() instead
    # History functions
    "list_runs",
    "get_run",
    "has_runs",
    "list_profiles",
    "get_profile",
    # Annotation functions
    "annotate",
    "get_annotations",
    "get_run_with_annotations",
    # Configuration functions
    "resolve",
    "config",
    "list_datasources",
    # Service/Agent support
    "set_config",
    "get_config_path",
    "list_rules",
    "describe_rule",
    "health",
    # Result types
    "ValidationResult",
    "RuleResult",
    "DryRunResult",
    "ExplainResult",
    "RuleExplainEntry",
    "Diff",
    "Suggestions",
    "SuggestedRule",
    "DatasetProfile",
    "ColumnProfile",
    "ProfileDiff",
    # Probe result types
    "CompareResult",
    "RelationshipProfile",
    # Query source
    "Query",
    # Rules helpers
    "rules",
    # Decorators
    "validate_decorator",
    # Errors
    "ValidationError",
    "StateCorruptedError",
    # Advanced usage
    "ValidationEngine",
    "ScoutProfiler",
    "KontraConfig",
]
