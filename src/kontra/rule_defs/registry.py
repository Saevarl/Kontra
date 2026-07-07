# src/kontra/rules/registry.py
from typing import Dict, Set, Type
from kontra.rule_defs.base import BaseRule

RULE_REGISTRY: Dict[str, Type[BaseRule]] = {}

# Hardcoded set of built-in rule names. Checked BEFORE lazy registration
# so that user code calling @register_rule("not_null") is always rejected,
# even if builtins haven't been lazily loaded yet.
_BUILTIN_RULE_NAMES: frozenset = frozenset({
    "not_null", "unique", "allowed_values", "disallowed_values",
    "range", "regex", "dtype", "length", "contains", "starts_with",
    "ends_with", "compare", "conditional_not_null", "conditional_range",
    "min_rows", "max_rows", "freshness", "custom_sql_check",
})

# Dynamically populated set (extended during lazy registration).
# Contains _BUILTIN_RULE_NAMES plus any rules registered with _builtin=True.
_BUILTIN_RULES: Set[str] = set()


def register_rule(name: str, *, _builtin: bool = False):
    """Decorator to register rule classes in the global registry.

    Args:
        name: Rule name (e.g. "not_null", "range").
        _builtin: Internal flag — set True for Kontra's built-in rules.
            When True, the rule name is protected from being overwritten.

    Raises:
        ValueError: If attempting to overwrite a built-in rule.
    """
    def decorator(cls: Type[BaseRule]):
        # Validate rule name (BUG-036)
        if not name or not isinstance(name, str):
            raise ValueError("Rule name must be a non-empty string")
        if not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                f"Invalid rule name '{name}'. "
                f"Rule names must contain only alphanumeric characters, underscores, and hyphens."
            )
        # Check both static and dynamic sets (BUG-035)
        if not _builtin and (name in _BUILTIN_RULE_NAMES or name in _BUILTIN_RULES):
            raise ValueError(
                f"Cannot overwrite built-in rule '{name}'. "
                f"Use a different name for your custom rule."
            )
        RULE_REGISTRY[name] = cls
        cls.rule_key = name
        if _builtin:
            _BUILTIN_RULES.add(name)
        return cls
    return decorator

def get_rule(name: str) -> Type[BaseRule]:
    """Retrieves a rule class by name."""
    if name not in RULE_REGISTRY:
        raise KeyError(f"Rule '{name}' not found in registry.")
    return RULE_REGISTRY[name]


def get_all_rule_names() -> set:
    """Returns all registered rule names."""
    return set(RULE_REGISTRY.keys())
