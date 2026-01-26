# src/contra/rules/registry.py
from typing import Dict, Type
from kontra.rule_defs.base import BaseRule

RULE_REGISTRY: Dict[str, Type[BaseRule]] = {}

def register_rule(name: str):
    """Decorator to register rule classes in the global registry."""
    def decorator(cls: Type[BaseRule]):
        RULE_REGISTRY[name] = cls
        cls.rule_key = name
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
