# src/kontra/rules/__init__.py
"""
Kontra rules module - Rule definitions and execution planning.

Public API:
    - BaseRule: Abstract base class for custom rules
    - RuleFactory: Creates rule instances from contract specs
    - RuleExecutionPlan: Plans and executes rule validation

Built-in rules are auto-registered when kontra.engine is imported.
"""

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.factory import RuleFactory
from kontra.rule_defs.execution_plan import RuleExecutionPlan, CompiledPlan
from kontra.rule_defs.predicates import Predicate
from kontra.rule_defs.registry import (
    register_rule,
    get_rule,
    get_all_rule_names,
)

__all__ = [
    # Base classes
    "BaseRule",
    "Predicate",
    # Factory and planning
    "RuleFactory",
    "RuleExecutionPlan",
    "CompiledPlan",
    # Registry
    "register_rule",
    "get_rule",
    "get_all_rule_names",
]
