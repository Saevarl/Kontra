from __future__ import annotations

from typing import List, Dict, Any, Optional

from contra.rules.base import BaseRule
from contra.rules.registry import get_rule
from contra.config.models import RuleSpec


def _derive_rule_id(spec: RuleSpec) -> str:
    """
    Generate a stable, unique rule_id for a rule spec when no explicit id is provided.

    Policy:
      - If spec.id is set → return it as-is (caller must ensure uniqueness)
      - If column param exists and is a string → COL:{column}:{name}
      - Otherwise → DATASET:{name}
    """
    explicit: Optional[str] = getattr(spec, "id", None)
    if explicit:
        return explicit

    params: Dict[str, Any] = spec.params or {}
    col = params.get("column")
    if isinstance(col, str) and col:
        return f"COL:{col}:{spec.name}"
    return f"DATASET:{spec.name}"


class RuleFactory:
    """
    Translate contract RuleSpec objects into instantiated Rule instances.

    Responsibilities:
      - Resolve the rule class from the registry
      - Instantiate with (name, params)
      - Assign rule_id per our identity policy
      - Provide helpful errors on unknown/failed rules
    """

    def __init__(self, rule_specs: List[RuleSpec]):
        self.rule_specs = rule_specs

    def build_rules(self) -> List[BaseRule]:
        """Instantiate all rules declared in the contract."""
        rules: List[BaseRule] = []

        for spec in self.rule_specs:
            rule_name = spec.name
            rule_params = spec.params or {}

            try:
                rule_cls = get_rule(rule_name)
            except KeyError as e:
                raise ValueError(f"Unknown rule '{rule_name}' — not found in registry.") from e

            try:
                # IMPORTANT: constructor accepts (name, params) only
                rule_instance: BaseRule = rule_cls(rule_name, rule_params)
                # Assign rule_id after construction
                rule_instance.rule_id = _derive_rule_id(spec)
                rules.append(rule_instance)
            except Exception as e:
                raise RuntimeError(f"Failed to instantiate rule '{rule_name}': {e}") from e

        return rules

    @staticmethod
    def summarize_rules(rules: List[BaseRule]) -> List[Dict[str, Any]]:
        """Return a summary of all rule configurations (for debug/reporting)."""
        return [
            {
                "rule_id": getattr(rule, "rule_id", rule.name),
                "params": rule.params,
                "class": rule.__class__.__name__,
            }
            for rule in rules
        ]
