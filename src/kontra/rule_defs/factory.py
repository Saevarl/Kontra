from __future__ import annotations

from typing import List, Dict, Any, Optional

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import get_rule, get_all_rule_names
from kontra.config.models import RuleSpec
from kontra.errors import DuplicateRuleIdError


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
        seen_ids: Dict[str, int] = {}  # rule_id -> index in rule_specs (for error messages)

        for idx, spec in enumerate(self.rule_specs):
            rule_name = spec.name
            rule_params = spec.params or {}

            try:
                rule_cls = get_rule(rule_name)
            except KeyError:
                available = sorted(get_all_rule_names())
                raise ValueError(
                    f"Unknown rule '{rule_name}'. "
                    f"Available rules: {', '.join(available)}"
                )

            try:
                # IMPORTANT: constructor accepts (name, params) only
                rule_instance: BaseRule = rule_cls(rule_name, rule_params)
                # Assign rule_id after construction
                rule_id = _derive_rule_id(spec)

                # Check for duplicate rule IDs
                if rule_id in seen_ids:
                    prev_idx = seen_ids[rule_id]
                    column = rule_params.get("column")
                    raise DuplicateRuleIdError(
                        rule_id=rule_id,
                        rule_name=rule_name,
                        rule_index=idx,
                        conflict_index=prev_idx,
                        column=column if isinstance(column, str) else None,
                    )
                seen_ids[rule_id] = idx

                rule_instance.rule_id = rule_id
                rule_instance.severity = spec.severity
                rule_instance.tally = spec.tally  # None = use global default
                rule_instance.context = spec.context or {}

                # Warn if tally is set on a rule that doesn't support it
                if spec.tally is not None and not rule_instance.supports_tally:
                    import warnings
                    warnings.warn(
                        f"Rule '{rule_name}' (scope: {rule_instance.rule_scope}) does not support tally; "
                        f"tally={spec.tally} will be ignored",
                        UserWarning,
                        stacklevel=2,
                    )

                rules.append(rule_instance)
            except (ValueError, DuplicateRuleIdError):
                raise  # Re-raise validation errors as-is
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
