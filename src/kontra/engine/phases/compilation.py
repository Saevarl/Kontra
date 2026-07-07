# src/kontra/engine/phases/compilation.py
"""
Rule compilation phase.

Builds rules from contract specs, compiles execution plan,
and creates mapping dicts for severity, tally, and context.
"""

from __future__ import annotations

from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.config.models import Contract

from kontra.engine.types import CompilationContext
from kontra.rule_defs.execution_plan import RuleExecutionPlan
from kontra.rule_defs.factory import RuleFactory


# Built-ins registered lazily - see _ensure_builtin_rules_registered()
_builtin_rules_registered = False


def _ensure_builtin_rules_registered() -> None:
    """
    Lazy import builtin rules to register them.

    This defers loading polars until we actually need to build rules.
    Called by compile_rules() when loading contracts.

    Raises:
        ImportError: If polars is not installed (rules depend on it).
    """
    global _builtin_rules_registered
    if _builtin_rules_registered:
        return

    try:
        import kontra.rule_defs.builtin.allowed_values  # noqa: F401
        import kontra.rule_defs.builtin.disallowed_values  # noqa: F401
        import kontra.rule_defs.builtin.custom_sql_check  # noqa: F401
        import kontra.rule_defs.builtin.dtype  # noqa: F401
        import kontra.rule_defs.builtin.freshness  # noqa: F401
        import kontra.rule_defs.builtin.max_rows  # noqa: F401
        import kontra.rule_defs.builtin.min_rows  # noqa: F401
        import kontra.rule_defs.builtin.not_null  # noqa: F401
        import kontra.rule_defs.builtin.range  # noqa: F401
        import kontra.rule_defs.builtin.length  # noqa: F401
        import kontra.rule_defs.builtin.regex  # noqa: F401
        import kontra.rule_defs.builtin.contains  # noqa: F401
        import kontra.rule_defs.builtin.starts_with  # noqa: F401
        import kontra.rule_defs.builtin.ends_with  # noqa: F401
        import kontra.rule_defs.builtin.unique  # noqa: F401
        import kontra.rule_defs.builtin.compare  # noqa: F401
        import kontra.rule_defs.builtin.conditional_not_null  # noqa: F401
        import kontra.rule_defs.builtin.conditional_range  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Failed to load builtin rules. This usually means polars is not installed. "
            "Install with: pip install polars"
        ) from e

    # Builtins now self-register with _builtin=True, so _BUILTIN_RULES
    # is populated during import. No need for post-hoc protection.

    _builtin_rules_registered = True


def _get_rule_columns(rule: Any) -> set:
    """
    Get all columns a rule touches.

    Checks required_columns() first, then falls back to inferring from
    params (column, columns, left, right) — same heuristic used by
    _extract_columns_from_rules in execution_plan.py.

    Returns:
        Set of column names, or empty set for dataset-level rules.
    """
    cols = rule.required_columns()
    if cols:
        return cols

    # Infer from params (same logic as execution_plan._extract_columns_from_rules)
    params = getattr(rule, "params", {}) or {}
    inferred: set = set()
    col = params.get("column")
    if isinstance(col, str) and col:
        inferred.add(col)
    cols_list = params.get("columns")
    if isinstance(cols_list, (list, tuple)):
        inferred.update(c for c in cols_list if isinstance(c, str))
    # Cross-column rules (compare)
    left = params.get("left")
    right = params.get("right")
    if isinstance(left, str) and left:
        inferred.add(left)
    if isinstance(right, str) and right:
        inferred.add(right)
    return inferred


def _filter_rules(
    rules: List[Any],
    only_rules: Optional[List[str]],
    only_columns: Optional[List[str]],
) -> List[Any]:
    """
    Filter rules by name/ID or by column.

    Args:
        rules: Built rule objects
        only_rules: Keep rules matching these names or rule IDs
        only_columns: Keep rules that touch any of these columns.
            Dataset-level rules (no required columns) are always included.

    Returns:
        Filtered list of rules
    """
    if only_rules is None and only_columns is None:
        return rules

    # Empty list means "match nothing" — return zero rules (BUG-027)
    if only_rules is not None and len(only_rules) == 0:
        return []
    if only_columns is not None and len(only_columns) == 0:
        return []

    # Warn if both only and columns are specified (BUG-028)
    if only_rules is not None and only_columns is not None:
        import warnings
        warnings.warn(
            "Both 'only' and 'columns' specified; 'only' takes precedence and 'columns' is ignored.",
            UserWarning,
            stacklevel=4,
        )

    filtered = []
    if only_rules:
        only_set = set(only_rules)
        for rule in rules:
            if rule.name in only_set or rule.rule_id in only_set:
                filtered.append(rule)
        # Warn about unmatched filter values (BUG-029)
        if not filtered:
            import warnings
            matched_nothing = only_set
            warnings.warn(
                f"only={sorted(matched_nothing)} matched no rules. "
                f"Check for typos in rule names/IDs.",
                UserWarning,
                stacklevel=4,
            )
    elif only_columns:
        cols_set = set(only_columns)
        for rule in rules:
            rule_cols = _get_rule_columns(rule)
            if rule_cols and rule_cols & cols_set:
                # Rule touches at least one requested column
                filtered.append(rule)
            elif not rule_cols:
                # Dataset-level rules (min_rows, max_rows, etc.) — always include
                filtered.append(rule)
    return filtered


def compile_rules(
    contract: "Contract",
    inline_built_rules: List[Any],
    global_tally: Optional[bool],
    tally_is_override: bool,
    only_rules: Optional[List[str]] = None,
    only_columns: Optional[List[str]] = None,
) -> CompilationContext:
    """
    Compile rules from contract and create execution plan.

    Args:
        contract: Loaded contract with rule specifications
        inline_built_rules: Pre-built BaseRule instances (from Python API)
        global_tally: Global tally setting (None = use per-rule)
        tally_is_override: True = CLI override, False = per-rule wins
        only_rules: Filter to only these rule names or IDs
        only_columns: Filter to only rules touching these columns

    Returns:
        CompilationContext with rules, plan, and mappings
    """
    # Ensure builtin rules are registered (lazy import to defer polars loading)
    _ensure_builtin_rules_registered()

    rules = RuleFactory(contract.rules).build_rules()
    # Merge with any pre-built rule instances passed directly
    if inline_built_rules:
        rules = rules + inline_built_rules

    # Apply goal-directed filtering
    rules = _filter_rules(rules, only_rules, only_columns)

    # Create execution plan
    plan = RuleExecutionPlan(rules)
    compiled_full = plan.compile()

    # Build rule_id -> severity mapping for injecting into preplan/SQL results
    severity_map = {r.rule_id: r.severity for r in rules}

    # Build rule_id -> effective tally mapping
    # Precedence:
    #   1. CLI --tally flag (tally_is_override=True) - explicit user intent
    #   2. Per-rule tally setting in contract
    #   3. API tally= parameter (tally_is_override=False)
    #   4. Default (False for speed)
    def _effective_tally(rule) -> bool:
        # Schema-level rules (dtype) ignore tally - they're binary, not countable
        # Always return False so preplan can handle them
        if rule.name == "dtype":
            return False
        # CLI tally override beats everything
        if tally_is_override and global_tally is not None:
            return global_tally
        # Per-rule setting beats API default
        if rule.tally is not None:
            return rule.tally
        # API default (if set)
        if global_tally is not None:
            return global_tally
        # Ultimate default - False enables preplan/early-exit optimizations
        return False

    tally_map = {r.rule_id: _effective_tally(r) for r in rules}

    # Build rule_id -> context mapping for injecting into results
    context_map = {r.rule_id: r.context for r in rules if r.context}

    return CompilationContext(
        rules=rules,
        plan=plan,
        compiled_full=compiled_full,
        severity_map=severity_map,
        tally_map=tally_map,
        context_map=context_map,
    )
