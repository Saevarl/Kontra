# src/kontra/__init__.py
"""
Kontra - Developer-first Data Quality Engine

Usage:
    # CLI
    $ kontra validate contract.yml
    $ kontra scout data.parquet

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
    profile = kontra.scout(df)
    print(profile)

    # Python API - Suggest rules from profile
    suggestions = kontra.suggest_rules(profile)
    suggestions.save("contracts/users.yml")
"""

from kontra.version import VERSION as __version__

# Type imports
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

# Core engine (for advanced usage)
from kontra.engine.engine import ValidationEngine

# Scout profiler (for advanced usage)
from kontra.scout.profiler import ScoutProfiler

# Scout types
from kontra.scout.types import DatasetProfile, ColumnProfile, ProfileDiff

# Logging
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)

# API types
from kontra.api.results import (
    ValidationResult,
    RuleResult,
    Diff,
    Suggestions,
    SuggestedRule,
)

# Rules helpers
from kontra.api.rules import rules

# Configuration
from kontra.config.settings import (
    resolve_datasource,
    resolve_effective_config,
    list_datasources,
    KontraConfig,
)


# =============================================================================
# Core Functions
# =============================================================================


def validate(
    data: Union[str, pl.DataFrame, "pd.DataFrame"],
    contract: Optional[str] = None,
    *,
    rules: Optional[List[Dict[str, Any]]] = None,
    emit_report: bool = False,
    save: bool = True,
    preplan: str = "auto",
    pushdown: str = "auto",
    projection: bool = True,
    csv_mode: str = "auto",
    env: Optional[str] = None,
    stats: str = "none",
    dry_run: bool = False,
    **kwargs,
) -> ValidationResult:
    """
    Validate data against a contract and/or inline rules.

    Args:
        data: DataFrame (Polars or pandas) or path/URI to data file
        contract: Path to contract YAML file (optional if rules provided)
        rules: List of inline rule dicts (optional if contract provided)
        emit_report: Print validation report to console
        save: Save result to history (default: True)
        preplan: "on" | "off" | "auto"
        pushdown: "on" | "off" | "auto"
        projection: Enable column pruning
        csv_mode: "auto" | "duckdb" | "parquet"
        env: Environment name from config
        stats: "none" | "summary" | "profile"
        dry_run: Validate contract without running (returns check result)
        **kwargs: Additional arguments passed to ValidationEngine

    Returns:
        ValidationResult with .passed, .rules, .to_llm(), etc.

    Example:
        # With contract file
        result = kontra.validate(df, "contract.yml")

        # With inline rules
        from kontra import rules
        result = kontra.validate(df, rules=[
            rules.not_null("user_id"),
            rules.unique("email"),
        ])

        # Mix both
        result = kontra.validate(df, "base.yml", rules=[
            rules.freshness("updated_at", max_age="24h"),
        ])

        # Check result
        if result.passed:
            print("All rules passed!")
        else:
            for r in result.blocking_failures:
                print(f"FAILED: {r.rule_id}")
    """
    # Validate inputs
    if contract is None and rules is None:
        raise ValueError("Either contract or rules must be provided")

    # Resolve environment config
    if env:
        cfg = resolve_effective_config(env_name=env)
        # Apply config defaults (CLI args take precedence)
        if preplan == "auto" and cfg.preplan:
            preplan = cfg.preplan
        if pushdown == "auto" and cfg.pushdown:
            pushdown = cfg.pushdown

    # Build engine kwargs
    engine_kwargs = {
        "contract_path": contract,
        "emit_report": emit_report,
        "save_state": save,
        "preplan": preplan,
        "pushdown": pushdown,
        "enable_projection": projection,
        "csv_mode": csv_mode,
        "stats_mode": stats,
        "inline_rules": rules,
        **kwargs,
    }

    # Create engine
    if isinstance(data, str):
        # File path/URI or datasource name
        engine = ValidationEngine(data_path=data, **engine_kwargs)
    else:
        # DataFrame
        engine = ValidationEngine(dataframe=data, **engine_kwargs)

    # Run validation
    raw_result = engine.run()

    # Get dataset name
    dataset = "unknown"
    if isinstance(data, str):
        dataset = data
    elif engine.contract:
        dataset = engine.contract.dataset or engine.contract.name or "dataframe"

    # Wrap in ValidationResult
    return ValidationResult.from_engine_result(raw_result, dataset=dataset)


def scout(
    data: Union[str, pl.DataFrame],
    preset: str = "standard",
    *,
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    save: bool = True,
    **kwargs,
) -> DatasetProfile:
    """
    Profile a dataset.

    Args:
        data: DataFrame (Polars) or path/URI to data file
        preset: Profiling depth ("lite", "standard", "deep", "llm")
        columns: Only profile these columns
        sample: Sample N rows (default: all)
        save: Save profile to history
        **kwargs: Additional arguments passed to ScoutProfiler

    Returns:
        DatasetProfile with column statistics

    Example:
        profile = kontra.scout("data.parquet")
        print(f"Rows: {profile.row_count}")
        for col in profile.columns:
            print(f"{col.name}: {col.dtype}")
    """
    if isinstance(data, pl.DataFrame):
        # For DataFrame input, write to temp file
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name
            data.write_parquet(temp_path)

        try:
            profiler = ScoutProfiler(
                temp_path,
                preset=preset,
                columns=columns,
                sample_size=sample,
                **kwargs,
            )
            return profiler.profile()
        finally:
            os.unlink(temp_path)
    else:
        profiler = ScoutProfiler(
            data,
            preset=preset,
            columns=columns,
            sample_size=sample,
            **kwargs,
        )
        return profiler.profile()


def suggest_rules(
    profile: DatasetProfile,
    min_confidence: float = 0.5,
) -> Suggestions:
    """
    Generate validation rule suggestions from a profile.

    Args:
        profile: DatasetProfile from kontra.scout()
        min_confidence: Minimum confidence score (0.0-1.0)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()

    Example:
        profile = kontra.scout(df, preset="deep")
        suggestions = kontra.suggest_rules(profile)

        # Filter high confidence
        high_conf = suggestions.filter(min_confidence=0.9)

        # Save to file
        high_conf.save("contracts/users.yml")

        # Or use directly
        result = kontra.validate(df, rules=suggestions.to_dict())
    """
    return Suggestions.from_profile(profile, min_confidence=min_confidence)


def explain(
    data: Union[str, pl.DataFrame],
    contract: str,
    **kwargs,
) -> Dict[str, Any]:
    """
    Show execution plan without running validation.

    Args:
        data: DataFrame or path/URI to data file
        contract: Path to contract YAML file

    Returns:
        Dict with preplan_rules, sql_rules, polars_rules, required_columns

    Example:
        plan = kontra.explain(df, "contract.yml")
        print(f"Columns needed: {plan['required_columns']}")
        for rule in plan['sql_rules']:
            print(f"{rule['rule_id']}: {rule['sql']}")
    """
    # For now, return basic plan info
    # TODO: Implement full explain with SQL preview
    from kontra.config.loader import ContractLoader
    from kontra.rules.factory import RuleFactory
    from kontra.rules.execution_plan import RuleExecutionPlan

    contract_obj = ContractLoader.from_path(contract)
    rules = RuleFactory(contract_obj.rules).build_rules()
    plan = RuleExecutionPlan(rules)
    compiled = plan.compile()

    # sql_rules may be Rule objects or dicts depending on compilation
    sql_rules_info = []
    for r in compiled.sql_rules:
        if hasattr(r, "rule_id"):
            sql_rules_info.append({"rule_id": r.rule_id, "name": r.name})
        elif isinstance(r, dict):
            sql_rules_info.append({"rule_id": r.get("rule_id", ""), "name": r.get("name", "")})

    return {
        "required_columns": list(compiled.required_cols or []),
        "total_rules": len(rules),
        "predicates": len(compiled.predicates),
        "fallback_rules": len(compiled.fallback_rules),
        "sql_rules": sql_rules_info,
    }


def diff(
    contract: str,
    *,
    since: Optional[str] = None,
    before: Optional[str] = None,
    after: Optional[str] = None,
) -> Optional[Diff]:
    """
    Compare validation runs over time.

    Args:
        contract: Contract name or path
        since: Compare to run from this time ago ("7d", "24h", "2024-01-15")
        before: Specific run ID for before state
        after: Specific run ID for after state (default: latest)

    Returns:
        Diff with .has_changes, .regressed, .new_failures, .to_llm()
        Returns None if no history available

    Example:
        diff = kontra.diff("users_contract", since="7d")
        if diff and diff.regressed:
            print("Quality regressed!")
            for failure in diff.new_failures:
                print(f"  NEW: {failure['rule_id']}")
    """
    from kontra.state.backends import get_default_store
    from kontra.state.types import StateDiff

    store = get_default_store()
    if store is None:
        return None

    # Get states
    try:
        states = store.list_states(contract)
        if len(states) < 2:
            return None

        # Get before and after states
        after_state = store.get_state(contract)  # Latest
        if after_state is None:
            return None

        # Find before state
        if before:
            before_state = store.get_state(contract, run_id=before)
        elif since:
            # Parse since and find appropriate state
            # TODO: Implement time-based lookup
            before_state = states[-2] if len(states) > 1 else None
        else:
            # Compare to previous
            before_state = states[-2] if len(states) > 1 else None

        if before_state is None:
            return None

        # Compute diff
        state_diff = StateDiff.compute(before_state, after_state)
        return Diff.from_state_diff(state_diff)

    except Exception as e:
        log_exception(_logger, "Failed to compute diff", e)
        return None


def scout_diff(
    source: str,
    *,
    since: Optional[str] = None,
) -> Optional[ProfileDiff]:
    """
    Compare profile runs over time.

    Args:
        source: Data source path or name
        since: Compare to profile from this time ago

    Returns:
        ProfileDiff with .has_changes, .schema_changes, .to_llm()
        Returns None if no history available

    Example:
        diff = kontra.scout_diff("data.parquet", since="7d")
        if diff and diff.has_schema_changes:
            print("Schema changed!")
            for col in diff.columns_added:
                print(f"  NEW: {col}")
    """
    # TODO: Implement profile history lookup
    return None


# =============================================================================
# History Functions
# =============================================================================


def list_runs(contract: str) -> List[Dict[str, Any]]:
    """
    List past validation runs for a contract.

    Args:
        contract: Contract name or path

    Returns:
        List of run summaries with id, timestamp, passed, etc.
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return []

    try:
        states = store.list_states(contract)
        return [
            {
                "id": s.contract_fingerprint,
                "timestamp": s.run_at,
                "passed": s.summary.passed,
                "total_rules": s.summary.total_rules,
                "failed_count": s.summary.failed_rules,
                "dataset": s.dataset_uri,
            }
            for s in states
        ]
    except Exception as e:
        log_exception(_logger, "Failed to list runs", e)
        return []


def get_run(
    contract: str,
    run_id: Optional[str] = None,
) -> Optional[ValidationResult]:
    """
    Get a specific validation run.

    Args:
        contract: Contract name or path
        run_id: Specific run ID (default: latest)

    Returns:
        ValidationResult or None if not found
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return None

    try:
        state = store.get_state(contract, run_id=run_id)
        if state is None:
            return None

        # Convert state to ValidationResult
        return ValidationResult(
            passed=state.summary.passed,
            dataset=state.dataset_uri,
            total_rules=state.summary.total_rules,
            passed_count=state.summary.passed_rules,
            failed_count=state.summary.blocking_failures,
            warning_count=state.summary.warning_failures,
            rules=[
                RuleResult(
                    rule_id=r.rule_id,
                    name=r.rule_name,
                    passed=r.passed,
                    failed_count=r.failed_count,
                    message=r.message or "",
                    severity=r.severity,
                    source=r.execution_source,
                    column=r.column,
                )
                for r in state.rules
            ],
        )
    except Exception as e:
        log_exception(_logger, "Failed to get run", e)
        return None


def has_runs(contract: str) -> bool:
    """
    Check if any validation history exists for a contract.

    Args:
        contract: Contract name or path

    Returns:
        True if history exists
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return False

    try:
        states = store.list_states(contract)
        return len(states) > 0
    except Exception as e:
        log_exception(_logger, "Failed to check runs", e)
        return False


def list_profiles(source: str) -> List[Dict[str, Any]]:
    """
    List past profile runs for a data source.

    Args:
        source: Data source path or name

    Returns:
        List of profile summaries
    """
    # TODO: Implement profile history
    return []


def get_profile(
    source: str,
    run_id: Optional[str] = None,
) -> Optional[DatasetProfile]:
    """
    Get a specific profile run.

    Args:
        source: Data source path or name
        run_id: Specific run ID (default: latest)

    Returns:
        DatasetProfile or None if not found
    """
    # TODO: Implement profile history lookup
    return None


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
    return resolve_datasource(name)


def config(env: Optional[str] = None) -> KontraConfig:
    """
    Get effective configuration.

    Args:
        env: Environment name (default: use KONTRA_ENV or defaults)

    Returns:
        KontraConfig with preplan, pushdown, etc.

    Example:
        cfg = kontra.config()
        cfg = kontra.config(env="production")
        print(cfg.preplan)  # "auto"
    """
    return resolve_effective_config(env_name=env)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Version
    "__version__",
    # Core functions
    "validate",
    "scout",
    "suggest_rules",
    "explain",
    "diff",
    "scout_diff",
    # History functions
    "list_runs",
    "get_run",
    "has_runs",
    "list_profiles",
    "get_profile",
    # Configuration functions
    "resolve",
    "config",
    "list_datasources",
    # Result types
    "ValidationResult",
    "RuleResult",
    "Diff",
    "Suggestions",
    "SuggestedRule",
    "DatasetProfile",
    "ColumnProfile",
    "ProfileDiff",
    # Rules helpers
    "rules",
    # Advanced usage
    "ValidationEngine",
    "ScoutProfiler",
    "KontraConfig",
]
