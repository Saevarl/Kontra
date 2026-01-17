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

import os
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


def _is_pandas_dataframe(obj: Any) -> bool:
    """Check if object is a pandas DataFrame without importing pandas."""
    # Check module name to avoid importing pandas
    return type(obj).__module__.startswith("pandas") and type(obj).__name__ == "DataFrame"


# API types
from kontra.api.results import (
    ValidationResult,
    RuleResult,
    DryRunResult,
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
    data: Union[str, pl.DataFrame, "pd.DataFrame", List[Dict[str, Any]], Dict[str, Any], Any],
    contract: Optional[str] = None,
    *,
    table: Optional[str] = None,
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
) -> Union[ValidationResult, DryRunResult]:
    """
    Validate data against a contract and/or inline rules.

    Args:
        data: Data to validate. Accepts:
            - str: File path, URI, or named datasource (e.g., "data.parquet", "s3://...", "prod_db.users")
            - DataFrame: Polars or pandas DataFrame
            - list[dict]: Flat tabular JSON (e.g., API response data)
            - dict: Single record (converted to 1-row DataFrame)
            - Database connection: psycopg2/pyodbc/SQLAlchemy connection (requires `table` param)
        table: Table name for BYOC (Bring Your Own Connection) pattern.
            Required when `data` is a database connection object.
            Formats: "table", "schema.table", or "database.schema.table"
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
        dry_run: If True, validate contract/rules syntax without executing
            against data. Returns DryRunResult with .valid, .rules_count,
            .columns_needed. Use to check contracts before running.
        **kwargs: Additional arguments passed to ValidationEngine

    Returns:
        ValidationResult with .passed, .rules, .to_llm(), etc.
        DryRunResult if dry_run=True, with .valid, .rules_count, .columns_needed

    Example:
        # With contract file
        result = kontra.validate(df, "contract.yml")

        # With inline rules
        from kontra import rules
        result = kontra.validate(df, rules=[
            rules.not_null("user_id"),
            rules.unique("email"),
        ])

        # With list of dicts (e.g., API response)
        data = [{"id": 1, "email": "a@b.com"}, {"id": 2, "email": "c@d.com"}]
        result = kontra.validate(data, rules=[rules.not_null("email")])

        # With single dict (single record validation)
        record = {"id": 1, "email": "test@example.com"}
        result = kontra.validate(record, rules=[rules.regex("email", r".*@.*")])

        # BYOC (Bring Your Own Connection) - database connection + table
        import psycopg2
        conn = psycopg2.connect(host="localhost", dbname="mydb")
        result = kontra.validate(conn, table="public.users", rules=[
            rules.not_null("user_id"),
        ])
        # Note: Kontra does NOT close your connection. You manage its lifecycle.

        # Mix contract and inline rules
        result = kontra.validate(df, "base.yml", rules=[
            rules.freshness("updated_at", max_age="24h"),
        ])

        # Check result
        if result.passed:
            print("All rules passed!")
        else:
            for r in result.blocking_failures:
                print(f"FAILED: {r.rule_id}")

        # Dry run - validate contract syntax without running
        check = kontra.validate(df, "contract.yml", dry_run=True)
        if check.valid:
            print(f"Contract OK: {check.rules_count} rules, needs columns: {check.columns_needed}")
        else:
            print(f"Contract errors: {check.errors}")
    """
    from kontra.errors import InvalidDataError, InvalidPathError
    from kontra.connectors.detection import is_database_connection, is_cursor_object

    # ==========================================================================
    # Input validation - catch invalid data types early with clear errors
    # ==========================================================================

    # Validate inputs
    if contract is None and rules is None:
        raise ValueError("Either contract or rules must be provided")

    # ==========================================================================
    # Dry run - validate contract/rules syntax without executing
    # Data can be None for dry_run since we're not actually validating
    # ==========================================================================
    if dry_run:
        from kontra.config.loader import ContractLoader
        from kontra.rules.factory import RuleFactory
        from kontra.rules.execution_plan import RuleExecutionPlan

        errors: List[str] = []
        contract_name: Optional[str] = None
        datasource: Optional[str] = None
        all_rule_specs: List[Any] = []

        # Load contract if provided
        if contract is not None:
            try:
                contract_obj = ContractLoader.from_path(contract)
                contract_name = contract_obj.name
                datasource = contract_obj.datasource
                all_rule_specs.extend(contract_obj.rules)
            except FileNotFoundError as e:
                errors.append(f"Contract not found: {e}")
            except ValueError as e:
                errors.append(f"Contract parse error: {e}")
            except Exception as e:
                errors.append(f"Contract error: {e}")

        # Add inline rules if provided
        if rules is not None:
            # Convert inline rules to RuleSpec format
            from kontra.config.models import RuleSpec
            for i, r in enumerate(rules):
                try:
                    if isinstance(r, dict):
                        spec = RuleSpec(
                            name=r.get("name", ""),
                            id=r.get("id"),
                            params=r.get("params", {}),
                            severity=r.get("severity", "blocking"),
                        )
                        all_rule_specs.append(spec)
                    else:
                        errors.append(f"Inline rule {i} is not a dict")
                except Exception as e:
                    errors.append(f"Inline rule {i} error: {e}")

        # Try to build rules and extract required columns
        columns_needed: List[str] = []
        rules_count = 0

        if not errors and all_rule_specs:
            try:
                built_rules = RuleFactory(all_rule_specs).build_rules()
                rules_count = len(built_rules)

                # Extract required columns
                plan = RuleExecutionPlan(built_rules)
                compiled = plan.compile()
                columns_needed = list(compiled.required_cols or [])
            except Exception as e:
                errors.append(f"Rule build error: {e}")

        return DryRunResult(
            valid=len(errors) == 0,
            rules_count=rules_count,
            columns_needed=columns_needed,
            contract_name=contract_name,
            datasource=datasource,
            errors=errors,
        )

    # ==========================================================================
    # Input validation for actual validation (not dry_run)
    # ==========================================================================

    # Check for None
    if data is None:
        raise InvalidDataError("NoneType", detail="Data cannot be None")

    # Check for cursor instead of connection (common mistake)
    if is_cursor_object(data):
        raise InvalidDataError(
            type(data).__name__,
            detail="Expected database connection, got cursor object. Pass the connection, not the cursor."
        )

    # Check for BYOC pattern: connection object + table

    is_byoc = False
    if is_database_connection(data):
        if table is None:
            raise ValueError(
                "When passing a database connection, the 'table' parameter is required.\n"
                "Example: kontra.validate(conn, table='public.users', rules=[...])"
            )
        is_byoc = True
    elif table is not None:
        raise ValueError(
            "The 'table' parameter is only valid when 'data' is a database connection.\n"
            "For other data types, use file paths, URIs, or named datasources."
        )

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

    # Normalize and create engine
    if is_byoc:
        # BYOC: database connection + table
        from kontra.connectors.handle import DatasetHandle

        handle = DatasetHandle.from_connection(data, table)
        engine = ValidationEngine(handle=handle, **engine_kwargs)
    elif isinstance(data, str):
        # File path/URI or datasource name
        # Validate: check if it's a directory (common mistake)
        if os.path.isdir(data):
            raise InvalidPathError(data, "Path is a directory, not a file")
        engine = ValidationEngine(data_path=data, **engine_kwargs)
    elif isinstance(data, list):
        # list[dict] - flat tabular JSON (e.g., API response)
        if not data:
            # Empty list - create empty DataFrame
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(data)
        engine = ValidationEngine(dataframe=df, **engine_kwargs)
    elif isinstance(data, dict) and not isinstance(data, pl.DataFrame):
        # Single dict - convert to 1-row DataFrame
        # Note: check for pl.DataFrame first since it's also dict-like in some contexts
        df = pl.DataFrame([data])
        engine = ValidationEngine(dataframe=df, **engine_kwargs)
    elif isinstance(data, pl.DataFrame):
        # Polars DataFrame
        engine = ValidationEngine(dataframe=data, **engine_kwargs)
    elif _is_pandas_dataframe(data):
        # pandas DataFrame - will be converted by engine
        engine = ValidationEngine(dataframe=data, **engine_kwargs)
    else:
        # Invalid data type
        raise InvalidDataError(type(data).__name__)

    # Run validation
    try:
        raw_result = engine.run()
    except OSError as e:
        # Catch internal errors about unsupported formats and wrap in user-friendly error
        error_str = str(e)
        if "Unsupported format" in error_str or "PolarsConnectorMaterializer" in error_str:
            # Extract the problematic value from the error
            if isinstance(data, str):
                raise InvalidDataError(
                    "str",
                    detail=f"'{data}' is not a valid file path, URI, or datasource name"
                ) from None
            else:
                raise InvalidDataError(type(data).__name__) from None
        raise

    # Determine data source for sample_failures()
    # Priority: DataFrame > handle > data path
    if isinstance(data, pl.DataFrame):
        data_source = data
    elif is_byoc:
        # Store the handle for BYOC
        data_source = engine._handle
    elif isinstance(data, str):
        data_source = data
    else:
        # list[dict] or dict - store as DataFrame
        data_source = engine.df

    # Wrap in ValidationResult with data source and rules for sample_failures()
    return ValidationResult.from_engine_result(
        raw_result,
        data_source=data_source,
        rule_objects=engine._rules,
    )


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
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    store = get_default_store()
    if store is None:
        return None

    # Resolve contract to fingerprint
    try:
        # If it's a file path, load contract and compute semantic fingerprint
        if os.path.isfile(contract):
            contract_obj = ContractLoader.from_path(contract)
            contract_fp = fingerprint_contract(contract_obj)
        else:
            # Assume it's a contract name - search stored states
            # Look through all contracts for matching name
            contract_fp = None
            for fp in store.list_contracts():
                history = store.get_history(fp, limit=1)
                if history and history[0].contract_name == contract:
                    contract_fp = fp
                    break

            if contract_fp is None:
                return None

        # Get history for this contract
        states = store.get_history(contract_fp, limit=100)
        if len(states) < 2:
            return None

        # states are newest first, so [0] is latest, [1] is previous
        after_state = states[0]
        before_state = states[1]

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


def _resolve_contract_fingerprint(contract: str, store: Any) -> Optional[str]:
    """
    Resolve a contract name or path to its fingerprint.

    Args:
        contract: Contract name or file path
        store: State store instance

    Returns:
        Contract fingerprint or None if not found
    """
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    # If it's a file path, load contract and compute semantic fingerprint
    if os.path.isfile(contract):
        contract_obj = ContractLoader.from_path(contract)
        return fingerprint_contract(contract_obj)

    # Assume it's a contract name - search stored states
    for fp in store.list_contracts():
        history = store.get_history(fp, limit=1)
        if history and history[0].contract_name == contract:
            return fp

    return None


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
        contract_fp = _resolve_contract_fingerprint(contract, store)
        if contract_fp is None:
            return []

        states = store.get_history(contract_fp, limit=100)
        return [
            {
                "id": s.run_at.isoformat(),
                "fingerprint": s.contract_fingerprint,
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
        contract_fp = _resolve_contract_fingerprint(contract, store)
        if contract_fp is None:
            return None

        # Get history and find specific run or latest
        states = store.get_history(contract_fp, limit=100)
        if not states:
            return None

        state = None
        if run_id:
            # Find specific run by timestamp ID
            for s in states:
                if s.run_at.isoformat() == run_id:
                    state = s
                    break
        else:
            # Get latest (first in list, newest first)
            state = states[0]

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
        contract_fp = _resolve_contract_fingerprint(contract, store)
        if contract_fp is None:
            return False

        states = store.get_history(contract_fp, limit=1)
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
# Service/Agent Support Functions
# =============================================================================

# Global config path override for service/agent use
_config_path_override: Optional[str] = None


def set_config(path: Optional[str]) -> None:
    """
    Set config file path for service/agent use.

    By default, Kontra discovers config from cwd (.kontra/config.yml).
    For long-running services or agents, use this to set an explicit path.

    Args:
        path: Path to config.yml (or None to reset to auto-discovery)

    Example:
        kontra.set_config("/etc/kontra/config.yml")
        result = kontra.validate(df, rules=[...])

        # Reset to default behavior
        kontra.set_config(None)
    """
    global _config_path_override
    _config_path_override = path


def get_config_path() -> Optional[str]:
    """
    Get the current config path override.

    Returns:
        The overridden config path, or None if using auto-discovery.
    """
    return _config_path_override


def list_rules() -> List[Dict[str, Any]]:
    """
    List all available validation rules.

    For agents and integrations that need to discover what rules exist.

    Returns:
        List of rule info dicts with name, description, params

    Example:
        rules = kontra.list_rules()
        for rule in rules:
            print(f"{rule['name']}: {rule['description']}")
    """
    from kontra.rules.registry import RULE_REGISTRY

    # Rule metadata - manually maintained for quality descriptions
    # This is better than parsing docstrings which may be inconsistent
    RULE_METADATA = {
        "not_null": {
            "description": "Fails where column contains NULL values (optionally NaN)",
            "params": {"column": "required", "include_nan": "optional (default: False)"},
            "scope": "column",
        },
        "unique": {
            "description": "Fails where column contains duplicate values",
            "params": {"column": "required"},
            "scope": "column",
        },
        "allowed_values": {
            "description": "Fails where column contains values not in allowed list",
            "params": {"column": "required", "values": "required (list)"},
            "scope": "column",
        },
        "range": {
            "description": "Fails where column values are outside [min, max] range",
            "params": {"column": "required", "min": "optional", "max": "optional"},
            "scope": "column",
        },
        "regex": {
            "description": "Fails where column values don't match regex pattern",
            "params": {"column": "required", "pattern": "required"},
            "scope": "column",
        },
        "dtype": {
            "description": "Fails if column data type doesn't match expected type",
            "params": {"column": "required", "expected": "required"},
            "scope": "column",
        },
        "min_rows": {
            "description": "Fails if dataset has fewer than threshold rows",
            "params": {"threshold": "required (int)"},
            "scope": "dataset",
        },
        "max_rows": {
            "description": "Fails if dataset has more than threshold rows",
            "params": {"threshold": "required (int)"},
            "scope": "dataset",
        },
        "freshness": {
            "description": "Fails if timestamp column is older than max_age",
            "params": {"column": "required", "max_age": "required (e.g., '24h', '7d')"},
            "scope": "column",
        },
        "custom_sql_check": {
            "description": "Escape hatch: run arbitrary SQL that returns violation count",
            "params": {"sql": "required", "threshold": "optional (default: 0)"},
            "scope": "dataset",
        },
        "compare": {
            "description": "Fails where left column doesn't satisfy comparison with right column",
            "params": {
                "left": "required (column name)",
                "right": "required (column name)",
                "op": "required (>, >=, <, <=, ==, !=)",
            },
            "scope": "cross-column",
        },
        "conditional_not_null": {
            "description": "Fails where column is NULL when a condition is met",
            "params": {
                "column": "required (column to check)",
                "when": "required (e.g., \"status == 'shipped'\")",
            },
            "scope": "cross-column",
        },
    }

    result = []
    for name in sorted(RULE_REGISTRY.keys()):
        info = {"name": name}

        # Add metadata if available
        if name in RULE_METADATA:
            meta = RULE_METADATA[name]
            info["description"] = meta.get("description", "")
            info["params"] = meta.get("params", {})
            info["scope"] = meta.get("scope", "unknown")
        else:
            # Fallback for rules not in metadata
            info["description"] = f"Validation rule: {name}"
            info["params"] = {}
            info["scope"] = "unknown"

        result.append(info)

    return result


def health() -> Dict[str, Any]:
    """
    Health check for service/agent use.

    Returns version, config status, and available rules.
    Use this to verify Kontra is properly installed and configured.

    Returns:
        Dict with version, config_found, config_path, rule_count, status

    Example:
        health = kontra.health()
        if health["status"] == "ok":
            print(f"Kontra {health['version']} ready")
        else:
            print(f"Issue: {health['status']}")
    """
    from kontra.rules.registry import RULE_REGISTRY
    from kontra.config.settings import find_config_file
    from pathlib import Path

    result: Dict[str, Any] = {
        "version": __version__,
        "status": "ok",
    }

    # Check config
    if _config_path_override:
        config_path = Path(_config_path_override)
        result["config_path"] = str(config_path)
        result["config_found"] = config_path.exists()
        if not config_path.exists():
            result["status"] = "config_not_found"
    else:
        found = find_config_file()
        result["config_path"] = str(found) if found else None
        result["config_found"] = found is not None

    # Rule count
    result["rule_count"] = len(RULE_REGISTRY)

    # List available rules
    result["rules"] = sorted(RULE_REGISTRY.keys())

    return result


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
    # Service/Agent support
    "set_config",
    "get_config_path",
    "list_rules",
    "health",
    # Result types
    "ValidationResult",
    "RuleResult",
    "DryRunResult",
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
