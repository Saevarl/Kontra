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

# Type imports
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

import json
import os

# Heavy imports are lazy-loaded for faster `import kontra`
# polars, ValidationEngine, ScoutProfiler are imported when first needed
if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from kontra.engine.engine import ValidationEngine
    from kontra.scout.profiler import ScoutProfiler

# Scout types (lightweight - just dataclasses)
from kontra.scout.types import DatasetProfile, ColumnProfile, ProfileDiff

# Logging (lightweight)
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


# =============================================================================
# Lazy Loading Support
# =============================================================================

# Cache for lazily loaded modules/classes
_lazy_cache: Dict[str, Any] = {}


def __getattr__(name: str) -> Any:
    """
    Lazy load heavy dependencies on first access.

    This allows `import kontra` to be fast while still supporting:
    - kontra.ValidationEngine (for advanced usage)
    - kontra.ScoutProfiler (for advanced usage)
    """
    if name == "ValidationEngine":
        if "ValidationEngine" not in _lazy_cache:
            from kontra.engine.engine import ValidationEngine
            _lazy_cache["ValidationEngine"] = ValidationEngine
        return _lazy_cache["ValidationEngine"]

    if name == "ScoutProfiler":
        if "ScoutProfiler" not in _lazy_cache:
            from kontra.scout.profiler import ScoutProfiler
            _lazy_cache["ScoutProfiler"] = ScoutProfiler
        return _lazy_cache["ScoutProfiler"]

    if name == "pl":
        # Support kontra.pl for users who expect polars to be accessible
        if "pl" not in _lazy_cache:
            import polars as pl
            _lazy_cache["pl"] = pl
        return _lazy_cache["pl"]

    # Transformation probes (import polars)
    if name == "compare":
        if "compare" not in _lazy_cache:
            from kontra.probes import compare
            _lazy_cache["compare"] = compare
        return _lazy_cache["compare"]

    if name == "profile_relationship":
        if "profile_relationship" not in _lazy_cache:
            from kontra.probes import profile_relationship
            _lazy_cache["profile_relationship"] = profile_relationship
        return _lazy_cache["profile_relationship"]

    raise AttributeError(f"module 'kontra' has no attribute '{name}'")


def _is_pandas_dataframe(obj: Any) -> bool:
    """Check if object is a pandas DataFrame without importing pandas."""
    # Check module name to avoid importing pandas
    return type(obj).__module__.startswith("pandas") and type(obj).__name__ == "DataFrame"


# Data file extensions that should not be passed to state functions
_DATA_FILE_EXTENSIONS = {".parquet", ".csv", ".json", ".ndjson", ".jsonl", ".arrow", ".feather"}


def _validate_contract_path(path: str, function_name: str) -> None:
    """
    Validate that a path looks like a contract file, not a data file.

    Raises ValueError with a helpful message if the file appears to be a data file.
    """
    lower = path.lower()
    for ext in _DATA_FILE_EXTENSIONS:
        if lower.endswith(ext):
            raise ValueError(
                f"{function_name}() requires a contract YAML file path, not a data file. "
                f"Received: '{path}' (appears to be a {ext[1:].upper()} file). "
                f"Example: kontra.{function_name}('contract.yml')"
            )


# API types
from kontra.api.results import (
    ValidationResult,
    RuleResult,
    DryRunResult,
    Diff,
    Suggestions,
    SuggestedRule,
)

# Probe types (lightweight - just dataclasses)
from kontra.api.compare import CompareResult, RelationshipProfile

# Transformation probes - lazy loaded via __getattr__ (they import polars)
# Users access via: kontra.compare(), kontra.profile_relationship()

# Rules helpers
from kontra.api.rules import rules

# Decorators
from kontra.api.decorators import validate as validate_decorator

# Errors
from kontra.errors import ValidationError, StateCorruptedError, ContractNotFoundError

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
    data: Union[str, "pl.DataFrame", "pd.DataFrame", List[Dict[str, Any]], Dict[str, Any], Any],
    contract: Optional[str] = None,
    *,
    table: Optional[str] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
    emit_report: bool = False,
    save: bool = True,
    preplan: str = "auto",
    pushdown: str = "auto",
    tally: Optional[bool] = None,
    projection: bool = True,
    csv_mode: str = "auto",
    env: Optional[str] = None,
    stats: str = "none",
    dry_run: bool = False,
    sample: int = 0,
    sample_budget: int = 50,
    sample_columns: Optional[Union[List[str], str]] = None,
    storage_options: Optional[Dict[str, Any]] = None,
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
        tally: Global tally override. None = use per-rule settings (default),
            True = count all violations (exact), False = early-stop (fast, ≥1)
        projection: Enable column pruning
        csv_mode: "auto" | "duckdb" | "parquet"
        env: Environment name from config
        stats: "none" | "summary" | "profile"
        dry_run: If True, validate contract/rules syntax without executing
            against data. Returns DryRunResult with .valid, .rules_count,
            .columns_needed. Use to check contracts before running.
        sample: Per-rule sample cap for failing rows (default: 0 disabled, set to 5 to enable)
        sample_budget: Global sample cap across all rules (default: 50)
        sample_columns: Columns to include in samples for token efficiency.
            - None (default): All columns
            - ["col1", "col2"]: Only specified columns
            - "relevant": Rule's columns + row_index only
        storage_options: Cloud storage credentials (S3, Azure, GCS).
            For S3/MinIO:
                - aws_access_key_id, aws_secret_access_key
                - aws_region (required for Polars)
                - endpoint_url (for MinIO/S3-compatible)
            For Azure:
                - account_name, account_key, sas_token, etc.
            These override environment variables when provided.
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
    from kontra.engine.paths import detect_execution_path

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
        from kontra.rule_defs.factory import RuleFactory
        from kontra.rule_defs.execution_plan import RuleExecutionPlan

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
            except ContractNotFoundError as e:
                errors.append(str(e))
            except ValueError as e:
                errors.append(f"Contract parse error: {e}")
            except Exception as e:
                errors.append(f"Contract error: {e}")

        # Add inline rules if provided
        inline_built_rules = []  # Already-built BaseRule instances
        if rules is not None:
            # Convert inline rules to RuleSpec format (or pass through BaseRule instances)
            from kontra.config.models import RuleSpec
            from kontra.rule_defs.base import BaseRule as BaseRuleType
            for i, r in enumerate(rules):
                try:
                    if isinstance(r, BaseRuleType):
                        # Already a rule instance - use directly
                        inline_built_rules.append(r)
                    elif isinstance(r, dict):
                        spec = RuleSpec(
                            name=r.get("name", ""),
                            id=r.get("id"),
                            params=r.get("params", {}),
                            severity=r.get("severity", "blocking"),
                            context=r.get("context", {}),
                        )
                        all_rule_specs.append(spec)
                    else:
                        errors.append(
                            f"Inline rule {i}: expected dict or BaseRule, "
                            f"got {type(r).__name__}"
                        )
                except Exception as e:
                    errors.append(f"Inline rule {i} error: {e}")

        # Try to build rules and extract required columns
        columns_needed: List[str] = []
        rules_count = 0

        if not errors and (all_rule_specs or inline_built_rules):
            try:
                built_rules = RuleFactory(all_rule_specs).build_rules() if all_rule_specs else []
                # Merge with already-built rule instances
                built_rules = list(built_rules) + inline_built_rules
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

    # Resolve config (always, for severity_weights and other settings)
    cfg = resolve_effective_config(env_name=env)

    # Apply config defaults (CLI args take precedence)
    if env:
        if preplan == "auto" and cfg.preplan:
            preplan = cfg.preplan
        if pushdown == "auto" and cfg.pushdown:
            pushdown = cfg.pushdown

    # Detect execution path early (enables lazy loading optimization in engine)
    execution_path = detect_execution_path(data, table=table)

    # Lazy import heavy dependencies (only loaded when validate() is called)
    import polars as pl
    from kontra.engine.engine import ValidationEngine

    # Build engine kwargs
    engine_kwargs = {
        "contract_path": contract,
        "emit_report": emit_report,
        "save_state": save,
        "preplan": preplan,
        "pushdown": pushdown,
        "tally": tally,
        "enable_projection": projection,
        "csv_mode": csv_mode,
        "stats_mode": stats,
        "inline_rules": rules,
        "storage_options": storage_options,
        "execution_path": execution_path,
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
            # Empty list - create empty DataFrame (valid for dataset-level rules like min_rows)
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(data)
        engine = ValidationEngine(dataframe=df, **engine_kwargs)
    elif isinstance(data, dict) and not isinstance(data, pl.DataFrame):
        # Single dict - convert to 1-row DataFrame
        # Note: check for pl.DataFrame first since it's also dict-like in some contexts
        if not data:
            # Empty dict - create empty DataFrame
            df = pl.DataFrame()
        else:
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
        # Catch internal errors about data sources and wrap in user-friendly error
        error_str = str(e)
        data_errors = [
            "Unsupported format",
            "PolarsConnectorMaterializer",
            "Data file not found",
            "Unsupported data source URI",
            "Unsupported file format",
            "No data path specified",
        ]
        if any(err in error_str for err in data_errors):
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
    # Priority: DataFrame > handle (with db_params) > data path
    if isinstance(data, pl.DataFrame):
        data_source = data
    elif is_byoc:
        # Store the handle for BYOC
        data_source = engine._handle
    elif isinstance(data, str):
        # For URI strings, store the handle if available (has db_params for reconnection)
        # Otherwise fall back to the string
        if engine._handle is not None and engine._handle.db_params is not None:
            data_source = engine._handle
        else:
            data_source = engine._handle if engine._handle is not None else data
    else:
        # list[dict] or dict - store as DataFrame
        data_source = engine.df

    # Determine loaded data to expose via result.data
    # Priority: engine.df (loaded for Polars) > input DataFrame
    if engine.df is not None:
        loaded_data = engine.df
    elif isinstance(data, pl.DataFrame):
        loaded_data = data  # User passed DataFrame directly
    else:
        loaded_data = None  # Preplan/pushdown handled everything, no data loaded

    # Wrap in ValidationResult with data source and rules for sample_failures()
    return ValidationResult.from_engine_result(
        raw_result,
        data_source=data_source,
        rule_objects=engine._rules,
        sample=sample,
        sample_budget=sample_budget,
        sample_columns=sample_columns,
        severity_weights=cfg.severity_weights,
        data=loaded_data,
    )


def profile(
    data: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    preset: str = "scan",
    *,
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    save: bool = True,
    storage_options: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> DatasetProfile:
    """
    Profile a dataset.

    Args:
        data: DataFrame (Polars), list[dict], dict, or path/URI to data file
        preset: Profiling depth:
            - "scout": Quick recon (metadata only)
            - "scan": Systematic pass (full stats) [default]
            - "interrogate": Deep investigation (everything + percentiles)
        columns: Only profile these columns
        sample: Sample N rows (default: all)
        save: Save profile to history
        storage_options: Cloud storage credentials (S3, Azure, GCS).
            For S3/MinIO: aws_access_key_id, aws_secret_access_key, aws_region, endpoint_url
            For Azure: account_name, account_key, sas_token, etc.
            These override environment variables when provided.
        **kwargs: Additional arguments passed to ScoutProfiler

    Returns:
        DatasetProfile with column statistics

    Example:
        profile = kontra.profile("data.parquet")
        print(f"Rows: {profile.row_count}")
        for col in profile.columns:
            print(f"{col.name}: {col.dtype}")

        # Quick metadata-only profile
        profile = kontra.profile("big_data.parquet", preset="scout")

        # Deep profile with percentiles
        profile = kontra.profile("data.parquet", preset="interrogate")
    """
    import warnings
    import polars as pl
    from kontra.scout.profiler import ScoutProfiler, _DEPRECATED_PRESETS

    # Warn on deprecated preset names
    if preset in _DEPRECATED_PRESETS:
        new_name = _DEPRECATED_PRESETS[preset]
        warnings.warn(
            f"Preset '{preset}' is deprecated, use '{new_name}' instead",
            DeprecationWarning,
            stacklevel=2,
        )

    # Convert list/dict/pandas to Polars DataFrame
    if isinstance(data, list):
        if not data:
            data = pl.DataFrame()
        else:
            data = pl.DataFrame(data)
    elif isinstance(data, dict) and not isinstance(data, pl.DataFrame):
        if not data:
            data = pl.DataFrame()
        else:
            data = pl.DataFrame([data])
    elif hasattr(data, "__dataframe__"):
        # Pandas DataFrame (or any dataframe-protocol compatible)
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)

    if isinstance(data, pl.DataFrame):
        # Handle empty DataFrame (no columns) - DuckDB can't read parquet with no columns
        if data.width == 0:
            from datetime import datetime, timezone
            from kontra.version import VERSION
            return DatasetProfile(
                source_uri="<inline DataFrame>",
                source_format="dataframe",
                profiled_at=datetime.now(timezone.utc).isoformat(),
                engine_version=VERSION,
                row_count=data.height,
                column_count=0,
                columns=[],
            )

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
            profile = profiler.profile()
            # Replace temp file path with friendly name for DataFrame input
            profile.source_uri = f"<DataFrame: {data.height:,} rows, {data.width} cols>"
            return profile
        finally:
            os.unlink(temp_path)
    else:
        # Resolve named datasources (e.g., "prod_db.users" -> actual URI)
        resolved_data = data
        if isinstance(data, str):
            try:
                resolved_data = resolve_datasource(data)
            except ValueError:
                # Not a named datasource - use as-is (file path or URI)
                pass

        profiler = ScoutProfiler(
            resolved_data,
            preset=preset,
            columns=columns,
            sample_size=sample,
            storage_options=storage_options,
            **kwargs,
        )
        return profiler.profile()


def draft(
    profile: DatasetProfile,
    min_confidence: float = 0.5,
) -> Suggestions:
    """
    Draft validation rules from a profile.

    Analyzes the profile and suggests rules based on observed patterns.
    These are starting points - refine them based on domain knowledge.

    Args:
        profile: DatasetProfile from kontra.profile()
        min_confidence: Minimum confidence score (0.0-1.0)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()

    Example:
        profile = kontra.profile(df, preset="interrogate")
        suggestions = kontra.draft(profile)

        # Filter high confidence
        high_conf = suggestions.filter(min_confidence=0.9)

        # Save to file
        high_conf.save("contracts/users.yml")

        # Or use directly
        result = kontra.validate(df, rules=suggestions.to_dict())
    """
    return Suggestions.from_profile(profile, min_confidence=min_confidence)


def get_history(
    contract: str,
    *,
    limit: int = 20,
    since: Optional[str] = None,
    failed_only: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get validation history for a contract.

    Args:
        contract: Path to contract YAML file
        limit: Maximum number of runs to return (default: 20)
        since: Only return runs after this date/time. Formats:
            - "24h", "7d" - relative time
            - "2026-01-15" - specific date
        failed_only: Only return failed runs

    Returns:
        List of run summaries, newest first. Each summary contains:
        - run_id: Unique identifier
        - timestamp: When the run occurred (ISO format)
        - passed: Overall pass/fail
        - failed_count: Total failures
        - total_rows: Row count (if available)
        - contract_name: Name of the contract

    Example:
        history = kontra.get_history("contract.yml")
        for run in history:
            print(f"{run['timestamp']}: {'PASS' if run['passed'] else 'FAIL'}")

        # Last 7 days only
        recent = kontra.get_history("contract.yml", since="7d")

        # Only failed runs
        failures = kontra.get_history("contract.yml", failed_only=True)
    """
    from datetime import datetime, timedelta, timezone
    from kontra.config.loader import ContractLoader
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.state.backends import get_default_store

    # Validate that contract is a YAML file, not a data file (BUG-014)
    _validate_contract_path(contract, "get_history")

    # Load contract to get fingerprint
    contract_obj = ContractLoader.from_path(contract)
    fp = fingerprint_contract(contract_obj)

    # Parse since parameter
    since_dt = None
    if since:
        now = datetime.now(timezone.utc)
        since_lower = since.lower().strip()

        if since_lower.endswith("h"):
            hours = int(since_lower[:-1])
            since_dt = now - timedelta(hours=hours)
        elif since_lower.endswith("d"):
            days = int(since_lower[:-1])
            since_dt = now - timedelta(days=days)
        else:
            # Try parsing as date
            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"Invalid since format: {since}. Use '24h', '7d', or 'YYYY-MM-DD'")

    # Get history from store
    store = get_default_store()
    if store is None:
        return []

    summaries = store.get_run_summaries(
        contract_fingerprint=fp,
        limit=limit,
        since=since_dt,
        failed_only=failed_only,
    )

    return [s.to_dict() for s in summaries]


# =============================================================================
# Deprecated Aliases (for backward compatibility)
# =============================================================================


def scout(
    data: Union[str, "pl.DataFrame"],
    preset: str = "standard",
    *,
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    save: bool = True,
    **kwargs,
) -> DatasetProfile:
    """
    DEPRECATED: Use kontra.profile() instead.

    Profile a dataset.
    """
    import warnings
    warnings.warn(
        "kontra.scout() is deprecated, use kontra.profile() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return profile(data, preset=preset, columns=columns, sample=sample, save=save, **kwargs)


def suggest_rules(
    data: Union[str, DatasetProfile, "pl.DataFrame"],
    min_confidence: float = 0.5,
) -> Suggestions:
    """
    DEPRECATED: Use kontra.profile() then kontra.draft() instead.

    Generate validation rule suggestions from data or a profile.

    Args:
        data: File path, DataFrame, or DatasetProfile
        min_confidence: Minimum confidence score (0.0-1.0)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()
    """
    import warnings
    import polars as pl
    warnings.warn(
        "kontra.suggest_rules() is deprecated, use kontra.profile() then kontra.draft() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    # Handle different input types
    if isinstance(data, DatasetProfile):
        prof = data
    elif isinstance(data, (str, pl.DataFrame)):
        prof = profile(data, preset="scan")
    else:
        raise TypeError(
            f"suggest_rules() expects str, DataFrame, or DatasetProfile, got {type(data).__name__}"
        )
    return draft(prof, min_confidence=min_confidence)


def explain(
    data: Union[str, "pl.DataFrame"],
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
    from kontra.rule_defs.factory import RuleFactory
    from kontra.rule_defs.execution_plan import RuleExecutionPlan

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
    from kontra.errors import StateCorruptedError

    store = get_default_store()
    if store is None:
        return None

    # Validate that contract is a YAML file, not a data file (BUG-014)
    if os.path.isfile(contract):
        _validate_contract_path(contract, "diff")

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

    except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
        # These indicate corrupted state data
        raise StateCorruptedError(contract, str(e))
    except FileNotFoundError:
        # No history available - this is normal
        return None
    except Exception as e:
        # For other exceptions, log and re-raise as state corruption
        # since we've already handled the "no history" case
        log_exception(_logger, "Failed to compute diff", e)
        raise StateCorruptedError(contract, str(e))


def profile_diff(
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
        diff = kontra.profile_diff("data.parquet", since="7d")
        if diff and diff.has_schema_changes:
            print("Schema changed!")
            for col in diff.columns_added:
                print(f"  NEW: {col}")
    """
    # TODO: Implement profile history lookup
    return None


def scout_diff(
    source: str,
    *,
    since: Optional[str] = None,
) -> Optional[ProfileDiff]:
    """
    DEPRECATED: Use kontra.profile_diff() instead.

    Compare profile runs over time.
    """
    import warnings
    warnings.warn(
        "kontra.scout_diff() is deprecated, use kontra.profile_diff() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return profile_diff(source, since=since)


# =============================================================================
# History Functions
# =============================================================================


def _resolve_contract_fingerprint(contract: str, store: Any, caller: str = "state function") -> Optional[str]:
    """
    Resolve a contract name or path to its fingerprint.

    Args:
        contract: Contract name or file path
        store: State store instance
        caller: Name of the calling function (for error messages)

    Returns:
        Contract fingerprint or None if not found
    """
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    # If it's a file path, load contract and compute semantic fingerprint
    if os.path.isfile(contract):
        # Validate that it's not a data file (BUG-014)
        _validate_contract_path(contract, caller)
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
        contract_fp = _resolve_contract_fingerprint(contract, store, "list_runs")
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
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_run")
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
            total_rows=state.summary.row_count or 0,
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
        contract_fp = _resolve_contract_fingerprint(contract, store, "has_runs")
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
# Annotation Functions
# =============================================================================


def annotate(
    contract: str,
    *,
    run_id: Optional[str] = None,
    rule_id: Optional[str] = None,
    actor_type: str = "agent",
    actor_id: str,
    annotation_type: str,
    summary: str,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Save an annotation on a validation run or specific rule.

    Annotations provide "memory without authority" - agents and humans can
    record context about runs (resolutions, root causes, acknowledgments)
    without affecting Kontra's validation behavior.

    Invariants:
    - Append-only: annotations are never updated or deleted
    - Uninterpreted: Kontra stores annotation_type but doesn't define vocabulary
    - Never read during validation or diff

    Args:
        contract: Contract name or path
        run_id: Run ID to annotate (default: latest run).
            For file-based backends: string like "2024-01-15T09-30-00_abc123"
            For database backends: integer ID as string
        rule_id: Optional rule ID to annotate a specific rule
        actor_type: Who is creating the annotation ("agent" | "human" | "system")
        actor_id: Identifier for the actor (e.g., "repair-agent-v2", "alice@example.com")
        annotation_type: Type of annotation (e.g., "resolution", "root_cause", "acknowledged")
        summary: Human-readable summary
        payload: Optional structured data (dict)

    Returns:
        Annotation ID (integer)

    Raises:
        ValueError: If contract or run not found, or rule_id not found in run
        RuntimeError: If annotation save fails

    Common annotation_type values (suggested, not enforced):
    - "resolution": I fixed this
    - "root_cause": This failed because...
    - "false_positive": This isn't actually a problem
    - "acknowledged": I saw this, will address later
    - "suppressed": Intentionally ignoring this
    - "note": General comment

    Example:
        # Annotate the latest run for a contract
        kontra.annotate(
            "users_contract.yml",
            actor_type="agent",
            actor_id="repair-agent-v2",
            annotation_type="resolution",
            summary="Fixed null emails by backfilling from user_profiles table",
        )

        # Annotate a specific rule
        kontra.annotate(
            "users_contract.yml",
            rule_id="COL:email:not_null",
            actor_type="human",
            actor_id="alice@example.com",
            annotation_type="false_positive",
            summary="These are service accounts, nulls are expected",
        )

        # Annotate with structured payload
        kontra.annotate(
            "users_contract.yml",
            actor_type="agent",
            actor_id="analysis-agent",
            annotation_type="root_cause",
            summary="Upstream data source failed validation",
            payload={
                "upstream_source": "crm_export",
                "failure_time": "2024-01-15T08:30:00Z",
                "affected_rows": 1523,
            },
        )
    """
    from kontra.state.backends import get_default_store
    from kontra.state.types import Annotation
    from kontra.state.fingerprint import fingerprint_contract
    from kontra.config.loader import ContractLoader

    store = get_default_store()
    if store is None:
        raise RuntimeError("State store not available")

    # Resolve contract to fingerprint
    contract_fp = _resolve_contract_fingerprint(contract, store, "annotate")
    if contract_fp is None:
        raise ValueError(f"Contract not found: {contract}")

    # Get the run state
    if run_id is None:
        # Get latest run
        state = store.get_latest(contract_fp)
        if state is None:
            raise ValueError(f"No runs found for contract: {contract}")
    else:
        # Find specific run
        states = store.get_history(contract_fp, limit=100)
        state = None

        # Try to match run_id as integer (database backends) or string timestamp
        for s in states:
            # Check run_at timestamp match
            if s.run_at.isoformat() == run_id:
                state = s
                break
            # Check ID match (for database backends)
            if s.id is not None and str(s.id) == run_id:
                state = s
                break

        if state is None:
            raise ValueError(f"Run not found: {run_id}")

    # If annotating a specific rule, find the rule_result_id
    rule_result_id = None
    if rule_id is not None:
        found = False
        for rule in state.rules:
            if rule.rule_id == rule_id:
                found = True
                rule_result_id = rule.id  # May be None for file backends
                break

        if not found:
            raise ValueError(f"Rule not found in run: {rule_id}")

    # Create the annotation
    annotation = Annotation(
        run_id=state.id or 0,
        rule_result_id=rule_result_id,
        rule_id=rule_id,  # Store semantic rule ID for cross-run queries
        actor_type=actor_type,
        actor_id=actor_id,
        annotation_type=annotation_type,
        summary=summary,
        payload=payload,
    )

    # Save annotation - method depends on backend type
    try:
        # For database backends, save_annotation works directly
        if hasattr(store, "save_annotation") and not isinstance(store, type):
            try:
                return store.save_annotation(annotation)
            except NotImplementedError:
                pass

        # For file-based backends, need to find the run_id string
        if hasattr(store, "save_annotation_for_run"):
            # Find the run_id string by scanning the runs directory
            run_id_str = _find_run_id_string(store, contract_fp, state)
            if run_id_str is None:
                raise RuntimeError("Could not find run file for annotation")
            return store.save_annotation_for_run(contract_fp, run_id_str, annotation)

        raise RuntimeError("Backend does not support annotations")

    except Exception as e:
        raise RuntimeError(f"Failed to save annotation: {e}") from e


def _find_run_id_string(store: Any, contract_fp: str, state: Any) -> Optional[str]:
    """
    Find the run_id string for a state in file-based backends.

    This is needed because file-based backends use string run IDs but
    ValidationState.id is an integer hash.
    """
    from pathlib import Path

    # LocalStore
    if hasattr(store, "_runs_dir"):
        runs_dir = store._runs_dir(contract_fp)
        if runs_dir.exists():
            for filepath in runs_dir.glob("*.json"):
                if filepath.name.endswith(".ann.jsonl"):
                    continue
                loaded = store._load_state(filepath)
                if loaded and loaded.id == state.id:
                    return filepath.stem
        return None

    # S3Store - similar pattern but via fsspec
    if hasattr(store, "_runs_prefix") and hasattr(store, "_get_fs"):
        fs = store._get_fs()
        prefix = store._runs_prefix(contract_fp)
        try:
            all_files = fs.glob(f"s3://{prefix}/*.json")
            files = [f for f in all_files if not f.endswith(".ann.jsonl")]
            for filepath in files:
                loaded = store._load_state(filepath)
                if loaded and loaded.id == state.id:
                    return filepath.rsplit("/", 1)[-1].replace(".json", "")
        except (OSError, IOError, ValueError):
            # S3 access failed - can't look up run ID
            pass
        return None

    return None


def get_run_with_annotations(
    contract: str,
    run_id: Optional[str] = None,
) -> Optional[ValidationResult]:
    """
    Get a validation run with its annotations loaded.

    By default, annotations are not loaded (they're opt-in for performance).
    Use this function when you need to see annotations.

    Args:
        contract: Contract name or path
        run_id: Run ID (default: latest run)

    Returns:
        ValidationResult with annotations, or None if not found

    Example:
        result = kontra.get_run_with_annotations("users_contract.yml")
        if result:
            for rule in result.rules:
                print(f"{rule.rule_id}: {rule.annotations}")
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return None

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_run_with_annotations")
        if contract_fp is None:
            return None

        # Convert run_id string to integer if needed
        run_id_int = None
        if run_id is not None:
            try:
                run_id_int = int(run_id)
            except ValueError:
                # It's a timestamp or string ID - need to find the matching state
                states = store.get_history(contract_fp, limit=100)
                for s in states:
                    if s.run_at.isoformat() == run_id:
                        run_id_int = s.id
                        break

        state = store.get_run_with_annotations(contract_fp, run_id_int)
        if state is None:
            return None

        # Convert to ValidationResult
        return ValidationResult(
            passed=state.summary.passed,
            dataset=state.dataset_uri,
            total_rows=state.summary.row_count or 0,
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
                    annotations=[a.to_dict() for a in r.annotations] if r.annotations else None,
                )
                for r in state.rules
            ],
            annotations=[a.to_dict() for a in state.annotations] if state.annotations else None,
        )
    except Exception as e:
        log_exception(_logger, "Failed to get run with annotations", e)
        return None


def get_annotations(
    contract: str,
    *,
    rule_id: Optional[str] = None,
    annotation_type: Optional[str] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Retrieve annotations across runs for a contract.

    Primary use case: Agent sees a failure, wants to check if past runs
    have hints about this rule. This provides cross-session memory.

    Args:
        contract: Contract name or path
        rule_id: Filter to annotations on this rule (recommended)
        annotation_type: Filter by type (e.g., "resolution", "false_positive")
        limit: Max annotations to return (default 20)

    Returns:
        List of annotation dicts, most recent first. Each dict contains:
        - id: Annotation ID
        - run_id: Which run this was attached to
        - rule_id: Semantic rule ID (e.g., "COL:email:not_null") or None for run-level
        - actor_type: "agent" | "human" | "system"
        - actor_id: Who created it
        - annotation_type: Type (e.g., "resolution", "root_cause")
        - summary: Human-readable summary
        - payload: Optional structured data
        - created_at: When it was created

    Example:
        # Agent sees COL:email:not_null failing, checks for past hints
        hints = kontra.get_annotations(
            "users_contract.yml",
            rule_id="COL:email:not_null",
        )

        for hint in hints:
            print(f"[{hint['annotation_type']}] {hint['summary']}")

        # Get only resolutions
        resolutions = kontra.get_annotations(
            "users_contract.yml",
            rule_id="COL:email:not_null",
            annotation_type="resolution",
        )
    """
    from kontra.state.backends import get_default_store

    store = get_default_store()
    if store is None:
        return []

    try:
        contract_fp = _resolve_contract_fingerprint(contract, store, "get_annotations")
        if contract_fp is None:
            return []

        annotations = store.get_annotations_for_contract(
            contract_fp,
            rule_id=rule_id,
            annotation_type=annotation_type,
            limit=limit,
        )

        return [a.to_dict() for a in annotations]
    except Exception as e:
        log_exception(_logger, "Failed to get annotations", e)
        return []


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
    from kontra.rule_defs.registry import RULE_REGISTRY

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
        "disallowed_values": {
            "description": "Fails where column contains values that ARE in the disallowed list",
            "params": {"column": "required", "values": "required (list)"},
            "scope": "column",
        },
        "range": {
            "description": "Fails where column values are outside [min, max] range",
            "params": {"column": "required", "min": "optional", "max": "optional"},
            "scope": "column",
        },
        "length": {
            "description": "Fails where string length is outside [min, max] bounds",
            "params": {"column": "required", "min": "optional", "max": "optional"},
            "scope": "column",
        },
        "regex": {
            "description": "Fails where column values don't match regex pattern",
            "params": {"column": "required", "pattern": "required"},
            "scope": "column",
        },
        "contains": {
            "description": "Fails where column values don't contain the substring",
            "params": {"column": "required", "substring": "required"},
            "scope": "column",
        },
        "starts_with": {
            "description": "Fails where column values don't start with the prefix",
            "params": {"column": "required", "prefix": "required"},
            "scope": "column",
        },
        "ends_with": {
            "description": "Fails where column values don't end with the suffix",
            "params": {"column": "required", "suffix": "required"},
            "scope": "column",
        },
        "dtype": {
            "description": "Fails if column data type doesn't match expected type",
            "params": {"column": "required", "type": "required"},
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
        "conditional_range": {
            "description": "Fails where column is outside range when a condition is met",
            "params": {
                "column": "required (column to check)",
                "when": "required (e.g., \"customer_type == 'premium'\")",
                "min": "optional (minimum value, inclusive)",
                "max": "optional (maximum value, inclusive)",
            },
            "scope": "cross-column",
        },
    }

    # Use RULE_METADATA as source of truth (avoids triggering heavy imports)
    # All 18 built-in rules are documented in RULE_METADATA
    result = []
    for name in sorted(RULE_METADATA.keys()):
        meta = RULE_METADATA[name]
        info = {
            "name": name,
            "description": meta.get("description", ""),
            "params": meta.get("params", {}),
            "scope": meta.get("scope", "unknown"),
        }
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
    from kontra.rule_defs.registry import RULE_REGISTRY
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
    "profile",
    "draft",
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
    # Probe result types
    "CompareResult",
    "RelationshipProfile",
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
