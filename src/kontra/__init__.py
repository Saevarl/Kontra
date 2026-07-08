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
    try:
        module_name, attr = _LAZY_ATTRS[name]
    except KeyError:
        raise AttributeError(f"module 'kontra' has no attribute '{name}'") from None
    import importlib

    module = importlib.import_module(module_name)
    value = module if attr is None else getattr(module, attr)
    globals()[name] = value  # cache so later access bypasses __getattr__
    return value


def _is_pandas_dataframe(obj: Any) -> bool:
    """Check if object is a pandas DataFrame without importing pandas."""
    return (
        hasattr(obj, "__dataframe__")
        and type(obj).__module__.startswith("pandas")
        and type(obj).__name__ == "DataFrame"
    )


def _is_polars_dataframe(obj: Any) -> bool:
    """Check if object is a polars DataFrame without importing polars.

    If polars was never imported, obj cannot be a polars DataFrame — this
    keeps string-path validations from paying the ~115ms polars import.
    """
    import sys

    pl_mod = sys.modules.get("polars")
    return pl_mod is not None and isinstance(obj, pl_mod.DataFrame)


def _normalize_to_dataframe(data: Any) -> Any:
    """
    Convert list[dict], dict, or pandas DataFrame to Polars DataFrame.

    Returns the original data unchanged if it's not one of these types.
    """
    import polars as pl

    if isinstance(data, list):
        if not data:
            return pl.DataFrame()
        # Only list-of-dicts is valid per docs (BUG-058)
        if not isinstance(data[0], dict):
            from kontra.errors import InvalidDataError
            raise InvalidDataError(
                f"List data must be a list of dicts, got list of {type(data[0]).__name__}. "
                f"Example: [{{'col': 1}}, {{'col': 2}}]"
            )
        return pl.DataFrame(data)
    if isinstance(data, dict) and not isinstance(data, pl.DataFrame):
        if not data:
            return pl.DataFrame()
        first_val = next(iter(data.values()))
        is_columnar = isinstance(first_val, (list, tuple))
        return pl.DataFrame(data) if is_columnar else pl.DataFrame([data])
    if _is_pandas_dataframe(data):
        return pl.from_pandas(data)
    return data


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
    ExplainResult,
    RuleExplainEntry,
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

# Configuration symbols (KontraConfig, resolve_datasource, ...) are lazy via
# __getattr__: config.settings pulls in pydantic, which costs ~80ms at import.
#
# The subpackage import below must stay: loading `kontra.config` here lets the
# import machinery bind the `config` attribute NOW, so that `def config(...)`
# further down permanently overwrites it (pitfall #14: submodule/attribute
# clash). Without it, the first lazy import of kontra.config.* would clobber
# the config() function with the subpackage module. The package __init__ is
# lazy, so this costs nothing.
import kontra.config  # noqa: F401

if TYPE_CHECKING:
    from kontra.config.settings import KontraConfig


# =============================================================================
# Core Functions
# =============================================================================


def validate(
    data: Union[str, "pl.DataFrame", "pd.DataFrame", List[Dict[str, Any]], Dict[str, Any], Any, None] = None,
    contract: Optional[str] = None,
    *,
    table: Optional[str] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
    emit_report: bool = False,
    save: bool = True,
    preplan: Optional[str] = "on",
    pushdown: Optional[str] = "on",
    tally: Optional[bool] = None,
    projection: bool = True,
    csv_mode: Optional[str] = "auto",
    env: Optional[str] = None,
    stats: str = "none",
    dry_run: bool = False,
    sample: int = 0,
    sample_budget: int = 50,
    sample_columns: Optional[Union[List[str], str]] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    only: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    explain: bool = False,
    **kwargs,
) -> Union["ValidationResult", "DryRunResult", "ExplainResult"]:
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
        preplan: "on" | "off" - Use metadata for fast validation (default: on)
        pushdown: "on" | "off" - Push validation to SQL engine (default: on)
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
        only: Filter to rules matching these names or rule IDs.
            Example: ["not_null", "unique"] or ["COL:email:not_null"]
        columns: Filter to rules that touch any of these columns.
            Dataset-level rules (min_rows, max_rows) are always included.
            Example: ["email", "user_id"]
        explain: If True, return ExplainResult showing which tier each rule
            would execute on, without running validation.
        **kwargs: Additional arguments passed to ValidationEngine

    Returns:
        ValidationResult with .passed, .rules, .to_llm(), etc.
        DryRunResult if dry_run=True, with .valid, .rules_count, .columns_needed
        ExplainResult if explain=True, with .rules, .summary, .to_llm()

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
    # Phase dispatch. Side-effect order is load-bearing: arg normalization ->
    # dry_run/explain early exits -> data resolution -> config resolution ->
    # engine construction -> run -> result assembly.
    data, contract = _normalize_and_check_args(
        data=data, contract=contract, rules=rules,
        tally=tally, only=only, columns=columns,
    )

    if dry_run:
        return _dry_run(contract, rules)

    if explain:
        return _explain_plan(
            data=data,
            contract=contract,
            rules=rules,
            preplan=preplan,
            pushdown=pushdown,
            tally=tally,
            storage_options=storage_options,
            only=only,
            columns=columns,
        )

    # Resolve data from the contract's datasource, then validate the argument.
    data = _resolve_data_from_contract(data=data, contract=contract)
    is_byoc = _check_data_and_detect_byoc(data=data, table=table)

    # Resolve config (always, for severity_weights and other settings)
    from kontra.config.settings import resolve_effective_config

    cfg = resolve_effective_config(env_name=env)

    # Detect execution path early (enables lazy loading optimization in engine)
    from kontra.engine.paths import detect_execution_path

    execution_path = detect_execution_path(data, table=table)

    engine_kwargs = _build_engine_kwargs(
        contract=contract,
        emit_report=emit_report,
        save=save,
        preplan=preplan,
        pushdown=pushdown,
        tally=tally,
        projection=projection,
        csv_mode=csv_mode,
        stats=stats,
        rules=rules,
        storage_options=storage_options,
        execution_path=execution_path,
        only=only,
        columns=columns,
        extra_kwargs=kwargs,
    )

    engine = _construct_engine(
        data=data, table=table, is_byoc=is_byoc, engine_kwargs=engine_kwargs,
    )

    raw_result = _run_engine(engine=engine, data=data)

    return _assemble_result(
        engine=engine,
        data=data,
        raw_result=raw_result,
        is_byoc=is_byoc,
        sample=sample,
        sample_budget=sample_budget,
        sample_columns=sample_columns,
        severity_weights=cfg.severity_weights,
        tally=tally,
    )


def _normalize_and_check_args(
    *,
    data: Any,
    contract: Optional[str],
    rules: Optional[List[Dict[str, Any]]],
    tally: Optional[bool],
    only: Optional[List[str]],
    columns: Optional[List[str]],
) -> tuple:
    """Auto-detect a YAML `data` arg as the contract, then type/presence-check call args.

    Returns the possibly-rewritten (data, contract). Raises early with clear
    errors so bad arguments never reach the engine.
    """
    # Auto-detect: if data looks like a YAML contract file and contract is None, use it as contract
    if isinstance(data, str) and (data.endswith('.yaml') or data.endswith('.yml')) and contract is None:
        contract = data
        data = None  # Will be extracted from contract's datasource field

    # Validate inputs
    if contract is None and rules is None:
        raise ValueError("Either contract or rules must be provided")

    # A data file passed as the contract would be read as YAML and raise an
    # opaque UnicodeDecodeError deep in the loader. Fail early and clearly.
    if isinstance(contract, str):
        _validate_contract_path(contract, "validate")

    # Type-check tally — string 'no'/'false' are truthy in Python (BUG F-017)
    if tally is not None and not isinstance(tally, bool):
        raise TypeError(
            f"'tally' must be a bool or None, got {type(tally).__name__}: {tally!r}. "
            f"Use tally=True or tally=False"
        )

    # Type-check only and columns — strings iterate chars silently (BUG-026)
    if isinstance(only, str):
        raise TypeError(
            f"'only' must be a list of strings, not a string. "
            f"Use only=[{only!r}] instead of only={only!r}"
        )
    if isinstance(columns, str):
        raise TypeError(
            f"'columns' must be a list of strings, not a string. "
            f"Use columns=[{columns!r}] instead of columns={columns!r}"
        )

    return data, contract


def _resolve_data_from_contract(*, data: Any, contract: Optional[str]) -> Any:
    """If no data was given, pull the datasource from the contract YAML."""
    # If data is None but contract is provided, try to extract datasource from contract
    if data is None and contract is not None:
        from kontra.config.loader import ContractLoader
        try:
            contract_obj = ContractLoader.from_path(contract)
            # "inline" is the default when no datasource specified
            if contract_obj.datasource and contract_obj.datasource != "inline":
                data = contract_obj.datasource
            else:
                raise ValueError(
                    f"Contract '{contract}' has no 'datasource:' field.\n"
                    "Either add 'datasource: path/to/file.parquet' to your contract YAML,\n"
                    "or provide data explicitly: kontra.validate(data, contract='...')"
                )
        except ContractNotFoundError:
            raise  # Re-raise contract not found
        except ValueError:
            raise  # Re-raise our custom error

    return data


def _check_data_and_detect_byoc(*, data: Any, table: Optional[str]) -> bool:
    """Reject None/cursor data and detect the BYOC (connection + table) pattern."""
    from kontra.errors import InvalidDataError
    from kontra.connectors.detection import is_database_connection, is_cursor_object

    # Check for None
    if data is None:
        raise InvalidDataError("NoneType", detail="Data cannot be None. Provide a file path, DataFrame, or datasource name.")

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

    return is_byoc


def _build_engine_kwargs(
    *,
    contract: Optional[str],
    emit_report: bool,
    save: bool,
    preplan: Optional[str],
    pushdown: Optional[str],
    tally: Optional[bool],
    projection: bool,
    csv_mode: Optional[str],
    stats: str,
    rules: Optional[List[Dict[str, Any]]],
    storage_options: Optional[Dict[str, Any]],
    execution_path: Any,
    only: Optional[List[str]],
    columns: Optional[List[str]],
    extra_kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    """Coerce None toggles to defaults and assemble the ValidationEngine kwargs."""
    # Coerce None toggles to defaults (BUG-038)
    if preplan is None:
        preplan = "on"
    if pushdown is None:
        pushdown = "on"
    if csv_mode is None:
        csv_mode = "auto"

    return {
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
        "only_rules": only,
        "only_columns": columns,
        **extra_kwargs,
    }


def _construct_engine(
    *,
    data: Any,
    table: Optional[str],
    is_byoc: bool,
    engine_kwargs: Dict[str, Any],
) -> "ValidationEngine":
    """Dispatch on the input data type to build the appropriate ValidationEngine."""
    from kontra.errors import InvalidDataError, InvalidPathError
    # Lazy import heavy dependencies (only loaded when validate() is called)
    from kontra.engine.engine import ValidationEngine

    # Normalize and create engine
    if is_byoc:
        # BYOC: database connection + table
        from kontra.connectors.handle import DatasetHandle

        handle = DatasetHandle.from_connection(data, table)
        return ValidationEngine(handle=handle, **engine_kwargs)
    elif isinstance(data, str):
        # File path/URI or datasource name
        # Validate: check if it's a directory (common mistake)
        if os.path.isdir(data):
            raise InvalidPathError(data, "Path is a directory, not a file")
        return ValidationEngine(data_path=data, **engine_kwargs)
    elif isinstance(data, (list, dict)) or _is_pandas_dataframe(data):
        df = _normalize_to_dataframe(data)
        return ValidationEngine(dataframe=df, **engine_kwargs)
    elif _is_polars_dataframe(data):
        return ValidationEngine(dataframe=data, **engine_kwargs)
    else:
        # Invalid data type
        raise InvalidDataError(type(data).__name__)


def _run_engine(*, engine: "ValidationEngine", data: Any) -> Any:
    """Run the engine, rewrapping internal data-source OSErrors as InvalidDataError."""
    from kontra.errors import InvalidDataError

    # Run validation
    try:
        return engine.run()
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


def _assemble_result(
    *,
    engine: "ValidationEngine",
    data: Any,
    raw_result: Any,
    is_byoc: bool,
    sample: int,
    sample_budget: int,
    sample_columns: Optional[Union[List[str], str]],
    severity_weights: Any,
    tally: Optional[bool],
) -> "ValidationResult":
    """Resolve sample data source + loaded frame, then build the ValidationResult."""
    # Data source for sample_failures(): DataFrame > handle (has db_params
    # for reconnection) > raw path string > loaded frame (list/dict input)
    if _is_polars_dataframe(data):
        data_source = data
    elif is_byoc:
        data_source = engine._handle
    elif isinstance(data, str):
        data_source = engine._handle if engine._handle is not None else data
    else:
        data_source = engine.df

    # Loaded data exposed via result.data: engine.df when Polars ran; the
    # input frame when passed directly; None when preplan/pushdown handled all
    if engine.df is not None:
        loaded_data = engine.df
    elif _is_polars_dataframe(data):
        loaded_data = data
    else:
        loaded_data = None

    return ValidationResult.from_engine_result(
        raw_result,
        data_source=data_source,
        rule_objects=engine._rules,
        sample=sample,
        sample_budget=sample_budget,
        sample_columns=sample_columns,
        severity_weights=severity_weights,
        data=loaded_data,
        tally=tally,
    )


def _dry_run(contract: Optional[str], rules: Optional[List[Dict[str, Any]]]) -> "DryRunResult":
    """Check contract/rule syntax without touching data (validate(dry_run=True))."""
    from kontra.config.loader import ContractLoader
    from kontra.rule_defs.factory import RuleFactory
    from kontra.rule_defs.execution_plan import RuleExecutionPlan
    from kontra.engine.phases.compilation import _ensure_builtin_rules_registered

    _ensure_builtin_rules_registered()

    errors: List[str] = []
    contract_name: Optional[str] = None
    datasource: Optional[str] = None
    all_rule_specs: List[Any] = []

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
        except Exception as e:  # any load failure is a *finding*, not a crash
            errors.append(f"Contract error: {e}")

    inline_built_rules = []  # Already-built BaseRule instances
    if rules is not None:
        from kontra.config.models import RuleSpec
        from kontra.rule_defs.base import BaseRule as BaseRuleType
        for i, r in enumerate(rules):
            try:
                if isinstance(r, BaseRuleType):
                    inline_built_rules.append(r)
                elif isinstance(r, dict):
                    all_rule_specs.append(RuleSpec(
                        name=r.get("name", ""),
                        id=r.get("id"),
                        params=r.get("params", {}),
                        severity=r.get("severity", "blocking"),
                        context=r.get("context", {}),
                    ))
                else:
                    errors.append(
                        f"Inline rule {i}: expected dict or BaseRule, "
                        f"got {type(r).__name__}"
                    )
            except Exception as e:  # bad rule spec is a finding, not a crash
                errors.append(f"Inline rule {i} error: {e}")

    columns_needed: List[str] = []
    rules_count = 0
    if not errors and (all_rule_specs or inline_built_rules):
        try:
            built_rules = RuleFactory(all_rule_specs).build_rules() if all_rule_specs else []
            built_rules = list(built_rules) + inline_built_rules
            rules_count = len(built_rules)
            compiled = RuleExecutionPlan(built_rules).compile()
            columns_needed = list(compiled.required_cols or [])
        except Exception as e:  # rule construction failure is a finding
            errors.append(f"Rule build error: {e}")

    return DryRunResult(
        valid=len(errors) == 0,
        rules_count=rules_count,
        columns_needed=columns_needed,
        contract_name=contract_name,
        datasource=datasource,
        errors=errors,
    )


def _explain_plan(
    *,
    data: Any,
    contract: Optional[str],
    rules: Optional[List[Dict[str, Any]]],
    preplan: Optional[str],
    pushdown: Optional[str],
    tally: Optional[bool],
    storage_options: Optional[Dict[str, Any]],
    only: Optional[List[str]],
    columns: Optional[List[str]],
) -> "ExplainResult":
    """Show which tier each rule would run on (validate(explain=True))."""
    from kontra.engine.engine import ValidationEngine

    # Resolve data source (needed for tier detection)
    explain_data_path = None
    if isinstance(data, str):
        explain_data_path = data
    elif data is None and contract is not None:
        from kontra.config.loader import ContractLoader
        try:
            contract_obj = ContractLoader.from_path(contract)
            if contract_obj.datasource and contract_obj.datasource != "inline":
                explain_data_path = contract_obj.datasource
        except (ContractNotFoundError, ValueError):
            pass  # Will be caught by engine

    engine_kwargs = {
        "contract_path": contract,
        "data_path": explain_data_path,
        "emit_report": False,
        "save_state": False,
        "preplan": preplan,
        "pushdown": pushdown,
        "tally": tally,
        "inline_rules": rules,
        "storage_options": storage_options,
        "only_rules": only,
        "only_columns": columns,
    }

    if data is not None and not isinstance(data, str):
        import polars as pl
        if isinstance(data, (list, dict)) or _is_pandas_dataframe(data):
            engine_kwargs["dataframe"] = _normalize_to_dataframe(data)
        elif isinstance(data, pl.DataFrame):
            engine_kwargs["dataframe"] = data

    return ValidationEngine(**engine_kwargs).explain()


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
    from kontra.errors import InvalidDataError

    # Input validation — match validate() behavior (BUG-059)
    if data is None:
        raise InvalidDataError(
            "NoneType",
            detail="profile() requires data. Pass a file path, DataFrame, or named datasource.",
        )
    if isinstance(data, bool) or (isinstance(data, (int, float)) and not isinstance(data, bool)):
        raise InvalidDataError(
            type(data).__name__,
            detail=f"profile() received {type(data).__name__}, expected file path, DataFrame, or list of dicts.",
        )

    # Warn on deprecated preset names
    if preset in _DEPRECATED_PRESETS:
        new_name = _DEPRECATED_PRESETS[preset]
        warnings.warn(
            f"Preset '{preset}' is deprecated, use '{new_name}' instead",
            DeprecationWarning,
            stacklevel=2,
        )

    # Convert list/dict/pandas to Polars DataFrame
    data = _normalize_to_dataframe(data)

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
            from kontra.config.settings import resolve_datasource

            try:
                resolved_data = resolve_datasource(data)
            except ValueError:
                # Not a named datasource - use as-is (file path or URI).
                # DatasourceTableError (datasource found, table missing) inherits
                # from KontraError, not ValueError, so it propagates correctly.
                pass

        try:
            profiler = ScoutProfiler(
                resolved_data,
                preset=preset,
                columns=columns,
                sample_size=sample,
                storage_options=storage_options,
                **kwargs,
            )
            return profiler.profile()
        except FileNotFoundError as e:
            raise InvalidDataError(
                "file",
                detail=f"File not found: {resolved_data}",
            ) from e
        except Exception as e:
            # Only a bare LOCAL PATH gets the friendly "file not found". Any
            # URI source (s3://, az://, http://, postgres://, clickhouse://, …)
            # must surface its REAL error instead — an Azure blob-listing
            # failure, a credential/permission error, a glob-expansion failure,
            # or a DB "relation does not exist" often contains "not found" /
            # "does not exist" in its text, and flattening that to "file not
            # found" hides the actual cause (the reported bug: an az:// glob
            # error misdiagnosed as a missing file).
            src = resolved_data if isinstance(resolved_data, str) else ""
            is_uri = "://" in src
            err_str = str(e).lower()
            is_not_found = (
                "no such file" in err_str
                or "does not exist" in err_str
                or "no files found" in err_str
            )
            if is_not_found and not is_uri:
                raise InvalidDataError(
                    "file",
                    detail=f"File not found: {resolved_data}",
                ) from e
            # Propagate the real error (all URI/engine failures included).
            raise


def draft(
    data: Any,
    min_confidence: float = 0.5,
    *,
    preset: str = "interrogate",
    storage_options: Optional[Dict[str, Any]] = None,
) -> Suggestions:
    """
    Draft validation rules from data or a profile.

    Analyzes the data and suggests rules based on observed patterns.
    These are starting points - refine them based on domain knowledge.

    Args:
        data: DatasetProfile, DataFrame, file path, or any data source.
              If not a DatasetProfile, will profile the data first using
              the specified preset.
        min_confidence: Minimum confidence score (0.0-1.0)
        preset: Profile preset when profiling data ("scout", "scan", "interrogate").
                Defaults to "interrogate" for thorough rule suggestions.
        storage_options: Cloud storage credentials (for S3/Azure paths)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()

    Example:
        # From file path (profiles internally)
        suggestions = kontra.draft("data/users.parquet")

        # From DataFrame
        suggestions = kontra.draft(df)

        # From profile (explicit)
        profile = kontra.profile(df, preset="interrogate")
        suggestions = kontra.draft(profile)

        # Filter high confidence
        high_conf = suggestions.filter(min_confidence=0.9)

        # Save to file
        high_conf.save("contracts/users.yml")

        # Or use directly
        result = kontra.validate(df, rules=suggestions.to_rules_list())
    """
    # If already a DatasetProfile, use directly
    if isinstance(data, DatasetProfile):
        return Suggestions.from_profile(data, min_confidence=min_confidence)

    # Otherwise, profile the data first
    data_profile = profile(data, preset=preset, storage_options=storage_options)
    return Suggestions.from_profile(data_profile, min_confidence=min_confidence)


def compare_profiles(
    a: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    b: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    *,
    preset: str = "scan",
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    a_label: Optional[str] = None,
    b_label: Optional[str] = None,
) -> ProfileDiff:
    """
    Profile two sources and diff them, aligned by column, in one call.

    The two-source "bisect" verb: point it at two pipeline stages (a file, a
    table, a DataFrame, a database URI, a named datasource — any mix) and get a
    structured, column-aligned delta — row-count change, columns added/removed,
    and per-column dtype / null-rate / cardinality / value shifts — without
    holding two full profiles in context. Read it with ``.to_llm()``.

    Args:
        a: The "before" source (any source ``kontra.profile()`` accepts).
        b: The "after" source.
        preset: Profiling preset (scout | scan | interrogate).
        columns: Restrict profiling to these columns on both sides.
        sample: Sample size for profiling (None = full).
        storage_options: Cloud storage credentials.
        a_label / b_label: Display names for the two sides (default: the source
            string, truncated).

    Returns:
        ProfileDiff — ``.has_changes``, ``.has_schema_changes``,
        ``.columns_added/.columns_removed/.columns_changed``, ``.to_llm()``,
        ``.to_dict()``.

    Example:
        diff = kontra.compare_profiles("stage1.parquet", "stage2.parquet")
        print(diff.to_llm())
        if diff.has_schema_changes:
            ...
    """
    prof_a = profile(a, preset=preset, columns=columns, sample=sample,
                     save=False, storage_options=storage_options)
    prof_b = profile(b, preset=preset, columns=columns, sample=sample,
                     save=False, storage_options=storage_options)

    def _label(src: Any, given: Optional[str]) -> str:
        if given:
            return given
        text = src if isinstance(src, str) else type(src).__name__
        return text if len(text) <= 60 else text[:57] + "..."

    from kontra.connectors.db_utils import mask_credentials

    return ProfileDiff.from_profiles(
        prof_a, prof_b,
        before_label=mask_credentials(_label(a, a_label)),
        after_label=mask_credentials(_label(b, b_label)),
    )


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
    data: Union[str, "pl.DataFrame", "pd.DataFrame", List[Dict[str, Any]], Dict[str, Any], Any, None] = None,
    contract: Optional[str] = None,
    *,
    rules: Optional[List[Dict[str, Any]]] = None,
    preplan: str = "on",
    pushdown: str = "on",
    only: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> ExplainResult:
    """
    Show execution plan without running validation.

    Args:
        data: Data source (file path, URI, DataFrame, etc.)
        contract: Path to contract YAML file
        rules: Inline rule dicts
        preplan: "on" | "off"
        pushdown: "on" | "off"
        only: Filter to rule names or IDs
        columns: Filter to rules touching these columns
        storage_options: Cloud storage credentials

    Returns:
        ExplainResult with tier assignment per rule

    Example:
        plan = kontra.explain("data.parquet", "contract.yml")
        print(plan.render())
        for entry in plan.rules:
            print(f"{entry.rule_id}: {entry.tier}")
    """
    return validate(
        data=data,
        contract=contract,
        rules=rules,
        preplan=preplan,
        pushdown=pushdown,
        only=only,
        columns=columns,
        storage_options=storage_options,
        explain=True,
    )


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
        return Diff.empty("No state store configured")

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
                return Diff.empty(f"No history found for contract '{contract}'")

        # Get history for this contract
        states = store.get_history(contract_fp, limit=100)
        if len(states) < 2:
            return Diff.empty("Need at least 2 validation runs to compute a diff")

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
        return Diff.empty("No history available")
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

    .. deprecated::
        Use :func:`get_history` instead, which supports filtering by date,
        limit, and failed-only.

    Args:
        contract: Contract name or path

    Returns:
        List of run summaries (same schema as get_history).
    """
    import warnings
    warnings.warn(
        "kontra.list_runs() is deprecated, use kontra.get_history() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_history(contract, limit=100)


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
            # Find specific run - try multiple match strategies:
            # 1. Match by state.id (the run_id from get_history)
            # 2. Match by timestamp ISO format
            for s in states:
                state_id = str(s.id) if s.id else None
                if state_id == run_id or s.run_at.isoformat() == run_id:
                    state = s
                    break
        else:
            # Get latest (first in list, newest first)
            state = states[0]

        if state is None:
            if run_id:
                raise ValueError(f"Run not found: {run_id}")
            return None

        # Convert state to ValidationResult
        # Ensure passed is bool, not None (BUG-043)
        passed = state.summary.passed
        if passed is None:
            passed = state.summary.blocking_failures == 0
        return ValidationResult(
            passed=passed,
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
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
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
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
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
    from kontra.config.settings import resolve_datasource

    return resolve_datasource(name)


def config(env: Optional[str] = None) -> "KontraConfig":
    """
    Get effective configuration.

    Args:
        env: Environment name (default: use KONTRA_ENV or defaults)

    Returns:
        KontraConfig with preplan, pushdown, etc.

    Example:
        cfg = kontra.config()
        cfg = kontra.config(env="production")
        print(cfg.preplan)  # "on"
    """
    from kontra.config.settings import resolve_effective_config

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
    # Validate annotation inputs (BUG-044, BUG-045)
    _VALID_ANNOTATION_TYPES = {
        "resolution", "root_cause", "false_positive",
        "acknowledged", "suppressed", "note",
    }
    if annotation_type not in _VALID_ANNOTATION_TYPES:
        raise ValueError(
            f"Invalid annotation_type '{annotation_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_ANNOTATION_TYPES))}"
        )
    if not summary or not summary.strip():
        raise ValueError("summary must be a non-empty string")

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

    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
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
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
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
    except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError) as e:
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
    if path is not None and not os.path.exists(path):
        import warnings
        warnings.warn(
            f"Config path does not exist: {path}. "
            f"Config will be ignored until the file is created.",
            UserWarning,
            stacklevel=2,
        )
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
    from kontra.engine.phases.compilation import _ensure_builtin_rules_registered
    from pathlib import Path

    # Ensure builtin rules are registered before checking the registry
    _ensure_builtin_rules_registered()

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
