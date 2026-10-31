"""
kontra.validate() and kontra.explain().
"""

import os
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from kontra.api.inputs import (
    _is_pandas_dataframe,
    _is_polars_dataframe,
    _normalize_to_dataframe,
    _validate_contract_path,
)
from kontra.api.results import DryRunResult, ExplainResult, ValidationResult
from kontra.errors import ContractNotFoundError

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from kontra.engine.engine import ValidationEngine


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
