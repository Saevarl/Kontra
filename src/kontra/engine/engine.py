from __future__ import annotations

"""
Validation Engine — preplan-aware, hybrid, projection-efficient, deterministic.

Flow
----
  1) Load contract
  2) Build rules → compile plan (required columns + SQL-capable candidates)
  3) (Optional) Preplan (metadata-only, Parquet): prove PASS/FAIL, build scan manifest
  4) Pick materializer (e.g., DuckDB for S3 / staged CSV)
  5) (Optional) SQL pushdown for eligible *remaining* rules (may stage CSV → Parquet)
  6) Materialize residual slice for Polars (row-groups + projection)
  7) Execute residual rules in Polars
  8) Merge results (preplan → SQL → Polars), summarize, attach small stats dict

Principles
----------
- Deterministic: identical inputs → identical outputs
- Layered & independent toggles:
    * Preplan (metadata) — independent of pushdown/projection
    * Pushdown (SQL execution) — independent of preplan/projection
    * Projection (contract-driven columns) — independent of preplan/pushdown
- Performance-first: plan → prune → load minimal slice → execute
- Clear separation: engine orchestrates; preplan is a leaf; reporters format/print
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, TYPE_CHECKING, Union

if TYPE_CHECKING:
    import polars as pl
    from kontra.state.backends.base import StateBackend
    from kontra.state.types import ValidationState

import pyarrow as pa
import pyarrow.fs as pafs  # <-- Added
import pyarrow.parquet as pq

from kontra.config.loader import ContractLoader
from kontra.config.models import Contract
from kontra.connectors.handle import DatasetHandle
from kontra.engine.executors.registry import (
    pick_executor,
    register_default_executors,
    register_executors_for_path,
)
from kontra.engine.materializers.registry import (
    pick_materializer,
    register_default_materializers,
    register_materializers_for_path,
)
from kontra.engine.paths import ExecutionPath, get_database_type
from kontra.engine.stats import RunTimers, basic_summary, columns_touched, now_ms, profile_for
from kontra.reporters.rich_reporter import report_failure, report_success
from kontra.rule_defs.execution_plan import RuleExecutionPlan
from kontra.rule_defs.factory import RuleFactory
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)

# Preplan (metadata-only) + static predicate extraction
from kontra.preplan.planner import preplan_single_parquet
from kontra.preplan.types import PrePlan
from kontra.rule_defs.static_predicates import extract_static_predicates

# Built-ins registered lazily - see _ensure_builtin_rules_registered()
_builtin_rules_registered = False


def _ensure_builtin_rules_registered() -> None:
    """
    Lazy import builtin rules to register them.

    This defers loading polars until we actually need to build rules.
    Called by ValidationEngine when loading contracts.

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

    _builtin_rules_registered = True


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

# Lazy loading cache for heavy imports
_lazy_polars = None
_lazy_polars_backend = None


def _get_polars():
    """
    Lazy load polars module.

    Raises:
        ImportError: If polars is not installed.
    """
    global _lazy_polars
    if _lazy_polars is None:
        try:
            import polars
            _lazy_polars = polars
        except ImportError as e:
            raise ImportError(
                "Polars is required for validation but not installed. "
                "Install with: pip install polars"
            ) from e
    return _lazy_polars


def _get_polars_backend():
    """
    Lazy load PolarsBackend class.

    Raises:
        ImportError: If polars is not installed (PolarsBackend depends on it).
    """
    global _lazy_polars_backend
    if _lazy_polars_backend is None:
        try:
            from kontra.engine.backends.polars_backend import PolarsBackend
            _lazy_polars_backend = PolarsBackend
        except ImportError as e:
            raise ImportError(
                "Polars backend could not be loaded. "
                "Ensure polars is installed: pip install polars"
            ) from e
    return _lazy_polars_backend


def _resolve_datasource_uri(reference: str) -> str:
    """
    Resolve a datasource reference to a concrete URI.

    Tries to resolve named datasources (e.g., "prod_db.users") through config.
    Falls back to returning the reference as-is if not found in config.

    Args:
        reference: Named datasource ("prod_db.users") or direct URI/path

    Returns:
        Resolved URI (e.g., "postgres://host/db/public.users" or "./data.parquet")
    """
    try:
        from kontra.config.settings import resolve_datasource
        return resolve_datasource(reference)
    except (ValueError, ImportError):
        # Not a named datasource or config not available - use as-is
        return reference


def _get_display_name(contract: Optional["Contract"]) -> str:
    """
    Get display name for validation output.

    Prefers contract name over datasource for clearer user-facing output.
    Falls back to datasource if no name is set.

    Args:
        contract: Contract object (may be None for inline rules)

    Returns:
        Display name (contract name, datasource, or "dataframe")
    """
    if contract is None:
        return "dataframe"
    # Prefer contract.name if set, otherwise use datasource
    if contract.name:
        return contract.name
    return contract.datasource


def _is_s3_uri(val: str | None) -> bool:
    return isinstance(val, str) and val.lower().startswith("s3://")


def _is_azure_uri(val: str | None) -> bool:
    """Check if URI is an Azure storage URI (ADLS Gen2 or Blob)."""
    if not isinstance(val, str):
        return False
    lower = val.lower()
    return lower.startswith(("abfs://", "abfss://", "az://"))


def _s3_uri_to_path(uri: str) -> str:
    """Convert s3://bucket/key to bucket/key (PyArrow S3FileSystem format)."""
    if uri.lower().startswith("s3://"):
        return uri[5:]  # Strip 's3://'
    return uri


def _create_s3_filesystem(handle: DatasetHandle) -> pafs.S3FileSystem:
    """
    Create a PyArrow S3FileSystem from handle's fs_opts (populated from env vars).
    Supports MinIO and other S3-compatible storage via custom endpoints.
    """
    opts = handle.fs_opts or {}

    # Map our fs_opts keys to PyArrow S3FileSystem kwargs
    kwargs: Dict[str, Any] = {}
    if opts.get("s3_access_key_id") and opts.get("s3_secret_access_key"):
        kwargs["access_key"] = opts["s3_access_key_id"]
        kwargs["secret_key"] = opts["s3_secret_access_key"]
    if opts.get("s3_session_token"):
        kwargs["session_token"] = opts["s3_session_token"]
    if opts.get("s3_region"):
        kwargs["region"] = opts["s3_region"]
    if opts.get("s3_endpoint"):
        # PyArrow expects endpoint_override without the scheme
        endpoint = opts["s3_endpoint"]
        # Strip scheme if present and set scheme kwarg
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
            kwargs["scheme"] = "http"
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
            kwargs["scheme"] = "https"
        kwargs["endpoint_override"] = endpoint

    # MinIO and some S3-compatible storage require path-style URLs (not virtual-hosted)
    # DUCKDB_S3_URL_STYLE=path -> force_virtual_addressing=False
    url_style = opts.get("s3_url_style", "").lower()
    if url_style == "path":
        kwargs["force_virtual_addressing"] = False
    elif url_style == "host":
        kwargs["force_virtual_addressing"] = True
    # If endpoint is set but no url_style, default to path-style (common for MinIO)
    elif opts.get("s3_endpoint"):
        kwargs["force_virtual_addressing"] = False

    return pafs.S3FileSystem(**kwargs)


def _create_azure_filesystem(handle: DatasetHandle) -> pafs.FileSystem:
    """
    Create a PyArrow AzureFileSystem from handle's fs_opts (populated from env vars).
    Supports account key and SAS token authentication.

    Priority: account_key > sas_token (only one auth method should be used)
    """
    opts = handle.fs_opts or {}

    kwargs: Dict[str, Any] = {}
    if opts.get("azure_account_name"):
        kwargs["account_name"] = opts["azure_account_name"]

    # Use only ONE auth method - account_key takes priority over sas_token
    # PyArrow can crash or behave unexpectedly when both are provided
    if opts.get("azure_account_key"):
        kwargs["account_key"] = opts["azure_account_key"]
    elif opts.get("azure_sas_token"):
        # PyArrow requires SAS token WITH the leading '?'
        sas = opts["azure_sas_token"]
        if not sas.startswith("?"):
            sas = "?" + sas
        kwargs["sas_token"] = sas

    return pafs.AzureFileSystem(**kwargs)


def _azure_uri_to_path(uri: str) -> str:
    """
    Convert Azure URI to container/path format for PyArrow AzureFileSystem.

    abfss://container@account.dfs.core.windows.net/path -> container/path
    """
    from urllib.parse import urlparse
    parsed = urlparse(uri)
    # netloc is "container@account.dfs.core.windows.net"
    if "@" in parsed.netloc:
        container = parsed.netloc.split("@", 1)[0]
    else:
        container = parsed.netloc.split(".")[0]
    path_part = parsed.path.lstrip("/")
    return f"{container}/{path_part}"


def _is_parquet(path: str | None) -> bool:
    return isinstance(path, str) and path.lower().endswith(".parquet")


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #

class ValidationEngine:
    """
    Orchestrates:
      - Rule planning
      - Preplan (metadata-only; Parquet)  [independent]
      - SQL pushdown (optional)           [independent]
      - Residual Polars execution
      - Reporting + stats

    Usage:
        # From file paths
        engine = ValidationEngine(contract_path="contract.yml")
        result = engine.run()

        # With DataFrame (skips preplan/pushdown, uses Polars directly)
        import polars as pl
        df = pl.read_parquet("data.parquet")
        engine = ValidationEngine(contract_path="contract.yml", dataframe=df)
        result = engine.run()

        # With pandas DataFrame
        import pandas as pd
        pdf = pd.read_parquet("data.parquet")
        engine = ValidationEngine(contract_path="contract.yml", dataframe=pdf)
        result = engine.run()
    """

    def __init__(
        self,
        contract_path: Optional[str] = None,
        data_path: Optional[str] = None,
        dataframe: Optional[Union["pl.DataFrame", "pd.DataFrame"]] = None,
        handle: Optional[DatasetHandle] = None,  # BYOC: pre-built handle
        emit_report: bool = True,
        stats_mode: Literal["none", "summary", "profile"] = "none",
        # Independent toggles
        preplan: Literal["on", "off", "auto"] = "auto",
        pushdown: Literal["on", "off", "auto"] = "auto",
        tally: Optional[bool] = None,  # Global tally setting (None = use per-rule)
        tally_is_override: bool = False,  # True = tally overrides per-rule (CLI), False = per-rule wins (API)
        enable_projection: bool = True,
        csv_mode: Literal["auto", "duckdb", "parquet"] = "auto",
        # Diagnostics
        show_plan: bool = False,
        explain_preplan: bool = False,
        # State management
        state_store: Optional["StateBackend"] = None,
        save_state: bool = True,
        # Inline rules (Python API)
        inline_rules: Optional[List[Dict[str, Any]]] = None,
        # Cloud storage credentials (S3, Azure, GCS)
        storage_options: Optional[Dict[str, Any]] = None,
        # Execution path hint (for lazy loading optimization)
        execution_path: Optional[ExecutionPath] = None,
    ):
        # Validate inputs
        if contract_path is None and inline_rules is None:
            raise ValueError("Either contract_path or inline_rules must be provided")

        # Validate toggle parameters
        valid_csv_modes = {"auto", "duckdb", "parquet"}
        if csv_mode not in valid_csv_modes:
            raise ValueError(
                f"Invalid csv_mode '{csv_mode}'. "
                f"Must be one of: {', '.join(sorted(valid_csv_modes))}"
            )

        valid_toggles = {"on", "off", "auto"}
        if preplan not in valid_toggles:
            raise ValueError(
                f"Invalid preplan '{preplan}'. "
                f"Must be one of: {', '.join(sorted(valid_toggles))}"
            )
        if pushdown not in valid_toggles:
            raise ValueError(
                f"Invalid pushdown '{pushdown}'. "
                f"Must be one of: {', '.join(sorted(valid_toggles))}"
            )

        valid_stats_modes = {"none", "summary", "profile"}
        if stats_mode not in valid_stats_modes:
            raise ValueError(
                f"Invalid stats_mode '{stats_mode}'. "
                f"Must be one of: {', '.join(sorted(valid_stats_modes))}"
            )

        self.contract_path = str(contract_path) if contract_path else None
        self.data_path = data_path
        self._input_dataframe = dataframe  # Store user-provided DataFrame
        self._inline_rules = inline_rules  # Store inline rules for merging
        self._inline_built_rules = []  # Populated in _load_contract() if BaseRule instances passed
        self.emit_report = emit_report
        self.stats_mode = stats_mode

        self.preplan = preplan
        self.pushdown = pushdown
        self.tally = tally  # Global tally setting
        self.tally_is_override = tally_is_override  # CLI sets True to override per-rule
        self.enable_projection = bool(enable_projection)
        self.csv_mode = csv_mode
        self.show_plan = show_plan
        self.explain_preplan = explain_preplan

        # State management
        self.state_store = state_store
        self.save_state = save_state
        self._last_state: Optional["ValidationState"] = None

        self.contract: Optional[Contract] = None
        self.df: Optional["pl.DataFrame"] = None
        self._handle: Optional[DatasetHandle] = handle  # BYOC: pre-built handle
        self._rules: Optional[List] = None  # Built rules, for sample_failures()
        self._storage_options = storage_options  # Cloud storage credentials
        self._execution_path = execution_path  # Hint for lazy loading optimization

        # Register materializers/executors based on execution path (lazy loading)
        self._register_components_for_path()

    def _register_components_for_path(self) -> None:
        """
        Register materializers and executors based on the execution path.

        This enables lazy loading - we only import heavy dependencies when needed:
        - Database path: only load the specific DB connector (psycopg2/pymssql)
        - File/DataFrame path: load DuckDB and Polars (current behavior)

        If no execution_path hint was provided, falls back to loading everything.
        """
        if self._execution_path is None:
            # No hint provided - use legacy behavior (load everything)
            register_default_materializers()
            register_default_executors()
            return

        # Determine database type for database paths
        database_type = None
        if self._execution_path == "database":
            # Get database type from data_path or handle
            if self.data_path:
                database_type = get_database_type(self.data_path)
            elif self._handle and self._handle.scheme in ("postgres", "postgresql"):
                database_type = "postgres"
            elif self._handle and self._handle.scheme in ("mssql", "sqlserver"):
                database_type = "sqlserver"
            elif self._handle and self._handle.dialect:
                # BYOC handle with dialect
                if self._handle.dialect == "postgresql":
                    database_type = "postgres"
                elif self._handle.dialect == "sqlserver":
                    database_type = "sqlserver"

        # Register only what's needed for this path
        try:
            register_materializers_for_path(self._execution_path, database_type)
            register_executors_for_path(self._execution_path, database_type)
        except (ImportError, ValueError):
            # If path-aware registration fails, fall back to default
            register_default_materializers()
            register_default_executors()

    # --------------------------------------------------------------------- #

    def run(self) -> Dict[str, Any]:
        timers = RunTimers()
        self._staging_tmpdir = None  # Track for cleanup in finally block

        try:
            result = self._run_impl(timers)

            # Save state if enabled
            if self.save_state:
                self._save_validation_state(result)

            return result
        finally:
            # Cleanup staged temp directory (CSV -> Parquet staging)
            if self._staging_tmpdir is not None:
                try:
                    self._staging_tmpdir.cleanup()
                except Exception as e:
                    log_exception(_logger, "Failed to cleanup staging directory", e)
                self._staging_tmpdir = None

    def _save_validation_state(self, result: Dict[str, Any]) -> None:
        """Save validation state if a store is configured."""
        try:
            from kontra.state.types import ValidationState
            from kontra.state.fingerprint import fingerprint_contract, fingerprint_dataset
            from kontra.state.backends import get_default_store

            # Get or create store
            store = self.state_store
            if store is None and self.save_state:
                store = get_default_store()

            if store is None:
                return

            # Generate fingerprints
            contract_fp = fingerprint_contract(self.contract) if self.contract else "unknown"

            source_ref = self.data_path or (self.contract.datasource if self.contract else "")
            source_uri = _resolve_datasource_uri(source_ref) if source_ref else ""
            dataset_fp = None
            try:
                handle = DatasetHandle.from_uri(source_uri, storage_options=self._storage_options)
                dataset_fp = fingerprint_dataset(handle)
            except Exception as e:
                log_exception(_logger, "Could not fingerprint dataset", e)

            # Derive contract name (from contract, or from path)
            contract_name = "unknown"
            if self.contract:
                contract_name = self.contract.name or Path(self.contract_path).stem

            # Create state from result
            state = ValidationState.from_validation_result(
                result=result,
                contract_fingerprint=contract_fp,
                dataset_fingerprint=dataset_fp,
                contract_name=contract_name,
                dataset_uri=source_uri,
            )

            # Save
            store.save(state)
            self._last_state = state

        except Exception as e:
            # Don't fail validation if state save fails
            if os.getenv("KONTRA_VERBOSE"):
                print(f"Warning: Failed to save validation state: {e}")

    def get_last_state(self) -> Optional["ValidationState"]:
        """Get the state from the last validation run."""
        return self._last_state

    def diff_from_last(self) -> Optional[Dict[str, Any]]:
        """
        Compare current state to previous state.

        Returns a dict with changes, or None if no previous state exists.
        """
        if self._last_state is None:
            return None

        try:
            from kontra.state.backends import get_default_store

            store = self.state_store or get_default_store()
            previous = store.get_previous(
                self._last_state.contract_fingerprint,
                before=self._last_state.run_at,
            )

            if previous is None:
                return None

            # Build simple diff
            return self._build_diff(previous, self._last_state)

        except Exception as e:
            log_exception(_logger, "Failed to compute diff", e)
            return None

    def _build_diff(
        self,
        before: "ValidationState",
        after: "ValidationState",
    ) -> Dict[str, Any]:
        """Build a diff between two validation states."""
        diff: Dict[str, Any] = {
            "before_run_at": before.run_at.isoformat(),
            "after_run_at": after.run_at.isoformat(),
            "summary_changed": before.summary.passed != after.summary.passed,
            "rules_changed": [],
            "new_failures": [],
            "resolved_failures": [],
        }

        # Index before rules by ID
        before_rules = {r.rule_id: r for r in before.rules}
        after_rules = {r.rule_id: r for r in after.rules}

        # Find changes
        for rule_id, after_rule in after_rules.items():
            before_rule = before_rules.get(rule_id)

            if before_rule is None:
                # New rule
                if not after_rule.passed:
                    diff["new_failures"].append({
                        "rule_id": rule_id,
                        "failed_count": after_rule.failed_count,
                    })
            elif before_rule.passed != after_rule.passed:
                # Status changed
                if after_rule.passed:
                    diff["resolved_failures"].append(rule_id)
                else:
                    diff["new_failures"].append({
                        "rule_id": rule_id,
                        "failed_count": after_rule.failed_count,
                        "was_passing": True,
                    })
            elif before_rule.failed_count != after_rule.failed_count:
                # Count changed
                diff["rules_changed"].append({
                    "rule_id": rule_id,
                    "before_count": before_rule.failed_count,
                    "after_count": after_rule.failed_count,
                    "delta": after_rule.failed_count - before_rule.failed_count,
                })

        diff["has_regressions"] = len(diff["new_failures"]) > 0 or any(
            r["delta"] > 0 for r in diff["rules_changed"]
        )

        return diff

    def _run_dataframe_mode(
        self,
        timers: RunTimers,
        rules: List,
        plan: "RuleExecutionPlan",
        compiled_full,
        rule_severity_map: Dict[str, str],
        rule_tally_map: Dict[str, bool],
        rule_context_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Execute validation directly on a user-provided DataFrame.

        This path:
        - Skips preplan (no file metadata)
        - Skips SQL pushdown (data already in memory)
        - Uses Polars-only execution
        """
        t0 = now_ms()

        # Convert pandas to polars if needed
        pl = _get_polars()
        df = self._input_dataframe
        if not isinstance(df, pl.DataFrame):
            try:
                # Assume it's pandas-like
                df = pl.from_pandas(df)
            except Exception as e:
                raise ValueError(
                    f"Could not convert DataFrame to Polars: {e}. "
                    "Pass a Polars DataFrame or a pandas DataFrame."
                )

        self.df = df
        timers.data_load_ms = now_ms() - t0

        # Execute all rules via Polars
        t0 = now_ms()
        PolarsBackend = _get_polars_backend()
        polars_exec = PolarsBackend(executor=plan.execute_compiled)
        exec_result = polars_exec.execute(self.df, compiled_full, rule_tally_map)
        polars_results = exec_result.get("results", [])
        timers.polars_ms = now_ms() - t0

        # Merge results (all from Polars in this mode)
        all_results: List[Dict[str, Any]] = []
        for res in polars_results:
            res["execution_source"] = "polars"
            res["severity"] = rule_severity_map.get(res["rule_id"], "blocking")
            res["tally"] = rule_tally_map.get(res["rule_id"], False)
            # Inject context if present
            ctx = rule_context_map.get(res["rule_id"])
            if ctx:
                res["context"] = ctx
            all_results.append(res)

        # Sort deterministically
        all_results.sort(key=lambda r: r["rule_id"])

        # Summary (use the plan's summary method for consistency)
        summary = plan.summary(all_results)
        summary["dataset_name"] = _get_display_name(self.contract)
        summary["total_rows"] = int(self.df.height) if self.df is not None else 0
        engine_label = "polars (dataframe mode)"

        # Report
        if self.emit_report:
            if summary["passed"]:
                report_success(
                    name=summary.get("dataset_name", "dataframe"),
                    results=all_results,
                    summary=summary,
                )
            else:
                report_failure(
                    name=summary.get("dataset_name", "dataframe"),
                    results=all_results,
                    summary=summary,
                )

        result = {
            "summary": summary,
            "results": all_results,
        }

        # Stats
        if self.stats_mode != "none":
            stats: Dict[str, Any] = {
                "run_meta": {
                    "contract_path": self.contract_path,
                    "engine": engine_label,
                    "materializer": "dataframe",
                    "preplan": "off",
                    "pushdown": "off",
                },
                "durations_ms": {
                    "contract_load": timers.contract_load_ms,
                    "compile": timers.compile_ms,
                    "data_load": timers.data_load_ms,
                    "polars": timers.polars_ms,
                    "total": timers.total_ms(),
                },
            }

            if self.stats_mode == "summary":
                stats["dataset"] = basic_summary(self.df)
            elif self.stats_mode == "profile":
                stats["dataset"] = profile_for(self.df, self.df.columns)

            result["stats"] = stats

        return result

    def _derive_contract_name(self, dataset: str) -> str:
        """
        Derive a user-friendly contract name from dataset path.

        Examples:
            "users.parquet" -> "users.parquet"
            "s3://bucket/data/users.parquet" -> "users.parquet"
            "postgres:///public.users" -> "public.users"
            "inline_validation" -> "inline_validation"
        """
        if not dataset:
            return "inline_validation"

        # For URIs, extract the last meaningful part
        if "://" in dataset:
            # postgres:///schema.table -> schema.table
            if dataset.startswith(("postgres://", "mssql://")):
                parts = dataset.split("/")
                return parts[-1] if parts[-1] else "validation"
            # s3://bucket/path/file.parquet -> file.parquet
            # abfss://container@account.../path/file.parquet -> file.parquet
            parts = dataset.rstrip("/").split("/")
            return parts[-1] if parts[-1] else "validation"

        # For file paths, use the filename
        from pathlib import Path
        return Path(dataset).name or dataset

    def _load_contract(self) -> Contract:
        """
        Load contract from file and/or merge with inline rules.

        Returns a Contract object with all rules to validate.
        """
        from kontra.config.models import RuleSpec

        # Convert inline rules to RuleSpec objects (or pass through BaseRule instances)
        inline_specs = []
        inline_built_rules = []  # Already-built BaseRule instances
        if self._inline_rules:
            from kontra.rule_defs.base import BaseRule as BaseRuleType
            for rule in self._inline_rules:
                if isinstance(rule, BaseRuleType):
                    # Already a rule instance - use directly
                    inline_built_rules.append(rule)
                elif isinstance(rule, dict):
                    # Dict format - convert to RuleSpec
                    spec = RuleSpec(
                        name=rule.get("name", ""),
                        id=rule.get("id"),
                        params=rule.get("params", {}),
                        severity=rule.get("severity", "blocking"),
                        tally=rule.get("tally"),  # None = use global default
                        context=rule.get("context", {}),
                    )
                    inline_specs.append(spec)
                else:
                    raise ValueError(
                        f"Invalid rule type: {type(rule).__name__}. "
                        f"Expected dict or BaseRule instance."
                    )

        # Store built rules to merge with factory-built rules later
        self._inline_built_rules = inline_built_rules

        # Load from file if path provided
        if self.contract_path:
            contract = (
                ContractLoader.from_s3(self.contract_path)
                if _is_s3_uri(self.contract_path)
                else ContractLoader.from_path(self.contract_path)
            )
            # Merge inline rules with contract rules
            if inline_specs:
                contract.rules = list(contract.rules) + inline_specs
            return contract

        # No contract file - create synthetic contract from inline rules
        # Use data path as name for better UX (shows "users.parquet" instead of "inline_contract")
        dataset = self.data_path or "inline_validation"
        name = self._derive_contract_name(dataset)
        return Contract(
            name=name,
            dataset=dataset,
            rules=inline_specs,
        )

    def _run_impl(self, timers: RunTimers) -> Dict[str, Any]:
        # 1) Contract (load from file and/or inline rules)
        t0 = now_ms()
        self.contract = self._load_contract()
        timers.contract_load_ms = now_ms() - t0

        # 2) Rules & plan
        # Ensure builtin rules are registered (lazy import to defer polars loading)
        _ensure_builtin_rules_registered()
        t0 = now_ms()
        rules = RuleFactory(self.contract.rules).build_rules()
        # Merge with any pre-built rule instances passed directly
        if self._inline_built_rules:
            rules = rules + self._inline_built_rules
        self._rules = rules  # Store for sample_failures()
        plan = RuleExecutionPlan(rules)
        compiled_full = plan.compile()
        timers.compile_ms = now_ms() - t0

        # Build rule_id -> severity mapping for injecting into preplan/SQL results
        rule_severity_map = {r.rule_id: r.severity for r in rules}

        # Build rule_id -> effective tally mapping
        # Precedence:
        #   1. CLI --tally flag (tally_is_override=True) - explicit user intent
        #   2. Per-rule tally setting in contract
        #   3. API tally= parameter (tally_is_override=False)
        #   4. Default (False for speed)
        def _effective_tally(rule) -> bool:
            # CLI tally override beats everything
            if self.tally_is_override and self.tally is not None:
                return self.tally
            # Per-rule setting beats API default
            if rule.tally is not None:
                return rule.tally
            # API default (if set)
            if self.tally is not None:
                return self.tally
            # Ultimate default - False enables preplan/early-exit optimizations
            return False

        rule_tally_map = {r.rule_id: _effective_tally(r) for r in rules}

        # Build rule_id -> context mapping for injecting into results
        rule_context_map = {r.rule_id: r.context for r in rules if r.context}

        # ------------------------------------------------------------------ #
        # DataFrame mode: If user provided a DataFrame, use Polars-only path
        # ------------------------------------------------------------------ #
        if self._input_dataframe is not None:
            return self._run_dataframe_mode(timers, rules, plan, compiled_full, rule_severity_map, rule_tally_map, rule_context_map)

        # Dataset handle (used across phases)
        # BYOC: if a pre-built handle was provided, use it directly
        if self._handle is not None:
            handle = self._handle
            source_uri = handle.uri
        else:
            source_ref = self.data_path or self.contract.datasource
            source_uri = _resolve_datasource_uri(source_ref)
            handle = DatasetHandle.from_uri(source_uri, storage_options=self._storage_options)
            self._handle = handle  # Store for sample_failures() to access db_params

        # ------------------------------------------------------------------ #
        # 3) Preplan (metadata-only; independent of pushdown/projection)
        preplan_effective = False
        handled_ids_meta: Set[str] = set()
        meta_results_by_id: Dict[str, Dict[str, Any]] = {}
        preplan_row_groups: Optional[List[int]] = None
        preplan_columns: Optional[List[str]] = None
        preplan_analyze_ms = 0
        preplan_total_rows: Optional[int] = None  # Track row count from preplan metadata
        preplan_summary: Dict[str, Any] = {
            "enabled": self.preplan in {"on", "auto"},
            "effective": False,
            "rules_pass_meta": 0,
            "rules_fail_meta": 0,
            "rules_unknown": len(compiled_full.required_cols or []),
            "row_groups_kept": None,
            "row_groups_total": None,
            "row_groups_pruned": None,
        }

        # Get filesystem from handle; preplan needs this for S3/Azure/remote access.
        preplan_fs: pafs.FileSystem | None = None
        if _is_s3_uri(handle.uri):
            try:
                preplan_fs = _create_s3_filesystem(handle)
            except Exception as e:
                # If S3 libs aren't installed, this will fail.
                # We'll let the ParquetFile call fail below and be caught.
                log_exception(_logger, "Could not create S3 filesystem for preplan", e)
        elif _is_azure_uri(handle.uri):
            try:
                preplan_fs = _create_azure_filesystem(handle)
            except Exception as e:
                # If Azure libs aren't available or creds invalid, fall back to no preplan
                log_exception(_logger, "Could not create Azure filesystem for preplan", e)

        if self.preplan in {"on", "auto"} and _is_parquet(handle.uri):
            try:
                t0 = now_ms()
                static_preds = extract_static_predicates(rules=rules)
                # PyArrow filesystems expect specific path formats:
                # - S3: 'bucket/key' (not 's3://bucket/key')
                # - Azure: 'container/path' (not 'abfss://container@account/path')
                if _is_s3_uri(handle.uri) and preplan_fs:
                    preplan_path = _s3_uri_to_path(handle.uri)
                elif _is_azure_uri(handle.uri) and preplan_fs:
                    preplan_path = _azure_uri_to_path(handle.uri)
                else:
                    preplan_path = handle.uri
                pre: PrePlan = preplan_single_parquet(
                    path=preplan_path,
                    required_columns=compiled_full.required_cols,  # DC-driven columns
                    predicates=static_preds,
                    filesystem=preplan_fs,
                )
                preplan_analyze_ms = now_ms() - t0

                # Register metadata-based rule decisions (pass/fail), unknowns remain
                # Skip rules with tally=True - preplan only returns binary (0/1), not exact counts
                pass_meta = fail_meta = unknown = skipped_tally = 0
                for rid, decision in pre.rule_decisions.items():
                    # If rule needs exact counts (tally=True), skip preplan for this rule
                    if rule_tally_map.get(rid, False):
                        skipped_tally += 1
                        unknown += 1
                        continue

                    if decision == "pass_meta":
                        meta_results_by_id[rid] = {
                            "rule_id": rid,
                            "passed": True,
                            "failed_count": 0,
                            "message": "Proven by metadata (Parquet stats)",
                            "execution_source": "metadata",
                            "severity": rule_severity_map.get(rid, "blocking"),
                            "tally": rule_tally_map.get(rid, False),
                        }
                        handled_ids_meta.add(rid)
                        pass_meta += 1
                    elif decision == "fail_meta":
                        meta_results_by_id[rid] = {
                            "rule_id": rid,
                            "passed": False,
                            "failed_count": 1,
                            "message": "Failed: violation proven by Parquet metadata (null values detected)",
                            "execution_source": "metadata",
                            "severity": rule_severity_map.get(rid, "blocking"),
                            "tally": rule_tally_map.get(rid, False),
                        }
                        handled_ids_meta.add(rid)
                        fail_meta += 1
                    else:
                        unknown += 1

                preplan_row_groups = list(pre.manifest_row_groups or [])
                preplan_columns = list(pre.manifest_columns or [])
                preplan_effective = True
                preplan_total_rows = pre.stats.get("total_rows")

                rg_total = pre.stats.get("rg_total", None)
                rg_kept = len(preplan_row_groups)
                preplan_summary.update({
                    "effective": True,
                    "rules_pass_meta": pass_meta,
                    "rules_fail_meta": fail_meta,
                    "rules_unknown": unknown,
                    "row_groups_kept": rg_kept if rg_total is not None else None,
                    "row_groups_total": rg_total,
                    "row_groups_pruned": (rg_total - rg_kept) if (rg_total is not None) else None,
                })

                if self.explain_preplan:
                    print(
                        "\n-- PREPLAN (metadata) --"
                        f"\n  Row-groups kept: {preplan_summary.get('row_groups_kept')}/{preplan_summary.get('row_groups_total')}"
                        f"\n  Rules: {pass_meta} pass, {fail_meta} fail, {unknown} unknown\n"
                    )

            except Exception as e:
                # Distinguish between "preplan not available" vs "real errors"
                err_str = str(e).lower()
                err_type = type(e).__name__

                # Re-raise errors that indicate real problems (auth, file not found, etc.)
                is_auth_error = (
                    "access denied" in err_str
                    or "forbidden" in err_str
                    or "unauthorized" in err_str
                    or "credentials" in err_str
                    or "authentication" in err_str
                )
                is_not_found = (
                    isinstance(e, FileNotFoundError)
                    or "not found" in err_str
                    or "no such file" in err_str
                    or "does not exist" in err_str
                )
                is_permission = isinstance(e, PermissionError)

                if is_auth_error or is_not_found or is_permission:
                    # These are real errors - don't silently skip
                    raise RuntimeError(
                        f"Unable to access file: {e}. "
                        "Check file path and credentials."
                    ) from e

                # Otherwise, preplan optimization just isn't available (e.g., no stats)
                if os.getenv("KONTRA_VERBOSE"):
                    print(f"[INFO] Preplan skipped ({err_type}): {e}")
                preplan_effective = False  # leave summary with effective=False

        # PostgreSQL preplan (uses pg_stats metadata)
        elif self.preplan in {"on", "auto"} and handle.scheme in ("postgres", "postgresql"):
            try:
                from kontra.preplan.postgres import preplan_postgres, can_preplan_postgres
                if can_preplan_postgres(handle):
                    t0 = now_ms()
                    static_preds = extract_static_predicates(rules=rules)
                    pre: PrePlan = preplan_postgres(
                        handle=handle,
                        required_columns=compiled_full.required_cols,
                        predicates=static_preds,
                    )
                    preplan_analyze_ms = now_ms() - t0

                    # Skip rules with tally=True - preplan only returns binary (0/1), not exact counts
                    pass_meta = fail_meta = unknown = 0
                    for rid, decision in pre.rule_decisions.items():
                        # If rule needs exact counts (tally=True), skip preplan for this rule
                        if rule_tally_map.get(rid, False):
                            unknown += 1
                            continue

                        if decision == "pass_meta":
                            meta_results_by_id[rid] = {
                                "rule_id": rid,
                                "passed": True,
                                "failed_count": 0,
                                "message": "Proven by metadata (pg_stats)",
                                "execution_source": "metadata",
                                "severity": rule_severity_map.get(rid, "blocking"),
                                "tally": rule_tally_map.get(rid, False),
                            }
                            handled_ids_meta.add(rid)
                            pass_meta += 1
                        else:
                            unknown += 1

                    preplan_effective = True
                    preplan_summary.update({
                        "effective": True,
                        "rules_pass_meta": pass_meta,
                        "rules_fail_meta": fail_meta,
                        "rules_unknown": unknown,
                    })
            except Exception as e:
                if os.getenv("KONTRA_VERBOSE"):
                    print(f"[INFO] PostgreSQL preplan skipped: {e}")

        # SQL Server preplan (uses sys.columns metadata)
        elif self.preplan in {"on", "auto"} and handle.scheme in ("mssql", "sqlserver"):
            try:
                from kontra.preplan.sqlserver import preplan_sqlserver, can_preplan_sqlserver
                if can_preplan_sqlserver(handle):
                    t0 = now_ms()
                    static_preds = extract_static_predicates(rules=rules)
                    pre: PrePlan = preplan_sqlserver(
                        handle=handle,
                        required_columns=compiled_full.required_cols,
                        predicates=static_preds,
                    )
                    preplan_analyze_ms = now_ms() - t0

                    # Skip rules with tally=True - preplan only returns binary (0/1), not exact counts
                    pass_meta = fail_meta = unknown = 0
                    for rid, decision in pre.rule_decisions.items():
                        # If rule needs exact counts (tally=True), skip preplan for this rule
                        if rule_tally_map.get(rid, False):
                            unknown += 1
                            continue

                        if decision == "pass_meta":
                            meta_results_by_id[rid] = {
                                "rule_id": rid,
                                "passed": True,
                                "failed_count": 0,
                                "message": "Proven by metadata (SQL Server constraints)",
                                "execution_source": "metadata",
                                "severity": rule_severity_map.get(rid, "blocking"),
                                "tally": rule_tally_map.get(rid, False),
                            }
                            handled_ids_meta.add(rid)
                            pass_meta += 1
                        else:
                            unknown += 1

                    preplan_effective = True
                    preplan_summary.update({
                        "effective": True,
                        "rules_pass_meta": pass_meta,
                        "rules_fail_meta": fail_meta,
                        "rules_unknown": unknown,
                    })
            except Exception as e:
                if os.getenv("KONTRA_VERBOSE"):
                    print(f"[INFO] SQL Server preplan skipped: {e}")

        # ------------------------------------------------------------------ #
        # 4) Materializer setup (orthogonal)
        materializer = pick_materializer(handle)
        materializer_name = getattr(materializer, "name", "duckdb")
        _staged_override_uri: Optional[str] = None

        # ------------------------------------------------------------------ #
        # 5) SQL pushdown (independent of preplan/projection)
        sql_results_by_id: Dict[str, Dict[str, Any]] = {}
        handled_ids_sql: Set[str] = set()
        available_cols: List[str] = []
        sql_row_count: Optional[int] = None
        executor_name = "none"
        pushdown_effective = False
        push_compile_ms = push_execute_ms = push_introspect_ms = 0

        executor = None
        if self.pushdown in {"on", "auto"}:
            # Exclude rules already decided by preplan
            sql_rules_remaining = [s for s in compiled_full.sql_rules if s.get("rule_id") not in handled_ids_meta]
            executor = pick_executor(handle, sql_rules_remaining)

        if executor:
            try:
                # Inject effective tally into SQL specs (global override takes precedence)
                sql_specs_for_compile = []
                for s in compiled_full.sql_rules:
                    if s.get("rule_id") not in handled_ids_meta:
                        spec = dict(s)  # Copy to avoid mutating original
                        rid = spec.get("rule_id")
                        # Use effective tally from rule_tally_map (includes global override)
                        spec["tally"] = rule_tally_map.get(rid, False)
                        sql_specs_for_compile.append(spec)

                # Compile
                t0 = now_ms()
                executor_name = getattr(executor, "name", "sql")
                sql_plan_str = executor.compile(sql_specs_for_compile)
                push_compile_ms = now_ms() - t0
                if self.show_plan and sql_plan_str:
                    print(f"\n-- {executor_name.upper()} SQL PLAN --\n{sql_plan_str}\n")

                # Execute
                t0 = now_ms()
                duck_out = executor.execute(handle, sql_plan_str, csv_mode=self.csv_mode)
                push_execute_ms = now_ms() - t0

                # Inject severity and tally into SQL results
                sql_results_raw = duck_out.get("results", [])
                for r in sql_results_raw:
                    r["severity"] = rule_severity_map.get(r.get("rule_id"), "blocking")
                    r["tally"] = rule_tally_map.get(r.get("rule_id"), False)
                sql_results_by_id = {r["rule_id"]: r for r in sql_results_raw}
                handled_ids_sql = set(sql_results_by_id.keys())

                # Get row count and cols from execute result (avoids separate introspect call)
                t0 = now_ms()
                sql_row_count = duck_out.get("row_count")
                available_cols = duck_out.get("available_cols") or []

                # Fallback to introspect if execute didn't return these
                if sql_row_count is None or not available_cols:
                    info = executor.introspect(handle, csv_mode=self.csv_mode)
                    push_introspect_ms = now_ms() - t0
                    sql_row_count = info.get("row_count") if sql_row_count is None else sql_row_count
                    available_cols = info.get("available_cols") or available_cols
                    staging = info.get("staging") or duck_out.get("staging")
                else:
                    push_introspect_ms = now_ms() - t0
                    staging = duck_out.get("staging")

                # Reuse staged Parquet (if the executor staged CSV → Parquet)
                staging = staging or duck_out.get("staging")
                if staging and staging.get("path"):
                    _staged_override_uri = staging["path"]
                    self._staging_tmpdir = staging.get("tmpdir")
                    handle = DatasetHandle.from_uri(_staged_override_uri)
                    materializer = pick_materializer(handle)
                    materializer_name = getattr(materializer, "name", materializer_name)

                pushdown_effective = True
            except Exception as e:
                if os.getenv("KONTRA_VERBOSE") or self.show_plan:
                    print(f"[WARN] SQL pushdown failed ({type(e).__name__}): {e}")
                executor = None  # fall back silently

        # ------------------------------------------------------------------ #
        # 6) Residual Polars execution (projection independent; manifest optional)
        handled_all = handled_ids_meta | handled_ids_sql
        compiled_residual = plan.without_ids(compiled_full, handled_all)

        # Projection is DC-driven; independent of preplan/pushdown
        required_cols_full = compiled_full.required_cols if self.enable_projection else []
        required_cols_residual = compiled_residual.required_cols if self.enable_projection else []

        if not compiled_residual.predicates and not compiled_residual.fallback_rules:
            self.df = None
            polars_out = {"results": []}
            timers.data_load_ms = timers.execute_ms = 0
        else:
            # Lazy load polars only when residual rules exist
            pl = _get_polars()

            # Materialize minimal slice:
            # If preplan produced a row-group manifest, honor it — otherwise let the materializer decide.
            t0 = now_ms()
            if preplan_effective and _is_parquet(handle.uri) and preplan_row_groups:
                cols = (required_cols_residual or None) if self.enable_projection else None

                # Reuse preplan filesystem if available, otherwise create from handle
                residual_fs = preplan_fs
                if residual_fs is None and _is_s3_uri(handle.uri):
                    try:
                        residual_fs = _create_s3_filesystem(handle)
                    except Exception as e:
                        # Let ParquetFile try default credentials
                        log_exception(_logger, "Could not create S3 filesystem for residual load", e)
                elif residual_fs is None and _is_azure_uri(handle.uri):
                    try:
                        residual_fs = _create_azure_filesystem(handle)
                    except Exception as e:
                        log_exception(_logger, "Could not create Azure filesystem for residual load", e)

                # PyArrow filesystems expect specific path formats
                if _is_s3_uri(handle.uri) and residual_fs:
                    residual_path = _s3_uri_to_path(handle.uri)
                elif _is_azure_uri(handle.uri) and residual_fs:
                    residual_path = _azure_uri_to_path(handle.uri)
                else:
                    residual_path = handle.uri
                pf = pq.ParquetFile(residual_path, filesystem=residual_fs)

                pa_cols = cols if cols else None
                rg_tables = [pf.read_row_group(i, columns=pa_cols) for i in preplan_row_groups]
                pa_tbl = pa.concat_tables(rg_tables) if len(rg_tables) > 1 else rg_tables[0]
                self.df = pl.from_arrow(pa_tbl)
            else:
                # Materializer respects projection (engine passes residual required cols)
                self.df = materializer.to_polars(required_cols_residual or None)
            timers.data_load_ms = now_ms() - t0

            # Execute residual rules in Polars
            t0 = now_ms()
            PolarsBackend = _get_polars_backend()
            polars_exec = PolarsBackend(executor=plan.execute_compiled)
            polars_art = polars_exec.compile(compiled_residual)
            polars_out = polars_exec.execute(self.df, polars_art, rule_tally_map)
            timers.execute_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # 7) Merge results — deterministic order: preplan → SQL → Polars
        results: List[Dict[str, Any]] = list(meta_results_by_id.values())
        results += [r for r in sql_results_by_id.values() if r["rule_id"] not in meta_results_by_id]
        # Inject severity and tally into Polars results
        for r in polars_out["results"]:
            if r["rule_id"] not in meta_results_by_id and r["rule_id"] not in sql_results_by_id:
                r["severity"] = rule_severity_map.get(r["rule_id"], "blocking")
                r["tally"] = rule_tally_map.get(r["rule_id"], False)
                results.append(r)

        # Inject context into all results
        for r in results:
            ctx = rule_context_map.get(r["rule_id"])
            if ctx:
                r["context"] = ctx

        # 8) Summary
        summary = plan.summary(results)
        summary["dataset_name"] = _get_display_name(self.contract)
        # Row count priority: SQL executor > DataFrame > preplan metadata > 0
        if sql_row_count is not None:
            summary["total_rows"] = int(sql_row_count)
        elif self.df is not None:
            summary["total_rows"] = int(self.df.height)
        elif preplan_total_rows is not None:
            summary["total_rows"] = int(preplan_total_rows)
        else:
            summary["total_rows"] = 0
        engine_label = (
            f"{materializer_name}+polars "
            f"(preplan:{'on' if preplan_effective else 'off'}, "
            f"pushdown:{'on' if pushdown_effective else 'off'}, "
            f"projection:{'on' if self.enable_projection else 'off'})"
        )

        if self.emit_report:
            t0 = now_ms()
            self._report(summary, results)
            timers.report_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # 9) Stats (feature-attributed)
        stats: Optional[Dict[str, Any]] = None
        if self.stats_mode != "none":
            if not available_cols:
                available_cols = self._peek_available_columns(handle.uri)

            ds_summary = basic_summary(self.df, available_cols=available_cols, nrows_override=sql_row_count)

            loaded_cols = list(self.df.columns) if self.df is not None else []
            proj = {
                "enabled": self.enable_projection,
                "available_count": len(available_cols or []) if available_cols is not None else len(loaded_cols),
                "full": {
                    "required_columns": required_cols_full or [],
                    "required_count": len(required_cols_full or []),
                },
                "residual": {
                    "required_columns": required_cols_residual or [],
                    "required_count": len(required_cols_residual or []),
                    "loaded_count": len(loaded_cols),
                    "effective": self.enable_projection and bool(required_cols_residual)
                                   and len(loaded_cols) <= len(required_cols_residual),
                },
            }

            push = {
                "enabled": self.pushdown in {"on", "auto"},
                "effective": bool(pushdown_effective),
                "executor": executor_name,
                "rules_pushed": len(sql_results_by_id),
                "breakdown_ms": {
                    "compile": push_compile_ms,
                    "execute": push_execute_ms,
                    "introspect": push_introspect_ms,
                },
            }

            res = {
                "rules_local": len(polars_out["results"]) if "polars_out" in locals() else 0,
            }

            phases_ms = {
                "contract_load": int(timers.contract_load_ms or 0),
                "compile": int(timers.compile_ms or 0),
                "preplan": int(preplan_analyze_ms or 0),
                "pushdown": int(push_compile_ms + push_execute_ms + push_introspect_ms),
                "data_load": int(timers.data_load_ms or 0),
                "execute": int(timers.execute_ms or 0),
                "report": int(timers.report_ms or 0),
            }

            stats = {
                "stats_version": "2",
                "run_meta": {
                    "phases_ms": phases_ms,
                    "duration_ms_total": sum(phases_ms.values()),
                    "dataset_path": self.data_path or self.contract.datasource,
                    "contract_path": self.contract_path,
                    "engine": engine_label,
                    "materializer": materializer_name,
                    "preplan_requested": self.preplan,
                    "preplan": "on" if preplan_effective else "off",
                    "pushdown_requested": self.pushdown,
                    "pushdown": "on" if pushdown_effective else "off",
                    "csv_mode": self.csv_mode,
                    "staged_override": bool(_staged_override_uri),
                },
                "dataset": ds_summary,
                "preplan": preplan_summary,
                "pushdown": push,
                "projection": proj,
                "residual": res,
                "columns_touched": columns_touched([{"name": r.name, "params": r.params} for r in self.contract.rules]),
                "columns_validated": columns_touched([{"name": r.name, "params": r.params} for r in self.contract.rules]),
                "columns_loaded": loaded_cols,
            }

            if self.stats_mode == "profile" and self.df is not None:
                stats["profile"] = profile_for(self.df, proj["residual"]["required_columns"])

            if os.getenv("KONTRA_IO_DEBUG"):
                io_dbg = getattr(materializer, "io_debug", None)
                if callable(io_dbg):
                    io = io_dbg()
                    if io:
                        stats["io"] = io

        out: Dict[str, Any] = {
            "dataset": self.contract.datasource,
            "results": results,
            "summary": summary,
        }
        if stats is not None:
            out["stats"] = stats
        out.setdefault("run_meta", {})["engine_label"] = engine_label

        # Ensure staged tempdir (if any) is cleaned after the whole run
        return out

    # --------------------------------------------------------------------- #

    def _report(self, summary: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
        if summary["passed"]:
            # Show warning/info counts if any
            warning_info = ""
            if summary.get("warning_failures", 0) > 0:
                warning_info = f" ({summary['warning_failures']} warnings)"
            elif summary.get("info_failures", 0) > 0:
                warning_info = f" ({summary['info_failures']} info)"

            report_success(
                f"{summary['dataset_name']} — PASSED "
                f"({summary['rules_passed']} of {summary['total_rules']} rules){warning_info}"
            )
        else:
            # Show severity breakdown
            blocking = summary.get("blocking_failures", summary["rules_failed"])
            warning = summary.get("warning_failures", 0)
            info = summary.get("info_failures", 0)

            severity_info = f" ({blocking} blocking"
            if warning > 0:
                warning_word = "warning" if warning == 1 else "warnings"
                severity_info += f", {warning} {warning_word}"
            if info > 0:
                severity_info += f", {info} info"
            severity_info += ")"

            report_failure(
                f"{summary['dataset_name']} — FAILED "
                f"({summary['rules_failed']} of {summary['total_rules']} rules){severity_info}"
            )

        # Show all rule results with execution source
        for r in results:
            source = r.get("execution_source", "polars")
            source_tag = f" [{source}]" if source else ""
            rule_id = r.get("rule_id", "<unknown>")
            passed = r.get("passed", False)
            severity = r.get("severity", "blocking")

            # Severity tag for non-blocking failures
            severity_tag = ""
            if not passed and severity != "blocking":
                severity_tag = f" [{severity}]"

            if passed:
                print(f"  ✅ {rule_id}{source_tag}")
            else:
                msg = r.get("message", "Failed")
                failed_count = r.get("failed_count", 0)
                is_tally = r.get("tally", True)
                # Include failure count if available
                detail = f": {msg}"
                if failed_count > 0:
                    failure_word = "failure" if failed_count == 1 else "failures"
                    if is_tally:
                        detail = f": {failed_count:,} {failure_word}"
                    else:
                        # Add ≥ prefix and hint for approximate counts (tally=False)
                        detail = f": ≥{failed_count:,} {failure_word}"
                        if not hasattr(self, '_tally_hint_shown'):
                            detail += " (use --tally for exact count)"
                            self._tally_hint_shown = True

                # Use different icon for warning/info
                icon = "❌" if severity == "blocking" else ("⚠️" if severity == "warning" else "ℹ️")
                print(f"  {icon} {rule_id}{source_tag}{severity_tag}{detail}")

                # Show detailed explanation if available
                details = r.get("details")
                if details:
                    self._print_failure_details(details)

    def _print_failure_details(self, details: Dict[str, Any]) -> None:
        """Print detailed failure explanation."""
        # Expected values (for allowed_values rule)
        expected = details.get("expected")
        if expected:
            expected_preview = ", ".join(expected[:5])
            if len(expected) > 5:
                expected_preview += f" ... ({len(expected)} total)"
            print(f"     Expected: {expected_preview}")

        # Unexpected values (for allowed_values rule)
        unexpected = details.get("unexpected_values")
        if unexpected:
            print("     Unexpected values:")
            for uv in unexpected[:5]:
                val = uv.get("value", "?")
                count = uv.get("count", 0)
                print(f"       - \"{val}\" ({count:,} rows)")
            if len(unexpected) > 5:
                print(f"       ... and {len(unexpected) - 5} more")

        # Suggestion
        suggestion = details.get("suggestion")
        if suggestion:
            print(f"     Suggestion: {suggestion}")

    # --------------------------------------------------------------------- #

    def _peek_available_columns(self, source: str) -> List[str]:
        """Cheap schema peek; used only for observability."""
        try:
            s = source.lower()
            # We can't easily peek S3 without a filesystem object,
            # so we'll just handle local files for now.
            if _is_s3_uri(s):
                return []
            pl = _get_polars()
            if s.endswith(".parquet"):
                return list(pl.scan_parquet(source).collect_schema().names())
            if s.endswith(".csv"):
                return list(pl.scan_csv(source).collect_schema().names())
        except Exception as e:
            log_exception(_logger, f"Could not peek columns from {source}", e)
        return []