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
from typing import Any, Dict, List, Literal, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow.fs as pafs
    from kontra.state.backends.base import StateBackend
    from kontra.state.types import ValidationState

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
from kontra.connectors.uri_utils import (
    is_s3_uri as _is_s3_uri,
    is_azure_uri as _is_azure_uri,
    is_parquet as _is_parquet,
    create_s3_filesystem as _create_s3_filesystem,
    create_azure_filesystem as _create_azure_filesystem,
)
from kontra.reporters.rich_reporter import report_failure, report_line, report_success
from kontra.rule_defs.execution_plan import RuleExecutionPlan
from kontra.logging import get_logger, log_exception

from kontra.engine.phases.compilation import compile_rules
from kontra.engine.phases.preplan import execute_preplan
from kontra.engine.phases.pushdown import execute_pushdown
from kontra.engine.phases.residual import execute_residual
from kontra.engine.phases.merge import merge_results, build_summary

_logger = get_logger(__name__)


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
        # Not a named datasource or config not available - use as-is.
        # Note: DatasourceTableError (datasource found, table missing) inherits
        # from KontraError, not ValueError, so it propagates with a clear message
        # instead of falling through to a misleading "Data file not found" error.
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


def _cloud_filesystem(handle: DatasetHandle) -> Optional["pafs.FileSystem"]:
    """PyArrow filesystem for S3/Azure URIs; None for local paths or on credential failure."""
    try:
        if _is_s3_uri(handle.uri):
            return _create_s3_filesystem(handle)
        if _is_azure_uri(handle.uri):
            return _create_azure_filesystem(handle)
    except Exception as e:  # credential/config errors are non-fatal: preplan is optional
        log_exception(_logger, f"Could not create cloud filesystem for {handle.scheme}", e)
    return None


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
        preplan: Literal["on", "off"] = "on",
        pushdown: Literal["on", "off"] = "on",
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
        # Goal-directed validation
        only_rules: Optional[List[str]] = None,
        only_columns: Optional[List[str]] = None,
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

        valid_toggles = {"on", "off"}
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

        # Type-check tally — string 'no'/'false' are truthy in Python (BUG F-017)
        if tally is not None and not isinstance(tally, bool):
            raise TypeError(
                f"'tally' must be a bool or None, got {type(tally).__name__}: {tally!r}. "
                f"Use tally=True or tally=False"
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
        self._only_rules = only_rules  # Goal-directed: filter by rule name/ID
        self._only_columns = only_columns  # Goal-directed: filter by column

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

        database_type = None
        if self._execution_path == "database":
            database_type = self._infer_database_type()

        # Register only what's needed for this path
        try:
            register_materializers_for_path(self._execution_path, database_type)
            register_executors_for_path(self._execution_path, database_type)
        except (ImportError, ValueError):
            # If path-aware registration fails, fall back to default
            register_default_materializers()
            register_default_executors()

    def _infer_database_type(self) -> Optional[str]:
        """Database flavor ("postgres"/"sqlserver") from the data path or handle."""
        if self.data_path:
            return get_database_type(self.data_path)
        if self._handle is None:
            return None
        # BYOC handles carry scheme="byoc" and identify the flavor via dialect
        for key in (self._handle.scheme, self._handle.dialect):
            if key in ("postgres", "postgresql"):
                return "postgres"
            if key in ("mssql", "sqlserver"):
                return "sqlserver"
        return None

    # --------------------------------------------------------------------- #

    def _build_handle(self, source_ref: str) -> DatasetHandle:
        """Resolve a datasource reference (named, path, or URI) to a DatasetHandle."""
        source_uri = _resolve_datasource_uri(source_ref)
        return DatasetHandle.from_uri(source_uri, storage_options=self._storage_options)

    def explain(self) -> "ExplainResult":
        """
        Show which tier each rule will execute on without running validation.

        Actually runs the preplan phase (metadata-only, fast) to give accurate
        tier predictions that match real execution behavior.

        Returns:
            ExplainResult with tier assignment for each rule
        """
        from kontra.api.results import ExplainResult, RuleExplainEntry

        # Phase 1: Contract loading
        self.contract = self._load_contract()

        # Phase 2: Rule compilation
        ctx = compile_rules(
            contract=self.contract,
            inline_built_rules=self._inline_built_rules,
            global_tally=self.tally,
            tally_is_override=self.tally_is_override,
            only_rules=self._only_rules,
            only_columns=self._only_columns,
        )

        # Phase 3: Determine data source characteristics
        is_dataframe = self._input_dataframe is not None
        handle = self._handle
        if not is_dataframe and handle is None:
            source_ref = self.data_path or self.contract.datasource
            if source_ref and source_ref != "inline":
                handle = self._build_handle(source_ref)

        # Build index of SQL-capable rule IDs
        sql_rule_ids = {spec["rule_id"] for spec in ctx.compiled_full.sql_rules}

        # Phase 4: Run actual preplan (metadata-only, fast) for accurate predictions
        preplan_handled_ids: set[str] = set()
        is_parquet = handle is not None and _is_parquet(handle.uri)
        is_postgres = handle is not None and handle.scheme in ("postgres", "postgresql")
        is_sqlserver = handle is not None and handle.scheme in ("mssql", "sqlserver")

        if not is_dataframe and handle is not None and self.preplan == "on":
            try:
                preplan_result = execute_preplan(
                    handle=handle,
                    ctx=ctx,
                    preplan_mode=self.preplan,
                    preplan_fs=_cloud_filesystem(handle),
                )
                preplan_handled_ids = preplan_result.handled_ids
            except Exception as e:
                # Preplan failed — not fatal for explain, just means no metadata tier
                _logger.info("Explain preplan skipped: %s", e)

        # Determine whether executor is available for SQL pushdown
        has_executor = (
            not is_dataframe
            and self.pushdown == "on"
            and handle is not None
        )

        # Build entries
        entries: list[RuleExplainEntry] = []
        tier_counts: Dict[str, int] = {"metadata": 0, "sql": 0, "polars": 0}

        for rule in ctx.rules:
            rid = rule.rule_id

            # Extract column from rule_id
            column = None
            if rid.startswith("COL:"):
                parts = rid.split(":")
                if len(parts) >= 2:
                    column = parts[1]

            # 1. Check if preplan actually resolved this rule
            if rid in preplan_handled_ids:
                if is_parquet:
                    reason = "Parquet metadata stats"
                elif is_postgres:
                    reason = "PostgreSQL catalog stats"
                elif is_sqlserver:
                    reason = "SQL Server catalog stats"
                else:
                    reason = "Metadata stats"
                entries.append(RuleExplainEntry(rid, rule.name, "metadata", reason, column))
                tier_counts["metadata"] += 1
                continue

            # 2. Check SQL pushdown eligibility
            if has_executor and rid in sql_rule_ids:
                if is_parquet or (handle and handle.uri and handle.uri.lower().endswith((".csv",))):
                    executor_name = "DuckDB"
                elif is_postgres:
                    executor_name = "PostgreSQL"
                elif is_sqlserver:
                    executor_name = "SQL Server"
                else:
                    executor_name = "SQL"
                reason = f"{executor_name} executor"
                entries.append(RuleExplainEntry(rid, rule.name, "sql", reason, column))
                tier_counts["sql"] += 1
                continue

            # 3. Polars (default)
            # Determine if vectorized or fallback
            has_pred = any(p.rule_id == rid for p in ctx.compiled_full.predicates)
            if has_pred:
                reason = "Vectorized predicate"
            elif is_dataframe:
                reason = "DataFrame mode"
            else:
                reason = "Fallback (non-vectorizable)"
            entries.append(RuleExplainEntry(rid, rule.name, "polars", reason, column))
            tier_counts["polars"] += 1

        contract_name = _get_display_name(self.contract)

        return ExplainResult(
            contract_name=contract_name,
            total_rules=len(entries),
            rules=entries,
            summary=tier_counts,
        )

    def run(self) -> Dict[str, Any]:
        timers = RunTimers()
        self._staging_tmpdir = None  # Track for cleanup in finally block

        try:
            result = self._run_impl(timers)

            # Save state if enabled
            # Skip saving when filters are active (BUG F-015):
            # Filtered runs produce partial results. Saving them would cause
            # diff() to report "resolved" issues that were merely filtered out.
            if self.save_state:
                if self._only_rules or self._only_columns:
                    import warnings
                    filters = []
                    if self._only_rules:
                        filters.append(f"only={self._only_rules}")
                    if self._only_columns:
                        filters.append(f"columns={self._only_columns}")
                    warnings.warn(
                        f"Skipping state save: validation was filtered ({', '.join(filters)}). "
                        f"Filtered runs produce partial results that would corrupt diff history.",
                        stacklevel=2,
                    )
                else:
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

            state = ValidationState.from_validation_result(
                result=result,
                contract_fingerprint=contract_fp,
                dataset_fingerprint=dataset_fp,
                contract_name=contract_name,
                dataset_uri=source_uri,
                tally=self.tally,
            )

            # Save
            store.save(state)
            self._last_state = state

        except Exception as e:
            # Don't fail validation if state save fails
            _logger.warning("Failed to save validation state: %s", e)

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

        if self.emit_report:
            self._report(summary, all_results)

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

        # Deferred: ContractLoader pulls yaml, Contract pulls pydantic —
        # neither should load before a contract is actually built.
        from kontra.config.loader import ContractLoader
        from kontra.config.models import Contract

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
            datasource=dataset,
            rules=inline_specs,
        )

    def _run_impl(self, timers: RunTimers) -> Dict[str, Any]:
        """
        Main validation implementation.

        Phases:
        1. Contract loading
        2. Rule compilation
        3. DataFrame mode (early exit if user provided DataFrame)
        4. Handle resolution
        5. Preplan (metadata-only optimization)
        6. Materializer setup
        7. SQL pushdown
        8. Residual Polars execution
        9. Result merging
        10. Summary and reporting
        11. Stats collection
        """
        # ------------------------------------------------------------------ #
        # Phase 1: Contract Loading
        # ------------------------------------------------------------------ #
        t0 = now_ms()
        self.contract = self._load_contract()
        timers.contract_load_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # Phase 2: Rule Compilation
        # ------------------------------------------------------------------ #
        t0 = now_ms()
        ctx = compile_rules(
            contract=self.contract,
            inline_built_rules=self._inline_built_rules,
            global_tally=self.tally,
            tally_is_override=self.tally_is_override,
            only_rules=self._only_rules,
            only_columns=self._only_columns,
        )
        self._rules = ctx.rules  # Store for sample_failures()
        timers.compile_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # Phase 3: DataFrame Mode (early exit)
        # ------------------------------------------------------------------ #
        if self._input_dataframe is not None:
            return self._run_dataframe_mode(
                timers, ctx.rules, ctx.plan, ctx.compiled_full,
                ctx.severity_map, ctx.tally_map, ctx.context_map
            )

        # ------------------------------------------------------------------ #
        # Phase 4: Handle Resolution
        # ------------------------------------------------------------------ #
        if self._handle is not None:
            handle = self._handle
        else:
            handle = self._build_handle(self.data_path or self.contract.datasource)
            self._handle = handle  # Store for sample_failures() to access db_params

        # ------------------------------------------------------------------ #
        # Phase 5: Preplan (metadata-only optimization)
        # ------------------------------------------------------------------ #
        preplan_fs = _cloud_filesystem(handle)

        preplan = execute_preplan(
            handle=handle,
            ctx=ctx,
            preplan_mode=self.preplan,
            preplan_fs=preplan_fs,
            explain_preplan=self.explain_preplan,
        )

        # ------------------------------------------------------------------ #
        # Phase 6: Materializer Setup
        # ------------------------------------------------------------------ #
        materializer = pick_materializer(handle)
        materializer_name = getattr(materializer, "name", "duckdb")

        # ------------------------------------------------------------------ #
        # Phase 7: SQL Pushdown
        # ------------------------------------------------------------------ #
        pushdown, handle, staging_tmpdir = execute_pushdown(
            handle=handle,
            ctx=ctx,
            handled_ids_meta=preplan.handled_ids,
            pushdown_mode=self.pushdown,
            csv_mode=self.csv_mode,
            show_plan=self.show_plan,
        )
        self._staging_tmpdir = staging_tmpdir

        # Update materializer if handle changed (CSV staged to Parquet)
        if pushdown.staged_path:
            materializer = pick_materializer(handle)
            materializer_name = getattr(materializer, "name", materializer_name)

        # ------------------------------------------------------------------ #
        # Phase 8: Residual Polars Execution
        # ------------------------------------------------------------------ #
        residual = execute_residual(
            handle=handle,
            ctx=ctx,
            preplan=preplan,
            pushdown=pushdown,
            materializer=materializer,
            preplan_fs=preplan_fs,
            enable_projection=self.enable_projection,
        )
        self.df = residual.df
        timers.data_load_ms = residual.load_ms
        timers.execute_ms = residual.execute_ms

        # ------------------------------------------------------------------ #
        # Phase 9: Result Merging
        # ------------------------------------------------------------------ #
        results = merge_results(preplan, pushdown, residual, ctx)

        # ------------------------------------------------------------------ #
        # Phase 10: Summary and Reporting
        # ------------------------------------------------------------------ #
        summary = build_summary(
            results=results,
            plan=ctx.plan,
            contract=self.contract,
            row_count=pushdown.row_count,
            df_height=self.df.height if self.df is not None else None,
            preplan_total_rows=preplan.total_rows,
        )

        # Build engine label for stats
        engine_label = (
            f"{materializer_name}+polars "
            f"(preplan:{'on' if preplan.effective else 'off'}, "
            f"pushdown:{'on' if pushdown.effective else 'off'}, "
            f"projection:{'on' if self.enable_projection else 'off'})"
        )

        if self.emit_report:
            t0 = now_ms()
            self._report(summary, results)
            timers.report_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # Phase 11: Stats Collection
        # ------------------------------------------------------------------ #
        stats = self._collect_stats(
            timers=timers,
            ctx=ctx,
            preplan=preplan,
            pushdown=pushdown,
            residual=residual,
            materializer=materializer,
            materializer_name=materializer_name,
            engine_label=engine_label,
            handle=handle,
        ) if self.stats_mode != "none" else None

        # ------------------------------------------------------------------ #
        # Assemble Final Result
        # ------------------------------------------------------------------ #
        out: Dict[str, Any] = {
            "dataset": self.contract.datasource,
            "results": results,
            "summary": summary,
        }
        if stats is not None:
            out["stats"] = stats
        out.setdefault("run_meta", {})["engine_label"] = engine_label

        return out

    def _collect_stats(
        self,
        timers: RunTimers,
        ctx: Any,  # CompilationContext
        preplan: Any,  # PreplanResult
        pushdown: Any,  # PushdownResult
        residual: Any,  # ResidualResult
        materializer: Any,
        materializer_name: str,
        engine_label: str,
        handle: DatasetHandle,
    ) -> Dict[str, Any]:
        """Collect validation statistics for stats_mode='summary' or 'profile'."""
        available_cols = pushdown.available_cols
        if not available_cols:
            available_cols = self._peek_available_columns(handle.uri)

        ds_summary = basic_summary(self.df, available_cols=available_cols, nrows_override=pushdown.row_count)

        loaded_cols = list(self.df.columns) if self.df is not None else []
        required_cols_full = ctx.compiled_full.required_cols if self.enable_projection else []
        compiled_residual = ctx.plan.without_ids(ctx.compiled_full, preplan.handled_ids | pushdown.handled_ids)
        required_cols_residual = compiled_residual.required_cols if self.enable_projection else []

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
            "enabled": self.pushdown == "on",
            "effective": pushdown.effective,
            "executor": pushdown.executor_name,
            "rules_pushed": len(pushdown.results_by_id),
            "breakdown_ms": {
                "compile": pushdown.compile_ms,
                "execute": pushdown.execute_ms,
                "introspect": pushdown.introspect_ms,
            },
        }

        res = {
            "rules_local": len(residual.results),
        }

        phases_ms = {
            "contract_load": int(timers.contract_load_ms or 0),
            "compile": int(timers.compile_ms or 0),
            "preplan": int(preplan.analyze_ms or 0),
            "pushdown": int(pushdown.compile_ms + pushdown.execute_ms + pushdown.introspect_ms),
            "data_load": int(timers.data_load_ms or 0),
            "execute": int(timers.execute_ms or 0),
            "report": int(timers.report_ms or 0),
        }

        stats: Dict[str, Any] = {
            "stats_version": "2",
            "run_meta": {
                "phases_ms": phases_ms,
                "duration_ms_total": sum(phases_ms.values()),
                "dataset_path": self.data_path or self.contract.datasource,
                "contract_path": self.contract_path,
                "engine": engine_label,
                "materializer": materializer_name,
                "preplan_requested": self.preplan,
                "preplan": "on" if preplan.effective else "off",
                "pushdown_requested": self.pushdown,
                "pushdown": "on" if pushdown.effective else "off",
                "csv_mode": self.csv_mode,
                "staged_override": bool(pushdown.staged_path),
            },
            "dataset": ds_summary,
            "preplan": preplan.summary,
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

        return stats

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
                report_line(f"  ✅ {rule_id}{source_tag}")
            else:
                msg = r.get("message", "Failed")
                failed_count = r.get("failed_count", 0)
                is_tally = r.get("tally", True)

                # dtype is binary (schema-level) - show message directly, no count
                is_dtype_rule = rule_id.endswith(":dtype")
                if is_dtype_rule:
                    detail = f": {msg}"
                elif failed_count > 0:
                    failure_word = "failure" if failed_count == 1 else "failures"
                    if is_tally:
                        detail = f": {failed_count:,} {failure_word}"
                    else:
                        # Add ≥ prefix and hint for approximate counts (tally=False)
                        detail = f": ≥{failed_count:,} {failure_word}"
                        if not hasattr(self, '_tally_hint_shown'):
                            detail += " (use --tally for exact count)"
                            self._tally_hint_shown = True
                else:
                    detail = f": {msg}"

                # Use different icon for warning/info
                icon = "❌" if severity == "blocking" else ("⚠️" if severity == "warning" else "ℹ️")
                report_line(f"  {icon} {rule_id}{source_tag}{severity_tag}{detail}")

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
            report_line(f"     Expected: {expected_preview}")

        unexpected = details.get("unexpected_values")
        if unexpected:
            report_line("     Unexpected values:")
            for uv in unexpected[:5]:
                val = uv.get("value", "?")
                count = uv.get("count", 0)
                report_line(f"       - \"{val}\" ({count:,} rows)")
            if len(unexpected) > 5:
                report_line(f"       ... and {len(unexpected) - 5} more")

        suggestion = details.get("suggestion")
        if suggestion:
            report_line(f"     Suggestion: {suggestion}")

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