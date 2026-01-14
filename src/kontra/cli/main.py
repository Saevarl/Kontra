from __future__ import annotations

"""
Kontra CLI — Developer-first Data Quality Engine

Thin layer: parse args → call engine → print via reporters.
"""

from typing import Literal, Optional

import typer

from kontra.engine.engine import ValidationEngine
from kontra.reporters.json_reporter import render_json
from kontra.version import VERSION

app = typer.Typer(help="Kontra CLI — Developer-first Data Quality Engine")

# Exit codes (stable for CI/CD)
EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILED = 1
EXIT_CONFIG_ERROR = 2
EXIT_RUNTIME_ERROR = 3


@app.callback()
def _version(
    version: Optional[bool] = typer.Option(
        None, "--version", help="Show the Kontra version and exit.", is_eager=True
    )
) -> None:
    if version:
        typer.echo(f"kontra {VERSION}")
        raise typer.Exit(code=0)


def _print_rich_stats(stats: dict | None) -> None:
    """Pretty-print the optional stats block (concise, high-signal)."""
    if not stats:
        return

    ds = stats.get("dataset", {}) or {}
    run = stats.get("run_meta", {}) or {}
    proj = stats.get("projection") or {}

    # Prefer the human-friendly engine label if present
    engine_label = run.get("engine") or run.get("engine_label")

    nrows = ds.get("nrows")
    ncols = ds.get("ncols")
    dur = run.get("duration_ms_total")

    if nrows is not None and ncols is not None and dur is not None:
        base = f"\nStats  •  rows={nrows:,}  cols={ncols}  duration={dur} ms"
        if engine_label:
            base += f"  engine={engine_label}"
        typer.secho(base, fg=typer.colors.BLUE)
    elif nrows is not None and ncols is not None:
        typer.secho(f"\nStats  •  rows={nrows:,}  cols={ncols}", fg=typer.colors.BLUE)

    # Preplan / pushdown timing (if available)
    preplan_ms = (run.get("preplan_breakdown_ms") or {}).get("analyze")
    push_ms = run.get("pushdown_breakdown_ms") or {}
    if preplan_ms is not None:
        typer.secho(f"Preplan: analyze={preplan_ms} ms", fg=typer.colors.BLUE)
    if push_ms:
        parts = []
        for k in ("compile", "execute", "introspect"):
            v = push_ms.get(k)
            if v is not None:
                parts.append(f"{k}={v} ms")
        if parts:
            typer.secho("SQL pushdown: " + ", ".join(parts), fg=typer.colors.BLUE)

    # If present, show RG pruning summary from preplan (engine may emit either key)
    manifest = stats.get("pushdown_manifest") or {}
    if manifest:
        kept = manifest.get("row_groups_kept")
        total = manifest.get("row_groups_total")
        if kept is not None and total is not None:
            typer.secho(f"Preplan manifest: row-groups {kept}/{total} kept", fg=typer.colors.BLUE)

    # Explicit validated vs loaded columns (short previews)
    validated = stats.get("columns_validated") or []
    loaded = stats.get("columns_loaded") or []

    if validated:
        v_preview = ", ".join(validated[:6]) + ("…" if len(validated) > 6 else "")
        typer.secho(
            f"Columns validated ({len(validated)}): {v_preview}",
            fg=typer.colors.BLUE,
        )

    if loaded:
        l_preview = ", ".join(loaded[:6]) + ("…" if len(loaded) > 6 else "")
        typer.secho(
            f"Columns loaded ({len(loaded)}): {l_preview}",
            fg=typer.colors.BLUE,
        )

    # Projection effectiveness (req/loaded/avail)
    if proj:
        enabled = proj.get("enabled", True)
        required = proj.get("required_count", 0)
        loaded_cnt = proj.get("loaded_count", 0)
        available = proj.get("available_count")
        effectiveness = "(pruned)" if proj.get("effective") else "(no reduction)"
        if available is not None:
            msg = (
                f"Projection [{'on' if enabled else 'off'}]: "
                f"{required}/{loaded_cnt}/{available} (req/loaded/avail) {effectiveness}"
            )
        else:
            msg = (
                f"Projection [{'on' if enabled else 'off'}]: "
                f"{required}/{loaded_cnt} (req/loaded) {effectiveness}"
            )
        typer.secho(msg, fg=typer.colors.BLUE)

    # Optional per-column profile (if requested)
    prof = stats.get("profile")
    if prof:
        typer.secho("Profile:", fg=typer.colors.BLUE)
        for col, s in prof.items():
            parts = [
                f"nulls={s.get('nulls', 0)}",
                f"distinct={s.get('distinct', 0)}",
            ]
            if {"min", "max", "mean"} <= s.keys():
                parts += [
                    f"min={s['min']}",
                    f"max={s['max']}",
                    f"mean={round(s['mean'], 3)}",
                ]
            typer.echo(f"  - {col}: " + ", ".join(parts))


def _handle_dry_run(contract_path: str, data_path: Optional[str], verbose: bool) -> None:
    """
    Validate contract syntax and rule definitions without executing.

    Checks:
    1. Contract file exists and is valid YAML
    2. Contract structure is valid (has dataset, rules list)
    3. All rules are recognized
    4. Dataset URI is parseable
    """
    from kontra.config.loader import ContractLoader
    from kontra.connectors.handle import DatasetHandle
    from kontra.rules.factory import RuleFactory
    from kontra.rules.registry import get_all_rule_names

    # Import built-in rules to populate registry
    import kontra.rules.builtin.allowed_values  # noqa: F401
    import kontra.rules.builtin.custom_sql_check  # noqa: F401
    import kontra.rules.builtin.dtype  # noqa: F401
    import kontra.rules.builtin.freshness  # noqa: F401
    import kontra.rules.builtin.max_rows  # noqa: F401
    import kontra.rules.builtin.min_rows  # noqa: F401
    import kontra.rules.builtin.not_null  # noqa: F401
    import kontra.rules.builtin.range  # noqa: F401
    import kontra.rules.builtin.regex  # noqa: F401
    import kontra.rules.builtin.unique  # noqa: F401

    checks_passed = 0
    checks_failed = 0
    issues = []

    typer.echo("\nDry run validation\n" + "=" * 40)

    # 1. Check contract exists and is valid YAML
    try:
        if contract_path.lower().startswith("s3://"):
            contract = ContractLoader.from_s3(contract_path)
        else:
            contract = ContractLoader.from_path(contract_path)
        typer.secho(f"  ✓ Contract syntax valid: {contract_path}", fg=typer.colors.GREEN)
        checks_passed += 1
    except FileNotFoundError as e:
        typer.secho(f"  ✗ Contract not found: {contract_path}", fg=typer.colors.RED)
        issues.append(str(e))
        checks_failed += 1
        typer.echo(f"\n{checks_passed} checks passed, {checks_failed} failed")
        raise typer.Exit(code=EXIT_CONFIG_ERROR)
    except Exception as e:
        typer.secho(f"  ✗ Contract parse error: {e}", fg=typer.colors.RED)
        issues.append(str(e))
        checks_failed += 1
        typer.echo(f"\n{checks_passed} checks passed, {checks_failed} failed")
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    # 2. Check dataset URI is parseable
    dataset_uri = data_path or contract.dataset
    try:
        handle = DatasetHandle.from_uri(dataset_uri)
        scheme_info = f" ({handle.scheme})" if handle.scheme else ""
        typer.secho(f"  ✓ Dataset URI parseable{scheme_info}: {dataset_uri}", fg=typer.colors.GREEN)
        checks_passed += 1
    except Exception as e:
        typer.secho(f"  ✗ Dataset URI invalid: {e}", fg=typer.colors.RED)
        issues.append(f"Invalid dataset URI: {e}")
        checks_failed += 1

    # 3. Check all rules are recognized
    known_rules = get_all_rule_names()
    unrecognized_rules = []
    rule_count = len(contract.rules)

    for rule_spec in contract.rules:
        # Normalize rule name (strip namespace prefix like "DATASET:" or "COL:")
        rule_name = rule_spec.name.split(":")[-1] if ":" in rule_spec.name else rule_spec.name
        if rule_name not in known_rules:
            unrecognized_rules.append(rule_spec.name)

    if unrecognized_rules:
        typer.secho(f"  ✗ {len(unrecognized_rules)} unrecognized rule(s): {', '.join(unrecognized_rules)}", fg=typer.colors.RED)
        typer.secho(f"    Known rules: {', '.join(sorted(known_rules))}", fg=typer.colors.YELLOW)
        issues.append(f"Unrecognized rules: {', '.join(unrecognized_rules)}")
        checks_failed += 1
    else:
        typer.secho(f"  ✓ All {rule_count} rules recognized", fg=typer.colors.GREEN)
        checks_passed += 1

    # 4. Try to build rules (validates parameters)
    try:
        rules = RuleFactory(contract.rules).build_rules()
        typer.secho(f"  ✓ All {len(rules)} rules valid", fg=typer.colors.GREEN)
        checks_passed += 1

        # Show rule breakdown
        if verbose:
            typer.echo("\n  Rules:")
            for r in rules:
                cols = getattr(r, "params", {}).get("column", "")
                col_info = f" ({cols})" if cols else ""
                typer.echo(f"    - {r.name}{col_info}")

    except Exception as e:
        typer.secho(f"  ✗ Rule validation failed: {e}", fg=typer.colors.RED)
        issues.append(f"Rule validation: {e}")
        checks_failed += 1

    # Summary
    typer.echo("")
    if checks_failed == 0:
        typer.secho(f"✓ Ready to validate ({checks_passed} checks passed)", fg=typer.colors.GREEN)
        typer.echo(f"\nRun without --dry-run to execute:")
        typer.echo(f"  kontra validate {contract_path}")
        raise typer.Exit(code=EXIT_SUCCESS)
    else:
        typer.secho(f"✗ Validation would fail ({checks_failed} issues)", fg=typer.colors.RED)
        for issue in issues:
            typer.echo(f"  - {issue}")
        raise typer.Exit(code=EXIT_CONFIG_ERROR)


@app.command("validate")
def validate(
    contract: str = typer.Argument(
        ..., help="Path or URI to the contract.yml (local or s3://…)"
    ),
    data: Optional[str] = typer.Option(
        None,
        "--data",
        help="Optional dataset path/URI override (e.g., data/users.parquet or s3://bucket/key)",
    ),
    # Config-aware options (None = use config, explicit = override)
    output_format: Optional[Literal["rich", "json"]] = typer.Option(
        None, "--output-format", "-o", help="Output format (default: from config or 'rich')."
    ),
    stats: Optional[Literal["none", "summary", "profile"]] = typer.Option(
        None,
        "--stats",
        help="Attach run statistics (default: from config or 'none').",
    ),
    # Independent execution controls
    preplan: Optional[Literal["on", "off", "auto"]] = typer.Option(
        None,
        "--preplan",
        help="Metadata preflight (default: from config or 'auto').",
    ),
    pushdown: Optional[Literal["on", "off", "auto"]] = typer.Option(
        None,
        "--pushdown",
        help="SQL pushdown (default: from config or 'auto').",
    ),
    projection: Optional[Literal["on", "off"]] = typer.Option(
        None,
        "--projection",
        help="Column projection/pruning (default: from config or 'on').",
    ),
    # CSV handling
    csv_mode: Optional[Literal["auto", "duckdb", "parquet"]] = typer.Option(
        None,
        "--csv-mode",
        help="CSV handling mode (default: from config or 'auto').",
    ),
    # Environment selection
    env: Optional[str] = typer.Option(
        None,
        "--env", "-e",
        help="Environment profile from .kontra/config.yml.",
        envvar="KONTRA_ENV",
    ),
    # Back-compat alias (deprecated): maps 'none' => pushdown=off
    sql_engine: Literal["auto", "none"] = typer.Option(
        "auto",
        "--sql-engine",
        help="(deprecated) Use '--pushdown off' instead. 'none' disables pushdown.",
    ),
    show_plan: bool = typer.Option(
        False,
        "--show-plan",
        help="If SQL pushdown is enabled, print the generated SQL for debugging.",
    ),
    explain_preplan: bool = typer.Option(
        False,
        "--explain-preplan",
        help="Print preplan manifest and metadata decisions (debug aid).",
    ),
    no_actions: bool = typer.Option(
        False,
        "--no-actions",
        help="Run without executing remediation actions (placeholder).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate contract syntax and rule definitions without executing against data.",
    ),
    # State management
    state_backend: Optional[str] = typer.Option(
        None,
        "--state-backend",
        help="State storage backend (default: from config or 'local').",
        envvar="KONTRA_STATE_BACKEND",
    ),
    no_state: bool = typer.Option(
        False,
        "--no-state",
        help="Disable state saving for this run.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose errors."
    ),
) -> None:
    """
    Validate data against a declarative contract.

    The CLI remains stateless and declarative:
      - Delegates to ValidationEngine for execution.
      - JSON output via reporters for CI/CD.
      - Rich output for humans.
    """
    del no_actions  # placeholder until actions are wired

    try:
        # --- DRY RUN MODE ---
        if dry_run:
            _handle_dry_run(contract, data, verbose)
            return

        # --- LOAD CONFIG ---
        # Resolve effective configuration from: CLI > env vars > config file > defaults
        from kontra.config.settings import resolve_effective_config

        cli_overrides = {
            "preplan": preplan,
            "pushdown": pushdown,
            "projection": projection,
            "output_format": output_format,
            "stats": stats,
            "state_backend": state_backend,
            "csv_mode": csv_mode,
        }

        try:
            config = resolve_effective_config(env_name=env, cli_overrides=cli_overrides)
        except Exception as e:
            from kontra.errors import format_error_for_cli
            typer.secho(f"Config error: {format_error_for_cli(e)}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        # Use resolved config values
        effective_output_format = config.output_format
        effective_stats = config.stats
        effective_csv_mode = config.csv_mode
        effective_state_backend = config.state_backend

        # --- RESOLVE DATASOURCE ---
        # Support named datasources: prod_db.users -> postgres://...
        resolved_data = data
        if data:
            from kontra.config.settings import resolve_datasource
            try:
                resolved_data = resolve_datasource(data)
            except ValueError as e:
                typer.secho(f"Datasource error: {e}", fg=typer.colors.RED)
                raise typer.Exit(code=EXIT_CONFIG_ERROR)

        emit_report = effective_output_format == "rich"

        # Deprecation nudge (once per process execution)
        if sql_engine == "none" and pushdown != "off":
            typer.secho(
                "⚠️  --sql-engine is deprecated; use '--pushdown off'.",
                fg=typer.colors.YELLOW,
                err=True,
            )

        # Effective SQL pushdown: explicit flag wins; back-compat maps sql_engine=none → off
        effective_pushdown: Literal["on", "off", "auto"]
        if sql_engine == "none":
            effective_pushdown = "off"
        else:
            effective_pushdown = config.pushdown  # type: ignore

        # Effective preplan
        effective_preplan: Literal["on", "off", "auto"]
        effective_preplan = config.preplan  # type: ignore

        # Effective projection
        enable_projection = config.projection == "on"

        # State backend
        state_store = None
        if effective_state_backend and effective_state_backend != "local" and not no_state:
            from kontra.state.backends import get_store
            state_store = get_store(effective_state_backend)

        eng = ValidationEngine(
            contract_path=contract,
            data_path=resolved_data,
            emit_report=emit_report,
            stats_mode=effective_stats,
            # Independent controls
            preplan=effective_preplan,            # metadata preflight
            pushdown=effective_pushdown,          # SQL pushdown
            enable_projection=enable_projection,  # bool
            csv_mode=effective_csv_mode,          # "auto" | "duckdb" | "parquet"
            # Diagnostics
            show_plan=show_plan,
            explain_preplan=explain_preplan,
            # State management
            state_store=state_store,
            save_state=not no_state,
        )
        result = eng.run()

        if effective_output_format == "json":
            payload = render_json(
                dataset_name=result["summary"]["dataset_name"],
                summary=result["summary"],
                results=result["results"],
                stats=result.get("stats"),
                quarantine=result.get("summary", {}).get("quarantine"),
                validate=False,  # set True once schema validator is bundled
            )
            typer.echo(payload)
        else:
            if effective_stats != "none":
                _print_rich_stats(result.get("stats"))

        raise typer.Exit(
            code=EXIT_SUCCESS if result["summary"]["passed"] else EXIT_VALIDATION_FAILED
        )

    except typer.Exit:
        raise

    except FileNotFoundError as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        if verbose:
            import traceback
            typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except ValueError as e:
        # Contract validation errors, rule parameter errors, etc.
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        if verbose:
            import traceback
            typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except ConnectionError as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        if verbose:
            import traceback
            typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)

    except Exception as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        if verbose:
            import traceback
            typer.secho(f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            typer.secho("Use --verbose for full traceback.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


# --------------------------------------------------------------------------- #
# Scout Command
# --------------------------------------------------------------------------- #


@app.command("scout")
def scout(
    source: str = typer.Argument(
        ..., help="Path or URI to the dataset (local file, s3://..., https://...)"
    ),
    output_format: Optional[Literal["rich", "json", "markdown", "llm"]] = typer.Option(
        None, "--output-format", "-o", help="Output format (default: 'rich')."
    ),
    # Config-aware options
    preset: Optional[Literal["lite", "standard", "deep", "llm"]] = typer.Option(
        None,
        "--preset", "-p",
        help="Profiling depth (default: from config or 'standard').",
    ),
    list_values_threshold: Optional[int] = typer.Option(
        None,
        "--list-values-threshold", "-l",
        help="List all values if distinct count <= threshold.",
    ),
    top_n: Optional[int] = typer.Option(
        None,
        "--top-n", "-t",
        help="Show top N most frequent values per column.",
    ),
    sample: Optional[int] = typer.Option(
        None,
        "--sample", "-s",
        help="Sample N rows for profiling (default: all rows).",
    ),
    include_patterns: Optional[bool] = typer.Option(
        None,
        "--include-patterns",
        help="Detect common patterns (default: from config or False).",
    ),
    columns: Optional[str] = typer.Option(
        None,
        "--columns", "-c",
        help="Comma-separated list of columns to profile (default: all).",
    ),
    suggest_rules: bool = typer.Option(
        False,
        "--suggest-rules",
        help="Generate suggested validation rules based on profile.",
    ),
    save_profile: Optional[bool] = typer.Option(
        None,
        "--save-profile",
        help="Save profile to state storage (default: from config or False).",
    ),
    # Environment selection
    env: Optional[str] = typer.Option(
        None,
        "--env", "-e",
        help="Environment profile from .kontra/config.yml.",
        envvar="KONTRA_ENV",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output."
    ),
) -> None:
    """
    Profile a dataset without a contract (Kontra Scout).

    Generates comprehensive column-level statistics optimized for
    LLM context compression and developer exploration.

    Presets control profiling depth:
      - lite: Fast. Schema + row count + null/distinct counts only.
      - standard: Balanced. Full stats with moderate top values.
      - deep: Comprehensive. Everything including percentiles.

    Examples:
        kontra scout data.parquet
        kontra scout s3://bucket/data.csv --sample 10000
        kontra scout data.parquet -o json --preset deep
        kontra scout data.parquet --suggest-rules > rules.yml
        kontra scout data.parquet --save-profile  # Save for diffing
    """
    import os

    if verbose:
        os.environ["KONTRA_VERBOSE"] = "1"

    try:
        from kontra.scout.profiler import ScoutProfiler
        from kontra.scout.reporters import render_profile
        from kontra.config.settings import resolve_effective_config

        # --- LOAD CONFIG ---
        cli_overrides = {
            "preset": preset,
            "save_profile": save_profile,
            "list_values_threshold": list_values_threshold,
            "top_n": top_n,
            "include_patterns": include_patterns,
        }

        try:
            config = resolve_effective_config(env_name=env, cli_overrides=cli_overrides)
        except Exception as e:
            from kontra.errors import format_error_for_cli
            typer.secho(f"Config error: {format_error_for_cli(e)}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        # Resolve effective values from config
        effective_preset = config.scout_preset
        effective_save_profile = config.scout_save_profile
        effective_list_values_threshold = config.scout_list_values_threshold
        effective_top_n = config.scout_top_n
        effective_include_patterns = config.scout_include_patterns

        # --- RESOLVE DATASOURCE ---
        # Support named datasources: prod_db.users -> postgres://...
        from kontra.config.settings import resolve_datasource
        try:
            resolved_source = resolve_datasource(source)
        except ValueError as e:
            typer.secho(f"Datasource error: {e}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        # Parse columns filter
        cols_filter = None
        if columns:
            cols_filter = [c.strip() for c in columns.split(",") if c.strip()]

        # Smart defaults: llm preset implies llm output format (unless explicitly set)
        effective_output_format = output_format or "rich"
        if effective_preset == "llm" and output_format is None:
            effective_output_format = "llm"

        profiler = ScoutProfiler(
            resolved_source,
            preset=effective_preset,
            list_values_threshold=effective_list_values_threshold,
            top_n=effective_top_n,
            sample_size=sample,
            include_patterns=effective_include_patterns,
            columns=cols_filter,
        )

        profile = profiler.profile()

        # Save profile if requested
        if effective_save_profile:
            from kontra.scout.store import create_profile_state, get_default_profile_store
            state = create_profile_state(profile)
            store = get_default_profile_store()
            store.save(state)
            typer.secho(f"Profile saved (fingerprint: {state.source_fingerprint})", fg=typer.colors.GREEN)

        # Handle rule suggestions
        if suggest_rules:
            from kontra.scout.suggest import generate_rules_yaml
            output = generate_rules_yaml(profile)
        else:
            output = render_profile(profile, format=effective_output_format)

        typer.echo(output)
        raise typer.Exit(code=EXIT_SUCCESS)

    except typer.Exit:
        raise

    except FileNotFoundError as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        if verbose:
            import traceback
            typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except ConnectionError as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        if verbose:
            import traceback
            typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)

    except Exception as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        if verbose:
            import traceback
            typer.secho(f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            typer.secho("Use --verbose for full traceback.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


# --------------------------------------------------------------------------- #
# Init Command - Initialize Kontra Project
# --------------------------------------------------------------------------- #


@app.command("init")
def init(
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Overwrite existing configuration.",
    ),
) -> None:
    """
    Initialize a Kontra project.

    Creates the .kontra/ directory and config.yml with documented defaults
    and example configurations.

    Examples:
        kontra init                     # Initialize project
        kontra init --force             # Overwrite existing config

    To generate a contract from data, use:
        kontra scout data.parquet --suggest-rules > contracts/data.yml
    """
    from pathlib import Path
    from kontra.config.settings import DEFAULT_CONFIG_TEMPLATE

    kontra_dir = Path.cwd() / ".kontra"
    config_path = kontra_dir / "config.yml"

    # Check if already initialized
    if config_path.exists() and not force:
        typer.secho(f"Kontra already initialized: {config_path}", fg=typer.colors.YELLOW)
        typer.echo("Use --force to reinitialize.")
        raise typer.Exit(code=EXIT_SUCCESS)

    # Create .kontra directory
    kontra_dir.mkdir(parents=True, exist_ok=True)

    # Write config template
    config_path.write_text(DEFAULT_CONFIG_TEMPLATE, encoding="utf-8")

    # Create contracts directory
    contracts_dir = Path.cwd() / "contracts"
    contracts_dir.mkdir(exist_ok=True)

    typer.secho("Kontra initialized!", fg=typer.colors.GREEN)
    typer.echo("")
    typer.echo("Created:")
    typer.echo(f"  {config_path}")
    typer.echo(f"  {contracts_dir}/")
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("  1. Edit .kontra/config.yml to configure datasources")
    typer.echo("  2. Profile your data:")
    typer.secho("     kontra scout data.parquet", fg=typer.colors.CYAN)
    typer.echo("  3. Generate a contract:")
    typer.secho("     kontra scout data.parquet --suggest-rules > contracts/data.yml", fg=typer.colors.CYAN)
    typer.echo("  4. Run validation:")
    typer.secho("     kontra validate contracts/data.yml", fg=typer.colors.CYAN)

    raise typer.Exit(code=EXIT_SUCCESS)


# --------------------------------------------------------------------------- #
# Diff Command
# --------------------------------------------------------------------------- #


def _parse_duration(duration_str: str) -> int:
    """
    Parse a duration string like '7d', '24h', '30m' into seconds.

    Supported formats:
    - Xd: X days
    - Xh: X hours
    - Xm: X minutes
    - Xs: X seconds
    """
    import re

    match = re.match(r"^(\d+)([dhms])$", duration_str.lower())
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}. Use '7d', '24h', '30m', or '60s'.")

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    return value * multipliers[unit]


def _render_diff_rich(diff) -> str:
    """Render diff in human-readable format."""
    from kontra.state.types import StateDiff

    lines = []

    # Header
    before_ts = diff.before.run_at.strftime("%Y-%m-%d %H:%M")
    after_ts = diff.after.run_at.strftime("%Y-%m-%d %H:%M")

    lines.append(f"Diff: {diff.after.contract_name}")
    lines.append(f"Comparing: {before_ts} → {after_ts}")
    lines.append("=" * 50)

    # Overall status
    if diff.status_changed:
        before_status = "PASSED" if diff.before.summary.passed else "FAILED"
        after_status = "PASSED" if diff.after.summary.passed else "FAILED"
        lines.append(f"\nOverall: {before_status} → {after_status}")
    else:
        status = "PASSED" if diff.after.summary.passed else "FAILED"
        lines.append(f"\nOverall: {status} (unchanged)")

    # Summary
    lines.append(f"\nRules: {diff.before.summary.passed_rules}/{diff.before.summary.total_rules} → "
                 f"{diff.after.summary.passed_rules}/{diff.after.summary.total_rules}")

    # Helper to get severity icon
    def severity_icon(severity: str) -> str:
        if severity == "warning":
            return "⚠️ "
        elif severity == "info":
            return "ℹ️ "
        else:  # blocking
            return "❌"

    # New failures - group by severity
    if diff.new_failures:
        # Separate by severity
        blocking = [rd for rd in diff.new_failures if rd.severity == "blocking"]
        warnings = [rd for rd in diff.new_failures if rd.severity == "warning"]
        infos = [rd for rd in diff.new_failures if rd.severity == "info"]

        if blocking:
            lines.append(f"\n❌ New Blocking Failures ({len(blocking)})")
            for rd in blocking:
                count_info = f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}{count_info}{mode_info}")

        if warnings:
            lines.append(f"\n⚠️  New Warnings ({len(warnings)})")
            for rd in warnings:
                count_info = f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}{count_info}{mode_info}")

        if infos:
            lines.append(f"\nℹ️  New Info Issues ({len(infos)})")
            for rd in infos:
                count_info = f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}{count_info}{mode_info}")

    # Regressions - group by severity
    if diff.regressions:
        blocking_reg = [rd for rd in diff.regressions if rd.severity == "blocking"]
        warning_reg = [rd for rd in diff.regressions if rd.severity == "warning"]
        info_reg = [rd for rd in diff.regressions if rd.severity == "info"]

        if blocking_reg:
            lines.append(f"\n❌ Blocking Regressions ({len(blocking_reg)})")
            for rd in blocking_reg:
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}")

        if warning_reg:
            lines.append(f"\n⚠️  Warning Regressions ({len(warning_reg)})")
            for rd in warning_reg:
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}")

        if info_reg:
            lines.append(f"\nℹ️  Info Regressions ({len(info_reg)})")
            for rd in info_reg:
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}")

    # Resolved
    if diff.resolved:
        lines.append(f"\n✅ Resolved ({len(diff.resolved)})")
        for rd in diff.resolved:
            lines.append(f"  - {rd.rule_id}")

    # Improvements
    if diff.improvements:
        lines.append(f"\n📈 Improvements ({len(diff.improvements)})")
        for rd in diff.improvements:
            lines.append(f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} ({rd.delta:,})")

    # No changes
    if not diff.new_failures and not diff.regressions and not diff.resolved and not diff.improvements:
        lines.append("\n✓ No changes detected")

    return "\n".join(lines)


@app.command("diff")
def diff_cmd(
    contract: Optional[str] = typer.Argument(
        None, help="Contract path or fingerprint. If not provided, uses most recent."
    ),
    output_format: Literal["rich", "json", "llm"] = typer.Option(
        "rich", "--output-format", "-o", help="Output format."
    ),
    since: Optional[str] = typer.Option(
        None, "--since", "-s",
        help="Compare to state from this duration ago (e.g., '7d', '24h', '1h').",
    ),
    run: Optional[str] = typer.Option(
        None, "--run", "-r",
        help="Compare to state from specific date (YYYY-MM-DD or YYYY-MM-DDTHH:MM).",
    ),
    state_backend: Optional[str] = typer.Option(
        None, "--state-backend",
        help="State storage backend (default: from config or 'local').",
        envvar="KONTRA_STATE_BACKEND",
    ),
    # Environment selection
    env: Optional[str] = typer.Option(
        None,
        "--env", "-e",
        help="Environment profile from .kontra/config.yml.",
        envvar="KONTRA_ENV",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output."
    ),
) -> None:
    """
    Show changes between validation runs.

    Compares the most recent validation state to a previous state
    and shows what changed (new failures, resolved issues, regressions).

    Examples:
        kontra diff                           # Compare last two runs
        kontra diff --since 7d                # Compare to 7 days ago
        kontra diff --run 2024-01-12          # Compare to specific date
        kontra diff -o llm                    # Token-optimized output
        kontra diff contracts/users.yml       # Specific contract
    """
    from datetime import datetime, timedelta, timezone

    try:
        from kontra.state.backends import get_store, get_default_store
        from kontra.state.types import StateDiff
        from kontra.state.fingerprint import fingerprint_contract_file
        from kontra.config.settings import resolve_effective_config

        # --- LOAD CONFIG ---
        cli_overrides = {"state_backend": state_backend}

        try:
            config = resolve_effective_config(env_name=env, cli_overrides=cli_overrides)
        except Exception as e:
            from kontra.errors import format_error_for_cli
            typer.secho(f"Config error: {format_error_for_cli(e)}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        effective_state_backend = config.state_backend

        # Get store
        if effective_state_backend and effective_state_backend != "local":
            store = get_store(effective_state_backend)
        else:
            store = get_default_store()

        # Determine contract fingerprint
        contract_fp = None
        if contract:
            # Could be a path or a fingerprint
            if len(contract) == 16 and all(c in "0123456789abcdef" for c in contract):
                # Looks like a fingerprint
                contract_fp = contract
            else:
                # Treat as path, compute fingerprint
                contract_fp = fingerprint_contract_file(contract)

        # If no contract specified, find most recent
        if not contract_fp:
            contracts = store.list_contracts()
            if not contracts:
                typer.secho("No validation state found. Run 'kontra validate' first.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=EXIT_SUCCESS)

            # Get most recent across all contracts
            most_recent = None
            most_recent_fp = None
            for fp in contracts:
                latest = store.get_latest(fp)
                if latest and (most_recent is None or latest.run_at > most_recent.run_at):
                    most_recent = latest
                    most_recent_fp = fp

            if not most_recent_fp:
                typer.secho("No validation state found.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=EXIT_SUCCESS)

            contract_fp = most_recent_fp

        # Get history for this contract
        history = store.get_history(contract_fp, limit=100)

        if len(history) < 1:
            typer.secho(f"No state history found for contract {contract_fp}.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=EXIT_SUCCESS)

        # Determine which states to compare
        after_state = history[0]  # Most recent
        before_state = None

        if since:
            # Parse duration and find state from that time ago
            try:
                seconds = _parse_duration(since)
                target_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)

                for state in history[1:]:
                    if state.run_at <= target_time:
                        before_state = state
                        break

                if not before_state:
                    typer.secho(f"No state found from {since} ago.", fg=typer.colors.YELLOW)
                    raise typer.Exit(code=EXIT_SUCCESS)

            except ValueError as e:
                typer.secho(f"Error: {e}", fg=typer.colors.RED)
                raise typer.Exit(code=EXIT_CONFIG_ERROR)

        elif run:
            # Parse specific date/time
            try:
                if "T" in run:
                    target_time = datetime.fromisoformat(run.replace("Z", "+00:00"))
                else:
                    target_time = datetime.strptime(run, "%Y-%m-%d").replace(tzinfo=timezone.utc)

                # Find state closest to this time
                for state in history:
                    if state.run_at.date() <= target_time.date():
                        before_state = state
                        break

                if not before_state:
                    typer.secho(f"No state found for date {run}.", fg=typer.colors.YELLOW)
                    raise typer.Exit(code=EXIT_SUCCESS)

            except ValueError as e:
                typer.secho(f"Invalid date format: {run}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM.", fg=typer.colors.RED)
                raise typer.Exit(code=EXIT_CONFIG_ERROR)

        else:
            # Default: compare to previous run
            if len(history) < 2:
                typer.secho("Only one state found. Need at least two runs to diff.", fg=typer.colors.YELLOW)
                typer.echo(f"\nLatest state: {after_state.run_at.strftime('%Y-%m-%d %H:%M')}")
                typer.echo(f"Result: {'PASSED' if after_state.summary.passed else 'FAILED'}")
                raise typer.Exit(code=EXIT_SUCCESS)

            before_state = history[1]

        # Compute diff
        diff = StateDiff.compute(before_state, after_state)

        # Render output
        if output_format == "json":
            typer.echo(diff.to_json())
        elif output_format == "llm":
            typer.echo(diff.to_llm())
        else:
            typer.echo(_render_diff_rich(diff))

        # Exit code based on regressions
        if diff.has_regressions:
            raise typer.Exit(code=EXIT_VALIDATION_FAILED)
        else:
            raise typer.Exit(code=EXIT_SUCCESS)

    except typer.Exit:
        raise

    except FileNotFoundError as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except Exception as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        if verbose:
            import traceback
            typer.secho(f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


# --------------------------------------------------------------------------- #
# Scout Diff Command
# --------------------------------------------------------------------------- #


def _render_profile_diff_rich(diff) -> str:
    """Render profile diff in human-readable format."""
    lines = []

    # Header
    lines.append(f"Profile Diff: {diff.after.source_uri}")
    lines.append(f"Comparing: {diff.before.profiled_at[:16]} → {diff.after.profiled_at[:16]}")
    lines.append("=" * 50)

    # Row count
    if diff.row_count_delta != 0:
        sign = "+" if diff.row_count_delta > 0 else ""
        lines.append(f"\nRows: {diff.row_count_before:,} → {diff.row_count_after:,} ({sign}{diff.row_count_delta:,}, {diff.row_count_pct_change:+.1f}%)")
    else:
        lines.append(f"\nRows: {diff.row_count_after:,} (unchanged)")

    # Column count
    if diff.column_count_before != diff.column_count_after:
        lines.append(f"Columns: {diff.column_count_before} → {diff.column_count_after}")

    # Schema changes
    if diff.columns_added:
        lines.append(f"\n➕ Columns Added ({len(diff.columns_added)})")
        for col in diff.columns_added[:10]:
            lines.append(f"  - {col}")
        if len(diff.columns_added) > 10:
            lines.append(f"  ... and {len(diff.columns_added) - 10} more")

    if diff.columns_removed:
        lines.append(f"\n➖ Columns Removed ({len(diff.columns_removed)})")
        for col in diff.columns_removed[:10]:
            lines.append(f"  - {col}")

    # Type changes
    if diff.dtype_changes:
        lines.append(f"\n🔄 Type Changes ({len(diff.dtype_changes)})")
        for cd in diff.dtype_changes[:10]:
            lines.append(f"  - {cd.column_name}: {cd.dtype_before} → {cd.dtype_after}")

    # Null rate increases (potential data quality issues)
    if diff.null_rate_increases:
        lines.append(f"\n⚠️  Null Rate Increases ({len(diff.null_rate_increases)})")
        for cd in diff.null_rate_increases[:10]:
            lines.append(f"  - {cd.column_name}: {cd.null_rate_before:.1%} → {cd.null_rate_after:.1%}")

    # Null rate decreases (improvements)
    if diff.null_rate_decreases:
        lines.append(f"\n✅ Null Rate Decreases ({len(diff.null_rate_decreases)})")
        for cd in diff.null_rate_decreases[:10]:
            lines.append(f"  - {cd.column_name}: {cd.null_rate_before:.1%} → {cd.null_rate_after:.1%}")

    # Cardinality changes
    if diff.cardinality_changes:
        lines.append(f"\n📊 Cardinality Changes ({len(diff.cardinality_changes)})")
        for cd in diff.cardinality_changes[:10]:
            sign = "+" if cd.distinct_count_delta > 0 else ""
            lines.append(f"  - {cd.column_name}: {cd.distinct_count_before:,} → {cd.distinct_count_after:,} ({sign}{cd.distinct_count_delta:,})")

    if not diff.has_changes:
        lines.append("\n✓ No significant changes detected")

    return "\n".join(lines)


@app.command("scout-diff")
def scout_diff_cmd(
    source: Optional[str] = typer.Argument(
        None, help="Source URI or fingerprint. If not provided, uses most recent."
    ),
    output_format: Literal["rich", "json", "llm"] = typer.Option(
        "rich", "--output-format", "-o", help="Output format."
    ),
    since: Optional[str] = typer.Option(
        None, "--since", "-s",
        help="Compare to profile from this duration ago (e.g., '7d', '24h', '1h').",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output."
    ),
) -> None:
    """
    Show changes between scout profiles over time.

    Compares the most recent profile to a previous one and shows
    schema changes, data quality shifts, and distribution changes.

    Prerequisites:
        Run `kontra scout <source> --save-profile` to save profiles.

    Examples:
        kontra scout-diff                    # Compare last two profiles
        kontra scout-diff data.parquet       # Specific source
        kontra scout-diff --since 7d         # Compare to 7 days ago
        kontra scout-diff -o llm             # Token-optimized output
    """
    try:
        from kontra.scout.store import get_default_profile_store, fingerprint_source
        from kontra.scout.types import ProfileDiff

        store = get_default_profile_store()

        # Determine source fingerprint
        source_fp = None
        if source:
            # Could be a URI or a fingerprint
            if len(source) == 16 and all(c in "0123456789abcdef" for c in source):
                source_fp = source
            else:
                source_fp = fingerprint_source(source)

        # If no source specified, find most recent
        if not source_fp:
            sources = store.list_sources()
            if not sources:
                typer.secho("No saved profiles found. Run 'kontra scout <source> --save-profile' first.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=EXIT_SUCCESS)

            # Get most recent across all sources
            most_recent = None
            most_recent_fp = None
            for fp in sources:
                latest = store.get_latest(fp)
                if latest and (most_recent is None or latest.profiled_at > most_recent.profiled_at):
                    most_recent = latest
                    most_recent_fp = fp

            if not most_recent_fp:
                typer.secho("No saved profiles found.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=EXIT_SUCCESS)

            source_fp = most_recent_fp

        # Get history for this source
        history = store.get_history(source_fp, limit=100)

        if len(history) < 1:
            typer.secho(f"No profile history found for source {source_fp}.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=EXIT_SUCCESS)

        # Determine which profiles to compare
        after_state = history[0]
        before_state = None

        if since:
            from datetime import timedelta, timezone
            try:
                seconds = _parse_duration(since)
                from datetime import datetime
                target_time = datetime.now(timezone.utc).isoformat()
                # Find target timestamp
                target_dt = datetime.fromisoformat(target_time.replace("Z", "+00:00")) - timedelta(seconds=seconds)
                target_str = target_dt.isoformat()

                for state in history[1:]:
                    if state.profiled_at <= target_str:
                        before_state = state
                        break

                if not before_state:
                    typer.secho(f"No profile found from {since} ago.", fg=typer.colors.YELLOW)
                    raise typer.Exit(code=EXIT_SUCCESS)

            except ValueError as e:
                typer.secho(f"Error: {e}", fg=typer.colors.RED)
                raise typer.Exit(code=EXIT_CONFIG_ERROR)
        else:
            # Default: compare to previous profile
            if len(history) < 2:
                typer.secho("Only one profile found. Need at least two to diff.", fg=typer.colors.YELLOW)
                typer.echo(f"\nLatest profile: {after_state.profiled_at[:16]}")
                typer.echo(f"Source: {after_state.source_uri}")
                typer.echo(f"Rows: {after_state.profile.row_count:,}, Columns: {after_state.profile.column_count}")
                raise typer.Exit(code=EXIT_SUCCESS)

            before_state = history[1]

        # Compute diff
        diff = ProfileDiff.compute(before_state, after_state)

        # Render output
        if output_format == "json":
            typer.echo(diff.to_json())
        elif output_format == "llm":
            typer.echo(diff.to_llm())
        else:
            typer.echo(_render_profile_diff_rich(diff))

        raise typer.Exit(code=EXIT_SUCCESS)

    except typer.Exit:
        raise

    except FileNotFoundError as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except Exception as e:
        from kontra.errors import format_error_for_cli
        msg = format_error_for_cli(e)
        if verbose:
            import traceback
            typer.secho(f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


# --------------------------------------------------------------------------- #
# Config Commands
# --------------------------------------------------------------------------- #


@app.command("config")
def config_cmd(
    action: str = typer.Argument(
        "show",
        help="Action: 'show' displays effective config, 'path' shows config file location.",
    ),
    env: Optional[str] = typer.Option(
        None, "--env", "-e",
        help="Environment to show (simulates --env flag).",
    ),
    output_format: Literal["yaml", "json"] = typer.Option(
        "yaml", "--output-format", "-o",
        help="Output format.",
    ),
) -> None:
    """
    Show Kontra configuration.

    Examples:
        kontra config show                  # Show effective config
        kontra config show --env production # Show with environment overlay
        kontra config path                  # Show config file path
    """
    from pathlib import Path
    from kontra.config.settings import resolve_effective_config, find_config_file

    config_path = find_config_file()

    if action == "path":
        if config_path:
            typer.echo(f"{config_path} (exists)")
        else:
            default_path = Path.cwd() / ".kontra" / "config.yml"
            typer.echo(f"{default_path} (not found)")
            typer.echo("\nRun 'kontra init' to create one.")
        raise typer.Exit(code=EXIT_SUCCESS)

    # Show effective configuration
    try:
        effective = resolve_effective_config(env_name=env)
    except Exception as e:
        from kontra.errors import format_error_for_cli
        typer.secho(f"Error: {format_error_for_cli(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    typer.secho("Effective configuration", fg=typer.colors.CYAN)
    if env:
        typer.echo(f"Environment: {env}")
    if config_path:
        typer.echo(f"Config file: {config_path}")
    else:
        typer.echo("Config file: (none, using defaults)")
    typer.echo("")

    config_dict = effective.to_dict()

    if output_format == "json":
        import json
        typer.echo(json.dumps(config_dict, indent=2))
    else:
        import yaml
        typer.echo(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))

    raise typer.Exit(code=EXIT_SUCCESS)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
