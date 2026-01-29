"""Validate command for Kontra CLI."""

from __future__ import annotations

from typing import Literal, Optional

import typer

from kontra.cli.constants import (
    EXIT_CONFIG_ERROR,
    EXIT_RUNTIME_ERROR,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
)
from kontra.cli.renderers import print_rich_stats
from kontra.errors import ContractNotFoundError


def handle_dry_run(contract_path: str, data_path: Optional[str], verbose: bool) -> None:
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
    from kontra.rule_defs.factory import RuleFactory
    from kontra.rule_defs.registry import get_all_rule_names

    # Import built-in rules to populate registry
    import kontra.rule_defs.builtin.allowed_values  # noqa: F401
    import kontra.rule_defs.builtin.disallowed_values  # noqa: F401
    import kontra.rule_defs.builtin.compare  # noqa: F401
    import kontra.rule_defs.builtin.conditional_not_null  # noqa: F401
    import kontra.rule_defs.builtin.conditional_range  # noqa: F401
    import kontra.rule_defs.builtin.contains  # noqa: F401
    import kontra.rule_defs.builtin.custom_sql_check  # noqa: F401
    import kontra.rule_defs.builtin.dtype  # noqa: F401
    import kontra.rule_defs.builtin.ends_with  # noqa: F401
    import kontra.rule_defs.builtin.freshness  # noqa: F401
    import kontra.rule_defs.builtin.length  # noqa: F401
    import kontra.rule_defs.builtin.max_rows  # noqa: F401
    import kontra.rule_defs.builtin.min_rows  # noqa: F401
    import kontra.rule_defs.builtin.not_null  # noqa: F401
    import kontra.rule_defs.builtin.range  # noqa: F401
    import kontra.rule_defs.builtin.regex  # noqa: F401
    import kontra.rule_defs.builtin.starts_with  # noqa: F401
    import kontra.rule_defs.builtin.unique  # noqa: F401

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
        typer.secho(
            f"  ✓ Contract syntax valid: {contract_path}", fg=typer.colors.GREEN
        )
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
    dataset_uri = data_path or contract.datasource
    try:
        handle = DatasetHandle.from_uri(dataset_uri)
        scheme_info = f" ({handle.scheme})" if handle.scheme else ""
        typer.secho(
            f"  ✓ Dataset URI parseable{scheme_info}: {dataset_uri}",
            fg=typer.colors.GREEN,
        )
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
        rule_name = (
            rule_spec.name.split(":")[-1] if ":" in rule_spec.name else rule_spec.name
        )
        if rule_name not in known_rules:
            unrecognized_rules.append(rule_spec.name)

    if unrecognized_rules:
        typer.secho(
            f"  ✗ {len(unrecognized_rules)} unrecognized rule(s): {', '.join(unrecognized_rules)}",
            fg=typer.colors.RED,
        )
        typer.secho(
            f"    Known rules: {', '.join(sorted(known_rules))}", fg=typer.colors.YELLOW
        )
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

        # Check for tally/exists implementation warnings
        warnings = []
        for r in rules:
            rule_tally = getattr(r, "tally", None)
            has_sql_agg = callable(getattr(r, "to_sql_agg", None))
            has_sql_exists = callable(getattr(r, "to_sql_exists", None))

            # Warn if tally=False but no to_sql_exists (will use COUNT, no early termination)
            if rule_tally is False and has_sql_agg and not has_sql_exists:
                warnings.append(
                    f"Rule '{r.rule_id}' has tally=False but no to_sql_exists() - "
                    f"will use COUNT (no early termination benefit)"
                )

        if warnings:
            typer.secho(f"  ⚠ {len(warnings)} tally warning(s):", fg=typer.colors.YELLOW)
            for w in warnings:
                typer.echo(f"    - {w}")

        # Show rule breakdown
        if verbose:
            typer.echo("\n  Rules:")
            for r in rules:
                cols = getattr(r, "params", {}).get("column", "")
                col_info = f" ({cols})" if cols else ""
                tally_info = ""
                if hasattr(r, "tally") and r.tally is not None:
                    tally_info = f" [tally={r.tally}]"
                typer.echo(f"    - {r.name}{col_info}{tally_info}")

    except Exception as e:
        typer.secho(f"  ✗ Rule validation failed: {e}", fg=typer.colors.RED)
        issues.append(f"Rule validation: {e}")
        checks_failed += 1

    # Summary
    typer.echo("")
    if checks_failed == 0:
        typer.secho(
            f"✓ Ready to validate ({checks_passed} checks passed)", fg=typer.colors.GREEN
        )
        typer.echo(f"\nRun without --dry-run to execute:")
        typer.echo(f"  kontra validate {contract_path}")
        raise typer.Exit(code=EXIT_SUCCESS)
    else:
        typer.secho(
            f"✗ Validation would fail ({checks_failed} issues)", fg=typer.colors.RED
        )
        for issue in issues:
            typer.echo(f"  - {issue}")
        raise typer.Exit(code=EXIT_CONFIG_ERROR)


def register(app: typer.Typer) -> None:
    """Register the validate command with the app."""

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
            None,
            "--output-format",
            "-o",
            help="Output format (default: from config or 'rich').",
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
        tally: Optional[bool] = typer.Option(
            None,
            "--tally/--no-tally",
            help="Global tally override. --tally = count all violations (exact), --no-tally = early-stop (fast).",
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
            "--env",
            "-e",
            help="Environment profile from .kontra/config.yml.",
            envvar="KONTRA_ENV",
        ),
        # Back-compat alias (deprecated): maps 'none' => pushdown=off
        sql_engine: Literal["auto", "none"] = typer.Option(
            "auto",
            "--sql-engine",
            hidden=True,  # Deprecated - hidden from help
            help="(deprecated) Use '--pushdown off' instead.",
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
        storage_options: Optional[str] = typer.Option(
            None,
            "--storage-options",
            help='Cloud storage credentials as JSON, e.g. \'{"aws_access_key_id": "...", "aws_region": "us-east-1"}\'',
        ),
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Enable verbose errors."
        ),
    ) -> None:
        """
        Validate data against a declarative contract.

        Examples:

            kontra validate contract.yml

            kontra validate contract.yml --data prod.parquet

            kontra validate contract.yml -o json

            kontra validate contract.yml --dry-run

        Exit codes: 0=passed, 1=failed, 2=config error, 3=runtime error
        """
        del no_actions  # placeholder until actions are wired

        # Validate contract path is not empty
        if not contract or not contract.strip():
            typer.secho("Error: Contract path cannot be empty", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        try:
            # --- DRY RUN MODE ---
            if dry_run:
                handle_dry_run(contract, data, verbose)
                return

            # --- LOAD CONFIG ---
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
                config = resolve_effective_config(
                    env_name=env, cli_overrides=cli_overrides
                )
            except Exception as e:
                from kontra.errors import format_error_for_cli

                typer.secho(
                    f"Config error: {format_error_for_cli(e)}", fg=typer.colors.RED
                )
                raise typer.Exit(code=EXIT_CONFIG_ERROR)

            # Use resolved config values
            effective_output_format = config.output_format
            effective_stats = config.stats
            effective_csv_mode = config.csv_mode
            effective_state_backend = config.state_backend

            # --- RESOLVE DATASOURCE ---
            from kontra.config.settings import resolve_datasource

            resolved_data = data
            if data:
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
            if (
                effective_state_backend
                and effective_state_backend != "local"
                and not no_state
            ):
                from kontra.state.backends import get_store

                state_store = get_store(effective_state_backend)

            from kontra.engine.engine import ValidationEngine

            # Parse storage_options JSON if provided
            parsed_storage_options = None
            if storage_options:
                import json
                try:
                    parsed_storage_options = json.loads(storage_options)
                except json.JSONDecodeError as e:
                    typer.secho(
                        f"Invalid --storage-options JSON: {e}",
                        fg=typer.colors.RED,
                    )
                    raise typer.Exit(code=EXIT_CONFIG_ERROR)

            eng = ValidationEngine(
                contract_path=contract,
                data_path=resolved_data,
                emit_report=emit_report,
                stats_mode=effective_stats,
                # Independent controls
                preplan=effective_preplan,
                pushdown=effective_pushdown,
                tally=tally,
                tally_is_override=(tally is not None),  # CLI flag overrides per-rule
                enable_projection=enable_projection,
                csv_mode=effective_csv_mode,
                # Diagnostics
                show_plan=show_plan,
                explain_preplan=explain_preplan,
                # State management
                state_store=state_store,
                save_state=not no_state,
                # Cloud storage
                storage_options=parsed_storage_options,
            )

            # Show progress spinner for non-JSON output
            if effective_output_format != "json":
                from rich.console import Console
                from rich.status import Status
                console = Console()
                with Status("Validating...", console=console, spinner="dots"):
                    result = eng.run()
            else:
                result = eng.run()

            if effective_output_format == "json":
                from kontra.reporters.json_reporter import render_json

                payload = render_json(
                    dataset_name=result["summary"]["dataset_name"],
                    summary=result["summary"],
                    results=result["results"],
                    stats=result.get("stats"),
                    quarantine=result.get("summary", {}).get("quarantine"),
                    validate=False,
                )
                typer.echo(payload)
            else:
                if effective_stats != "none":
                    print_rich_stats(result.get("stats"))

            raise typer.Exit(
                code=EXIT_SUCCESS
                if result["summary"]["passed"]
                else EXIT_VALIDATION_FAILED
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

        except ContractNotFoundError as e:
            from kontra.errors import format_error_for_cli

            msg = format_error_for_cli(e)
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            if verbose:
                import traceback

                typer.secho(f"\n{traceback.format_exc()}", fg=typer.colors.YELLOW)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        except ValueError as e:
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

                typer.secho(
                    f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED
                )
            else:
                typer.secho(f"Error: {msg}", fg=typer.colors.RED)
                typer.secho("Use --verbose for full traceback.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=EXIT_RUNTIME_ERROR)
