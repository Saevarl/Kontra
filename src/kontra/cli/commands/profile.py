"""Profile command for Kontra CLI."""

from __future__ import annotations

from typing import Literal, Optional

import typer

from kontra.cli.constants import (
    EXIT_CONFIG_ERROR,
    EXIT_RUNTIME_ERROR,
    EXIT_SUCCESS,
)


def register(app: typer.Typer) -> None:
    """Register the profile command with the app."""

    @app.command("profile")
    def profile(
        source: str = typer.Argument(
            ..., help="Path or URI to the dataset (local file, s3://..., https://...)"
        ),
        output_format: Optional[Literal["rich", "json", "markdown", "llm"]] = typer.Option(
            None, "--output-format", "-o", help="Output format (default: 'rich')."
        ),
        # Config-aware options
        preset: Optional[Literal["scout", "scan", "interrogate"]] = typer.Option(
            None,
            "--preset",
            "-p",
            help="Profiling depth (default: from config or 'scan').",
        ),
        list_values_threshold: Optional[int] = typer.Option(
            None,
            "--list-values-threshold",
            "-l",
            help="List all values if distinct count <= threshold.",
        ),
        top_n: Optional[int] = typer.Option(
            None,
            "--top-n",
            "-t",
            help="Show top N most frequent values per column.",
        ),
        sample: Optional[int] = typer.Option(
            None,
            "--sample",
            "-s",
            help="Sample N rows for profiling (default: all rows).",
        ),
        include_patterns: Optional[bool] = typer.Option(
            None,
            "--include-patterns",
            help="Detect common patterns (default: from config or False).",
        ),
        columns: Optional[str] = typer.Option(
            None,
            "--columns",
            "-c",
            help="Comma-separated list of columns to profile (default: all).",
        ),
        draft: bool = typer.Option(
            False,
            "--draft",
            help="Generate draft validation rules based on profile.",
        ),
        save_profile: Optional[bool] = typer.Option(
            None,
            "--save-profile",
            help="Save profile to state storage (default: from config or False).",
        ),
        # Environment selection
        env: Optional[str] = typer.Option(
            None,
            "--env",
            "-e",
            help="Environment profile from .kontra/config.yml.",
            envvar="KONTRA_ENV",
        ),
        storage_options: Optional[str] = typer.Option(
            None,
            "--storage-options",
            help='Cloud storage credentials as JSON, e.g. \'{"aws_access_key_id": "...", "aws_region": "us-east-1"}\'',
        ),
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Enable verbose output."
        ),
    ) -> None:
        """
        Profile a dataset to understand its structure and statistics.

        Generates column-level statistics including types, null rates,
        distinct counts, and value distributions.

        Presets control profiling depth:
          - scout: Quick recon. Metadata only (schema, row count, null/distinct counts).
          - scan: Systematic pass. Full stats with moderate top values. [default]
          - interrogate: Deep investigation. Everything including percentiles.

        Examples:
            kontra profile data.parquet
            kontra profile s3://bucket/data.csv --sample 10000
            kontra profile data.parquet -o json --preset interrogate
            kontra profile data.parquet --draft > rules.yml
            kontra profile data.parquet --save-profile  # Save for diffing
        """
        _run_profile(
            source=source,
            output_format=output_format,
            preset=preset,
            list_values_threshold=list_values_threshold,
            top_n=top_n,
            sample=sample,
            include_patterns=include_patterns,
            columns=columns,
            draft=draft,
            save_profile=save_profile,
            env=env,
            storage_options=storage_options,
            verbose=verbose,
        )



def _run_profile(
    source: str,
    output_format: Optional[str],
    preset: Optional[str],
    list_values_threshold: Optional[int],
    top_n: Optional[int],
    sample: Optional[int],
    include_patterns: Optional[bool],
    columns: Optional[str],
    draft: bool,
    save_profile: Optional[bool],
    env: Optional[str],
    storage_options: Optional[str],
    verbose: bool,
) -> None:
    """Shared implementation for profile and scout commands."""
    import os

    if verbose:
        os.environ["KONTRA_VERBOSE"] = "1"

    try:
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
            config = resolve_effective_config(
                env_name=env, cli_overrides=cli_overrides
            )
        except Exception as e:
            from kontra.errors import format_error_for_cli

            typer.secho(
                f"Config error: {format_error_for_cli(e)}", fg=typer.colors.RED
            )
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        # Resolve effective values from config
        effective_preset = config.scout_preset
        effective_save_profile = config.scout_save_profile
        effective_list_values_threshold = config.scout_list_values_threshold
        effective_top_n = config.scout_top_n
        effective_include_patterns = config.scout_include_patterns

        # --- RESOLVE DATASOURCE ---
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

        # Output format defaults
        effective_output_format = output_format or "rich"

        from kontra.scout.profiler import ScoutProfiler

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

        profiler = ScoutProfiler(
            resolved_source,
            preset=effective_preset,
            list_values_threshold=effective_list_values_threshold,
            top_n=effective_top_n,
            sample_size=sample,
            include_patterns=effective_include_patterns,
            columns=cols_filter,
            storage_options=parsed_storage_options,
        )

        # Show progress spinner for non-JSON output
        if effective_output_format not in ("json", "llm"):
            from rich.console import Console
            from rich.status import Status
            console = Console()
            with Status("Profiling...", console=console, spinner="dots"):
                profile_result = profiler.profile()
        else:
            profile_result = profiler.profile()

        # Save profile if requested
        if effective_save_profile:
            from kontra.scout.store import (
                create_profile_state,
                get_default_profile_store,
            )

            state = create_profile_state(profile_result)
            store = get_default_profile_store()
            store.save(state)
            typer.secho(
                f"Profile saved (fingerprint: {state.source_fingerprint})",
                fg=typer.colors.GREEN,
            )

        # Handle rule draft/suggestions
        if draft:
            from kontra.scout.suggest import generate_rules_yaml

            output = generate_rules_yaml(profile_result)
        else:
            from kontra.scout.reporters import render_profile

            output = render_profile(profile_result, format=effective_output_format)

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

            typer.secho(
                f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED
            )
        else:
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            typer.secho("Use --verbose for full traceback.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)
