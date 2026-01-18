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
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Enable verbose output."
        ),
    ) -> None:
        """
        Profile a dataset (Kontra Profile).

        Generates comprehensive column-level statistics optimized for
        developer exploration and LLM context compression.

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
            verbose=verbose,
        )

    # Deprecated alias: `kontra scout` -> `kontra profile`
    @app.command("scout", hidden=True)
    def scout(
        source: str = typer.Argument(
            ..., help="Path or URI to the dataset"
        ),
        output_format: Optional[Literal["rich", "json", "markdown", "llm"]] = typer.Option(
            None, "--output-format", "-o", help="Output format."
        ),
        preset: Optional[str] = typer.Option(
            None, "--preset", "-p", help="Profiling depth."
        ),
        list_values_threshold: Optional[int] = typer.Option(
            None, "--list-values-threshold", "-l", help="List values threshold."
        ),
        top_n: Optional[int] = typer.Option(
            None, "--top-n", "-t", help="Top N values."
        ),
        sample: Optional[int] = typer.Option(
            None, "--sample", "-s", help="Sample N rows."
        ),
        include_patterns: Optional[bool] = typer.Option(
            None, "--include-patterns", help="Detect patterns."
        ),
        columns: Optional[str] = typer.Option(
            None, "--columns", "-c", help="Columns to profile."
        ),
        suggest_rules: bool = typer.Option(
            False, "--suggest-rules", help="Generate suggested rules."
        ),
        save_profile: Optional[bool] = typer.Option(
            None, "--save-profile", help="Save profile."
        ),
        env: Optional[str] = typer.Option(
            None, "--env", "-e", help="Environment.", envvar="KONTRA_ENV"
        ),
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Verbose output."
        ),
    ) -> None:
        """
        DEPRECATED: Use 'kontra profile' instead.

        Profile a dataset without a contract.
        """
        import warnings
        warnings.warn(
            "'kontra scout' is deprecated, use 'kontra profile' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        typer.secho(
            "Warning: 'kontra scout' is deprecated, use 'kontra profile' instead",
            fg=typer.colors.YELLOW,
            err=True,
        )

        # Map old preset names to new
        mapped_preset = preset
        if preset in ("lite", "standard", "deep", "llm"):
            preset_map = {"lite": "scout", "standard": "scan", "deep": "interrogate", "llm": "scan"}
            mapped_preset = preset_map.get(preset, preset)
            typer.secho(
                f"Warning: preset '{preset}' is deprecated, use '{mapped_preset}' instead",
                fg=typer.colors.YELLOW,
                err=True,
            )

        _run_profile(
            source=source,
            output_format=output_format,
            preset=mapped_preset,
            list_values_threshold=list_values_threshold,
            top_n=top_n,
            sample=sample,
            include_patterns=include_patterns,
            columns=columns,
            draft=suggest_rules,
            save_profile=save_profile,
            env=env,
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

        profiler = ScoutProfiler(
            resolved_source,
            preset=effective_preset,
            list_values_threshold=effective_list_values_threshold,
            top_n=effective_top_n,
            sample_size=sample,
            include_patterns=effective_include_patterns,
            columns=cols_filter,
        )

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
