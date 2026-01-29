"""Diff commands for Kontra CLI."""

from __future__ import annotations

from typing import Literal, Optional

import typer

from kontra.cli.constants import (
    EXIT_CONFIG_ERROR,
    EXIT_RUNTIME_ERROR,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
)
from kontra.cli.renderers import render_diff_rich, render_profile_diff_rich
from kontra.cli.utils import parse_duration


def register(app: typer.Typer) -> None:
    """Register the diff and profile-diff commands with the app."""

    @app.command("diff")
    def diff_cmd(
        contract: Optional[str] = typer.Argument(
            None, help="Contract path or fingerprint. If not provided, uses most recent."
        ),
        output_format: Literal["rich", "json", "llm"] = typer.Option(
            "rich", "--output-format", "-o", help="Output format."
        ),
        since: Optional[str] = typer.Option(
            None,
            "--since",
            "-s",
            help="Compare to state from this duration ago (e.g., '7d', '24h', '1h').",
        ),
        run: Optional[str] = typer.Option(
            None,
            "--run",
            "-r",
            help="Compare to state from specific date (YYYY-MM-DD or YYYY-MM-DDTHH:MM).",
        ),
        state_backend: Optional[str] = typer.Option(
            None,
            "--state-backend",
            help="State storage backend (default: from config or 'local').",
            envvar="KONTRA_STATE_BACKEND",
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
        Compare validation results over time.

        Shows what changed between validation runs: new failures,
        resolved issues, and regressions. Use for tracking data quality trends.

        For comparing data profiles (schema, statistics), use `profile-diff`.

        Examples:
            kontra diff                           # Compare last two runs
            kontra diff --since 7d                # Compare to 7 days ago
            kontra diff --run 2024-01-12          # Compare to specific date
            kontra diff -o llm                    # Token-optimized output
            kontra diff contracts/users.yml       # Specific contract
        """
        from datetime import datetime, timedelta, timezone

        try:
            from kontra.config.settings import resolve_effective_config
            from kontra.config.loader import ContractLoader
            from kontra.state.backends import get_default_store, get_store
            from kontra.state.fingerprint import fingerprint_contract
            from kontra.state.types import StateDiff

            # --- LOAD CONFIG ---
            cli_overrides = {"state_backend": state_backend}

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
                if len(contract) == 16 and all(
                    c in "0123456789abcdef" for c in contract
                ):
                    # Looks like a fingerprint
                    contract_fp = contract
                else:
                    # Treat as path, load and compute semantic fingerprint
                    contract_obj = ContractLoader.from_path(contract)
                    contract_fp = fingerprint_contract(contract_obj)

            # If no contract specified, find most recent
            if not contract_fp:
                contracts = store.list_contracts()
                if not contracts:
                    typer.secho(
                        "No validation state found. Run 'kontra validate' first.",
                        fg=typer.colors.YELLOW,
                    )
                    raise typer.Exit(code=EXIT_SUCCESS)

                # Get most recent across all contracts
                most_recent = None
                most_recent_fp = None
                for fp in contracts:
                    latest = store.get_latest(fp)
                    if latest and (
                        most_recent is None or latest.run_at > most_recent.run_at
                    ):
                        most_recent = latest
                        most_recent_fp = fp

                if not most_recent_fp:
                    typer.secho("No validation state found.", fg=typer.colors.YELLOW)
                    raise typer.Exit(code=EXIT_SUCCESS)

                contract_fp = most_recent_fp

            # Get history for this contract
            history = store.get_history(contract_fp, limit=100)

            if len(history) < 1:
                typer.secho(
                    f"No state history found for contract {contract_fp}.",
                    fg=typer.colors.YELLOW,
                )
                raise typer.Exit(code=EXIT_SUCCESS)

            # Determine which states to compare
            after_state = history[0]  # Most recent
            before_state = None

            if since:
                # Parse duration and find state from that time ago
                try:
                    seconds = parse_duration(since)
                    target_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)

                    for state in history[1:]:
                        if state.run_at <= target_time:
                            before_state = state
                            break

                    if not before_state:
                        typer.secho(
                            f"No state found from {since} ago.", fg=typer.colors.YELLOW
                        )
                        raise typer.Exit(code=EXIT_SUCCESS)

                except ValueError as e:
                    typer.secho(f"Error: {e}", fg=typer.colors.RED)
                    raise typer.Exit(code=EXIT_CONFIG_ERROR)

            elif run:
                # Parse specific date/time
                try:
                    if "T" in run:
                        target_time = datetime.fromisoformat(
                            run.replace("Z", "+00:00")
                        )
                    else:
                        target_time = datetime.strptime(run, "%Y-%m-%d").replace(
                            tzinfo=timezone.utc
                        )

                    # Find state closest to this time
                    for state in history:
                        if state.run_at.date() <= target_time.date():
                            before_state = state
                            break

                    if not before_state:
                        typer.secho(
                            f"No state found for date {run}.", fg=typer.colors.YELLOW
                        )
                        raise typer.Exit(code=EXIT_SUCCESS)

                except ValueError:
                    typer.secho(
                        f"Invalid date format: {run}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM.",
                        fg=typer.colors.RED,
                    )
                    raise typer.Exit(code=EXIT_CONFIG_ERROR)

            else:
                # Default: compare to previous run
                if len(history) < 2:
                    typer.secho(
                        "Only one state found. Need at least two runs to diff.",
                        fg=typer.colors.YELLOW,
                    )
                    typer.echo(
                        f"\nLatest state: {after_state.run_at.strftime('%Y-%m-%d %H:%M')}"
                    )
                    typer.echo(
                        f"Result: {'PASSED' if after_state.summary.passed else 'FAILED'}"
                    )
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
                typer.echo(render_diff_rich(diff))

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

                typer.secho(
                    f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED
                )
            else:
                typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_RUNTIME_ERROR)

    @app.command("profile-diff")
    def profile_diff_cmd(
        source: Optional[str] = typer.Argument(
            None, help="Source URI or fingerprint. If not provided, uses most recent."
        ),
        output_format: Literal["rich", "json", "llm"] = typer.Option(
            "rich", "--output-format", "-o", help="Output format."
        ),
        since: Optional[str] = typer.Option(
            None,
            "--since",
            "-s",
            help="Compare to profile from this duration ago (e.g., '7d', '24h', '1h').",
        ),
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Enable verbose output."
        ),
    ) -> None:
        """
        Compare data profiles over time.

        Shows schema changes, row count deltas, null rate shifts, and
        distribution changes between profiling runs.

        For comparing validation results (pass/fail), use `diff`.

        Prerequisites:
            Run `kontra profile <source> --save-profile` to save profiles.

        Examples:
            kontra profile-diff                    # Compare last two profiles
            kontra profile-diff data.parquet       # Specific source
            kontra profile-diff --since 7d         # Compare to 7 days ago
            kontra profile-diff -o llm             # Token-optimized output
        """
        try:
            from kontra.scout.store import fingerprint_source, get_default_profile_store
            from kontra.scout.types import ProfileDiff

            store = get_default_profile_store()

            # Determine source fingerprint
            source_fp = None
            if source:
                # Could be a URI or a fingerprint
                if len(source) == 16 and all(
                    c in "0123456789abcdef" for c in source
                ):
                    source_fp = source
                else:
                    source_fp = fingerprint_source(source)

            # If no source specified, find most recent
            if not source_fp:
                sources = store.list_sources()
                if not sources:
                    typer.secho(
                        "No saved profiles found. Run 'kontra profile <source> --save-profile' first.",
                        fg=typer.colors.YELLOW,
                    )
                    raise typer.Exit(code=EXIT_SUCCESS)

                # Get most recent across all sources
                most_recent = None
                most_recent_fp = None
                for fp in sources:
                    latest = store.get_latest(fp)
                    if latest and (
                        most_recent is None
                        or latest.profiled_at > most_recent.profiled_at
                    ):
                        most_recent = latest
                        most_recent_fp = fp

                if not most_recent_fp:
                    typer.secho("No saved profiles found.", fg=typer.colors.YELLOW)
                    raise typer.Exit(code=EXIT_SUCCESS)

                source_fp = most_recent_fp

            # Get history for this source
            history = store.get_history(source_fp, limit=100)

            if len(history) < 1:
                typer.secho(
                    f"No profile history found for source {source_fp}.",
                    fg=typer.colors.YELLOW,
                )
                raise typer.Exit(code=EXIT_SUCCESS)

            # Determine which profiles to compare
            after_state = history[0]
            before_state = None

            if since:
                from datetime import datetime, timedelta, timezone

                try:
                    seconds = parse_duration(since)
                    target_dt = datetime.now(timezone.utc) - timedelta(seconds=seconds)
                    target_str = target_dt.isoformat()

                    for state in history[1:]:
                        if state.profiled_at <= target_str:
                            before_state = state
                            break

                    if not before_state:
                        typer.secho(
                            f"No profile found from {since} ago.",
                            fg=typer.colors.YELLOW,
                        )
                        raise typer.Exit(code=EXIT_SUCCESS)

                except ValueError as e:
                    typer.secho(f"Error: {e}", fg=typer.colors.RED)
                    raise typer.Exit(code=EXIT_CONFIG_ERROR)
            else:
                # Default: compare to previous profile
                if len(history) < 2:
                    typer.secho(
                        "Only one profile found. Need at least two to diff.",
                        fg=typer.colors.YELLOW,
                    )
                    typer.echo(f"\nLatest profile: {after_state.profiled_at[:16]}")
                    typer.echo(f"Source: {after_state.source_uri}")
                    typer.echo(
                        f"Rows: {after_state.profile.row_count:,}, "
                        f"Columns: {after_state.profile.column_count}"
                    )
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
                typer.echo(render_profile_diff_rich(diff))

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

                typer.secho(
                    f"Error: {msg}\n\n{traceback.format_exc()}", fg=typer.colors.RED
                )
            else:
                typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_RUNTIME_ERROR)

    # Deprecated alias for scout-diff
    @app.command("scout-diff", hidden=True)
    def scout_diff_cmd(
        source: Optional[str] = typer.Argument(None),
        output_format: Literal["rich", "json", "llm"] = typer.Option(
            "rich", "--output-format", "-o"
        ),
        since: Optional[str] = typer.Option(None, "--since", "-s"),
        verbose: bool = typer.Option(False, "--verbose", "-v"),
    ) -> None:
        """Deprecated: Use 'kontra profile-diff' instead."""
        typer.secho(
            "Warning: 'kontra scout-diff' is deprecated, use 'kontra profile-diff' instead.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        # Call profile-diff with same args
        profile_diff_cmd(source, output_format, since, verbose)