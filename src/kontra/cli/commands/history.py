# src/kontra/cli/commands/history.py
"""History command for Kontra CLI."""

from __future__ import annotations

from typing import Literal, Optional

import typer

from kontra.cli.constants import (
    EXIT_CONFIG_ERROR,
    EXIT_RUNTIME_ERROR,
    EXIT_SUCCESS,
)


def register(app: typer.Typer) -> None:
    """Register the history command with the app."""

    @app.command("history")
    def history(
        contract: str = typer.Argument(
            ..., help="Path to the contract.yml file"
        ),
        since: Optional[str] = typer.Option(
            None,
            "--since",
            "-s",
            help="Time filter: '24h', '7d', or date like '2026-01-15' (default: all)",
        ),
        limit: int = typer.Option(
            20,
            "--limit",
            "-n",
            help="Maximum runs to show (default: 20)",
        ),
        failed_only: bool = typer.Option(
            False,
            "--failed-only",
            "-f",
            help="Only show failed runs",
        ),
        output_format: Literal["table", "json"] = typer.Option(
            "table",
            "--output-format",
            "-o",
            help="Output format (default: table)",
        ),
        verbose: bool = typer.Option(
            False, "--verbose", "-v", help="Show additional details"
        ),
    ) -> None:
        """
        Show validation history for a contract.

        Displays past validation runs with timestamps, pass/fail status,
        and violation counts. Useful for tracking data quality over time.

        Examples:
            kontra history contract.yml
            kontra history contract.yml --since 7d
            kontra history contract.yml --failed-only
            kontra history contract.yml -o json
        """
        import json
        import os

        if verbose:
            os.environ["KONTRA_VERBOSE"] = "1"

        try:
            import kontra

            runs = kontra.get_history(
                contract,
                limit=limit,
                since=since,
                failed_only=failed_only,
            )

            if not runs:
                typer.echo("No validation history found for this contract.")
                raise typer.Exit(code=EXIT_SUCCESS)

            if output_format == "json":
                typer.echo(json.dumps(runs, indent=2, default=str))
            else:
                _render_table(runs, contract, verbose)

            raise typer.Exit(code=EXIT_SUCCESS)

        except typer.Exit:
            raise

        except FileNotFoundError as e:
            from kontra.errors import format_error_for_cli

            msg = format_error_for_cli(e)
            typer.secho(f"Error: {msg}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        except ValueError as e:
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
                typer.secho("Use --verbose for full traceback.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=EXIT_RUNTIME_ERROR)


def _render_table(runs: list, contract: str, verbose: bool) -> None:
    """Render history as a Rich table."""
    from datetime import datetime

    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()

        # Header
        contract_name = runs[0].get("contract_name", contract) if runs else contract
        console.print(f"\n[bold]Validation History: {contract_name}[/bold]")
        console.print(f"Showing {len(runs)} most recent runs\n")

        # Table
        table = Table(show_header=True, header_style="bold")
        table.add_column("Timestamp", style="dim")
        table.add_column("Status", justify="center")
        table.add_column("Failed", justify="right")
        table.add_column("Rows", justify="right")
        if verbose:
            table.add_column("Run ID", style="dim")

        for run in runs:
            # Parse timestamp
            ts = run.get("timestamp", "")
            if isinstance(ts, str):
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    ts_display = dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    ts_display = ts[:16]
            else:
                ts_display = str(ts)[:16]

            # Status
            passed = run.get("passed", False)
            if passed:
                status = "[green]PASS[/green]"
            else:
                status = "[red]FAIL[/red]"

            # Failed count
            failed_count = run.get("failed_count", 0)
            failed_display = str(failed_count) if failed_count > 0 else "-"

            # Rows
            total_rows = run.get("total_rows")
            if total_rows is not None:
                rows_display = f"{total_rows:,}"
            else:
                rows_display = "-"

            if verbose:
                run_id = run.get("run_id", "-")
                table.add_row(ts_display, status, failed_display, rows_display, run_id)
            else:
                table.add_row(ts_display, status, failed_display, rows_display)

        console.print(table)

    except ImportError:
        # Fallback to plain text if Rich not available
        typer.echo(f"\nValidation History: {contract}")
        typer.echo(f"Showing {len(runs)} most recent runs\n")
        typer.echo(f"{'Timestamp':<20} {'Status':<8} {'Failed':<8} {'Rows':<12}")
        typer.echo("-" * 50)

        for run in runs:
            ts = run.get("timestamp", "")[:16]
            passed = "PASS" if run.get("passed", False) else "FAIL"
            failed = str(run.get("failed_count", 0))
            rows = str(run.get("total_rows", "-"))
            typer.echo(f"{ts:<20} {passed:<8} {failed:<8} {rows:<12}")
