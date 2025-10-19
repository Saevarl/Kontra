# src/contra/cli/main.py
from __future__ import annotations

"""
Contra CLI — Developer-first Data Quality Engine

This module defines the public CLI entrypoints powered by Typer.
It keeps the CLI thin: argument parsing, delegating to the engine,
and handing off machine-readable output to reporters.
"""

from typing import Optional, Literal

import typer

from contra.engine.engine import ValidationEngine
from contra.reporters.json_reporter import render_json
from contra.version import VERSION

app = typer.Typer(help="Contra CLI — Developer-first Data Quality Engine")

# Exit codes (keep stable for CI/CD)
EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILED = 1
EXIT_CONFIG_ERROR = 2
EXIT_RUNTIME_ERROR = 3


@app.callback()
def _version(
    version: Optional[bool] = typer.Option(
        None, "--version", help="Show the Contra version and exit.", is_eager=True
    )
) -> None:
    """
    Global --version flag. Exits immediately when present.
    """
    if version:
        typer.echo(f"contra {VERSION}")
        raise typer.Exit(code=0)


def _print_rich_stats(stats: dict | None) -> None:
    """
    Pretty-print the (optional) stats block in rich mode.
    """
    if not stats:
        return

    ds = stats.get("dataset", {})
    run = stats.get("run_meta", {})
    cols = stats.get("columns_touched", [])
    proj = stats.get("projection")

    nrows = ds.get("nrows")
    ncols = ds.get("ncols")
    dur = run.get("duration_ms_total")
    engine = run.get("engine")
    if nrows is not None and ncols is not None and dur is not None:
        base = f"\nStats  •  rows={nrows:,}  cols={ncols}  duration={dur} ms"
        if engine:
            base += f"  engine={engine}"
        typer.secho(base, fg=typer.colors.BLUE)
    elif nrows is not None and ncols is not None:
        typer.secho(f"\nStats  •  rows={nrows:,}  cols={ncols}", fg=typer.colors.BLUE)

    if cols:
        preview = ", ".join(cols[:6]) + ("…" if len(cols) > 6 else "")
        typer.secho(f"Columns touched: {preview}", fg=typer.colors.BLUE)

    if proj:
        enabled = proj.get("enabled", True)
        required = proj.get("required_count", 0)
        loaded = proj.get("loaded_count", 0)
        available = proj.get("available_count")  # may be absent
        effectiveness = "(pruned)" if proj.get("effective") else "(no reduction)"
        if available is not None:
            msg = (
                f"Projection [{'on' if enabled else 'off'}]: "
                f"{required}/{loaded}/{available} (req/loaded/avail) {effectiveness}"
            )
        else:
            msg = f"Projection [{'on' if enabled else 'off'}]: {required}/{loaded} (req/loaded) {effectiveness}"
        typer.secho(msg, fg=typer.colors.BLUE)

    prof = stats.get("profile")
    if prof:
        typer.secho("Profile:", fg=typer.colors.BLUE)
        for col, s in prof.items():
            parts = [f"nulls={s.get('nulls', 0)}", f"distinct={s.get('distinct', 0)}"]
            if {"min", "max", "mean"} <= s.keys():
                parts += [f"min={s['min']}", f"max={s['max']}", f"mean={round(s['mean'], 3)}"]
            typer.echo(f"  - {col}: " + ", ".join(parts))


@app.command("validate")
def validate(
    contract: str = typer.Argument(..., help="Path or URI to the contract.yml (local or s3://…)"),
    data: Optional[str] = typer.Option(
        None, "--data", help="Optional dataset path/URI override (e.g., data/users.parquet or s3://bucket/key)"
    ),
    output_format: Literal["rich", "json"] = typer.Option(
        "rich", "--output-format", "-o", help="Output format."
    ),
    stats: Literal["none", "summary", "profile"] = typer.Option(
        "none", "--stats", help="Attach run statistics (summary) or lightweight per-column profile."
    ),
    engine: Literal["polars", "duckdb"] = typer.Option(  # NEW
        "polars",
        "--engine",
        help="Execution engine: 'polars' (default) or 'duckdb' (hybrid: SQL-able rules via DuckDB, rest via Polars).",
    ),
    show_plan: bool = typer.Option(  # NEW
        False,
        "--show-plan",
        help="When using --engine duckdb, print the generated DuckDB SQL for debugging.",
    ),
    no_projection: bool = typer.Option(
        False,
        "--no-projection",
        help="Disable column projection/pruning (load all columns). Useful for debugging and perf baselines.",
    ),
    no_actions: bool = typer.Option(  # reserved for future wiring
        False, "--no-actions", help="Run without executing remediation actions (placeholder)."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose errors."),
) -> None:
    """
    Validate data against a declarative contract.

    The CLI stays declarative and stateless:
    - Delegates execution to ValidationEngine.
    - Uses reporters for machine-readable output (JSON).
    - Prints human-friendly Rich output otherwise.
    """
    del no_actions  # placeholder until actions are wired

    try:
        # In JSON mode suppress Rich banners; reporters own serialized output.
        emit_report = output_format == "rich"

        eng = ValidationEngine(
            contract_path=contract,
            data_path=data,
            emit_report=emit_report,
            stats_mode=stats,
            enable_projection=not no_projection,
            engine=engine,
            show_plan=show_plan,
        )
        result = eng.run()

        if output_format == "json":
            # Delegate shape + determinism to JSONReporter
            payload = render_json(
                dataset_name=result["summary"]["dataset_name"],
                summary=result["summary"],
                results=result["results"],
                stats=result.get("stats"),
                quarantine=result.get("summary", {}).get("quarantine"),
                validate=False,  # flip to True once local JSON Schema & validator are bundled
            )
            typer.echo(payload)
        else:
            # Human-readable extras
            if stats != "none":
                _print_rich_stats(result.get("stats"))

        # Exit with CI-stable codes
        exit_code = EXIT_SUCCESS if result["summary"]["passed"] else EXIT_VALIDATION_FAILED
        raise typer.Exit(code=exit_code)

    except typer.Exit:
        # Let Typer-controlled exits pass through unchanged
        raise

    except FileNotFoundError as e:
        # Contract/data path issues → CONFIG error
        if verbose:
            typer.secho(f"[CONFIG_ERROR] {e}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except Exception as e:
        # Unknown failures → RUNTIME error
        if verbose:
            typer.secho(f"[RUNTIME_ERROR] {repr(e)}", fg=typer.colors.RED)
        else:
            typer.secho("An unexpected error occurred. Use --verbose for details.", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
