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

    # NEW: explicit validated vs loaded columns (short previews)
    validated = stats.get("columns_validated") or []
    loaded = stats.get("columns_loaded") or []

    if validated:
        v_preview = ", ".join(validated[:6]) + ("…" if len(validated) > 6 else "")
        typer.secho(f"Columns validated ({len(validated)}): {v_preview}", fg=typer.colors.BLUE)

    if loaded:
        l_preview = ", ".join(loaded[:6]) + ("…" if len(loaded) > 6 else "")
        typer.secho(f"Columns loaded ({len(loaded)}): {l_preview}", fg=typer.colors.BLUE)

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
            msg = f"Projection [{'on' if enabled else 'off'}]: {required}/{loaded_cnt} (req/loaded) {effectiveness}"
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
    output_format: Literal["rich", "json"] = typer.Option(
        "rich", "--output-format", "-o", help="Output format."
    ),
    stats: Literal["none", "summary", "profile"] = typer.Option(
        "none",
        "--stats",
        help="Attach run statistics (summary) or lightweight per-column profile.",
    ),
    # New, explicit toggle matching engine semantics
    pushdown: Optional[Literal["auto", "off"]] = typer.Option(
        None,
        "--pushdown",
        help="SQL pushdown: 'auto' (default) enables pushdown; 'off' disables it.",
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
        help="If pushdown is enabled, print the generated SQL for debugging.",
    ),
    no_projection: bool = typer.Option(
        False,
        "--no-projection",
        help="Disable column projection/pruning (load all columns).",
    ),
    no_actions: bool = typer.Option(  # reserved for future wiring
        False,
        "--no-actions",
        help="Run without executing remediation actions (placeholder).",
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
        emit_report = output_format == "rich"

        # Determine effective pushdown choice (new flag wins, else fall back)
        if pushdown is None:
            effective_pushdown: Literal["auto", "off"] = "off" if sql_engine == "none" else "auto"
        else:
            effective_pushdown = pushdown

        eng = ValidationEngine(
            contract_path=contract,
            data_path=data,
            emit_report=emit_report,
            stats_mode=stats,
            enable_projection=not no_projection,
            # Pass both for back-compat; engine normalizes internally
            pushdown=effective_pushdown,
            sql_engine=sql_engine,
            show_plan=show_plan,
        )
        result = eng.run()

        if output_format == "json":
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
            if stats != "none":
                _print_rich_stats(result.get("stats"))

        raise typer.Exit(
            code=EXIT_SUCCESS if result["summary"]["passed"] else EXIT_VALIDATION_FAILED
        )

    except typer.Exit:
        raise

    except FileNotFoundError as e:
        if verbose:
            typer.secho(f"[CONFIG_ERROR] {e}", fg=typer.colors.RED)
        else:
            typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_CONFIG_ERROR)

    except Exception as e:
        if verbose:
            typer.secho(f"[RUNTIME_ERROR] {repr(e)}", fg=typer.colors.RED)
        else:
            typer.secho("An unexpected error occurred. Use --verbose for details.", fg=typer.colors.RED)
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
