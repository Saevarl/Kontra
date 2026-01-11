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
    # Independent execution controls
    preplan: Literal["on", "off", "auto"] = typer.Option(
        "auto",
        "--preplan",
        help="Metadata preflight (Parquet min/max/null counts): 'on' to force, 'off' to disable, 'auto' to try when applicable.",
    ),
    pushdown: Literal["on", "off", "auto"] = typer.Option(
        "auto",
        "--pushdown",
        help="SQL pushdown: 'on' forces pushdown, 'off' disables it, 'auto' lets the executor decide.",
    ),
    projection: Literal["on", "off"] = typer.Option(
        "on",
        "--projection",
        help="Column projection/pruning at source: 'on' (default) or 'off' (load all columns).",
    ),
    # CSV handling (argument form; replaces env-only control)
    csv_mode: Literal["auto", "duckdb", "parquet"] = typer.Option(
        "auto",
        "--csv-mode",
        help="CSV handling for I/O and pushdown: "
        "'auto' (try DuckDB CSV, fallback to staging), "
        "'duckdb' (DuckDB read_csv_auto only), "
        "'parquet' (stage CSV → Parquet first).",
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
            effective_pushdown = pushdown if pushdown in {"on", "off", "auto"} else "auto"

        # Effective preplan
        effective_preplan: Literal["on", "off", "auto"]
        effective_preplan = preplan if preplan in {"on", "off", "auto"} else "auto"

        # Effective projection
        enable_projection = projection == "on"

        eng = ValidationEngine(
            contract_path=contract,
            data_path=data,
            emit_report=emit_report,
            stats_mode=stats,
            # Independent controls
            preplan=effective_preplan,            # NEW: metadata preflight
            pushdown=effective_pushdown,          # SQL pushdown
            enable_projection=enable_projection,  # bool
            csv_mode=csv_mode,                    # "auto" | "duckdb" | "parquet"
            # Diagnostics
            show_plan=show_plan,
            explain_preplan=explain_preplan,
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
            typer.secho(
                "An unexpected error occurred. Use --verbose for details.",
                fg=typer.colors.RED,
            )
        raise typer.Exit(code=EXIT_RUNTIME_ERROR)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
