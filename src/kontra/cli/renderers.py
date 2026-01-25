"""CLI output rendering functions."""

from __future__ import annotations

import typer


def print_rich_stats(stats: dict | None) -> None:
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
            typer.secho(
                f"Preplan manifest: row-groups {kept}/{total} kept",
                fg=typer.colors.BLUE,
            )

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


def render_diff_rich(diff) -> str:
    """Render validation diff in human-readable format."""
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
    lines.append(
        f"\nRules: {diff.before.summary.passed_rules}/{diff.before.summary.total_rules} → "
        f"{diff.after.summary.passed_rules}/{diff.after.summary.total_rules}"
    )

    # New failures - group by severity
    if diff.new_failures:
        # Separate by severity
        blocking = [rd for rd in diff.new_failures if rd.severity == "blocking"]
        warnings = [rd for rd in diff.new_failures if rd.severity == "warning"]
        infos = [rd for rd in diff.new_failures if rd.severity == "info"]

        if blocking:
            lines.append(f"\n❌ New Blocking Failures ({len(blocking)})")
            for rd in blocking:
                count_info = (
                    f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                )
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}{count_info}{mode_info}")

        if warnings:
            lines.append(f"\n⚠️  New Warnings ({len(warnings)})")
            for rd in warnings:
                count_info = (
                    f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                )
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(f"  - {rd.rule_id}{count_info}{mode_info}")

        if infos:
            lines.append(f"\nℹ️  New Info Issues ({len(infos)})")
            for rd in infos:
                count_info = (
                    f" ({rd.after_count:,} violations)" if rd.after_count > 0 else ""
                )
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
                lines.append(
                    f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}"
                )

        if warning_reg:
            lines.append(f"\n⚠️  Warning Regressions ({len(warning_reg)})")
            for rd in warning_reg:
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(
                    f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}"
                )

        if info_reg:
            lines.append(f"\nℹ️  Info Regressions ({len(info_reg)})")
            for rd in info_reg:
                mode_info = f" [{rd.failure_mode}]" if rd.failure_mode else ""
                lines.append(
                    f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} (+{rd.delta:,}){mode_info}"
                )

    # Resolved
    if diff.resolved:
        lines.append(f"\n✅ Resolved ({len(diff.resolved)})")
        for rd in diff.resolved:
            lines.append(f"  - {rd.rule_id}")

    # Improvements
    if diff.improvements:
        lines.append(f"\n📈 Improvements ({len(diff.improvements)})")
        for rd in diff.improvements:
            lines.append(
                f"  - {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} ({rd.delta:,})"
            )

    # No changes
    if (
        not diff.new_failures
        and not diff.regressions
        and not diff.resolved
        and not diff.improvements
    ):
        lines.append("\n✓ No changes detected")

    return "\n".join(lines)


def render_profile_diff_rich(diff) -> str:
    """Render profile diff in human-readable format."""
    from kontra.connectors.handle import mask_credentials

    lines = []

    # Header
    lines.append(f"Profile Diff: {mask_credentials(diff.after.source_uri)}")
    lines.append(
        f"Comparing: {diff.before.profiled_at[:16]} → {diff.after.profiled_at[:16]}"
    )
    lines.append("=" * 50)

    # Row count
    if diff.row_count_delta != 0:
        sign = "+" if diff.row_count_delta > 0 else ""
        lines.append(
            f"\nRows: {diff.row_count_before:,} → {diff.row_count_after:,} "
            f"({sign}{diff.row_count_delta:,}, {diff.row_count_pct_change:+.1f}%)"
        )
    else:
        lines.append(f"\nRows: {diff.row_count_after:,} (unchanged)")

    # Column count
    if diff.column_count_before != diff.column_count_after:
        lines.append(
            f"Columns: {diff.column_count_before} → {diff.column_count_after}"
        )

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
            lines.append(
                f"  - {cd.column_name}: {cd.null_rate_before:.1%} → {cd.null_rate_after:.1%}"
            )

    # Null rate decreases (improvements)
    if diff.null_rate_decreases:
        lines.append(f"\n✅ Null Rate Decreases ({len(diff.null_rate_decreases)})")
        for cd in diff.null_rate_decreases[:10]:
            lines.append(
                f"  - {cd.column_name}: {cd.null_rate_before:.1%} → {cd.null_rate_after:.1%}"
            )

    # Cardinality changes
    if diff.cardinality_changes:
        lines.append(f"\n📊 Cardinality Changes ({len(diff.cardinality_changes)})")
        for cd in diff.cardinality_changes[:10]:
            sign = "+" if cd.distinct_count_delta > 0 else ""
            lines.append(
                f"  - {cd.column_name}: {cd.distinct_count_before:,} → "
                f"{cd.distinct_count_after:,} ({sign}{cd.distinct_count_delta:,})"
            )

    if not diff.has_changes:
        lines.append("\n✓ No significant changes detected")

    return "\n".join(lines)
