# src/kontra/scout/reporters/rich_reporter.py
"""
Rich console reporter for Kontra Scout.
"""

from __future__ import annotations

from typing import List

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from kontra.scout.types import DatasetProfile, ColumnProfile


def render_rich(profile: DatasetProfile) -> str:
    """
    Render a DatasetProfile as Rich console output.

    Returns a string representation (for compatibility with other reporters).
    For direct console output, use print_rich() instead.
    """
    # Use a string buffer to capture output without duplicating
    from io import StringIO
    string_io = StringIO()
    console = Console(file=string_io, force_terminal=True, width=120)
    _print_to_console(console, profile)
    return string_io.getvalue()


def print_rich(profile: DatasetProfile) -> None:
    """Print profile directly to console with Rich formatting."""
    console = Console()
    _print_to_console(console, profile)


def _print_to_console(console: Console, profile: DatasetProfile) -> None:
    """Internal: render profile to a console instance."""
    from kontra.connectors.handle import mask_credentials

    # Header - use preset name in title
    preset_title = profile.preset.title() if profile.preset else "Scout"
    title = f"[bold cyan]Kontra {preset_title}[/bold cyan] - {mask_credentials(profile.source_uri)}"
    size_str = ""
    if profile.estimated_size_bytes:
        size_bytes = profile.estimated_size_bytes
        if size_bytes < 1024:
            size_str = f" | Size: {size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_kb = size_bytes / 1024
            size_str = f" | Size: {size_kb:.1f} KB"
        else:
            size_mb = size_bytes / (1024 * 1024)
            size_str = f" | Size: {size_mb:.1f} MB"
    sample_str = f" (sampled: {profile.sample_size:,} rows)" if profile.sampled else ""

    header = (
        f"Rows: [bold]{profile.row_count:,}[/bold] | "
        f"Columns: [bold]{profile.column_count}[/bold]{size_str} | "
        f"Duration: [bold]{profile.profile_duration_ms}[/bold] ms{sample_str}"
    )
    console.print(Panel(header, title=title, border_style="cyan"))

    # Column table
    table = Table(show_header=True, header_style="bold magenta", expand=True)
    table.add_column("Column", style="cyan", no_wrap=True)
    table.add_column("Type", style="green")
    table.add_column("Nulls", justify="right")
    table.add_column("Distinct", justify="right")
    table.add_column("Cardinality")
    table.add_column("Info")

    # Check if this is a metadata-only preset (scout/lite)
    is_metadata_only = profile.preset in ("scout", "lite")

    for col in profile.columns:
        null_pct = f"{col.null_rate * 100:.1f}%"

        # Show "—" for distinct count if not computed (metadata-only preset)
        if is_metadata_only and col.distinct_count == 0:
            distinct_str = "[dim]—[/dim]"
        else:
            distinct_str = f"{col.distinct_count:,}"

        # Cardinality classification
        if is_metadata_only and col.distinct_count == 0:
            # Can't determine cardinality without distinct count
            card = "[dim]—[/dim]"
        elif col.uniqueness_ratio >= 0.99 and col.null_rate == 0:
            card = "[bold green]unique[/bold green]"
        elif col.is_low_cardinality:
            if col.values:
                vals = ", ".join(str(v) for v in col.values[:5])
                if len(col.values) > 5:
                    vals += f" +{len(col.values) - 5} more"
                card = f"[yellow]low[/yellow] ({vals})"
            else:
                card = "[yellow]low[/yellow]"
        elif col.distinct_count < 100:
            card = "[blue]medium[/blue]"
        else:
            card = "high"

        # Info column
        info_parts: List[str] = []
        if col.semantic_type:
            info_parts.append(f"[dim]{col.semantic_type}[/dim]")
        if col.detected_patterns:
            info_parts.append(f"[magenta]{', '.join(col.detected_patterns)}[/magenta]")
        if col.numeric:
            info_parts.append(
                f"[dim]min={_fmt_num(col.numeric.min)}, "
                f"max={_fmt_num(col.numeric.max)}, "
                f"mean={_fmt_num(col.numeric.mean)}[/dim]"
            )
        if col.temporal:
            info_parts.append(f"[dim]{col.temporal.date_min} to {col.temporal.date_max}[/dim]")

        table.add_row(
            col.name,
            col.dtype,
            null_pct,
            distinct_str,
            card,
            " | ".join(info_parts) if info_parts else "",
        )

    console.print(table)

    # Top values section (if any columns have them)
    cols_with_top = [c for c in profile.columns if c.top_values and c.is_low_cardinality]
    if cols_with_top:
        console.print()
        console.print("[bold]Top Values:[/bold]")
        for col in cols_with_top[:5]:  # Limit to 5 columns
            vals = ", ".join(
                f"{tv.value} ({tv.pct:.1f}%)" for tv in col.top_values[:3]
            )
            console.print(f"  [cyan]{col.name}[/cyan]: {vals}")

    # Numeric summary
    numeric_cols = [c for c in profile.columns if c.numeric]
    if numeric_cols:
        console.print()
        console.print("[bold]Numeric Summary:[/bold]")
        for col in numeric_cols[:5]:  # Limit to 5
            n = col.numeric
            base_stats = (
                f"min={_fmt_num(n.min)}, max={_fmt_num(n.max)}, "
                f"mean={_fmt_num(n.mean)}, median={_fmt_num(n.median)}"
            )
            # Add percentiles if available (interrogate preset)
            if n.percentiles:
                pct_parts = [f"p{k.replace('p', '')}={_fmt_num(v)}" for k, v in sorted(n.percentiles.items())]
                base_stats += f", {', '.join(pct_parts)}"
            console.print(f"  [cyan]{col.name}[/cyan]: {base_stats}")


def _fmt_num(val: float | None) -> str:
    """Format a number for display."""
    if val is None:
        return "N/A"
    if abs(val) >= 1000:
        return f"{val:,.0f}"
    if abs(val) >= 1:
        return f"{val:.2f}"
    return f"{val:.4f}"
