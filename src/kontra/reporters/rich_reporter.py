from __future__ import annotations

from rich.console import Console

_console = Console()

def report_success(msg: str) -> None:
    _console.print(f"[bold green]✅ {msg}[/bold green]")

def report_failure(msg: str) -> None:
    _console.print(f"[bold red]❌ {msg}[/bold red]")


def report_line(msg: str) -> None:
    """Plain detail line; markup/emoji disabled so literal brackets ([polars])
    and colon-delimited rule IDs (COL:id:not_null) survive untouched."""
    _console.print(msg, markup=False, highlight=False, emoji=False)
