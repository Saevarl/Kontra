from __future__ import annotations

from rich.console import Console

_console = Console()

def report_success(msg: str) -> None:
    _console.print(f"[bold green]✅ {msg}[/bold green]")

def report_failure(msg: str) -> None:
    _console.print(f"[bold red]❌ {msg}[/bold red]")
