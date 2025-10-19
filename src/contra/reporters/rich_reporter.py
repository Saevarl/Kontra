from rich.console import Console

console = Console()

def report_success(msg: str):
    console.print(f"[bold green]✅ {msg}[/bold green]")

def report_failure(msg: str):
    console.print(f"[bold red]❌ {msg}[/bold red]")
