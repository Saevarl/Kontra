"""
Kontra CLI — Developer-first Data Quality Engine

Thin layer: parse args → call engine → print via reporters.
"""

from __future__ import annotations

from typing import Optional

import typer

from kontra.cli.commands import config, diff, history, profile, validate
from kontra.version import VERSION

app = typer.Typer(help="Kontra CLI — Developer-first Data Quality Engine")


@app.callback(invoke_without_command=True)
def _version(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(
        None, "--version", help="Show the Kontra version and exit.", is_eager=True
    ),
) -> None:
    if version:
        typer.echo(f"kontra {VERSION}")
        raise typer.Exit(code=0)
    # If no command given and no version flag, show help
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(code=0)


# Register all commands
validate.register(app)
profile.register(app)
diff.register(app)
history.register(app)
config.register(app)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
