"""
Kontra CLI — Developer-first Data Quality Engine

Thin layer: parse args → call engine → print via reporters.
"""

from __future__ import annotations

from typing import Optional

import typer

from kontra.cli.commands import config, diff, scout, validate
from kontra.version import VERSION

app = typer.Typer(help="Kontra CLI — Developer-first Data Quality Engine")


@app.callback()
def _version(
    version: Optional[bool] = typer.Option(
        None, "--version", help="Show the Kontra version and exit.", is_eager=True
    )
) -> None:
    if version:
        typer.echo(f"kontra {VERSION}")
        raise typer.Exit(code=0)


# Register all commands
validate.register(app)
scout.register(app)
diff.register(app)
config.register(app)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
