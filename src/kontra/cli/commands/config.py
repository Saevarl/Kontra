"""Config commands for Kontra CLI."""

from __future__ import annotations

from typing import Literal, Optional

import typer

from kontra.cli.constants import EXIT_CONFIG_ERROR, EXIT_SUCCESS


def register(app: typer.Typer) -> None:
    """Register the config and init commands with the app."""

    @app.command("init")
    def init(
        force: bool = typer.Option(
            False,
            "--force",
            "-f",
            help="Overwrite existing configuration.",
        ),
    ) -> None:
        """
        Initialize a Kontra project.

        Creates the .kontra/ directory and config.yml with documented defaults
        and example configurations.

        Examples:
            kontra init                     # Initialize project
            kontra init --force             # Overwrite existing config

        To generate a contract from data, use:
            kontra profile data.parquet --draft > contracts/data.yml
        """
        from pathlib import Path

        from kontra.config.settings import DEFAULT_CONFIG_TEMPLATE

        kontra_dir = Path.cwd() / ".kontra"
        config_path = kontra_dir / "config.yml"

        # Check if already initialized
        if config_path.exists() and not force:
            typer.secho("Kontra already initialized!", fg=typer.colors.GREEN)
            typer.echo(f"\nConfig: {config_path}")
            typer.echo("\nTo reinitialize (overwrites existing config):")
            typer.secho("  kontra init --force", fg=typer.colors.CYAN)
            raise typer.Exit(code=EXIT_SUCCESS)

        # Create .kontra directory
        kontra_dir.mkdir(parents=True, exist_ok=True)

        # Write config template
        config_path.write_text(DEFAULT_CONFIG_TEMPLATE, encoding="utf-8")

        # Create contracts directory
        contracts_dir = Path.cwd() / "contracts"
        contracts_dir.mkdir(exist_ok=True)

        typer.secho("Kontra initialized!", fg=typer.colors.GREEN)
        typer.echo("")
        typer.echo("Created:")
        typer.echo(f"  {config_path}")
        typer.echo(f"  {contracts_dir}/")
        typer.echo("")
        typer.echo("Next steps:")
        typer.echo("  1. Edit .kontra/config.yml to configure datasources")
        typer.echo("  2. Profile your data:")
        typer.secho("     kontra profile data.parquet", fg=typer.colors.CYAN)
        typer.echo("  3. Generate a contract:")
        typer.secho(
            "     kontra profile data.parquet --draft > contracts/data.yml",
            fg=typer.colors.CYAN,
        )
        typer.echo("  4. Run validation:")
        typer.secho("     kontra validate contracts/data.yml", fg=typer.colors.CYAN)
        typer.echo("")
        typer.echo("Or use the Python API:")
        typer.secho("     import kontra", fg=typer.colors.CYAN)
        typer.secho("     result = kontra.validate(df, rules=[...])", fg=typer.colors.CYAN)

        raise typer.Exit(code=EXIT_SUCCESS)

    @app.command("config")
    def config_cmd(
        action: str = typer.Argument(
            "show",
            help="Action: 'show' displays effective config, 'path' shows config file location.",
        ),
        env: Optional[str] = typer.Option(
            None,
            "--env",
            "-e",
            help="Environment to show (simulates --env flag).",
        ),
        output_format: Literal["yaml", "json"] = typer.Option(
            "yaml",
            "--output-format",
            "-o",
            help="Output format.",
        ),
    ) -> None:
        """
        Show Kontra configuration.

        Examples:
            kontra config show                  # Show effective config
            kontra config show --env production # Show with environment overlay
            kontra config path                  # Show config file path
        """
        from pathlib import Path

        from kontra.config.settings import find_config_file, resolve_effective_config

        config_path = find_config_file()

        if action == "path":
            if config_path:
                typer.echo(f"{config_path} (exists)")
            else:
                default_path = Path.cwd() / ".kontra" / "config.yml"
                typer.echo(f"{default_path} (not found)")
                typer.echo("\nRun 'kontra init' to create one.")
            raise typer.Exit(code=EXIT_SUCCESS)

        # Show effective configuration
        try:
            effective = resolve_effective_config(env_name=env)
        except Exception as e:
            from kontra.errors import format_error_for_cli

            typer.secho(f"Error: {format_error_for_cli(e)}", fg=typer.colors.RED)
            raise typer.Exit(code=EXIT_CONFIG_ERROR)

        typer.secho("Effective configuration", fg=typer.colors.CYAN)
        if env:
            typer.echo(f"Environment: {env}")
        if config_path:
            typer.echo(f"Config file: {config_path}")
        else:
            typer.echo("Config file: (none, using defaults)")
        typer.echo("")

        config_dict = effective.to_dict()

        if output_format == "json":
            import json

            typer.echo(json.dumps(config_dict, indent=2))
        else:
            import yaml

            typer.echo(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))

        raise typer.Exit(code=EXIT_SUCCESS)
