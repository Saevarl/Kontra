# src/kontra/config/settings.py
"""
Kontra configuration file system.

Loads project-level config from .kontra/config.yml with:
- Environment variable substitution (${VAR} syntax)
- Named environments (--env production)
- Precedence: CLI > env vars > config file > defaults
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Environment Variable Substitution
# =============================================================================

ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def substitute_env_vars(value: str) -> str:
    """
    Replace ${VAR} with environment variable value.

    Args:
        value: String potentially containing ${VAR} patterns

    Returns:
        String with env vars substituted (missing vars become empty string)
    """
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, "")

    return ENV_VAR_PATTERN.sub(replacer, value)


def substitute_env_vars_recursive(obj: Any) -> Any:
    """
    Recursively substitute ${VAR} in strings throughout a nested structure.

    Args:
        obj: Any Python object (dict, list, str, etc.)

    Returns:
        Same structure with env vars substituted in strings
    """
    if isinstance(obj, str):
        return substitute_env_vars(obj)
    elif isinstance(obj, dict):
        return {k: substitute_env_vars_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [substitute_env_vars_recursive(item) for item in obj]
    return obj


# =============================================================================
# Pydantic Models
# =============================================================================

# =============================================================================
# Datasource Models
# =============================================================================


class PostgresDatasourceConfig(BaseModel):
    """PostgreSQL datasource configuration."""

    type: Literal["postgres"] = "postgres"
    host: str = "${PGHOST}"
    port: int = 5432
    user: str = "${PGUSER}"
    password: str = "${PGPASSWORD}"
    database: str = "${PGDATABASE}"
    # Tables: map alias -> schema.table
    tables: Dict[str, str] = Field(default_factory=dict)


class FilesDatasourceConfig(BaseModel):
    """File-based datasource configuration (Parquet, CSV)."""

    type: Literal["files", "file"] = "files"
    base_path: str = "./"
    path: str = ""  # Alias for base_path
    # Tables: map alias -> relative path
    tables: Dict[str, str] = Field(default_factory=dict)
    datasets: Dict[str, str] = Field(default_factory=dict)  # Alias for tables


class S3DatasourceConfig(BaseModel):
    """S3 datasource configuration."""

    type: Literal["s3"] = "s3"
    bucket: str
    prefix: str = ""
    # Tables: map alias -> relative key
    tables: Dict[str, str] = Field(default_factory=dict)


class MSSQLDatasourceConfig(BaseModel):
    """SQL Server datasource configuration."""

    type: Literal["mssql"] = "mssql"
    host: str = "localhost"
    port: int = 1433
    user: str = "sa"
    password: str = ""
    database: str = ""
    # Tables: map alias -> schema.table
    tables: Dict[str, str] = Field(default_factory=dict)


# Union type for datasource configs
DatasourceConfig = PostgresDatasourceConfig | FilesDatasourceConfig | S3DatasourceConfig | MSSQLDatasourceConfig


class DefaultsConfig(BaseModel):
    """Default values for CLI options."""

    preplan: Literal["on", "off", "auto"] = "auto"
    pushdown: Literal["on", "off", "auto"] = "auto"
    projection: Literal["on", "off"] = "on"
    output_format: Literal["rich", "json"] = "rich"
    stats: Literal["none", "summary", "profile"] = "none"
    state_backend: str = "local"
    csv_mode: Literal["auto", "duckdb", "parquet"] = "auto"


class ScoutConfig(BaseModel):
    """Profile-specific settings (also known as Scout internally)."""

    # Accept both new (scout/scan/interrogate) and old (lite/standard/deep) preset names
    preset: Literal["scout", "scan", "interrogate", "lite", "standard", "deep", "llm"] = "scan"
    save_profile: bool = False
    list_values_threshold: Optional[int] = None
    top_n: Optional[int] = None
    include_patterns: bool = False


class EnvironmentConfig(BaseModel):
    """
    Environment-specific overrides.

    All fields are optional - only specified fields override defaults.
    """

    preplan: Optional[Literal["on", "off", "auto"]] = None
    pushdown: Optional[Literal["on", "off", "auto"]] = None
    projection: Optional[Literal["on", "off"]] = None
    output_format: Optional[Literal["rich", "json"]] = None
    stats: Optional[Literal["none", "summary", "profile"]] = None
    state_backend: Optional[str] = None
    csv_mode: Optional[Literal["auto", "duckdb", "parquet"]] = None


class KontraConfig(BaseModel):
    """
    Root configuration model for .kontra/config.yml
    """

    version: str = "1"
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    # Accept both "profile" and "scout" as the config key (profile is preferred)
    scout: ScoutConfig = Field(default_factory=ScoutConfig, alias="profile")
    datasources: Dict[str, Any] = Field(default_factory=dict)  # Flexible for different types
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}  # Allow both 'scout' and 'profile'

    # LLM juice: user-defined severity weights (Kontra carries but never acts on these)
    severity_weights: Optional[Dict[str, float]] = Field(
        default=None,
        description="User-defined numeric weights for severity levels. Kontra carries these but never uses them internally."
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        if v != "1":
            raise ValueError(f"Unsupported config version: {v}. Expected '1'.")
        return v

    def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        """
        Get a datasource config by name.

        Returns None if not found.
        """
        if name not in self.datasources:
            return None

        ds_data = self.datasources[name]
        ds_type = ds_data.get("type", "files")

        if ds_type == "postgres":
            return PostgresDatasourceConfig.model_validate(ds_data)
        elif ds_type == "mssql":
            return MSSQLDatasourceConfig.model_validate(ds_data)
        elif ds_type == "s3":
            return S3DatasourceConfig.model_validate(ds_data)
        elif ds_type in ("files", "file"):
            return FilesDatasourceConfig.model_validate(ds_data)
        else:
            # Default to files for unknown types
            return FilesDatasourceConfig.model_validate(ds_data)


# =============================================================================
# Effective Config (resolved values)
# =============================================================================

@dataclass
class EffectiveConfig:
    """
    Fully resolved configuration after merging all sources.

    This is what the CLI commands actually use.
    """

    # Execution controls
    preplan: str = "auto"
    pushdown: str = "auto"
    projection: str = "on"

    # Output
    output_format: str = "rich"
    stats: str = "none"

    # State
    state_backend: str = "local"

    # CSV
    csv_mode: str = "auto"

    # Scout
    scout_preset: str = "standard"
    scout_save_profile: bool = False
    scout_list_values_threshold: Optional[int] = None
    scout_top_n: Optional[int] = None
    scout_include_patterns: bool = False

    # Metadata
    config_file_path: Optional[Path] = None
    environment: Optional[str] = None

    # LLM juice: user-defined severity weights (None if unconfigured)
    severity_weights: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for display."""
        d = {
            "preplan": self.preplan,
            "pushdown": self.pushdown,
            "projection": self.projection,
            "output_format": self.output_format,
            "stats": self.stats,
            "state_backend": self.state_backend,
            "csv_mode": self.csv_mode,
            "profile": {
                "preset": self.scout_preset,
                "save_profile": self.scout_save_profile,
                "list_values_threshold": self.scout_list_values_threshold,
                "top_n": self.scout_top_n,
                "include_patterns": self.scout_include_patterns,
            },
        }
        if self.severity_weights is not None:
            d["severity_weights"] = self.severity_weights
        return d


# =============================================================================
# Config Loading
# =============================================================================

def find_config_file(start_path: Optional[Path] = None) -> Optional[Path]:
    """
    Find .kontra/config.yml in current directory.

    Args:
        start_path: Directory to search (default: cwd)

    Returns:
        Path to config file if found, None otherwise
    """
    base = start_path or Path.cwd()
    config_path = base / ".kontra" / "config.yml"

    if config_path.exists():
        return config_path

    return None


def load_config_file(path: Path) -> KontraConfig:
    """
    Load and parse a config file.

    Args:
        path: Path to config.yml

    Returns:
        Parsed KontraConfig

    Raises:
        ConfigParseError: If YAML is invalid
        ConfigValidationError: If structure is invalid
    """
    from kontra.errors import ConfigParseError, ConfigValidationError

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ConfigParseError(str(path), f"Cannot read file: {e}")

    # Parse YAML
    try:
        raw = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ConfigParseError(str(path), f"Invalid YAML: {e}")

    if raw is None:
        raw = {}

    # Substitute environment variables
    raw = substitute_env_vars_recursive(raw)

    # Validate with Pydantic
    try:
        return KontraConfig.model_validate(raw)
    except Exception as e:
        raise ConfigValidationError([str(e)], str(path))


# --- Config overlay helpers ---

# Core validation fields (same name in all config layers)
_CORE_OVERLAY_FIELDS = [
    "preplan",
    "pushdown",
    "projection",
    "output_format",
    "stats",
    "state_backend",
    "csv_mode",
]

# CLI override to effective config field mappings (for scout fields)
_CLI_FIELD_MAPPINGS = {
    "preset": "scout_preset",
    "save_profile": "scout_save_profile",
    "list_values_threshold": "scout_list_values_threshold",
    "top_n": "scout_top_n",
    "include_patterns": "scout_include_patterns",
}


def _apply_optional_overrides(
    effective: "EffectiveConfig",
    source: Any,
    fields: List[str],
) -> None:
    """
    Apply non-None values from source object to effective config.

    Args:
        effective: Target EffectiveConfig to update
        source: Source object with same-named attributes
        fields: List of field names to copy
    """
    for field in fields:
        value = getattr(source, field, None)
        if value is not None:
            setattr(effective, field, value)


def _apply_cli_overrides(
    effective: "EffectiveConfig",
    cli_overrides: Dict[str, Any],
    core_fields: List[str],
    field_mappings: Dict[str, str],
) -> None:
    """
    Apply CLI override values to effective config.

    Args:
        effective: Target EffectiveConfig to update
        cli_overrides: Dict of CLI argument values
        core_fields: Fields with same name in CLI and effective config
        field_mappings: CLI name -> effective config name mappings
    """
    # Apply core fields (same name)
    for field in core_fields:
        if field in cli_overrides and cli_overrides[field] is not None:
            setattr(effective, field, cli_overrides[field])

    # Apply mapped fields (different names)
    for cli_name, effective_name in field_mappings.items():
        if cli_name in cli_overrides and cli_overrides[cli_name] is not None:
            setattr(effective, effective_name, cli_overrides[cli_name])


# --- End config overlay helpers ---


def resolve_effective_config(
    env_name: Optional[str] = None,
    cli_overrides: Optional[Dict[str, Any]] = None,
    config_path: Optional[Path] = None,
) -> EffectiveConfig:
    """
    Resolve final configuration from all sources.

    Precedence (highest to lowest):
    1. CLI overrides (explicit flags)
    2. Environment-specific config (if --env specified)
    3. Config file defaults
    4. Hardcoded defaults

    Args:
        env_name: Environment to activate (e.g., "production")
        cli_overrides: Values explicitly set on CLI (not Typer defaults)
        config_path: Explicit config file path (default: auto-discover)

    Returns:
        EffectiveConfig with resolved values
    """
    from kontra.errors import UnknownEnvironmentError

    cli_overrides = cli_overrides or {}

    # Start with hardcoded defaults
    effective = EffectiveConfig()

    # Try to load config file
    if config_path is None:
        config_path = find_config_file()

    file_config: Optional[KontraConfig] = None
    if config_path and config_path.exists():
        try:
            file_config = load_config_file(config_path)
            effective.config_file_path = config_path
        except Exception as e:
            # Fail-safe: continue with defaults if config is broken
            # Always warn when config fails to load (BUG-011)
            import warnings
            warnings.warn(
                f"Config file '{config_path}' failed to load: {e}. Using defaults.",
                UserWarning,
                stacklevel=2,
            )
            if os.getenv("KONTRA_VERBOSE"):
                import traceback
                traceback.print_exc()

    # Layer 1: Apply config file defaults
    if file_config:
        effective.preplan = file_config.defaults.preplan
        effective.pushdown = file_config.defaults.pushdown
        effective.projection = file_config.defaults.projection
        effective.output_format = file_config.defaults.output_format
        effective.stats = file_config.defaults.stats
        effective.state_backend = file_config.defaults.state_backend
        effective.csv_mode = file_config.defaults.csv_mode

        # Scout settings
        effective.scout_preset = file_config.scout.preset
        effective.scout_save_profile = file_config.scout.save_profile
        effective.scout_list_values_threshold = file_config.scout.list_values_threshold
        effective.scout_top_n = file_config.scout.top_n
        effective.scout_include_patterns = file_config.scout.include_patterns

        # LLM juice: severity weights (user-defined, Kontra carries but never acts)
        effective.severity_weights = file_config.severity_weights

    # Layer 2: Apply environment overlay
    if env_name:
        effective.environment = env_name

        if file_config and env_name in file_config.environments:
            env_config = file_config.environments[env_name]
            _apply_optional_overrides(effective, env_config, _CORE_OVERLAY_FIELDS)

        elif file_config:
            # Environment specified but not found
            available = list(file_config.environments.keys())
            raise UnknownEnvironmentError(env_name, available)
        else:
            # No config file, warn about ignored --env (BUG-012)
            import warnings
            warnings.warn(
                f"Environment '{env_name}' specified but no config file found. "
                "Create .kontra/config.yml with environments section.",
                UserWarning,
                stacklevel=2,
            )

    # Layer 3: Apply CLI overrides (core fields + scout fields with mappings)
    _apply_cli_overrides(effective, cli_overrides, _CORE_OVERLAY_FIELDS, _CLI_FIELD_MAPPINGS)

    return effective


# =============================================================================
# Datasource Resolution
# =============================================================================


def resolve_datasource(
    reference: str,
    config: Optional[KontraConfig] = None,
) -> str:
    """
    Resolve a datasource reference to a full URI.

    Supports both:
    - Named references: "prod_db.users" -> "postgres://user:pass@host/db/public.users"
    - Direct URIs: "postgres://..." -> returned as-is

    Args:
        reference: Either "datasource_name.table_name" or a direct URI
        config: KontraConfig with datasources (auto-loaded if None)

    Returns:
        Full URI string

    Raises:
        ValueError: If datasource or table not found
    """
    # Check if it's already a URI (has scheme)
    if "://" in reference or reference.startswith("/") or reference.endswith((".parquet", ".csv")):
        return reference

    # Check if it looks like a file path
    if "/" in reference:
        return reference

    # Load config if not provided
    if config is None:
        config_path = find_config_file()
        if config_path:
            config = load_config_file(config_path)
        else:
            config = None

    # Parse reference - could be "table", "datasource.table", or ambiguous
    if "." in reference:
        # Explicit datasource.table format
        parts = reference.split(".", 1)
        ds_name, table_name = parts
    else:
        # Just a table name - search all datasources
        table_name = reference
        ds_name = None

        if config is None:
            raise ValueError(
                f"Table '{reference}' not found. "
                "No config file exists. Run 'kontra init' to create one."
            )

        # Find which datasource(s) have this table
        matches = []
        for ds_key, ds_data in config.datasources.items():
            tables = ds_data.get("tables", {})
            if table_name in tables:
                matches.append(ds_key)

        if len(matches) == 0:
            # List all available tables
            all_tables = []
            for ds_key, ds_data in config.datasources.items():
                tables = ds_data.get("tables", {})
                for t in tables.keys():
                    all_tables.append(f"{ds_key}.{t}")
            tables_str = ", ".join(all_tables) if all_tables else "(none)"
            raise ValueError(
                f"Unknown table: '{reference}'. "
                f"Available tables: {tables_str}"
            )
        elif len(matches) > 1:
            matches_str = ", ".join(f"{m}.{table_name}" for m in matches)
            raise ValueError(
                f"Ambiguous table '{reference}' found in multiple datasources: {matches_str}. "
                f"Use explicit 'datasource.table' format."
            )
        else:
            ds_name = matches[0]

    # At this point we have ds_name and table_name
    if config is None:
        raise ValueError(
            f"Datasource '{ds_name}' not found. "
            "No config file exists. Run 'kontra init' to create one."
        )

    # Get datasource
    ds = config.get_datasource(ds_name)
    if ds is None:
        available = list(config.datasources.keys())
        available_str = ", ".join(available) if available else "(none)"
        raise ValueError(
            f"Unknown datasource: '{ds_name}'. "
            f"Available datasources: {available_str}"
        )

    # Resolve table reference
    if table_name not in ds.tables:
        available_tables = list(ds.tables.keys())
        tables_str = ", ".join(available_tables) if available_tables else "(none)"
        raise ValueError(
            f"Unknown table '{table_name}' in datasource '{ds_name}'. "
            f"Available tables: {tables_str}"
        )

    table_ref = ds.tables[table_name]

    # Build full URI based on datasource type
    if isinstance(ds, PostgresDatasourceConfig):
        # postgres://user:pass@host:port/database/schema.table
        user = ds.user
        password = ds.password
        host = ds.host
        port = ds.port
        database = ds.database

        if user and password:
            auth = f"{user}:{password}@"
        elif user:
            auth = f"{user}@"
        else:
            auth = ""

        return f"postgres://{auth}{host}:{port}/{database}/{table_ref}"

    elif isinstance(ds, S3DatasourceConfig):
        # s3://bucket/prefix/key
        prefix = ds.prefix.rstrip("/")
        if prefix:
            return f"s3://{ds.bucket}/{prefix}/{table_ref}"
        else:
            return f"s3://{ds.bucket}/{table_ref}"

    elif isinstance(ds, FilesDatasourceConfig):
        # Local file path
        from pathlib import Path
        base = Path(ds.base_path)
        return str(base / table_ref)

    elif isinstance(ds, MSSQLDatasourceConfig):
        # mssql://user:pass@host:port/database/schema.table
        user = ds.user
        password = ds.password
        host = ds.host
        port = ds.port
        database = ds.database

        if user and password:
            auth = f"{user}:{password}@"
        elif user:
            auth = f"{user}@"
        else:
            auth = ""

        return f"mssql://{auth}{host}:{port}/{database}/{table_ref}"

    else:
        raise ValueError(f"Unknown datasource type for '{ds_name}'")


def list_datasources(config: Optional[KontraConfig] = None) -> Dict[str, List[str]]:
    """
    List all datasources and their tables.

    Returns:
        Dict mapping datasource names to list of table names
    """
    if config is None:
        config_path = find_config_file()
        if config_path:
            config = load_config_file(config_path)
        else:
            return {}

    result = {}
    for ds_name in config.datasources:
        ds = config.get_datasource(ds_name)
        if ds:
            result[ds_name] = list(ds.tables.keys())

    return result


# =============================================================================
# Config Template
# =============================================================================

DEFAULT_CONFIG_TEMPLATE = '''# Kontra Configuration
# Generated by: kontra init
# Documentation: https://github.com/kontra-data/kontra
#
# CLI flags always take precedence over these settings.
# Environment variable substitution: ${VAR_NAME}

version: "1"

# ─────────────────────────────────────────────────────────────
# Default Settings
# ─────────────────────────────────────────────────────────────

defaults:
  # Execution controls
  preplan: "auto"       # on | off | auto - Parquet metadata preflight
  pushdown: "auto"      # on | off | auto - SQL pushdown to DuckDB
  projection: "on"      # on | off - Column pruning at source

  # Output
  output_format: "rich" # rich | json - Output format
  stats: "none"         # none | summary | profile - Statistics detail

  # State management
  state_backend: "local" # local | s3://bucket/prefix | postgres://...

  # CSV handling
  csv_mode: "auto"      # auto | duckdb | parquet

# ─────────────────────────────────────────────────────────────
# Profile Settings
# ─────────────────────────────────────────────────────────────

profile:
  preset: "scan"        # scout | scan | interrogate
  save_profile: false   # Save profile to state storage
  # list_values_threshold: 10  # List all values if distinct <= N
  # top_n: 5                   # Show top N frequent values
  # include_patterns: false    # Detect patterns (email, uuid, etc.)

# ─────────────────────────────────────────────────────────────
# Datasources
# ─────────────────────────────────────────────────────────────
# Named data sources referenced as: datasource_name.table_name
# Credentials stay in config, contracts stay clean and portable.
#
# Usage:
#   kontra validate contract.yml --data prod_db.users
#   kontra profile prod_db.orders
#
# Or in contract YAML:
#   dataset: prod_db.users

datasources: {}
  # PostgreSQL example:
  # prod_db:
  #   type: postgres
  #   host: ${PGHOST}
  #   port: 5432
  #   user: ${PGUSER}
  #   password: ${PGPASSWORD}
  #   database: ${PGDATABASE}
  #   tables:
  #     users: public.users
  #     orders: public.orders

  # Local files example:
  # local_data:
  #   type: files
  #   base_path: ./data
  #   tables:
  #     users: users.parquet
  #     orders: orders.csv

  # S3 example:
  # data_lake:
  #   type: s3
  #   bucket: ${S3_BUCKET}
  #   prefix: warehouse/
  #   tables:
  #     events: events.parquet
  #     metrics: metrics.parquet

# ─────────────────────────────────────────────────────────────
# Environments
# ─────────────────────────────────────────────────────────────
# Named configurations activated with --env <name>
# Only specified fields override defaults.

environments: {}
  # Example: Production environment
  # production:
  #   state_backend: postgres://${PGHOST}/${PGDATABASE}
  #   preplan: "on"
  #   pushdown: "on"
  #   output_format: "json"

  # Example: Staging environment
  # staging:
  #   state_backend: s3://${S3_BUCKET}/kontra-state/
  #   stats: "summary"

  # Example: Local development
  # local:
  #   state_backend: "local"
  #   stats: "profile"
'''
