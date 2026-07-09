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
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# NOTE: `yaml` is imported lazily inside load_config_file() so that importing
# this module (which happens on every validate() call to resolve defaults) does
# not pull in PyYAML when no config file exists. Pydantic is intentionally NOT
# used here: these settings-layer models are plain dataclasses with explicit
# validation to keep `import kontra` / cold-start cost low. Contract models in
# kontra.config.models still use pydantic.


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
# Config Models (plain dataclasses + explicit validation)
# =============================================================================
#
# These deliberately avoid pydantic. They provide a small, pydantic-compatible
# surface (keyword construction, `.model_validate(dict)`, attribute access) so
# existing callers/tests keep working, while validating and coercing values the
# same way the previous pydantic models did:
#   - Literal fields reject values outside their allowed set.
#   - `port` coerces numeric strings to int (e.g. "5433" -> 5433).
#   - `severity_weights` coerces values to float (8 -> 8.0, "8" -> 8.0).
#   - Unknown keys are ignored (pydantic's default `extra='ignore'`).
# Invalid input raises ValueError (pydantic's ValidationError is itself a
# ValueError subclass, so `except ValueError` handlers keep working).

_REQUIRED = object()  # sentinel for required fields with no default


def _require_mapping(data: Any, model_name: str) -> Dict[str, Any]:
    """Ensure model_validate input is a mapping (pydantic 'model_type' error)."""
    if not isinstance(data, Mapping):
        raise ValueError(
            f"{model_name}: expected a mapping, got {type(data).__name__}"
        )
    return dict(data)


def _validate_literal(value: Any, allowed: tuple, field_name: str) -> Any:
    """Validate that value is one of the allowed literals (pydantic Literal)."""
    if value not in allowed:
        opts = ", ".join(repr(a) for a in allowed)
        raise ValueError(
            f"{field_name}: invalid value {value!r}. Expected one of: {opts}"
        )
    return value


def _coerce_int(value: Any, field_name: str) -> int:
    """Coerce value to int the way pydantic's lax int mode does."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name}: expected an integer, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError(f"{field_name}: expected an integer, got {value!r}")
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            raise ValueError(
                f"{field_name}: unable to parse {value!r} as an integer"
            ) from None
    raise ValueError(f"{field_name}: expected an integer, got {type(value).__name__}")


def _coerce_optional_int(value: Any, field_name: str) -> Optional[int]:
    """Coerce to Optional[int]; None/'' pass through as None."""
    if value is None or value == "":
        return None
    return _coerce_int(value, field_name)


_BOOL_TRUE = {"true", "t", "yes", "y", "on", "1"}
_BOOL_FALSE = {"false", "f", "no", "n", "off", "0"}


def _coerce_bool(value: Any, field_name: str) -> bool:
    """Coerce to bool the way pydantic's lax bool mode does (so an
    env-substituted 'false' string is not silently truthy)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _BOOL_TRUE:
            return True
        if token in _BOOL_FALSE:
            return False
    raise ValueError(f"{field_name}: expected a boolean, got {value!r}")


def _coerce_float(value: Any, field_name: str) -> float:
    """Coerce value to float the way pydantic's lax float mode does."""
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            raise ValueError(
                f"{field_name}: unable to parse {value!r} as a number"
            ) from None
    raise ValueError(f"{field_name}: expected a number, got {type(value).__name__}")


def _pick_known(cls: type, data: Dict[str, Any]) -> Dict[str, Any]:
    """Select only known dataclass field names from data (ignore extras)."""
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in names}


# --- Datasource Models ---


@dataclass
class PostgresDatasourceConfig:
    """PostgreSQL datasource configuration."""

    type: str = "postgres"
    host: str = "${PGHOST}"
    port: int = 5432
    user: str = "${PGUSER}"
    password: str = "${PGPASSWORD}"
    database: str = "${PGDATABASE}"
    # Tables: map alias -> schema.table
    tables: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_literal(self.type, ("postgres",), "type")
        self.port = _coerce_int(self.port, "port")
        if self.tables is None:
            self.tables = {}

    @classmethod
    def model_validate(cls, data: Any) -> "PostgresDatasourceConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "PostgresDatasourceConfig")))


@dataclass
class FilesDatasourceConfig:
    """File-based datasource configuration (Parquet, CSV)."""

    type: str = "files"
    base_path: str = "./"
    path: str = ""  # Alias for base_path
    # Tables: map alias -> relative path
    tables: Dict[str, str] = field(default_factory=dict)
    datasets: Dict[str, str] = field(default_factory=dict)  # Alias for tables

    def __post_init__(self) -> None:
        _validate_literal(self.type, ("files", "file"), "type")
        if self.tables is None:
            self.tables = {}
        if self.datasets is None:
            self.datasets = {}

    @classmethod
    def model_validate(cls, data: Any) -> "FilesDatasourceConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "FilesDatasourceConfig")))


@dataclass
class S3DatasourceConfig:
    """S3 datasource configuration."""

    type: str = "s3"
    bucket: str = _REQUIRED  # type: ignore[assignment]
    prefix: str = ""
    # Tables: map alias -> relative key
    tables: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_literal(self.type, ("s3",), "type")
        if self.bucket is _REQUIRED:
            raise ValueError("bucket: Field required")
        if self.tables is None:
            self.tables = {}

    @classmethod
    def model_validate(cls, data: Any) -> "S3DatasourceConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "S3DatasourceConfig")))


@dataclass
class MSSQLDatasourceConfig:
    """SQL Server datasource configuration."""

    type: str = "mssql"
    host: str = "localhost"
    port: int = 1433
    user: str = "sa"
    password: str = ""
    database: str = ""
    # Entra ID (Azure AD) authentication. "sql" (default) = user/password.
    # Other values authenticate via the ODBC driver: entra_default, entra_mi,
    # entra_service_principal, entra_interactive.
    auth: str = "sql"
    client_id: str = ""      # user-assigned MI / service principal app id
    client_secret: str = ""  # service principal secret
    tenant_id: str = ""      # service principal tenant (carried; see connector docs)
    # Tables: map alias -> schema.table
    tables: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_literal(self.type, ("mssql",), "type")
        _validate_literal(
            self.auth,
            (
                "sql",
                "entra_default",
                "entra_mi",
                "entra_service_principal",
                "entra_interactive",
                "entra_password",
            ),
            "auth",
        )
        self.port = _coerce_int(self.port, "port")
        if self.tables is None:
            self.tables = {}

    @classmethod
    def model_validate(cls, data: Any) -> "MSSQLDatasourceConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "MSSQLDatasourceConfig")))


@dataclass
class ClickHouseDatasourceConfig:
    """ClickHouse datasource configuration."""

    type: str = "clickhouse"
    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    # Tables: map alias -> table (ClickHouse has no schema layer)
    tables: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_literal(self.type, ("clickhouse",), "type")
        self.port = _coerce_int(self.port, "port")
        self.secure = _coerce_bool(self.secure, "secure")
        if self.tables is None:
            self.tables = {}

    @classmethod
    def model_validate(cls, data: Any) -> "ClickHouseDatasourceConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "ClickHouseDatasourceConfig")))


# Union type for datasource configs
DatasourceConfig = Union[
    PostgresDatasourceConfig,
    FilesDatasourceConfig,
    S3DatasourceConfig,
    MSSQLDatasourceConfig,
    ClickHouseDatasourceConfig,
]


@dataclass
class DefaultsConfig:
    """Default values for CLI options."""

    preplan: str = "on"
    pushdown: str = "on"
    projection: str = "on"
    output_format: str = "rich"
    stats: str = "none"
    state_backend: str = "local"
    csv_mode: str = "auto"

    def __post_init__(self) -> None:
        _validate_literal(self.preplan, ("on", "off", "auto"), "preplan")
        _validate_literal(self.pushdown, ("on", "off"), "pushdown")
        _validate_literal(self.projection, ("on", "off"), "projection")
        _validate_literal(self.output_format, ("rich", "json"), "output_format")
        _validate_literal(self.stats, ("none", "summary", "profile"), "stats")
        _validate_literal(self.csv_mode, ("auto", "duckdb", "parquet"), "csv_mode")
        # state_backend is a free-form string (local | s3://... | postgres://...)

    @classmethod
    def model_validate(cls, data: Any) -> "DefaultsConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "DefaultsConfig")))


@dataclass
class ScoutConfig:
    """Profile-specific settings (also known as Scout internally)."""

    # Accept both new (scout/scan/interrogate) and old (lite/standard/deep) preset names
    preset: str = "scan"
    save_profile: bool = False
    list_values_threshold: Optional[int] = None
    top_n: Optional[int] = None
    include_patterns: bool = False

    def __post_init__(self) -> None:
        _validate_literal(
            self.preset,
            ("scout", "scan", "interrogate", "lite", "standard", "deep", "llm"),
            "preset",
        )
        # Coerce like pydantic did: env substitution (${VAR}) yields strings,
        # so "false" must not be truthy and "10" must become int 10 (else a
        # downstream numeric comparison raises TypeError).
        self.save_profile = _coerce_bool(self.save_profile, "save_profile")
        self.include_patterns = _coerce_bool(self.include_patterns, "include_patterns")
        self.list_values_threshold = _coerce_optional_int(
            self.list_values_threshold, "list_values_threshold"
        )
        self.top_n = _coerce_optional_int(self.top_n, "top_n")

    @classmethod
    def model_validate(cls, data: Any) -> "ScoutConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "ScoutConfig")))


@dataclass
class EnvironmentConfig:
    """
    Environment-specific overrides.

    All fields are optional - only specified fields override defaults.
    """

    preplan: Optional[str] = None
    pushdown: Optional[str] = None
    projection: Optional[str] = None
    output_format: Optional[str] = None
    stats: Optional[str] = None
    state_backend: Optional[str] = None
    csv_mode: Optional[str] = None

    def __post_init__(self) -> None:
        if self.preplan is not None:
            _validate_literal(self.preplan, ("on", "off", "auto"), "preplan")
        if self.pushdown is not None:
            _validate_literal(self.pushdown, ("on", "off"), "pushdown")
        if self.projection is not None:
            _validate_literal(self.projection, ("on", "off"), "projection")
        if self.output_format is not None:
            _validate_literal(self.output_format, ("rich", "json"), "output_format")
        if self.stats is not None:
            _validate_literal(self.stats, ("none", "summary", "profile"), "stats")
        if self.csv_mode is not None:
            _validate_literal(self.csv_mode, ("auto", "duckdb", "parquet"), "csv_mode")

    @classmethod
    def model_validate(cls, data: Any) -> "EnvironmentConfig":
        return cls(**_pick_known(cls, _require_mapping(data, "EnvironmentConfig")))


def _coerce_severity_weights(value: Any) -> Optional[Dict[str, float]]:
    """Coerce severity_weights mapping values to float (or None)."""
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(
            f"severity_weights: expected a mapping, got {type(value).__name__}"
        )
    return {
        str(k): _coerce_float(v, f"severity_weights.{k}") for k, v in value.items()
    }


@dataclass
class KontraConfig:
    """
    Root configuration model for .kontra/config.yml
    """

    version: str = "1"
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    # Accept both "profile" and "scout" as the config key (profile is preferred)
    scout: ScoutConfig = field(default_factory=ScoutConfig)
    datasources: Dict[str, Any] = field(default_factory=dict)  # Flexible for different types
    environments: Dict[str, EnvironmentConfig] = field(default_factory=dict)
    # LLM juice: user-defined severity weights (Kontra carries but never acts on these)
    severity_weights: Optional[Dict[str, float]] = None

    def __post_init__(self) -> None:
        if self.version != "1":
            raise ValueError(
                f"Unsupported config version: {self.version}. Expected '1'."
            )

        # Coerce nested mappings into their models (supports both direct
        # construction with dicts and model_validate).
        if isinstance(self.defaults, Mapping):
            self.defaults = DefaultsConfig.model_validate(self.defaults)
        elif not isinstance(self.defaults, DefaultsConfig):
            raise ValueError(
                f"defaults: expected a mapping, got {type(self.defaults).__name__}"
            )

        if isinstance(self.scout, Mapping):
            self.scout = ScoutConfig.model_validate(self.scout)
        elif not isinstance(self.scout, ScoutConfig):
            raise ValueError(
                f"scout: expected a mapping, got {type(self.scout).__name__}"
            )

        if self.datasources is None:
            self.datasources = {}
        elif not isinstance(self.datasources, Mapping):
            raise ValueError(
                f"datasources: expected a mapping, got {type(self.datasources).__name__}"
            )
        else:
            self.datasources = dict(self.datasources)

        envs: Dict[str, EnvironmentConfig] = {}
        for name, env in (self.environments or {}).items():
            if isinstance(env, EnvironmentConfig):
                envs[name] = env
            else:
                envs[name] = EnvironmentConfig.model_validate(env)
        self.environments = envs

        self.severity_weights = _coerce_severity_weights(self.severity_weights)

    @classmethod
    def model_validate(cls, data: Any) -> "KontraConfig":
        data = _require_mapping(data, "KontraConfig")
        kwargs: Dict[str, Any] = {}
        if "version" in data:
            kwargs["version"] = data["version"]
        if "defaults" in data:
            kwargs["defaults"] = data["defaults"]
        # Alias: accept both 'scout' and 'profile' (profile is preferred)
        if "scout" in data:
            kwargs["scout"] = data["scout"]
        elif "profile" in data:
            kwargs["scout"] = data["profile"]
        if "datasources" in data:
            kwargs["datasources"] = data["datasources"]
        if "environments" in data:
            kwargs["environments"] = data["environments"]
        if "severity_weights" in data:
            kwargs["severity_weights"] = data["severity_weights"]
        return cls(**kwargs)

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
        elif ds_type == "clickhouse":
            return ClickHouseDatasourceConfig.model_validate(ds_data)
        elif ds_type == "s3":
            return S3DatasourceConfig.model_validate(ds_data)
        elif ds_type in ("files", "file"):
            return FilesDatasourceConfig.model_validate(ds_data)
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
    preplan: str = "on"
    pushdown: str = "on"
    projection: str = "on"

    # Output
    output_format: str = "rich"
    stats: str = "none"

    # State
    state_backend: str = "local"

    # CSV
    csv_mode: str = "auto"

    # Scout
    scout_preset: str = "scan"
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
        # Always show severity_weights in config output (BUG-025)
        d["severity_weights"] = self.severity_weights
        return d


# =============================================================================
# Config Loading
# =============================================================================

# Explicit config override for services/agents that can't rely on cwd
# discovery (Windmill workers, long-running daemons). Set via
# kontra.set_config(); honored by find_config_file() below, which every
# config/datasource/state resolution routes through.
_config_path_override: Optional[str] = None


def set_config_path_override(path: Optional[str]) -> None:
    """Set (or clear with None) the explicit config path used by discovery."""
    global _config_path_override
    _config_path_override = path


def get_config_path_override() -> Optional[str]:
    return _config_path_override


def find_config_file(start_path: Optional[Path] = None) -> Optional[Path]:
    """
    Find the config file.

    Resolution:
      1. An explicit override set via kontra.set_config() (a config.yml path,
         or a directory containing .kontra/config.yml) — used when the caller
         does not pass an explicit start_path. This lets services that can't
         control their cwd point Kontra at a fixed config.
      2. Otherwise .kontra/config.yml under start_path (default: cwd).

    Args:
        start_path: Directory to search (default: cwd / override)

    Returns:
        Path to config file if found, None otherwise
    """
    if start_path is None and _config_path_override is not None:
        override = Path(_config_path_override)
        # Accept either a direct config.yml path or a directory containing one.
        if override.is_dir():
            candidate = override / ".kontra" / "config.yml"
            return candidate if candidate.exists() else None
        return override if override.exists() else None

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

    # Imported lazily: only pay the PyYAML import cost when a config file
    # actually exists to parse (keeps `import kontra` / validate() fast).
    import yaml

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

    # Validate structure (dataclass models with explicit validation)
    try:
        raw = substitute_env_vars_recursive(raw)
        return KontraConfig.model_validate(raw)
    except (ValueError, TypeError) as e:
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
        # Map legacy "auto" → "on" for backward compatibility (BUG-001)
        preplan_val = file_config.defaults.preplan
        effective.preplan = "on" if preplan_val == "auto" else preplan_val
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
        ds_name = matches[0]

    # At this point we have ds_name and table_name
    if config is None:
        raise ValueError(
            f"Datasource '{ds_name}' not found. "
            "No config file exists. Run 'kontra init' to create one."
        )

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
        from kontra.errors import DatasourceTableError

        available_tables = list(ds.tables.keys())
        raise DatasourceTableError(ds_name, table_name, available_tables)

    table_ref = ds.tables[table_name]

    # Build full URI based on datasource type
    if isinstance(ds, PostgresDatasourceConfig):
        # postgres://user:pass@host:port/database/schema.table
        # Percent-encode userinfo: a credential containing @ / # ? : would
        # otherwise make the URI unparseable (or silently reparse to a
        # different host). The parser unquotes on the way back in.
        from urllib.parse import quote

        user = quote(ds.user, safe="") if ds.user else ds.user
        password = quote(ds.password, safe="") if ds.password else ds.password
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
        return f"s3://{ds.bucket}/{table_ref}"

    elif isinstance(ds, FilesDatasourceConfig):
        # Local file path
        from pathlib import Path
        base = Path(ds.base_path)
        return str(base / table_ref)

    elif isinstance(ds, MSSQLDatasourceConfig):
        # mssql://user:pass@host:port/database/schema.table
        from urllib.parse import quote

        user = quote(ds.user, safe="") if ds.user else ds.user
        password = quote(ds.password, safe="") if ds.password else ds.password
        host = ds.host
        port = ds.port
        database = ds.database

        # entra_password authenticates with an Entra UPN + password, so it needs
        # the user:pass in the URI just like classic SQL auth. The token modes
        # (entra_default/entra_mi/entra_service_principal) do NOT — the ODBC
        # driver acquires the token and the userinfo is dropped.
        is_token_entra = bool(ds.auth) and ds.auth not in ("sql", "entra_password")
        if not is_token_entra and user and password:
            userinfo = f"{user}:{password}@"
        elif not is_token_entra and user:
            userinfo = f"{user}@"
        else:
            userinfo = ""

        uri = f"mssql://{userinfo}{host}:{port}/{database}/{table_ref}"

        # Bake the auth mode into the URI query so it flows through
        # resolve_connection_params() -> get_connection(). All non-sql modes
        # (token modes AND entra_password) carry auth=; entra_password also
        # keeps its userinfo above so the connector gets UID/PWD.
        query: Dict[str, str] = {}
        if ds.auth and ds.auth != "sql":
            query["auth"] = ds.auth
        if ds.client_id:
            query["client_id"] = ds.client_id
        if ds.client_secret:
            query["client_secret"] = ds.client_secret
        if ds.tenant_id:
            query["tenant_id"] = ds.tenant_id

        if query:
            from urllib.parse import urlencode

            uri += "?" + urlencode(query)

        return uri

    elif isinstance(ds, ClickHouseDatasourceConfig):
        # clickhouse://user:pass@host:port/database/table  (no schema layer)
        from urllib.parse import quote

        scheme = "clickhouses" if ds.secure else "clickhouse"
        user = quote(ds.user, safe="") if ds.user else ds.user
        password = quote(ds.password, safe="") if ds.password else ds.password

        if user and password:
            userinfo = f"{user}:{password}@"
        elif user:
            userinfo = f"{user}@"
        else:
            userinfo = ""

        return f"{scheme}://{userinfo}{ds.host}:{ds.port}/{ds.database}/{table_ref}"

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
  preplan: "on"         # on | off - Parquet metadata preflight
  pushdown: "on"        # on | off - SQL pushdown to DuckDB
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
