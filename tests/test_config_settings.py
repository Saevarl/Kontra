# tests/test_config_settings.py
"""Tests for Kontra configuration system."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from kontra.config.settings import (
    substitute_env_vars,
    substitute_env_vars_recursive,
    DefaultsConfig,
    ScoutConfig,
    EnvironmentConfig,
    KontraConfig,
    EffectiveConfig,
    PostgresDatasourceConfig,
    FilesDatasourceConfig,
    S3DatasourceConfig,
    load_config_file,
    resolve_effective_config,
    resolve_datasource,
    list_datasources,
    find_config_file,
    DEFAULT_CONFIG_TEMPLATE,
)
from kontra.errors import (
    ConfigParseError,
    ConfigValidationError,
    UnknownEnvironmentError,
)


class TestEnvVarSubstitution:
    """Tests for environment variable substitution."""

    def test_simple_substitution(self):
        """${VAR} is replaced with env var value."""
        os.environ["TEST_VAR"] = "hello"
        try:
            result = substitute_env_vars("prefix_${TEST_VAR}_suffix")
            assert result == "prefix_hello_suffix"
        finally:
            del os.environ["TEST_VAR"]

    def test_missing_var_becomes_empty(self):
        """Missing env var becomes empty string."""
        # Ensure var doesn't exist
        os.environ.pop("NONEXISTENT_VAR", None)
        result = substitute_env_vars("prefix_${NONEXISTENT_VAR}_suffix")
        assert result == "prefix__suffix"

    def test_multiple_vars(self):
        """Multiple vars in same string."""
        os.environ["VAR1"] = "one"
        os.environ["VAR2"] = "two"
        try:
            result = substitute_env_vars("${VAR1}-${VAR2}")
            assert result == "one-two"
        finally:
            del os.environ["VAR1"]
            del os.environ["VAR2"]

    def test_recursive_substitution(self):
        """Substitution works recursively in nested structures."""
        os.environ["HOST"] = "localhost"
        os.environ["PORT"] = "5432"
        try:
            data = {
                "connection": "postgres://${HOST}:${PORT}/db",
                "nested": {
                    "url": "http://${HOST}",
                },
                "list": ["${HOST}", "${PORT}"],
            }
            result = substitute_env_vars_recursive(data)
            assert result["connection"] == "postgres://localhost:5432/db"
            assert result["nested"]["url"] == "http://localhost"
            assert result["list"] == ["localhost", "5432"]
        finally:
            del os.environ["HOST"]
            del os.environ["PORT"]


class TestPydanticModels:
    """Tests for config Pydantic models."""

    def test_defaults_config_defaults(self):
        """DefaultsConfig has sensible defaults."""
        config = DefaultsConfig()
        assert config.preplan == "auto"
        assert config.pushdown == "auto"
        assert config.projection == "on"
        assert config.output_format == "rich"
        assert config.stats == "none"
        assert config.state_backend == "local"
        assert config.csv_mode == "auto"

    def test_scout_config_defaults(self):
        """ScoutConfig has sensible defaults."""
        config = ScoutConfig()
        assert config.preset == "scan"  # New default preset name (v0.7+)
        assert config.save_profile is False
        assert config.include_patterns is False

    def test_environment_config_all_optional(self):
        """EnvironmentConfig fields are all optional."""
        config = EnvironmentConfig()
        assert config.preplan is None
        assert config.pushdown is None
        assert config.projection is None

    def test_kontra_config_from_dict(self):
        """KontraConfig can be created from dict."""
        data = {
            "version": "1",
            "defaults": {
                "preplan": "on",
                "state_backend": "s3://bucket/prefix",
            },
            "scout": {
                "preset": "deep",
            },
            "environments": {
                "production": {
                    "preplan": "on",
                    "pushdown": "on",
                }
            },
        }
        config = KontraConfig.model_validate(data)
        assert config.defaults.preplan == "on"
        assert config.defaults.state_backend == "s3://bucket/prefix"
        assert config.scout.preset == "deep"
        assert "production" in config.environments
        assert config.environments["production"].preplan == "on"

    def test_invalid_version_raises(self):
        """Invalid version raises validation error."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            KontraConfig.model_validate({"version": "2"})


class TestConfigFileLoading:
    """Tests for loading config files."""

    def test_load_valid_config(self, tmp_path):
        """Valid config file loads successfully."""
        config_path = tmp_path / "config.yml"
        config_path.write_text("""
version: "1"
defaults:
  preplan: "on"
  state_backend: local
scout:
  preset: lite
""")
        config = load_config_file(config_path)
        assert config.defaults.preplan == "on"
        assert config.scout.preset == "lite"

    def test_load_with_env_substitution(self, tmp_path):
        """Env vars are substituted when loading."""
        os.environ["TEST_BUCKET"] = "my-bucket"
        try:
            config_path = tmp_path / "config.yml"
            config_path.write_text("""
version: "1"
defaults:
  state_backend: s3://${TEST_BUCKET}/kontra-state
""")
            config = load_config_file(config_path)
            assert config.defaults.state_backend == "s3://my-bucket/kontra-state"
        finally:
            del os.environ["TEST_BUCKET"]

    def test_load_invalid_yaml(self, tmp_path):
        """Invalid YAML raises ConfigParseError."""
        config_path = tmp_path / "config.yml"
        config_path.write_text("""
version: "1"
defaults:
  preplan: "on
  missing_quote
""")
        with pytest.raises(ConfigParseError):
            load_config_file(config_path)

    def test_load_invalid_structure(self, tmp_path):
        """Invalid structure raises ConfigValidationError."""
        config_path = tmp_path / "config.yml"
        config_path.write_text("""
version: "1"
defaults:
  preplan: "invalid_value"
""")
        with pytest.raises(ConfigValidationError):
            load_config_file(config_path)


class TestConfigResolution:
    """Tests for config resolution and precedence."""

    def test_defaults_when_no_config(self, tmp_path, monkeypatch):
        """Defaults are used when no config file exists."""
        monkeypatch.chdir(tmp_path)
        config = resolve_effective_config()
        assert config.preplan == "auto"
        assert config.pushdown == "auto"
        assert config.projection == "on"

    def test_config_file_overrides_defaults(self, tmp_path, monkeypatch):
        """Config file values override defaults."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "on"
  pushdown: "off"
""")
        config = resolve_effective_config()
        assert config.preplan == "on"
        assert config.pushdown == "off"
        # Non-specified defaults still apply
        assert config.projection == "on"

    def test_environment_overrides_defaults(self, tmp_path, monkeypatch):
        """Environment overlay overrides defaults."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "auto"
  state_backend: local
environments:
  production:
    preplan: "on"
    state_backend: postgres://prod-db/kontra
""")
        config = resolve_effective_config(env_name="production")
        assert config.preplan == "on"
        assert config.state_backend == "postgres://prod-db/kontra"
        assert config.environment == "production"

    def test_cli_overrides_everything(self, tmp_path, monkeypatch):
        """CLI overrides take precedence over everything."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "on"
environments:
  production:
    preplan: "on"
""")
        cli_overrides = {"preplan": "off"}
        config = resolve_effective_config(env_name="production", cli_overrides=cli_overrides)
        assert config.preplan == "off"  # CLI wins

    def test_unknown_environment_raises(self, tmp_path, monkeypatch):
        """Unknown environment raises UnknownEnvironmentError."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
environments:
  production:
    preplan: "on"
""")
        with pytest.raises(UnknownEnvironmentError):
            resolve_effective_config(env_name="staging")


class TestEffectiveConfig:
    """Tests for EffectiveConfig dataclass."""

    def test_to_dict(self):
        """to_dict produces serializable output."""
        config = EffectiveConfig(
            preplan="on",
            pushdown="auto",
            scout_preset="deep",
        )
        d = config.to_dict()
        assert d["preplan"] == "on"
        assert d["pushdown"] == "auto"
        assert d["profile"]["preset"] == "deep"


class TestConfigTemplate:
    """Tests for config template."""

    def test_template_is_valid_yaml(self):
        """Template can be parsed as YAML."""
        data = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE)
        assert data["version"] == "1"
        assert "defaults" in data
        assert "profile" in data  # Renamed from "scout" in v0.7
        assert "environments" in data

    def test_template_creates_valid_config(self, tmp_path):
        """Template produces valid KontraConfig."""
        config_path = tmp_path / "config.yml"
        config_path.write_text(DEFAULT_CONFIG_TEMPLATE)
        config = load_config_file(config_path)
        assert config.version == "1"
        assert config.defaults.preplan == "auto"

    def test_template_has_datasources_section(self):
        """Template includes datasources section."""
        data = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE)
        assert "datasources" in data


class TestDatasourceModels:
    """Tests for datasource Pydantic models."""

    def test_postgres_datasource_config(self):
        """PostgresDatasourceConfig with defaults."""
        config = PostgresDatasourceConfig(
            host="localhost",
            database="testdb",
            tables={"users": "public.users"}
        )
        assert config.type == "postgres"
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.tables["users"] == "public.users"

    def test_files_datasource_config(self):
        """FilesDatasourceConfig with defaults."""
        config = FilesDatasourceConfig(
            base_path="./data",
            tables={"users": "users.parquet"}
        )
        assert config.type == "files"
        assert config.base_path == "./data"
        assert config.tables["users"] == "users.parquet"

    def test_s3_datasource_config(self):
        """S3DatasourceConfig."""
        config = S3DatasourceConfig(
            bucket="my-bucket",
            prefix="data/",
            tables={"events": "events.parquet"}
        )
        assert config.type == "s3"
        assert config.bucket == "my-bucket"
        assert config.tables["events"] == "events.parquet"


class TestDatasourceResolution:
    """Tests for datasource resolution."""

    def test_direct_uri_passthrough(self):
        """Direct URIs are returned unchanged."""
        assert resolve_datasource("postgres://localhost/db") == "postgres://localhost/db"
        assert resolve_datasource("s3://bucket/key.parquet") == "s3://bucket/key.parquet"
        assert resolve_datasource("/absolute/path.parquet") == "/absolute/path.parquet"
        assert resolve_datasource("data.parquet") == "data.parquet"
        assert resolve_datasource("data.csv") == "data.csv"
        assert resolve_datasource("path/to/file.parquet") == "path/to/file.parquet"

    def test_resolve_postgres_datasource(self, tmp_path, monkeypatch):
        """Resolve postgres datasource reference."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: db.example.com
    port: 5432
    user: admin
    password: secret
    database: production
    tables:
      users: public.users
      orders: sales.orders
""")
        uri = resolve_datasource("prod_db.users")
        assert uri == "postgres://admin:secret@db.example.com:5432/production/public.users"

        uri2 = resolve_datasource("prod_db.orders")
        assert uri2 == "postgres://admin:secret@db.example.com:5432/production/sales.orders"

    def test_resolve_files_datasource(self, tmp_path, monkeypatch):
        """Resolve files datasource reference."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  local_data:
    type: files
    base_path: ./data
    tables:
      users: users.parquet
      orders: orders/orders.csv
""")
        uri = resolve_datasource("local_data.users")
        assert uri == "data/users.parquet"

        uri2 = resolve_datasource("local_data.orders")
        assert uri2 == "data/orders/orders.csv"

    def test_resolve_s3_datasource(self, tmp_path, monkeypatch):
        """Resolve S3 datasource reference."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  data_lake:
    type: s3
    bucket: my-bucket
    prefix: warehouse/
    tables:
      events: events.parquet
""")
        uri = resolve_datasource("data_lake.events")
        assert uri == "s3://my-bucket/warehouse/events.parquet"

    def test_unknown_datasource_raises(self, tmp_path, monkeypatch):
        """Unknown datasource raises ValueError."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
""")
        with pytest.raises(ValueError) as exc_info:
            resolve_datasource("staging_db.users")
        assert "Unknown datasource" in str(exc_info.value)
        assert "staging_db" in str(exc_info.value)

    def test_unknown_table_raises(self, tmp_path, monkeypatch):
        """Unknown table raises ValueError."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
""")
        with pytest.raises(ValueError) as exc_info:
            resolve_datasource("prod_db.orders")
        assert "Unknown table" in str(exc_info.value)
        assert "orders" in str(exc_info.value)

    def test_env_var_substitution_in_datasource(self, tmp_path, monkeypatch):
        """Environment variables are substituted in datasource configs."""
        monkeypatch.chdir(tmp_path)
        os.environ["TEST_DB_HOST"] = "prod-db.example.com"
        os.environ["TEST_DB_USER"] = "produser"
        os.environ["TEST_DB_PASS"] = "prodpass"
        try:
            (tmp_path / ".kontra").mkdir()
            (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: ${TEST_DB_HOST}
    user: ${TEST_DB_USER}
    password: ${TEST_DB_PASS}
    database: production
    tables:
      users: public.users
""")
            uri = resolve_datasource("prod_db.users")
            assert "prod-db.example.com" in uri
            assert "produser:prodpass" in uri
        finally:
            del os.environ["TEST_DB_HOST"]
            del os.environ["TEST_DB_USER"]
            del os.environ["TEST_DB_PASS"]


class TestListDatasources:
    """Tests for listing datasources."""

    def test_list_datasources(self, tmp_path, monkeypatch):
        """List all datasources and tables."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
      orders: public.orders
  local_data:
    type: files
    base_path: ./data
    tables:
      events: events.parquet
""")
        sources = list_datasources()
        assert "prod_db" in sources
        assert "local_data" in sources
        assert "users" in sources["prod_db"]
        assert "orders" in sources["prod_db"]
        assert "events" in sources["local_data"]

    def test_list_datasources_empty(self, tmp_path, monkeypatch):
        """List returns empty dict when no config."""
        monkeypatch.chdir(tmp_path)
        sources = list_datasources()
        assert sources == {}


class TestResolveTableOnly:
    """Tests for resolving just a table name (without datasource prefix)."""

    def test_resolve_table_only(self, tmp_path, monkeypatch):
        """Resolve just a table name when unique across datasources."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
      orders: public.orders
""")
        # "users" is unique, should resolve to prod_db.users
        uri = resolve_datasource("users")
        assert "localhost" in uri
        assert "public.users" in uri

    def test_resolve_table_only_ambiguous_raises(self, tmp_path, monkeypatch):
        """Ambiguous table name (in multiple datasources) raises error."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
  staging_db:
    type: postgres
    host: localhost
    database: staging
    tables:
      users: public.users
""")
        with pytest.raises(ValueError) as exc_info:
            resolve_datasource("users")
        assert "Ambiguous" in str(exc_info.value)
        assert "prod_db.users" in str(exc_info.value)
        assert "staging_db.users" in str(exc_info.value)

    def test_resolve_table_only_not_found_raises(self, tmp_path, monkeypatch):
        """Unknown table name raises error with available tables."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
datasources:
  prod_db:
    type: postgres
    host: localhost
    database: db
    tables:
      users: public.users
      orders: public.orders
""")
        with pytest.raises(ValueError) as exc_info:
            resolve_datasource("products")
        assert "Unknown table" in str(exc_info.value)
        assert "products" in str(exc_info.value)
        assert "prod_db.users" in str(exc_info.value)

    def test_resolve_table_only_no_config_raises(self, tmp_path, monkeypatch):
        """Table lookup without config file raises error."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValueError) as exc_info:
            resolve_datasource("users")
        assert "No config file exists" in str(exc_info.value)
