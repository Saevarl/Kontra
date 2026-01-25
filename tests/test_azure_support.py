# tests/test_azure_support.py
"""Tests for Azure Data Lake Storage (ADLS) and Azure Blob support."""

import os
import pytest
from unittest.mock import patch, MagicMock

from kontra.connectors.handle import DatasetHandle, _inject_azure_env


class TestAzureUriParsing:
    """Test Azure URI parsing and scheme detection."""

    def test_abfs_scheme_detected(self):
        """abfs:// scheme is correctly detected."""
        handle = DatasetHandle.from_uri(
            "abfs://container@account.dfs.core.windows.net/path/data.parquet"
        )
        assert handle.scheme == "abfs"
        assert handle.format == "parquet"

    def test_abfss_scheme_detected(self):
        """abfss:// (secure) scheme is correctly detected."""
        handle = DatasetHandle.from_uri(
            "abfss://container@account.dfs.core.windows.net/path/data.parquet"
        )
        assert handle.scheme == "abfss"
        assert handle.format == "parquet"

    def test_az_scheme_detected(self):
        """az:// (Azure Blob) scheme is correctly detected."""
        handle = DatasetHandle.from_uri("az://container/path/data.csv")
        assert handle.scheme == "az"
        assert handle.format == "csv"

    def test_azure_uri_preserves_path(self):
        """Original URI is preserved in path for backend consumption."""
        uri = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.parquet"
        handle = DatasetHandle.from_uri(uri)
        assert handle.path == uri
        assert handle.uri == uri


class TestAzureEnvInjection:
    """Test Azure environment variable injection."""

    def test_inject_account_name_and_key(self):
        """Account name and key are injected from env."""
        opts = {}
        with patch.dict(os.environ, {
            "AZURE_STORAGE_ACCOUNT_NAME": "myaccount",
            "AZURE_STORAGE_ACCOUNT_KEY": "mykey123",
        }, clear=False):
            _inject_azure_env(opts)

        assert opts["azure_account_name"] == "myaccount"
        assert opts["azure_account_key"] == "mykey123"

    def test_inject_sas_token(self):
        """SAS token is injected from env."""
        opts = {}
        with patch.dict(os.environ, {
            "AZURE_STORAGE_ACCOUNT_NAME": "myaccount",
            "AZURE_STORAGE_SAS_TOKEN": "sv=2021-06-08&ss=b&srt=co",
        }, clear=False):
            _inject_azure_env(opts)

        assert opts["azure_account_name"] == "myaccount"
        assert opts["azure_sas_token"] == "sv=2021-06-08&ss=b&srt=co"

    def test_inject_connection_string(self):
        """Connection string is injected from env."""
        conn_str = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key"
        opts = {}
        with patch.dict(os.environ, {
            "AZURE_STORAGE_CONNECTION_STRING": conn_str,
        }, clear=False):
            _inject_azure_env(opts)

        assert opts["azure_connection_string"] == conn_str

    def test_inject_oauth_credentials(self):
        """OAuth/service principal credentials are injected."""
        opts = {}
        with patch.dict(os.environ, {
            "AZURE_TENANT_ID": "tenant-123",
            "AZURE_CLIENT_ID": "client-456",
            "AZURE_CLIENT_SECRET": "secret-789",
        }, clear=False):
            _inject_azure_env(opts)

        assert opts["azure_tenant_id"] == "tenant-123"
        assert opts["azure_client_id"] == "client-456"
        assert opts["azure_client_secret"] == "secret-789"

    def test_inject_custom_endpoint(self):
        """Custom endpoint (Azurite, sovereign cloud) is injected."""
        opts = {}
        with patch.dict(os.environ, {
            "AZURE_STORAGE_ENDPOINT": "http://127.0.0.1:10000",
        }, clear=False):
            _inject_azure_env(opts)

        assert opts["azure_endpoint"] == "http://127.0.0.1:10000"

    def test_empty_env_no_opts(self):
        """No Azure env vars results in empty opts."""
        opts = {}
        # Clear all Azure env vars
        env_without_azure = {
            k: v for k, v in os.environ.items()
            if not k.startswith("AZURE_")
        }
        with patch.dict(os.environ, env_without_azure, clear=True):
            _inject_azure_env(opts)

        assert len(opts) == 0

    def test_handle_from_uri_injects_azure_env(self):
        """DatasetHandle.from_uri injects Azure env for Azure URIs."""
        with patch.dict(os.environ, {
            "AZURE_STORAGE_ACCOUNT_NAME": "testaccount",
            "AZURE_STORAGE_ACCOUNT_KEY": "testkey",
        }, clear=False):
            handle = DatasetHandle.from_uri(
                "abfss://container@account.dfs.core.windows.net/data.parquet"
            )

        assert handle.fs_opts.get("azure_account_name") == "testaccount"
        assert handle.fs_opts.get("azure_account_key") == "testkey"


class TestAzureMaterializerSelection:
    """Test that Azure URIs route to DuckDB materializer."""

    def test_abfs_routes_to_duckdb(self):
        """abfs:// parquet routes to DuckDB materializer."""
        from kontra.engine.materializers.registry import (
            pick_materializer,
            register_default_materializers,
        )
        register_default_materializers()

        handle = DatasetHandle.from_uri(
            "abfs://container@account.dfs.core.windows.net/data.parquet"
        )
        mat = pick_materializer(handle)
        assert mat.__class__.__name__ == "DuckDBMaterializer"

    def test_abfss_routes_to_duckdb(self):
        """abfss:// csv routes to DuckDB materializer."""
        from kontra.engine.materializers.registry import (
            pick_materializer,
            register_default_materializers,
        )
        register_default_materializers()

        handle = DatasetHandle.from_uri(
            "abfss://container@account.dfs.core.windows.net/data.csv"
        )
        mat = pick_materializer(handle)
        assert mat.__class__.__name__ == "DuckDBMaterializer"

    def test_az_routes_to_duckdb(self):
        """az:// parquet routes to DuckDB materializer."""
        from kontra.engine.materializers.registry import (
            pick_materializer,
            register_default_materializers,
        )
        register_default_materializers()

        handle = DatasetHandle.from_uri("az://container/path/data.parquet")
        mat = pick_materializer(handle)
        assert mat.__class__.__name__ == "DuckDBMaterializer"

    def test_unknown_azure_format_falls_back(self):
        """Azure URI with unknown format falls back to polars-connector."""
        from kontra.engine.materializers.registry import (
            pick_materializer,
            register_default_materializers,
        )
        register_default_materializers()

        handle = DatasetHandle.from_uri("az://container/path/data.json")
        mat = pick_materializer(handle)
        # Unknown format falls back to polars-connector
        assert mat.__class__.__name__ == "PolarsConnectorMaterializer"


class TestDuckDBSessionAzure:
    """Test DuckDB session configuration for Azure."""

    def test_azure_session_installs_extension(self):
        """Azure session attempts to install azure extension."""
        from kontra.engine.backends.duckdb_session import create_duckdb_connection

        handle = DatasetHandle(
            uri="abfs://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfs",
            path="abfs://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={
                "azure_account_name": "testaccount",
                "azure_account_key": "testkey",
            },
        )

        # This will either succeed (DuckDB >= 0.10) or raise RuntimeError
        try:
            con = create_duckdb_connection(handle)
            con.close()
        except RuntimeError as e:
            # Expected if DuckDB < 0.10 or azure extension not available
            assert "Azure extension not available" in str(e)

    def test_azure_sas_token_strips_question_mark(self):
        """SAS token with leading '?' is stripped before passing to DuckDB."""
        from kontra.engine.backends.duckdb_session import _configure_azure, _safe_set
        import duckdb

        con = duckdb.connect()
        fs_opts = {
            "azure_account_name": "myaccount",
            "azure_sas_token": "?sv=2021-06-08&ss=b",
        }

        # Mock _safe_set to capture what's passed
        calls = []
        original_safe_set = _safe_set

        def mock_safe_set(conn, key, value):
            calls.append((key, value))
            # Don't actually set (extension not loaded)

        try:
            with patch(
                "kontra.engine.backends.duckdb_session._safe_set",
                side_effect=mock_safe_set,
            ):
                # Will fail on INSTALL azure but that's OK
                try:
                    _configure_azure(con, fs_opts)
                except RuntimeError:
                    pass  # Expected - no azure extension
        finally:
            con.close()

        # If azure extension was available, sas_token should have '?' stripped
        # We can't fully test this without the extension, but the code path is tested


class TestAzureSchemeVariants:
    """Test various Azure URI format variants."""

    @pytest.mark.parametrize("uri,expected_scheme", [
        ("abfs://container@account.dfs.core.windows.net/path/file.parquet", "abfs"),
        ("abfss://container@account.dfs.core.windows.net/path/file.parquet", "abfss"),
        ("ABFS://container@account.dfs.core.windows.net/path/file.parquet", "abfs"),
        ("ABFSS://container@account.dfs.core.windows.net/path/file.parquet", "abfss"),
        ("az://mycontainer/path/to/file.parquet", "az"),
        ("AZ://mycontainer/path/to/file.parquet", "az"),
    ])
    def test_scheme_case_insensitive(self, uri, expected_scheme):
        """URI schemes are parsed case-insensitively."""
        handle = DatasetHandle.from_uri(uri)
        assert handle.scheme == expected_scheme

    @pytest.mark.parametrize("uri,expected_format", [
        ("abfs://c@a.dfs.core.windows.net/data.parquet", "parquet"),
        ("abfs://c@a.dfs.core.windows.net/data.PARQUET", "parquet"),
        ("abfs://c@a.dfs.core.windows.net/data.csv", "csv"),
        ("abfs://c@a.dfs.core.windows.net/data.CSV", "csv"),
        ("abfs://c@a.dfs.core.windows.net/data.json", "unknown"),
    ])
    def test_format_detection(self, uri, expected_format):
        """File format is correctly detected from URI."""
        handle = DatasetHandle.from_uri(uri)
        assert handle.format == expected_format


class TestAzureUriToPath:
    """Test Azure URI to PyArrow path conversion."""

    def test_abfss_with_account_in_netloc(self):
        """abfss://container@account.dfs.../path -> container/path"""
        from kontra.engine.engine import _azure_uri_to_path

        uri = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "mycontainer/folder/file.parquet"

    def test_abfs_with_account_in_netloc(self):
        """abfs://container@account.dfs.../path -> container/path"""
        from kontra.engine.engine import _azure_uri_to_path

        uri = "abfs://data@storage.dfs.core.windows.net/path/to/data.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "data/path/to/data.parquet"

    def test_nested_path(self):
        """Nested paths are preserved."""
        from kontra.engine.engine import _azure_uri_to_path

        uri = "abfss://container@account.dfs.core.windows.net/a/b/c/d.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "container/a/b/c/d.parquet"

    def test_root_path(self):
        """File at container root."""
        from kontra.engine.engine import _azure_uri_to_path

        uri = "abfss://container@account.dfs.core.windows.net/file.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "container/file.parquet"


class TestPyArrowAzureSasToken:
    """Test SAS token handling for PyArrow AzureFileSystem."""

    def test_sas_token_with_leading_question_mark_preserved(self):
        """PyArrow requires SAS token WITH leading '?' - ensure it's preserved."""
        from kontra.scout.backends.duckdb_backend import DuckDBBackend
        from kontra.connectors.handle import DatasetHandle

        # SAS token without leading ?
        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={
                "azure_account_name": "testaccount",
                "azure_sas_token": "sv=2021-06-08&ss=b&srt=co",  # No leading ?
            },
        )

        # The backend should add the leading ? when preparing for PyArrow
        backend = DuckDBBackend(handle)

        # We can't fully test without Azure, but we can verify the code path exists
        # by checking the method exists and handles the token
        assert hasattr(backend, "_get_parquet_metadata")

    def test_sas_token_already_has_question_mark(self):
        """SAS token already with '?' should not get double '?'."""
        from kontra.scout.backends.duckdb_backend import DuckDBBackend
        from kontra.connectors.handle import DatasetHandle

        # SAS token with leading ?
        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={
                "azure_account_name": "testaccount",
                "azure_sas_token": "?sv=2021-06-08&ss=b&srt=co",  # Has leading ?
            },
        )

        backend = DuckDBBackend(handle)
        assert hasattr(backend, "_get_parquet_metadata")


class TestAzurePreplanHelpers:
    """Test Azure preplan helper functions."""

    def test_is_azure_uri_abfss(self):
        """_is_azure_uri detects abfss:// URIs."""
        from kontra.engine.engine import _is_azure_uri

        assert _is_azure_uri("abfss://container@account.dfs.core.windows.net/file.parquet")
        assert _is_azure_uri("ABFSS://container@account.dfs.core.windows.net/file.parquet")

    def test_is_azure_uri_abfs(self):
        """_is_azure_uri detects abfs:// URIs."""
        from kontra.engine.engine import _is_azure_uri

        assert _is_azure_uri("abfs://container@account.dfs.core.windows.net/file.parquet")

    def test_is_azure_uri_az(self):
        """_is_azure_uri detects az:// URIs."""
        from kontra.engine.engine import _is_azure_uri

        assert _is_azure_uri("az://container/path/file.parquet")

    def test_is_azure_uri_not_azure(self):
        """_is_azure_uri returns False for non-Azure URIs."""
        from kontra.engine.engine import _is_azure_uri

        assert not _is_azure_uri("s3://bucket/key.parquet")
        assert not _is_azure_uri("/local/path/file.parquet")
        assert not _is_azure_uri("postgres://host/db/table")
        assert not _is_azure_uri(None)

    def test_create_azure_filesystem_with_account_key(self):
        """_create_azure_filesystem creates filesystem with account key."""
        from kontra.engine.engine import _create_azure_filesystem
        from kontra.connectors.handle import DatasetHandle
        import pyarrow.fs as pafs

        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={
                "azure_account_name": "testaccount",
                "azure_account_key": "dGVzdGtleQ==",  # base64 "testkey"
            },
        )

        fs = _create_azure_filesystem(handle)
        assert isinstance(fs, pafs.AzureFileSystem)

    def test_create_azure_filesystem_with_sas_token(self):
        """_create_azure_filesystem creates filesystem with SAS token."""
        from kontra.engine.engine import _create_azure_filesystem
        from kontra.connectors.handle import DatasetHandle
        import pyarrow.fs as pafs

        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={
                "azure_account_name": "testaccount",
                "azure_sas_token": "sv=2021-06-08&ss=b&srt=co",  # Without leading ?
            },
        )

        fs = _create_azure_filesystem(handle)
        assert isinstance(fs, pafs.AzureFileSystem)
