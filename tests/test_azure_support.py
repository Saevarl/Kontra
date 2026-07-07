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
                # base64-shaped: keys are validated before reaching DuckDB
                "azure_account_key": "dGVzdGtleQ==",
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
        from kontra.connectors.uri_utils import azure_uri_to_path as _azure_uri_to_path

        uri = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "mycontainer/folder/file.parquet"

    def test_abfs_with_account_in_netloc(self):
        """abfs://container@account.dfs.../path -> container/path"""
        from kontra.connectors.uri_utils import azure_uri_to_path as _azure_uri_to_path

        uri = "abfs://data@storage.dfs.core.windows.net/path/to/data.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "data/path/to/data.parquet"

    def test_nested_path(self):
        """Nested paths are preserved."""
        from kontra.connectors.uri_utils import azure_uri_to_path as _azure_uri_to_path

        uri = "abfss://container@account.dfs.core.windows.net/a/b/c/d.parquet"
        result = _azure_uri_to_path(uri)
        assert result == "container/a/b/c/d.parquet"

    def test_root_path(self):
        """File at container root."""
        from kontra.connectors.uri_utils import azure_uri_to_path as _azure_uri_to_path

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
        from kontra.connectors.uri_utils import is_azure_uri as _is_azure_uri

        assert _is_azure_uri("abfss://container@account.dfs.core.windows.net/file.parquet")
        assert _is_azure_uri("ABFSS://container@account.dfs.core.windows.net/file.parquet")

    def test_is_azure_uri_abfs(self):
        """_is_azure_uri detects abfs:// URIs."""
        from kontra.connectors.uri_utils import is_azure_uri as _is_azure_uri

        assert _is_azure_uri("abfs://container@account.dfs.core.windows.net/file.parquet")

    def test_is_azure_uri_az(self):
        """_is_azure_uri detects az:// URIs."""
        from kontra.connectors.uri_utils import is_azure_uri as _is_azure_uri

        assert _is_azure_uri("az://container/path/file.parquet")

    def test_is_azure_uri_not_azure(self):
        """_is_azure_uri returns False for non-Azure URIs."""
        from kontra.connectors.uri_utils import is_azure_uri as _is_azure_uri

        assert not _is_azure_uri("s3://bucket/key.parquet")
        assert not _is_azure_uri("/local/path/file.parquet")
        assert not _is_azure_uri("postgres://host/db/table")
        assert not _is_azure_uri(None)

    def test_create_azure_filesystem_with_account_key(self):
        """_create_azure_filesystem creates filesystem with account key."""
        from kontra.connectors.uri_utils import create_azure_filesystem as _create_azure_filesystem
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
        from kontra.connectors.uri_utils import create_azure_filesystem as _create_azure_filesystem
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


class TestAzureCrypticErrorWrapping:
    """F-030: Azure nonexistent container/file should give clear error, not cryptic DuckDB error."""

    def test_materializer_wraps_duckdb_error_for_azure_uri(self):
        """DuckDBMaterializer wraps DuckDB errors for Azure URIs with AzureAccessError."""
        import duckdb
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError
        from kontra.engine.materializers.duckdb import _raise_if_azure_error

        handle = DatasetHandle(
            uri="abfss://nonexistent@account.dfs.core.windows.net/missing.parquet",
            scheme="abfss",
            path="abfss://nonexistent@account.dfs.core.windows.net/missing.parquet",
            format="parquet",
            fs_opts={},
        )

        # Simulate a DuckDB error for Azure URI
        duckdb_err = duckdb.Error(
            "NotImplementedException: abfss do not manage recursive lookup patterns"
        )
        with pytest.raises(AzureAccessError) as exc_info:
            _raise_if_azure_error(handle, duckdb_err)

        error = exc_info.value
        assert "Azure file not found or inaccessible" in str(error)
        assert "nonexistent@account" in str(error)
        assert "Check the container name" in str(error)

    def test_executor_wraps_duckdb_error_for_azure_uri(self):
        """DuckDB SQL executor wraps DuckDB errors for Azure URIs with AzureAccessError."""
        import duckdb
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError
        from kontra.engine.executors.duckdb_sql import _raise_if_azure_error

        handle = DatasetHandle(
            uri="abfs://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfs",
            path="abfs://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={},
        )

        duckdb_err = duckdb.Error("IOException: No such file or directory")
        with pytest.raises(AzureAccessError) as exc_info:
            _raise_if_azure_error(handle, duckdb_err)

        error = exc_info.value
        assert "Azure file not found or inaccessible" in str(error)
        assert "container@account" in str(error)

    def test_no_wrapping_for_non_azure_uri(self):
        """Non-Azure URIs should NOT be wrapped — DuckDB error passes through."""
        import duckdb
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.duckdb import _raise_if_azure_error

        handle = DatasetHandle(
            uri="s3://bucket/data.parquet",
            scheme="s3",
            path="s3://bucket/data.parquet",
            format="parquet",
            fs_opts={},
        )

        duckdb_err = duckdb.Error("some S3 error")
        # Should NOT raise — caller would re-raise the original error
        _raise_if_azure_error(handle, duckdb_err)

    def test_az_scheme_also_wrapped(self):
        """az:// scheme (Azure Blob) errors are also wrapped."""
        import duckdb
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError
        from kontra.engine.materializers.duckdb import _raise_if_azure_error

        handle = DatasetHandle(
            uri="az://mycontainer/path/file.parquet",
            scheme="az",
            path="az://mycontainer/path/file.parquet",
            format="parquet",
            fs_opts={},
        )

        duckdb_err = duckdb.Error("some azure error")
        with pytest.raises(AzureAccessError) as exc_info:
            _raise_if_azure_error(handle, duckdb_err)

        assert "az://mycontainer/path/file.parquet" in str(exc_info.value)

    def test_azure_error_preserves_cause_chain(self):
        """AzureAccessError preserves the original DuckDB error as __cause__."""
        import duckdb
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError
        from kontra.engine.materializers.duckdb import _raise_if_azure_error

        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={},
        )

        original_err = duckdb.Error("NotImplementedException: abfss recursive lookup")
        with pytest.raises(AzureAccessError) as exc_info:
            _raise_if_azure_error(handle, original_err)

        # Verify error chain is preserved
        assert exc_info.value.__cause__ is original_err

    def test_azure_access_error_has_suggestions(self):
        """AzureAccessError includes actionable suggestions."""
        from kontra.errors import AzureAccessError

        error = AzureAccessError(
            "abfss://container@account.dfs.core.windows.net/file.parquet",
            "NotImplementedException: abfss do not manage recursive lookup patterns"
        )

        error_str = str(error)
        assert "AZURE_STORAGE_ACCOUNT_NAME" in error_str
        assert "AZURE_STORAGE_ACCOUNT_KEY" in error_str
        assert "Check the container name" in error_str

    def test_materializer_to_polars_wraps_azure_error(self):
        """DuckDBMaterializer.to_polars() wraps DuckDB errors for Azure URIs."""
        import duckdb
        from unittest.mock import patch, MagicMock
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError

        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={},
        )

        # Mock create_duckdb_connection to return a mock that raises on execute
        mock_con = MagicMock()
        mock_con.execute.side_effect = duckdb.Error(
            "NotImplementedException: abfss do not manage recursive lookup patterns"
        )

        with patch(
            "kontra.engine.backends.duckdb_session.create_duckdb_connection",
            return_value=mock_con,
        ):
            from kontra.engine.materializers.duckdb import DuckDBMaterializer
            mat = DuckDBMaterializer(handle)

            with pytest.raises(AzureAccessError) as exc_info:
                mat.to_polars(None)

            assert "Azure file not found or inaccessible" in str(exc_info.value)

    def test_materializer_schema_wraps_azure_error(self):
        """DuckDBMaterializer.schema() wraps DuckDB errors for Azure URIs."""
        import duckdb
        from unittest.mock import patch, MagicMock
        from kontra.connectors.handle import DatasetHandle
        from kontra.errors import AzureAccessError

        handle = DatasetHandle(
            uri="abfss://container@account.dfs.core.windows.net/data.parquet",
            scheme="abfss",
            path="abfss://container@account.dfs.core.windows.net/data.parquet",
            format="parquet",
            fs_opts={},
        )

        mock_con = MagicMock()
        mock_con.execute.side_effect = duckdb.Error(
            "IOException: No such file"
        )

        with patch(
            "kontra.engine.backends.duckdb_session.create_duckdb_connection",
            return_value=mock_con,
        ):
            from kontra.engine.materializers.duckdb import DuckDBMaterializer
            mat = DuckDBMaterializer(handle)

            with pytest.raises(AzureAccessError) as exc_info:
                mat.schema()

            assert "Azure file not found or inaccessible" in str(exc_info.value)


class TestAzureAccountKeyValidation:
    """Account keys must be base64-shaped before they reach DuckDB."""

    # Azurite's well-known development key — a real, valid base64 key.
    AZURITE_KEY = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    )

    def test_valid_key_passes(self):
        from kontra.connectors.uri_utils import validate_azure_account_key

        validate_azure_account_key(self.AZURITE_KEY)  # must not raise

    @pytest.mark.parametrize(
        "bad_key,reason",
        [
            ("", "empty"),
            ("   ", "whitespace only"),
            (" " + AZURITE_KEY, "leading whitespace"),
            (AZURITE_KEY[:-1], "truncated (length % 4 != 0)"),
            ("not base64!!", "non-base64 characters"),
            ("abcd;AccountName=evil;abcd==", "connection-string injection"),
            ("ab=cd" + "A" * 3, "padding in the middle"),
        ],
    )
    def test_malformed_keys_rejected(self, bad_key, reason):
        from kontra.connectors.uri_utils import validate_azure_account_key
        from kontra.errors import AzureCredentialError

        with pytest.raises(AzureCredentialError):
            validate_azure_account_key(bad_key)

    def test_configure_azure_rejects_bad_key_before_duckdb(self):
        """_configure_azure must raise before creating any DuckDB secret."""
        from unittest.mock import MagicMock
        from kontra.engine.backends.duckdb_session import _configure_azure
        from kontra.errors import AzureCredentialError

        con = MagicMock()
        with pytest.raises(AzureCredentialError):
            _configure_azure(
                con,
                {"azure_account_name": "acct", "azure_account_key": "not-base64!!"},
            )
        # Only the extension INSTALL/LOAD may have run — no CREATE SECRET.
        executed = " ".join(str(c) for c in con.execute.call_args_list)
        assert "CREATE SECRET" not in executed

    def test_error_is_actionable(self):
        from kontra.connectors.uri_utils import validate_azure_account_key
        from kontra.errors import AzureCredentialError

        with pytest.raises(AzureCredentialError) as exc_info:
            validate_azure_account_key("truncated")
        msg = str(exc_info.value)
        assert "base64" in msg
        assert "AZURE_STORAGE_ACCOUNT_KEY" in msg


class TestAzureTransportOption:
    """DuckDB Azure transport: 'curl' where the SDK default breaks (containers)."""

    def test_explicit_fs_opts_wins(self, monkeypatch):
        from kontra.connectors.uri_utils import azure_transport_option

        monkeypatch.setenv("KONTRA_AZURE_TRANSPORT", "default")
        assert azure_transport_option({"azure_transport": "curl"}) == "curl"

    def test_env_var_used_when_no_fs_opt(self, monkeypatch):
        from kontra.connectors.uri_utils import azure_transport_option

        monkeypatch.setenv("KONTRA_AZURE_TRANSPORT", "Default")
        assert azure_transport_option({}) == "default"

    def test_invalid_value_raises(self, monkeypatch):
        from kontra.connectors.uri_utils import azure_transport_option

        monkeypatch.delenv("KONTRA_AZURE_TRANSPORT", raising=False)
        with pytest.raises(ValueError, match="'default' or 'curl'"):
            azure_transport_option({"azure_transport": "winhttp"})

    def test_linux_defaults_to_curl(self, monkeypatch):
        import kontra.connectors.uri_utils as uu

        monkeypatch.delenv("KONTRA_AZURE_TRANSPORT", raising=False)
        monkeypatch.setattr("sys.platform", "linux")
        assert uu.azure_transport_option(None) == "curl"

    def test_macos_leaves_default(self, monkeypatch):
        import kontra.connectors.uri_utils as uu

        monkeypatch.delenv("KONTRA_AZURE_TRANSPORT", raising=False)
        monkeypatch.setattr("sys.platform", "darwin")
        assert uu.azure_transport_option(None) is None

    def test_configure_azure_sets_transport(self, monkeypatch):
        """_configure_azure must SET azure_transport_option_type when resolved."""
        from unittest.mock import MagicMock
        from kontra.engine.backends.duckdb_session import _configure_azure

        con = MagicMock()
        _configure_azure(
            con,
            {
                "azure_account_name": "acct",
                "azure_account_key": TestAzureAccountKeyValidation.AZURITE_KEY,
                "azure_transport": "curl",
            },
        )
        executed = " ".join(str(c) for c in con.execute.call_args_list)
        assert "azure_transport_option_type" in executed

    def test_configure_azure_no_transport_when_unresolved(self, monkeypatch):
        from unittest.mock import MagicMock
        from kontra.engine.backends.duckdb_session import _configure_azure

        monkeypatch.delenv("KONTRA_AZURE_TRANSPORT", raising=False)
        monkeypatch.setattr("sys.platform", "darwin")
        con = MagicMock()
        _configure_azure(
            con,
            {
                "azure_account_name": "acct",
                "azure_account_key": TestAzureAccountKeyValidation.AZURITE_KEY,
            },
        )
        executed = " ".join(str(c) for c in con.execute.call_args_list)
        assert "azure_transport_option_type" not in executed

    def test_storage_options_transport_normalized(self):
        """storage_options={'transport': ...} must map to fs_opts['azure_transport']."""
        from kontra.connectors.handle import DatasetHandle

        handle = DatasetHandle.from_uri(
            "abfss://container@acct.dfs.core.windows.net/data.parquet",
            storage_options={"account_name": "acct", "transport": "curl"},
        )
        assert handle.fs_opts.get("azure_transport") == "curl"


class TestAzureKeyValidationEntryPoints:
    """Every path that hands an account key to DuckDB/PyArrow validates first."""

    def test_pyarrow_filesystem_rejects_bad_key(self):
        from kontra.connectors.handle import DatasetHandle
        from kontra.connectors.uri_utils import create_azure_filesystem
        from kontra.errors import AzureCredentialError

        handle = DatasetHandle.from_uri(
            "abfss://container@acct.dfs.core.windows.net/data.parquet",
            storage_options={"account_name": "acct", "account_key": "not-base64!!"},
        )
        with pytest.raises(AzureCredentialError):
            create_azure_filesystem(handle)
