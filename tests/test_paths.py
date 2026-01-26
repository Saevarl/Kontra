# tests/test_paths.py
"""Tests for execution path detection."""

import pytest
import polars as pl

from kontra.engine.paths import (
    detect_execution_path,
    get_database_type,
    _is_database_connection,
    _has_file_extension,
)


class TestDetectExecutionPath:
    """Tests for detect_execution_path function."""

    # Database URIs
    @pytest.mark.parametrize("uri", [
        "postgres://localhost/db/users",
        "postgres://user:pass@host:5432/db/schema.table",
        "postgresql://localhost/db",
        "mssql://localhost/db/dbo.orders",
        "sqlserver://host:1433/db/table",
    ])
    def test_database_uris(self, uri: str):
        assert detect_execution_path(uri) == "database"

    # File URIs
    @pytest.mark.parametrize("uri", [
        "data.parquet",
        "./path/to/file.parquet",
        "/absolute/path/data.csv",
        "s3://bucket/key.parquet",
        "s3://bucket/prefix/data.csv",
        "abfss://container@account.dfs.core.windows.net/path/file.parquet",
        "az://container/blob.parquet",
        "gs://bucket/object.parquet",
        "https://example.com/data.parquet",
        "http://localhost:8000/file.csv",
    ])
    def test_file_uris(self, uri: str):
        assert detect_execution_path(uri) == "file"

    # DataFrame inputs
    def test_polars_dataframe(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        assert detect_execution_path(df) == "dataframe"

    def test_polars_lazyframe(self):
        lf = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        assert detect_execution_path(lf) == "dataframe"

    def test_list_of_dicts(self):
        data = [{"a": 1}, {"a": 2}]
        assert detect_execution_path(data) == "dataframe"

    def test_single_dict(self):
        data = {"a": 1, "b": 2}
        assert detect_execution_path(data) == "dataframe"

    def test_empty_list(self):
        assert detect_execution_path([]) == "dataframe"

    def test_empty_dict(self):
        assert detect_execution_path({}) == "dataframe"

    # BYOC connection objects
    def test_mock_database_connection(self):
        class MockConnection:
            def cursor(self): pass
            def commit(self): pass

        conn = MockConnection()
        assert detect_execution_path(conn) == "database"


class TestGetDatabaseType:
    """Tests for get_database_type function."""

    def test_postgres(self):
        assert get_database_type("postgres://host/db") == "postgres"
        assert get_database_type("postgresql://host/db") == "postgres"
        assert get_database_type("POSTGRES://HOST/DB") == "postgres"

    def test_sqlserver(self):
        assert get_database_type("mssql://host/db") == "sqlserver"
        assert get_database_type("sqlserver://host/db") == "sqlserver"
        assert get_database_type("MSSQL://HOST/DB") == "sqlserver"

    def test_non_database(self):
        assert get_database_type("data.parquet") is None
        assert get_database_type("s3://bucket/key") is None
        assert get_database_type("https://example.com") is None


class TestIsDatabaseConnection:
    """Tests for _is_database_connection helper."""

    def test_duck_typed_connection(self):
        class FakeConn:
            def cursor(self): pass
            def commit(self): pass

        assert _is_database_connection(FakeConn()) is True

    def test_not_a_connection(self):
        assert _is_database_connection("string") is False
        assert _is_database_connection(123) is False
        assert _is_database_connection([1, 2]) is False
        assert _is_database_connection({"a": 1}) is False

    def test_object_with_only_cursor(self):
        class PartialConn:
            def cursor(self): pass

        # Has cursor but not commit - not a full connection
        assert _is_database_connection(PartialConn()) is False


class TestHasFileExtension:
    """Tests for _has_file_extension helper."""

    @pytest.mark.parametrize("uri", [
        "data.parquet",
        "file.pq",
        "data.csv",
        "file.tsv",
        "data.json",
        "file.ndjson",
        "data.jsonl",
        "file.arrow",
        "data.feather",
        "file.ipc",
    ])
    def test_valid_extensions(self, uri: str):
        assert _has_file_extension(uri) is True

    @pytest.mark.parametrize("uri", [
        "prod_db.users",
        "schema.table",
        "no_extension",
        "file.unknown",
    ])
    def test_invalid_extensions(self, uri: str):
        assert _has_file_extension(uri) is False
