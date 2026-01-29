# tests/test_cloud_backends.py
"""Tests for cloud state storage backends (S3, PostgreSQL)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest

from kontra.state.backends import get_store
from kontra.state.types import ValidationState, RuleState, StateSummary


def create_test_state(contract_fp: str = "abc123def456") -> ValidationState:
    """Create a test ValidationState."""
    return ValidationState(
        contract_fingerprint=contract_fp,
        dataset_fingerprint="data123",
        contract_name="test_contract",
        dataset_uri="data.parquet",
        run_at=datetime.now(timezone.utc),
        summary=StateSummary(
            passed=True,
            total_rules=5,
            passed_rules=5,
            failed_rules=0,
        ),
        rules=[
            RuleState(
                rule_id="COL:id:not_null",
                rule_name="not_null",
                passed=True,
                failed_count=0,
                execution_source="polars",
            )
        ],
    )


class TestGetStore:
    """Tests for the get_store factory function."""

    def test_get_store_local_default(self):
        """Default backend is local."""
        store = get_store()
        assert store.__class__.__name__ == "LocalStore"

    def test_get_store_local_explicit(self):
        """Explicit 'local' returns LocalStore."""
        store = get_store("local")
        assert store.__class__.__name__ == "LocalStore"

    def test_get_store_s3_creates_s3store(self):
        """S3 URI creates S3Store."""
        with patch.dict("sys.modules", {"fsspec": MagicMock()}):
            store = get_store("s3://my-bucket/state-prefix")
            assert store.__class__.__name__ == "S3Store"
            assert store.bucket == "my-bucket"
            assert "state-prefix" in store.prefix

    def test_get_store_postgres_creates_postgresstore(self):
        """PostgreSQL URI creates PostgresStore."""
        try:
            store = get_store("postgres://user:pass@localhost:5432/testdb")
            assert store.__class__.__name__ == "PostgresStore"
        except (ConnectionError, RuntimeError):
            # Skip if psycopg not installed or postgres not running
            pytest.skip("PostgreSQL not available")

    def test_get_store_postgresql_creates_postgresstore(self):
        """postgresql:// URI also creates PostgresStore."""
        try:
            store = get_store("postgresql://user:pass@localhost:5432/testdb")
            assert store.__class__.__name__ == "PostgresStore"
        except (ConnectionError, RuntimeError):
            # Skip if psycopg not installed or postgres not running
            pytest.skip("PostgreSQL not available")

    def test_get_store_invalid_backend_raises(self):
        """Invalid backend raises ValueError."""
        with pytest.raises(ValueError, match="Unknown state backend"):
            get_store("redis://localhost:6379")


class TestS3StoreUnit:
    """Unit tests for S3Store (no actual S3 connection)."""

    def test_s3_uri_parsing(self):
        """S3Store correctly parses bucket and prefix from URI."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://my-bucket/my/prefix")
        assert store.bucket == "my-bucket"
        assert store.prefix == "my/prefix/state"

    def test_s3_uri_parsing_no_prefix(self):
        """S3Store handles URI with no prefix."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://my-bucket")
        assert store.bucket == "my-bucket"
        assert store.prefix == "state"

    def test_s3_runs_prefix(self):
        """S3Store generates correct runs prefix."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://my-bucket/prefix")
        prefix = store._runs_prefix("abc123def456gh")
        assert prefix == "my-bucket/prefix/state/abc123def456gh/runs"

    def test_s3_run_key(self):
        """S3Store generates correct run key."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://my-bucket/prefix")
        key = store._run_key("abc123def456gh", "2024-01-15T10-30-00_abc123")
        assert key.startswith("my-bucket/prefix/state/abc123def456gh/runs/")
        assert key.endswith(".json")
        assert "2024-01-15" in key

    def test_s3_storage_options_from_env(self):
        """S3Store reads storage options from environment."""
        from kontra.state.backends.s3 import S3Store

        with patch.dict("os.environ", {
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "AWS_REGION": "us-east-1",
        }):
            opts = S3Store._storage_options()
            assert opts["key"] == "test-key"
            assert opts["secret"] == "test-secret"
            assert opts["client_kwargs"]["region_name"] == "us-east-1"

    def test_s3_storage_options_custom_endpoint(self):
        """S3Store handles custom endpoint (MinIO)."""
        from kontra.state.backends.s3 import S3Store

        with patch.dict("os.environ", {
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
        }):
            opts = S3Store._storage_options()
            assert opts["client_kwargs"]["endpoint_url"] == "http://localhost:9000"
            assert opts["use_ssl"] is False

    def test_s3_repr(self):
        """S3Store has useful repr."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://my-bucket/prefix")
        assert "S3Store" in repr(store)
        assert "s3://my-bucket/prefix" in repr(store)


class TestPostgresStoreUnit:
    """Unit tests for PostgresStore (no actual database connection)."""

    def test_postgres_uri_parsing(self):
        """PostgresStore correctly parses connection params from URI."""
        from kontra.state.backends.postgres import PostgresStore

        store = PostgresStore("postgres://myuser:mypass@myhost:5433/mydb")
        params = store._conn_params
        assert params["host"] == "myhost"
        assert params["port"] == 5433
        assert params["user"] == "myuser"
        assert params["password"] == "mypass"
        assert params["dbname"] == "mydb"

    def test_postgres_env_vars(self):
        """PostgresStore reads from PGXXX environment variables."""
        from kontra.state.backends.postgres import PostgresStore

        with patch.dict("os.environ", {
            "PGHOST": "envhost",
            "PGPORT": "5434",
            "PGUSER": "envuser",
            "PGPASSWORD": "envpass",
            "PGDATABASE": "envdb",
        }, clear=False):
            store = PostgresStore("postgres:///")
            params = store._conn_params
            assert params["host"] == "envhost"
            assert params["port"] == 5434
            assert params["user"] == "envuser"
            assert params["password"] == "envpass"
            assert params["dbname"] == "envdb"

    def test_postgres_uri_overrides_env(self):
        """PostgresStore URI values override environment variables."""
        from kontra.state.backends.postgres import PostgresStore

        with patch.dict("os.environ", {
            "PGHOST": "envhost",
            "PGUSER": "envuser",
        }, clear=False):
            store = PostgresStore("postgres://uriuser@urihost/uridb")
            params = store._conn_params
            # URI values should override
            assert params["host"] == "urihost"
            assert params["user"] == "uriuser"
            assert params["dbname"] == "uridb"

    def test_postgres_database_url(self):
        """PostgresStore reads from DATABASE_URL (PaaS style)."""
        from kontra.state.backends.postgres import PostgresStore

        with patch.dict("os.environ", {
            "DATABASE_URL": "postgres://paasuser:paaspass@paashost:5432/paasdb",
        }, clear=False):
            store = PostgresStore("postgres:///")
            params = store._conn_params
            assert params["host"] == "paashost"
            assert params["user"] == "paasuser"
            assert params["password"] == "paaspass"
            assert params["dbname"] == "paasdb"

    def test_postgres_repr(self):
        """PostgresStore has useful repr."""
        from kontra.state.backends.postgres import PostgresStore

        store = PostgresStore("postgres://user:pass@myhost:5432/mydb")
        repr_str = repr(store)
        assert "PostgresStore" in repr_str
        assert "myhost" in repr_str
        assert "mydb" in repr_str
        # Password should NOT be in repr
        assert "pass" not in repr_str

    def test_postgres_create_tables_sql(self):
        """PostgresStore has valid CREATE TABLES SQL."""
        from kontra.state.backends.postgres import PostgresStore

        sql = PostgresStore.CREATE_TABLES_SQL
        assert "CREATE TABLE IF NOT EXISTS kontra_runs" in sql
        assert "contract_fingerprint TEXT NOT NULL" in sql
        assert "CREATE TABLE IF NOT EXISTS kontra_rule_results" in sql
        assert "CREATE INDEX IF NOT EXISTS" in sql


class TestS3StoreMocked:
    """Tests for S3Store with mocked S3 filesystem."""

    @pytest.fixture
    def mock_fs(self):
        """Create a mock S3 filesystem."""
        mock = MagicMock()
        return mock

    @pytest.fixture
    def s3_store(self, mock_fs):
        """Create S3Store with mocked filesystem."""
        from kontra.state.backends.s3 import S3Store

        store = S3Store("s3://test-bucket/test-prefix")
        store._fs = mock_fs
        return store

    def test_s3_save(self, s3_store, mock_fs):
        """S3Store.save() writes state to S3."""
        state = create_test_state()

        # Mock the open context manager
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(return_value=mock_file)
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        s3_store.save(state)

        # Verify open was called with correct path
        mock_fs.open.assert_called_once()
        call_args = mock_fs.open.call_args[0][0]
        assert "s3://test-bucket/test-prefix/state/abc123def456" in call_args
        assert call_args.endswith(".json")

    def test_s3_get_history_empty(self, s3_store, mock_fs):
        """S3Store.get_history() returns empty list when no files."""
        mock_fs.glob.return_value = []

        history = s3_store.get_history("abc123def456gh")
        assert history == []

    def test_s3_list_contracts(self, s3_store, mock_fs):
        """S3Store.list_contracts() returns fingerprints."""
        # Fingerprints are 16 hex characters (0-9, a-f only)
        mock_fs.ls.return_value = [
            "test-bucket/test-prefix/state/abc123def4560012/",
            "test-bucket/test-prefix/state/0123456789abcdef/",
        ]

        contracts = s3_store.list_contracts()
        assert "abc123def4560012" in contracts
        assert "0123456789abcdef" in contracts


class TestPostgresStoreMocked:
    """Tests for PostgresStore with mocked database connection."""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock database connection."""
        mock = MagicMock()
        mock_cursor = MagicMock()
        mock.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock, mock_cursor

    @pytest.fixture
    def pg_store(self, mock_conn):
        """Create PostgresStore with mocked connection."""
        from kontra.state.backends.postgres import PostgresStore

        store = PostgresStore("postgres://user:pass@localhost/testdb")
        store._conn = mock_conn[0]
        store._table_created = True
        return store, mock_conn[1]

    def test_postgres_save(self, pg_store):
        """PostgresStore.save() inserts state into database."""
        store, mock_cursor = pg_store
        state = create_test_state()

        # Mock the fetchone to return run_id and rule_result_id
        mock_cursor.fetchone.return_value = (1,)

        store.save(state)

        # Verify INSERT was executed (now uses normalized tables)
        assert mock_cursor.execute.call_count >= 1
        # Check that we inserted into kontra_runs
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("INSERT INTO" in c and "kontra_runs" in c for c in calls)

    def test_postgres_get_latest(self, pg_store):
        """PostgresStore.get_latest() queries database."""
        store, mock_cursor = pg_store

        # Mock the run row (tuple matching SELECT column order)
        mock_cursor.fetchone.return_value = (
            1,  # id
            "abc123def456",  # contract_fingerprint
            "test_contract",  # contract_name
            "data123",  # dataset_fingerprint
            "data.parquet",  # dataset_name
            datetime.now(timezone.utc),  # run_at
            None,  # duration_ms
            True,  # passed
            None,  # total_rows
            5,  # total_rules
            5,  # passed_rules
            0,  # failed_rules
            0,  # blocking_failures
            0,  # warning_failures
            0,  # info_failures
            None,  # execution_stats
            "2.0",  # schema_version
            "0.4.1",  # engine_version
        )
        mock_cursor.fetchall.return_value = []  # No rule results

        result = store.get_latest("abc123def456")

        # Verify SELECT was executed
        assert mock_cursor.execute.call_count >= 1
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("SELECT" in c and "kontra_runs" in c for c in calls)

    def test_postgres_get_latest_not_found(self, pg_store):
        """PostgresStore.get_latest() returns None when not found."""
        store, mock_cursor = pg_store
        mock_cursor.fetchone.return_value = None

        result = store.get_latest("nonexistent")
        assert result is None

    def test_postgres_get_history(self, pg_store):
        """PostgresStore.get_history() returns states from database."""
        store, mock_cursor = pg_store

        # Mock run rows (tuple matching SELECT column order)
        mock_cursor.fetchall.return_value = [
            (
                1,  # id
                "abc123def456",  # contract_fingerprint
                "test_contract",  # contract_name
                "data123",  # dataset_fingerprint
                "data.parquet",  # dataset_name
                datetime.now(timezone.utc),  # run_at
                None,  # duration_ms
                True,  # passed
                None,  # total_rows
                5,  # total_rules
                5,  # passed_rules
                0,  # failed_rules
                0,  # blocking_failures
                0,  # warning_failures
                0,  # info_failures
                None,  # execution_stats
                "2.0",  # schema_version
                "0.4.1",  # engine_version
            ),
        ]

        history = store.get_history("abc123def456", limit=10)

        # Verify LIMIT was passed
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("LIMIT" in c for c in calls)

    def test_postgres_list_contracts(self, pg_store):
        """PostgresStore.list_contracts() returns fingerprints."""
        store, mock_cursor = pg_store
        mock_cursor.fetchall.return_value = [
            ("abc123def456gh",),
            ("xyz789012345ab",),
        ]

        contracts = store.list_contracts()

        assert "abc123def456gh" in contracts
        assert "xyz789012345ab" in contracts

    def test_postgres_clear_specific_contract(self, pg_store):
        """PostgresStore.clear() deletes specific contract's states."""
        store, mock_cursor = pg_store
        mock_cursor.rowcount = 5

        deleted = store.clear("abc123def456")

        # Verify DELETE with WHERE clause
        sql = mock_cursor.execute.call_args[0][0]
        assert "DELETE FROM" in sql
        assert "WHERE contract_fingerprint" in sql
        assert deleted == 5

    def test_postgres_clear_all(self, pg_store):
        """PostgresStore.clear() deletes all states when no fingerprint."""
        store, mock_cursor = pg_store
        mock_cursor.rowcount = 100

        deleted = store.clear()

        # Verify DELETE without WHERE
        sql = mock_cursor.execute.call_args[0][0]
        assert "DELETE FROM" in sql
        assert "WHERE" not in sql
        assert deleted == 100
