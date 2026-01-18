# tests/test_scout_backends.py
"""Unit tests for Scout profiler backends (mocked, no real database required)."""

from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest


class TestPostgreSQLBackendUnit:
    """Unit tests for PostgreSQLBackend with mocked database."""

    @pytest.fixture
    def mock_handle(self):
        """Create a mock DatasetHandle for PostgreSQL."""
        handle = MagicMock()
        handle.db_params = MagicMock()
        handle.db_params.schema = "public"
        handle.db_params.table = "users"
        return handle

    @pytest.fixture
    def pg_backend(self, mock_handle):
        """Create PostgreSQLBackend with mocked connection."""
        from kontra.scout.backends.postgres_backend import PostgreSQLBackend

        with patch(
            "kontra.scout.backends.postgres_backend.get_connection"
        ) as mock_conn:
            backend = PostgreSQLBackend(mock_handle)
            backend._conn = MagicMock()
            return backend

    def test_supports_metadata_only(self, pg_backend):
        """PostgreSQLBackend supports metadata-only profiling."""
        assert pg_backend.supports_metadata_only() is True

    def test_supports_strategic_standard(self, pg_backend):
        """PostgreSQLBackend supports strategic standard profiling."""
        assert pg_backend.supports_strategic_standard() is True

    def test_profile_metadata_only_basic(self, pg_backend):
        """profile_metadata_only parses pg_stats correctly."""
        # Mock pg_stats query result
        pg_backend._pg_stats = {
            "user_id": {
                "null_frac": 0.0,
                "n_distinct": -1.0,  # All unique
                "most_common_vals": None,
                "most_common_freqs": None,
            },
            "status": {
                "null_frac": 0.02,
                "n_distinct": 4,  # Low cardinality
                "most_common_vals": "{active,inactive,pending,suspended}",
                "most_common_freqs": "{0.4,0.3,0.2,0.1}",
            },
            "email": {
                "null_frac": 0.05,
                "n_distinct": -0.95,  # 95% unique
                "most_common_vals": None,
                "most_common_freqs": None,
            },
        }

        schema = [("user_id", "integer"), ("status", "varchar"), ("email", "varchar")]
        row_count = 1000

        result = pg_backend.profile_metadata_only(schema, row_count)

        # user_id: no nulls, all unique
        assert result["user_id"]["null_count"] == 0
        assert result["user_id"]["distinct_count"] == 1000  # -1.0 * 1000

        # status: 2% nulls, 4 distinct values
        assert result["status"]["null_count"] == 20  # 0.02 * 1000
        assert result["status"]["distinct_count"] == 4
        assert result["status"]["most_common_vals"] == [
            "active",
            "inactive",
            "pending",
            "suspended",
        ]

        # email: 5% nulls, 95% unique
        assert result["email"]["null_count"] == 50
        assert result["email"]["distinct_count"] == 950  # 0.95 * 1000

        # All should be marked as estimates
        for col in result.values():
            assert col["is_estimate"] is True

    def test_get_table_freshness(self, pg_backend):
        """get_table_freshness returns correct staleness metrics."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            10000,  # n_live_tup
            500,  # n_mod_since_analyze (5% stale)
            datetime(2024, 1, 15, 10, 30, 0),  # last_analyze
            datetime(2024, 1, 14, 5, 0, 0),  # last_autoanalyze
        )
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = pg_backend.get_table_freshness()

        assert result["n_live_tup"] == 10000
        assert result["n_mod_since_analyze"] == 500
        assert result["stale_ratio"] == 0.05
        assert result["is_fresh"] is True  # 5% < 20%

    def test_get_table_freshness_stale(self, pg_backend):
        """get_table_freshness detects stale statistics."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            10000,  # n_live_tup
            5000,  # n_mod_since_analyze (50% stale)
            None,
            None,
        )
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = pg_backend.get_table_freshness()

        assert result["stale_ratio"] == 0.5
        assert result["is_fresh"] is False  # 50% > 20%

    def test_get_table_freshness_no_stats(self, pg_backend):
        """get_table_freshness handles missing stats."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = pg_backend.get_table_freshness()

        assert result["stale_ratio"] == 1.0
        assert result["is_fresh"] is False

    def test_classify_columns(self, pg_backend):
        """classify_columns correctly categorizes column cardinality."""
        pg_backend._pg_stats = {
            "status": {"null_frac": 0.0, "n_distinct": 4},  # Low cardinality
            "category": {"null_frac": 0.0, "n_distinct": 500},  # Medium
            "user_id": {"null_frac": 0.0, "n_distinct": -1.0},  # High (all unique)
            "email": {"null_frac": 0.0, "n_distinct": -0.99},  # High (99% unique)
        }

        schema = [
            ("status", "varchar"),
            ("category", "varchar"),
            ("user_id", "integer"),
            ("email", "varchar"),
        ]
        row_count = 100000

        result = pg_backend.classify_columns(schema, row_count)

        # status: 4 distinct → low cardinality
        assert result["status"]["cardinality"] == "low"
        assert result["status"]["strategy"] == "group_by"
        assert result["status"]["estimated_distinct"] == 4

        # category: 500 distinct → medium cardinality
        assert result["category"]["cardinality"] == "medium"
        assert result["category"]["strategy"] == "sample"
        assert result["category"]["estimated_distinct"] == 500

        # user_id: -1.0 * 100000 = 100000 → high cardinality
        assert result["user_id"]["cardinality"] == "high"
        assert result["user_id"]["strategy"] == "metadata_only"
        assert result["user_id"]["estimated_distinct"] == 100000

        # email: -0.99 * 100000 = 99000 → high cardinality
        assert result["email"]["cardinality"] == "high"
        assert result["email"]["strategy"] == "metadata_only"

    def test_execute_sampled_stats_query(self, pg_backend):
        """execute_sampled_stats_query uses TABLESAMPLE SYSTEM."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (10.5, 100.5, 5000)
        mock_cursor.description = [("min_age",), ("max_age",), ("total",)]
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        exprs = [
            'MIN("age") AS min_age',
            'MAX("age") AS max_age',
            "COUNT(*) AS total",
        ]
        result = pg_backend.execute_sampled_stats_query(exprs, sample_pct=1.0)

        # Check SQL contains TABLESAMPLE SYSTEM
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "TABLESAMPLE SYSTEM" in executed_sql
        assert "BERNOULLI" not in executed_sql

        assert result == {"min_age": 10.5, "max_age": 100.5, "total": 5000}

    def test_fetch_low_cardinality_values_batched(self, pg_backend):
        """fetch_low_cardinality_values_batched batches GROUP BY queries."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("status", "active", 400),
            ("status", "inactive", 300),
            ("status", "pending", 200),
            ("status", "suspended", 100),
            ("country", "US", 500),
            ("country", "UK", 300),
            ("country", "DE", 200),
        ]
        pg_backend._conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        pg_backend._conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        columns = ["status", "country"]
        result = pg_backend.fetch_low_cardinality_values_batched(columns)

        # Check SQL contains UNION ALL
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "UNION ALL" in executed_sql

        # Check results
        assert len(result["status"]) == 4
        assert result["status"][0] == ("active", 400)
        assert len(result["country"]) == 3
        assert result["country"][0] == ("US", 500)


class TestSqlServerBackendUnit:
    """Unit tests for SqlServerBackend with mocked database."""

    @pytest.fixture
    def mock_handle(self):
        """Create a mock DatasetHandle for SQL Server."""
        handle = MagicMock()
        handle.db_params = MagicMock()
        handle.db_params.schema = "dbo"
        handle.db_params.table = "users"
        handle.db_params.database = "testdb"
        return handle

    @pytest.fixture
    def sqlserver_backend(self, mock_handle):
        """Create SqlServerBackend with mocked connection."""
        from kontra.scout.backends.sqlserver_backend import SqlServerBackend

        with patch(
            "kontra.scout.backends.sqlserver_backend.get_connection"
        ) as mock_conn:
            backend = SqlServerBackend(mock_handle)
            backend._conn = MagicMock()
            return backend

    def test_supports_metadata_only(self, sqlserver_backend):
        """SqlServerBackend supports metadata-only profiling."""
        assert sqlserver_backend.supports_metadata_only() is True

    def test_supports_strategic_standard(self, sqlserver_backend):
        """SqlServerBackend supports strategic standard profiling."""
        assert sqlserver_backend.supports_strategic_standard() is True


class TestDuckDBBackendUnit:
    """Unit tests for DuckDBBackend metadata methods."""

    @pytest.fixture
    def duckdb_backend(self, tmp_path):
        """Create DuckDBBackend with a small test parquet file."""
        import polars as pl
        from kontra.connectors.handle import DatasetHandle
        from kontra.scout.backends.duckdb_backend import DuckDBBackend

        # Create a small test parquet file
        df = pl.DataFrame(
            {
                "id": list(range(100)),
                "status": ["active"] * 40 + ["inactive"] * 30 + ["pending"] * 30,
                "value": [float(i) for i in range(100)],
            }
        )
        parquet_path = tmp_path / "test.parquet"
        df.write_parquet(str(parquet_path))

        handle = DatasetHandle.from_uri(str(parquet_path))
        backend = DuckDBBackend(handle)
        backend.connect()
        return backend

    def test_supports_metadata_only_parquet(self, duckdb_backend):
        """DuckDBBackend supports metadata-only for Parquet files."""
        assert duckdb_backend.supports_metadata_only() is True

    def test_profile_metadata_only_returns_estimates(self, duckdb_backend):
        """profile_metadata_only returns estimated counts from Parquet metadata."""
        schema = duckdb_backend.get_schema()
        row_count = duckdb_backend.get_row_count()

        result = duckdb_backend.profile_metadata_only(schema, row_count)

        # Should have entries for all columns
        assert "id" in result
        assert "status" in result
        assert "value" in result

        # All should be marked as estimates
        for col_name, col_data in result.items():
            assert col_data["is_estimate"] is True
            assert "null_count" in col_data
            assert "distinct_count" in col_data

    def test_profile_metadata_only_null_counts(self, duckdb_backend):
        """profile_metadata_only correctly estimates null counts."""
        schema = duckdb_backend.get_schema()
        row_count = duckdb_backend.get_row_count()

        result = duckdb_backend.profile_metadata_only(schema, row_count)

        # Test file has no nulls
        assert result["id"]["null_count"] == 0
        assert result["status"]["null_count"] == 0
        assert result["value"]["null_count"] == 0


class TestBackendInterface:
    """Tests to ensure all backends implement the same interface."""

    def test_postgres_has_required_methods(self):
        """PostgreSQLBackend has all required optimization methods."""
        from kontra.scout.backends.postgres_backend import PostgreSQLBackend

        # Check metadata-only methods
        assert hasattr(PostgreSQLBackend, "supports_metadata_only")
        assert hasattr(PostgreSQLBackend, "profile_metadata_only")

        # Check strategic standard methods
        assert hasattr(PostgreSQLBackend, "supports_strategic_standard")
        assert hasattr(PostgreSQLBackend, "get_table_freshness")
        assert hasattr(PostgreSQLBackend, "execute_sampled_stats_query")
        assert hasattr(PostgreSQLBackend, "fetch_low_cardinality_values_batched")
        assert hasattr(PostgreSQLBackend, "classify_columns")

    def test_sqlserver_has_required_methods(self):
        """SqlServerBackend has all required optimization methods."""
        from kontra.scout.backends.sqlserver_backend import SqlServerBackend

        # Check metadata-only methods
        assert hasattr(SqlServerBackend, "supports_metadata_only")
        assert hasattr(SqlServerBackend, "profile_metadata_only")

        # Check strategic standard methods
        assert hasattr(SqlServerBackend, "supports_strategic_standard")
        assert hasattr(SqlServerBackend, "get_table_freshness")
        assert hasattr(SqlServerBackend, "execute_sampled_stats_query")
        assert hasattr(SqlServerBackend, "fetch_low_cardinality_values_batched")
        assert hasattr(SqlServerBackend, "classify_columns")

    def test_duckdb_has_required_methods(self):
        """DuckDBBackend has required metadata methods."""
        from kontra.scout.backends.duckdb_backend import DuckDBBackend

        # Check metadata-only methods
        assert hasattr(DuckDBBackend, "supports_metadata_only")
        assert hasattr(DuckDBBackend, "profile_metadata_only")
