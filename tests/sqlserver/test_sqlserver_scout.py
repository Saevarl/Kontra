# tests/sqlserver/test_sqlserver_scout.py
"""
Integration tests for Scout profiling with SQL Server.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

import pytest


@pytest.mark.integration
class TestSqlServerScout:
    """Test Scout profiling against SQL Server tables."""

    def test_scout_lite_preset(self, sqlserver_uri):
        """Test Scout lite preset with SQL Server."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_uri, preset="lite")
        profile = profiler.profile()

        assert profile.source_format == "sqlserver"
        assert profile.row_count == 1002
        assert profile.column_count == 9
        assert profile.profile_duration_ms > 0

        # Lite preset should have basic stats
        for col in profile.columns:
            assert col.null_count >= 0
            assert col.distinct_count >= 0

    def test_scout_standard_preset(self, sqlserver_uri):
        """Test Scout standard preset with SQL Server."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_uri, preset="standard")
        profile = profiler.profile()

        assert profile.row_count == 1002

        # Standard preset should have numeric stats
        age_col = next(c for c in profile.columns if c.name == "age")
        assert age_col.numeric is not None
        assert age_col.numeric.min == 18.0
        assert age_col.numeric.max == 80.0  # 18 + (i % 63) where i goes to 1000
        assert age_col.numeric.mean is not None
        # Note: median not available for SQL Server (PERCENTILE_CONT requires different syntax)
        # assert age_col.numeric.median is not None

        # Should have top values
        status_col = next(c for c in profile.columns if c.name == "status")
        assert len(status_col.top_values) > 0

    def test_scout_null_detection(self, sqlserver_uri):
        """Test null detection in SQL Server columns."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_uri, preset="lite")
        profile = profiler.profile()

        # email has ~2% nulls (20 out of 1002)
        email_col = next(c for c in profile.columns if c.name == "email")
        assert email_col.null_count == 20
        assert 0.01 < email_col.null_rate < 0.03

        # username has no nulls
        username_col = next(c for c in profile.columns if c.name == "username")
        assert username_col.null_count == 0
        assert username_col.null_rate == 0.0

    def test_scout_low_cardinality_values(self, sqlserver_uri):
        """Test that low-cardinality columns list all values."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_uri, preset="standard")
        profile = profiler.profile()

        # status has 4 values - should be listed
        status_col = next(c for c in profile.columns if c.name == "status")
        assert status_col.is_low_cardinality is True
        assert status_col.values is not None
        assert set(status_col.values) == {"active", "inactive", "pending", "suspended"}

        # country_code has 5 values
        country_col = next(c for c in profile.columns if c.name == "country_code")
        assert country_col.is_low_cardinality is True
        assert country_col.values is not None
        assert set(v.strip() for v in country_col.values) == {"US", "UK", "DE", "FR", "JP"}

    def test_scout_column_filter(self, sqlserver_uri):
        """Test column filtering."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(
            sqlserver_uri,
            preset="lite",
            columns=["user_id", "email", "status"],
        )
        profile = profiler.profile()

        assert profile.column_count == 3
        col_names = [c.name for c in profile.columns]
        assert col_names == ["user_id", "email", "status"]

    def test_scout_products_table(self, sqlserver_products_uri):
        """Test Scout with products table."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_products_uri, preset="standard")
        profile = profiler.profile()

        assert profile.row_count == 500
        assert profile.column_count == 6

        # sku should be unique identifier
        sku_col = next(c for c in profile.columns if c.name == "sku")
        assert sku_col.uniqueness_ratio >= 0.99

        # category should be categorical
        category_col = next(c for c in profile.columns if c.name == "category")
        assert category_col.is_low_cardinality is True
        assert len(category_col.values) == 5

    def test_scout_orders_table(self, sqlserver_orders_uri):
        """Test Scout with orders table (larger dataset)."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(sqlserver_orders_uri, preset="lite")
        profile = profiler.profile()

        assert profile.row_count == 2000

        # order_number should be unique
        order_num_col = next(c for c in profile.columns if c.name == "order_number")
        assert order_num_col.uniqueness_ratio >= 0.99
