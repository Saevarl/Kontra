# tests/postgres/test_postgres_scout.py
"""
Integration tests for Scout profiling with PostgreSQL.

Requires PostgreSQL container to be running:
    cd tests/postgres && docker compose up -d
"""

import pytest


@pytest.mark.integration
class TestPostgresScout:
    """Test Scout profiling against PostgreSQL tables."""

    def test_scout_lite_preset(self, postgres_uri):
        """Test Scout lite preset with PostgreSQL."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="lite")
        profile = profiler.profile()

        assert profile.source_format == "postgres"
        assert profile.row_count == 1002
        assert profile.column_count == 9
        assert profile.profile_duration_ms > 0

        # Lite preset should have basic stats
        for col in profile.columns:
            assert col.null_count >= 0
            assert col.distinct_count >= 0

    def test_scout_standard_preset(self, postgres_uri):
        """Test Scout standard preset with PostgreSQL."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="standard")
        profile = profiler.profile()

        assert profile.row_count == 1002

        # Standard preset should have numeric stats
        age_col = next(c for c in profile.columns if c.name == "age")
        assert age_col.numeric is not None
        assert age_col.numeric.min == 18.0
        assert age_col.numeric.max == 80.0  # 18 + (i % 63) where i goes to 1002
        assert age_col.numeric.mean is not None
        assert age_col.numeric.median is not None

        # Should have top values
        status_col = next(c for c in profile.columns if c.name == "status")
        assert len(status_col.top_values) > 0

    def test_scout_deep_preset(self, postgres_uri):
        """Test Scout deep preset with PostgreSQL."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="deep")
        profile = profiler.profile()

        assert profile.row_count == 1002

        # Deep preset should have percentiles
        balance_col = next(c for c in profile.columns if c.name == "balance")
        if balance_col.numeric:
            assert balance_col.numeric.percentiles is not None

    def test_scout_low_cardinality_values(self, postgres_uri):
        """Test that low-cardinality columns list all values."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="standard")
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
        assert set(country_col.values) == {"US", "UK", "DE", "FR", "JP"}

    def test_scout_null_detection(self, postgres_uri):
        """Test null detection in PostgreSQL columns."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="lite")
        profile = profiler.profile()

        # email has ~2% nulls (20 out of 1002)
        email_col = next(c for c in profile.columns if c.name == "email")
        assert email_col.null_count == 20
        assert 0.01 < email_col.null_rate < 0.03

        # username has no nulls
        username_col = next(c for c in profile.columns if c.name == "username")
        assert username_col.null_count == 0
        assert username_col.null_rate == 0.0

    def test_scout_semantic_type_inference(self, postgres_uri):
        """Test semantic type inference."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="standard")
        profile = profiler.profile()

        # user_id should be identifier (unique, no nulls)
        user_id_col = next(c for c in profile.columns if c.name == "user_id")
        assert user_id_col.semantic_type == "identifier"

        # status should be category (low cardinality string)
        status_col = next(c for c in profile.columns if c.name == "status")
        assert status_col.semantic_type == "category"

        # age should be measure (numeric, not unique)
        age_col = next(c for c in profile.columns if c.name == "age")
        assert age_col.semantic_type == "measure"

    def test_scout_pattern_detection(self, postgres_uri):
        """Test pattern detection in string columns."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_uri, preset="standard", include_patterns=True)
        profile = profiler.profile()

        # email column should detect email pattern
        email_col = next(c for c in profile.columns if c.name == "email")
        assert "email" in email_col.detected_patterns

    def test_scout_column_filter(self, postgres_uri):
        """Test column filtering."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(
            postgres_uri,
            preset="lite",
            columns=["user_id", "email", "status"],
        )
        profile = profiler.profile()

        assert profile.column_count == 3
        col_names = [c.name for c in profile.columns]
        assert col_names == ["user_id", "email", "status"]

    def test_scout_products_table(self, postgres_products_uri):
        """Test Scout with products table."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_products_uri, preset="standard")
        profile = profiler.profile()

        assert profile.row_count == 500
        assert profile.column_count == 6

        # sku should be unique identifier
        sku_col = next(c for c in profile.columns if c.name == "sku")
        assert sku_col.uniqueness_ratio >= 0.99
        assert sku_col.semantic_type == "identifier"

        # category should be categorical
        category_col = next(c for c in profile.columns if c.name == "category")
        assert category_col.is_low_cardinality is True
        assert len(category_col.values) == 5

    def test_scout_orders_table(self, postgres_orders_uri):
        """Test Scout with orders table (larger dataset)."""
        from kontra.scout.profiler import ScoutProfiler

        profiler = ScoutProfiler(postgres_orders_uri, preset="lite")
        profile = profiler.profile()

        assert profile.row_count == 2000

        # order_number should be unique
        order_num_col = next(c for c in profile.columns if c.name == "order_number")
        assert order_num_col.uniqueness_ratio >= 0.99

    def test_scout_suggest_rules(self, postgres_uri):
        """Test rule suggestion generation."""
        from kontra.scout.profiler import ScoutProfiler
        from kontra.scout.suggest import generate_rules

        profiler = ScoutProfiler(postgres_uri, preset="standard")
        profile = profiler.profile()

        rules = generate_rules(profile)

        # Should suggest not_null for username (0% nulls)
        username_rules = [r for r in rules if r.get("params", {}).get("column") == "username"]
        assert any(r["name"] == "not_null" for r in username_rules)

        # Should suggest unique for user_id
        user_id_rules = [r for r in rules if r.get("params", {}).get("column") == "user_id"]
        assert any(r["name"] == "unique" for r in user_id_rules)

        # Should suggest allowed_values for status
        status_rules = [r for r in rules if r.get("params", {}).get("column") == "status"]
        assert any(r["name"] == "allowed_values" for r in status_rules)
