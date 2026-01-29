# tests/test_string_rules.py
"""
Tests for new string validation rules:
- disallowed_values
- length
- contains
- starts_with
- ends_with
"""
import pytest
import polars as pl
import kontra
from kontra import rules


class TestDisallowedValues:
    """Tests for disallowed_values rule."""

    def test_basic(self):
        """Basic disallowed values check."""
        df = pl.DataFrame({
            "status": ["active", "deleted", "pending", "banned", "inactive"]
        })
        result = kontra.validate(df, rules=[
            rules.disallowed_values("status", ["deleted", "banned"])
        ], tally=True)
        assert not result.passed
        assert result.failed_count == 1
        assert result.rules[0].failed_count == 2  # deleted and banned

    def test_all_pass(self):
        """All values are allowed."""
        df = pl.DataFrame({
            "status": ["active", "pending", "inactive"]
        })
        result = kontra.validate(df, rules=[
            rules.disallowed_values("status", ["deleted", "banned"])
        ])
        assert result.passed

    def test_null_handling(self):
        """NULL values should NOT be failures."""
        df = pl.DataFrame({
            "status": ["active", None, "pending", None]
        })
        result = kontra.validate(df, rules=[
            rules.disallowed_values("status", ["deleted", "banned"])
        ])
        assert result.passed  # NULLs are not in the disallowed list

    def test_numeric_values(self):
        """Disallowed values with numbers."""
        df = pl.DataFrame({
            "code": [1, 2, 3, 4, 5]
        })
        result = kontra.validate(df, rules=[
            rules.disallowed_values("code", [3, 4])
        ], tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 2


class TestLength:
    """Tests for length rule."""

    def test_min_length(self):
        """Test minimum length check."""
        df = pl.DataFrame({
            "username": ["jo", "valid_user", "x", "good_name"]
        })
        result = kontra.validate(df, rules=[
            rules.length("username", min=3)
        ], tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 2  # "jo" and "x"

    def test_max_length(self):
        """Test maximum length check."""
        df = pl.DataFrame({
            "username": ["short", "medium_name", "this_is_a_very_long_username_that_exceeds"]
        })
        result = kontra.validate(df, rules=[
            rules.length("username", max=20)
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_range(self):
        """Test length range check."""
        df = pl.DataFrame({
            "username": ["jo", "valid", "this_is_way_too_long_for_the_limit"]
        })
        result = kontra.validate(df, rules=[
            rules.length("username", min=3, max=20)
        ], tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 2  # too short and too long

    def test_null_handling(self):
        """NULL values should be failures."""
        df = pl.DataFrame({
            "username": ["valid", None, "ok"]
        })
        result = kontra.validate(df, rules=[
            rules.length("username", min=1, max=50)
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # NULL

    def test_empty_string(self):
        """Empty string has length 0."""
        df = pl.DataFrame({
            "username": ["", "valid", ""]
        })
        result = kontra.validate(df, rules=[
            rules.length("username", min=1)
        ], tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 2  # two empty strings

    def test_validation_errors(self):
        """Test parameter validation."""
        with pytest.raises(ValueError, match="at least one"):
            rules.length("col")  # no min or max

        with pytest.raises(ValueError, match="min.*<=.*max"):
            rules.length("col", min=10, max=5)


class TestContains:
    """Tests for contains rule."""

    def test_basic(self):
        """Basic substring check."""
        df = pl.DataFrame({
            "email": ["user@example.com", "invalid-email", "test@test.com"]
        })
        result = kontra.validate(df, rules=[
            rules.contains("email", "@")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # invalid-email

    def test_all_pass(self):
        """All values contain substring."""
        df = pl.DataFrame({
            "email": ["a@b.com", "c@d.org", "e@f.net"]
        })
        result = kontra.validate(df, rules=[
            rules.contains("email", "@")
        ])
        assert result.passed

    def test_null_handling(self):
        """NULL values should be failures."""
        df = pl.DataFrame({
            "email": ["user@example.com", None, "test@test.com"]
        })
        result = kontra.validate(df, rules=[
            rules.contains("email", "@")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # NULL

    def test_special_chars(self):
        """Test with special LIKE characters."""
        df = pl.DataFrame({
            "text": ["10%", "20% off", "no percent"]
        })
        result = kontra.validate(df, rules=[
            rules.contains("text", "%")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # "no percent"


class TestStartsWith:
    """Tests for starts_with rule."""

    def test_basic(self):
        """Basic prefix check."""
        df = pl.DataFrame({
            "url": ["https://example.com", "http://test.com", "ftp://bad.com"]
        })
        result = kontra.validate(df, rules=[
            rules.starts_with("url", "https://")
        ], tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 2  # http:// and ftp://

    def test_all_pass(self):
        """All values start with prefix."""
        df = pl.DataFrame({
            "url": ["https://a.com", "https://b.com", "https://c.com"]
        })
        result = kontra.validate(df, rules=[
            rules.starts_with("url", "https://")
        ])
        assert result.passed

    def test_null_handling(self):
        """NULL values should be failures."""
        df = pl.DataFrame({
            "url": ["https://example.com", None]
        })
        result = kontra.validate(df, rules=[
            rules.starts_with("url", "https://")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # NULL


class TestEndsWith:
    """Tests for ends_with rule."""

    def test_basic(self):
        """Basic suffix check."""
        df = pl.DataFrame({
            "filename": ["data.csv", "report.csv", "image.png"]
        })
        result = kontra.validate(df, rules=[
            rules.ends_with("filename", ".csv")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # image.png

    def test_all_pass(self):
        """All values end with suffix."""
        df = pl.DataFrame({
            "filename": ["a.csv", "b.csv", "c.csv"]
        })
        result = kontra.validate(df, rules=[
            rules.ends_with("filename", ".csv")
        ])
        assert result.passed

    def test_null_handling(self):
        """NULL values should be failures."""
        df = pl.DataFrame({
            "filename": ["data.csv", None]
        })
        result = kontra.validate(df, rules=[
            rules.ends_with("filename", ".csv")
        ])
        assert not result.passed
        assert result.rules[0].failed_count == 1  # NULL


class TestBatchedExecution:
    """Tests for batched execution of multiple string rules."""

    def test_all_rules_batched(self):
        """Test all new rules together."""
        df = pl.DataFrame({
            "email": ["user@example.com", "invalid-email", None],
            "url": ["https://example.com", "http://test.com", "https://good.com"],
            "filename": ["data.csv", "report.csv", "image.png"],
            "status": ["active", "deleted", "pending"],
            "username": ["jo", "valid_user", "x"],
        })
        result = kontra.validate(df, rules=[
            rules.disallowed_values("status", ["deleted", "banned"]),
            rules.length("username", min=3, max=20),
            rules.contains("email", "@"),
            rules.starts_with("url", "https://"),
            rules.ends_with("filename", ".csv"),
        ])
        assert not result.passed
        assert result.failed_count == 5  # 5 rules failed


class TestYAMLContract:
    """Test that rules work in YAML contracts."""

    def test_yaml_format(self):
        """Test that rules can be specified in YAML-like dict format."""
        df = pl.DataFrame({
            "email": ["user@example.com", "invalid"],
            "status": ["active", "banned"],
        })
        result = kontra.validate(df, rules=[
            {"name": "contains", "params": {"column": "email", "substring": "@"}},
            {"name": "disallowed_values", "params": {"column": "status", "values": ["banned"]}},
        ])
        assert not result.passed
        assert result.failed_count == 2
