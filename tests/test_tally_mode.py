# tests/test_tally_mode.py
"""Tests for tally mode functionality.

Tally mode controls whether rules use:
- COUNT queries (tally=True, default): Exact counts
- EXISTS queries (tally=False): Fast, early termination, failed_count=1
"""

import pytest
import polars as pl
import tempfile
import os


class TestTallyModeBasic:
    """Basic tally mode behavior tests."""

    @pytest.fixture
    def sample_data(self, tmp_path):
        """Create sample data with known violations."""
        df = pl.DataFrame({
            "id": list(range(1, 101)),
            "email": [f"user{i}@example.com" if i % 10 != 0 else None for i in range(1, 101)],  # 10 nulls
            "status": ["active" if i % 4 != 0 else "INVALID" for i in range(1, 101)],  # 25 invalid
            "score": [i if i <= 90 else 150 for i in range(1, 101)],  # 10 out of range
            "name": [f"user_{i}" if i % 5 != 0 else f"u{i}" for i in range(1, 101)],  # 20 short names
        })
        path = tmp_path / "data.csv"
        df.write_csv(str(path))
        return str(path)

    def test_tally_false_returns_one(self, sample_data):
        """tally=False should return failed_count=1 for any failure."""
        import kontra
        from kontra import rules

        result = kontra.validate(sample_data, rules=[
            rules.not_null("email", tally=False),  # 10 violations, but tally=False
        ])

        assert not result.passed
        rule = result.rules[0]
        assert rule.failed_count == 1  # EXISTS returns 1
        assert rule.tally is False
        assert "At least" in rule.message

    def test_tally_true_returns_exact(self, sample_data):
        """tally=True should return exact failed_count."""
        import kontra
        from kontra import rules

        result = kontra.validate(sample_data, rules=[
            rules.not_null("email", tally=True),  # 10 violations
        ])

        assert not result.passed
        rule = result.rules[0]
        assert rule.failed_count == 10  # Exact count
        assert rule.tally is True
        assert "At least" not in rule.message

    def test_tally_false_passes_correctly(self, sample_data):
        """tally=False should correctly identify passing rules."""
        import kontra
        from kontra import rules

        result = kontra.validate(sample_data, rules=[
            rules.not_null("id"),  # No nulls
        ])

        assert result.passed
        rule = result.rules[0]
        assert rule.failed_count == 0
        assert rule.passed is True

    def test_mixed_tally_settings(self, sample_data):
        """Mix of tally=True and tally=False in same validation."""
        import kontra
        from kontra import rules

        # Use explicit IDs to avoid duplicate rule ID error
        result = kontra.validate(sample_data, rules=[
            rules.not_null("email", id="email_not_null_fast", tally=False),  # Fast mode
            rules.not_null("email", id="email_not_null_exact", tally=True),  # Exact count
        ])

        assert not result.passed
        assert len(result.rules) == 2

        # Find rules by ID (order may vary)
        fast_rule = next(r for r in result.rules if r.rule_id == "email_not_null_fast")
        exact_rule = next(r for r in result.rules if r.rule_id == "email_not_null_exact")

        # Fast rule: EXISTS (failed_count=1)
        assert fast_rule.failed_count == 1
        assert fast_rule.tally is False

        # Exact rule: COUNT (failed_count=10)
        assert exact_rule.failed_count == 10
        assert exact_rule.tally is True


class TestTallyModeColumnRules:
    """Test tally mode for all column rules."""

    @pytest.fixture
    def violation_data(self, tmp_path):
        """Create data with violations for each rule type."""
        df = pl.DataFrame({
            "nullable": [1, None, 3, None, 5],  # 2 nulls
            "duplicated": ["a", "b", "a", "c", "b"],  # 2 duplicate groups
            "status": ["active", "inactive", "INVALID", "active", "BAD"],  # 2 invalid
            "banned": ["ok", "forbidden", "ok", "banned", "ok"],  # 2 disallowed
            "score": [50, 150, 75, -10, 80],  # 2 out of range
            "code": ["ABC", "A", "ABCDEF", "AB", "ABCDEFGH"],  # 3 wrong length (A=1, ABCDEF=6, ABCDEFGH=8)
            "pattern": ["A123", "B456", "invalid", "C789", "bad"],  # 2 no match
            "text": ["hello world", "foo", "hello there", "bar", "baz"],  # 3 missing substring
            "prefix": ["pre_1", "pre_2", "wrong", "pre_3", "bad"],  # 2 wrong prefix
            "suffix": ["file.txt", "doc.txt", "file.csv", "data.txt", "x.csv"],  # 2 wrong suffix
        })
        path = tmp_path / "violations.csv"
        df.write_csv(str(path))
        return str(path)

    def test_not_null_tally(self, violation_data):
        """not_null rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.not_null("nullable", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.not_null("nullable", tally=True)])
        assert result.rules[0].failed_count == 2

    def test_unique_tally(self, violation_data):
        """unique rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.unique("duplicated", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.unique("duplicated", tally=True)])
        assert result.rules[0].failed_count == 2  # 2 extra rows (duplicates)

    def test_allowed_values_tally(self, violation_data):
        """allowed_values rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[
            rules.allowed_values("status", ["active", "inactive"], tally=False)
        ])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[
            rules.allowed_values("status", ["active", "inactive"], tally=True)
        ])
        assert result.rules[0].failed_count == 2

    def test_disallowed_values_tally(self, violation_data):
        """disallowed_values rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[
            rules.disallowed_values("banned", ["forbidden", "banned"], tally=False)
        ])
        assert result.rules[0].failed_count == 1

        # tally=True
        result = kontra.validate(violation_data, rules=[
            rules.disallowed_values("banned", ["forbidden", "banned"], tally=True)
        ])
        assert result.rules[0].failed_count == 2

    def test_range_tally(self, violation_data):
        """range rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.range("score", min=0, max=100, tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.range("score", min=0, max=100, tally=True)])
        assert result.rules[0].failed_count == 2

    def test_length_tally(self, violation_data):
        """length rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.length("code", min=2, max=5, tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.length("code", min=2, max=5, tally=True)])
        assert result.rules[0].failed_count == 3  # A=1, ABCDEF=6, ABCDEFGH=8

    def test_regex_tally(self, violation_data):
        """regex rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.regex("pattern", r"^[A-Z]\d{3}$", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.regex("pattern", r"^[A-Z]\d{3}$", tally=True)])
        assert result.rules[0].failed_count == 2

    def test_contains_tally(self, violation_data):
        """contains rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.contains("text", "hello", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.contains("text", "hello", tally=True)])
        assert result.rules[0].failed_count == 3

    def test_starts_with_tally(self, violation_data):
        """starts_with rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.starts_with("prefix", "pre_", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.starts_with("prefix", "pre_", tally=True)])
        assert result.rules[0].failed_count == 2

    def test_ends_with_tally(self, violation_data):
        """ends_with rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(violation_data, rules=[rules.ends_with("suffix", ".txt", tally=False)])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(violation_data, rules=[rules.ends_with("suffix", ".txt", tally=True)])
        assert result.rules[0].failed_count == 2


class TestTallyModeCrossColumnRules:
    """Test tally mode for cross-column rules."""

    @pytest.fixture
    def cross_column_data(self, tmp_path):
        """Create data with cross-column violations."""
        df = pl.DataFrame({
            "start_date": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-01"],
            "end_date": ["2024-01-15", "2024-01-15", "2024-03-20", "2024-03-01", "2024-05-10"],  # 2 violations
            "status": ["shipped", "pending", "shipped", "shipped", "pending"],
            "shipping_date": ["2024-01-10", None, None, "2024-04-05", None],  # 1 violation (row 3: shipped but no date)
            "tier": ["premium", "basic", "premium", "basic", "premium"],
            "discount": [15, 5, 5, 10, 25],  # 1 violation (premium with discount < 10)
        })
        path = tmp_path / "cross.csv"
        df.write_csv(str(path))
        return str(path)

    def test_compare_tally(self, cross_column_data):
        """compare rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(cross_column_data, rules=[
            rules.compare("end_date", "start_date", ">=", tally=False)
        ])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(cross_column_data, rules=[
            rules.compare("end_date", "start_date", ">=", tally=True)
        ])
        assert result.rules[0].failed_count == 2

    def test_conditional_not_null_tally(self, cross_column_data):
        """conditional_not_null rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit) - still 1 because only row 3 violates
        result = kontra.validate(cross_column_data, rules=[
            rules.conditional_not_null("shipping_date", when="status == 'shipped'", tally=False)
        ])
        assert result.rules[0].failed_count == 1

        # tally=True - still 1 because only row 3 violates (shipped with null date)
        result = kontra.validate(cross_column_data, rules=[
            rules.conditional_not_null("shipping_date", when="status == 'shipped'", tally=True)
        ])
        assert result.rules[0].failed_count == 1

    def test_conditional_range_tally(self, cross_column_data):
        """conditional_range rule respects tally."""
        import kontra
        from kontra import rules

        # tally=False (explicit)
        result = kontra.validate(cross_column_data, rules=[
            rules.conditional_range("discount", when="tier == 'premium'", min=10, max=50, tally=False)
        ])
        assert result.rules[0].failed_count == 1

        # tally=True (default)
        result = kontra.validate(cross_column_data, rules=[
            rules.conditional_range("discount", when="tier == 'premium'", min=10, max=50, tally=True)
        ])
        assert result.rules[0].failed_count == 1  # Only 1 violation


class TestTallyModeDatasetRules:
    """Test that dataset rules don't support tally."""

    @pytest.fixture
    def simple_data(self, tmp_path):
        """Create simple data."""
        df = pl.DataFrame({"id": list(range(100))})
        path = tmp_path / "simple.csv"
        df.write_csv(str(path))
        return str(path)

    def test_min_rows_no_tally_param(self):
        """min_rows helper should not accept tally parameter."""
        from kontra import rules

        # min_rows doesn't support tally, so we can't pass it
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            rules.min_rows(50, tally=True)

    def test_max_rows_no_tally_param(self):
        """max_rows helper should not accept tally parameter."""
        from kontra import rules

        # max_rows doesn't support tally, so we can't pass it
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            rules.max_rows(200, tally=True)

    def test_dataset_rules_always_exact(self, simple_data):
        """Dataset rules should always return exact counts (not affected by tally)."""
        import kontra
        from kontra import rules

        # min_rows fails - should have exact count
        result = kontra.validate(simple_data, rules=[
            rules.min_rows(200),  # 100 < 200, fails by 100
        ])

        assert not result.passed
        # Dataset rules return exact deficit/excess
        assert result.rules[0].failed_count == 100


class TestTallyModeGlobalFlag:
    """Test global tally flag in validate()."""

    @pytest.fixture
    def multi_violation_data(self, tmp_path):
        """Create data with multiple violations per rule."""
        df = pl.DataFrame({
            "a": [1, None, 3, None, 5],  # 2 nulls
            "b": [1, None, None, None, 5],  # 3 nulls
        })
        path = tmp_path / "multi.csv"
        df.write_csv(str(path))
        return str(path)

    def test_global_tally_true(self, multi_violation_data):
        """Global tally=True should apply to all rules."""
        import kontra
        from kontra import rules

        result = kontra.validate(multi_violation_data, rules=[
            rules.not_null("a"),
            rules.not_null("b"),
        ], tally=True)

        assert result.rules[0].failed_count == 2
        assert result.rules[1].failed_count == 3

    def test_global_tally_false(self, multi_violation_data):
        """Global tally=False should apply to all rules."""
        import kontra
        from kontra import rules

        result = kontra.validate(multi_violation_data, rules=[
            rules.not_null("a"),
            rules.not_null("b"),
        ], tally=False)

        assert result.rules[0].failed_count == 1
        assert result.rules[1].failed_count == 1

    def test_per_rule_overrides_global(self, multi_violation_data):
        """Per-rule tally should override global setting."""
        import kontra
        from kontra import rules

        # Helper to find rule by rule_id
        def get_rule(result, rule_id):
            for r in result.rules:
                if r.rule_id == rule_id:
                    return r
            return None

        # Global tally=True, but first rule has tally=False
        result = kontra.validate(multi_violation_data, rules=[
            rules.not_null("a", tally=False),  # Override to False
            rules.not_null("b"),  # Uses global True
        ], tally=True)

        rule_a = get_rule(result, "COL:a:not_null")
        rule_b = get_rule(result, "COL:b:not_null")
        assert rule_a.failed_count == 1  # Per-rule override: EXISTS
        assert rule_b.failed_count == 3  # Global: COUNT

        # Global tally=False, but first rule has tally=True
        result = kontra.validate(multi_violation_data, rules=[
            rules.not_null("a", tally=True),  # Override to True
            rules.not_null("b"),  # Uses global False
        ], tally=False)

        rule_a = get_rule(result, "COL:a:not_null")
        rule_b = get_rule(result, "COL:b:not_null")
        assert rule_a.failed_count == 2  # Per-rule override: COUNT
        assert rule_b.failed_count == 1  # Global: EXISTS


class TestTallyModeYAML:
    """Test tally mode in YAML contracts."""

    def test_tally_from_yaml(self, tmp_path):
        """tally setting should be parsed from YAML contract."""
        import kontra

        # Create data
        df = pl.DataFrame({
            "email": ["a@b.com", None, None, "d@e.com"],  # 2 nulls
        })
        data_path = tmp_path / "data.csv"
        df.write_csv(str(data_path))

        # Create contract with tally
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: data.csv
rules:
  - name: not_null
    params:
      column: email
    tally: true
""")

        result = kontra.validate(str(data_path), str(contract_path))

        assert not result.passed
        assert result.rules[0].failed_count == 2
        assert result.rules[0].tally is True

    def test_tally_false_from_yaml(self, tmp_path):
        """tally: false should be explicit in YAML."""
        import kontra

        # Create data
        df = pl.DataFrame({
            "email": ["a@b.com", None, None, "d@e.com"],  # 2 nulls
        })
        data_path = tmp_path / "data.csv"
        df.write_csv(str(data_path))

        # Create contract with tally: false
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: data.csv
rules:
  - name: not_null
    params:
      column: email
    tally: false
""")

        result = kontra.validate(str(data_path), str(contract_path))

        assert not result.passed
        assert result.rules[0].failed_count == 1  # EXISTS
        assert result.rules[0].tally is False


class TestTallyModeMessages:
    """Test that messages reflect tally mode."""

    @pytest.fixture
    def message_data(self, tmp_path):
        """Create data for message testing."""
        df = pl.DataFrame({
            "value": [1, None, None, None, 5],  # 3 nulls
        })
        path = tmp_path / "msg.csv"
        df.write_csv(str(path))
        return str(path)

    def test_exists_message_says_at_least(self, message_data):
        """tally=False message should say 'At least'."""
        import kontra
        from kontra import rules

        result = kontra.validate(message_data, rules=[
            rules.not_null("value", tally=False),  # Explicit tally=False
        ])

        msg = result.rules[0].message
        assert "At least" in msg or "at least" in msg.lower()

    def test_count_message_has_exact_number(self, message_data):
        """tally=True message should have exact count."""
        import kontra
        from kontra import rules

        result = kontra.validate(message_data, rules=[
            rules.not_null("value", tally=True),
        ])

        msg = result.rules[0].message
        assert "3" in msg
        assert "At least" not in msg


class TestTallyModeResultOutput:
    """Test tally field in result output formats."""

    @pytest.fixture
    def result_data(self, tmp_path):
        """Create data for result testing."""
        df = pl.DataFrame({
            "id": [1, None, 3],
        })
        path = tmp_path / "result.csv"
        df.write_csv(str(path))
        return str(path)

    def test_tally_in_dict_output(self, result_data):
        """tally should appear in to_dict() output."""
        import kontra
        from kontra import rules

        result = kontra.validate(result_data, rules=[
            rules.not_null("id", tally=True),
        ])

        d = result.to_dict()
        assert "tally" in d["rules"][0]
        assert d["rules"][0]["tally"] is True

    def test_tally_in_json_output(self, result_data):
        """tally should appear in JSON output."""
        import kontra
        from kontra import rules
        import json

        result = kontra.validate(result_data, rules=[
            rules.not_null("id", tally=True),
        ])

        # JSON output includes tally
        json_str = json.dumps(result.to_dict())
        assert "tally" in json_str
        assert '"tally": true' in json_str
