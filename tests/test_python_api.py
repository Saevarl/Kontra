# tests/test_python_api.py
"""Tests for the Kontra Python API."""

import pytest
import polars as pl
from pathlib import Path

import kontra
from kontra import rules
from kontra.api.results import (
    ValidationResult,
    RuleResult,
    Suggestions,
    SuggestedRule,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "status": ["active", "active", "inactive", "active", "pending"],
        "age": [25, 30, 35, 40, 45],
    })


@pytest.fixture
def df_with_nulls():
    """DataFrame with null values."""
    return pl.DataFrame({
        "id": [1, 2, 3, None, 5],
        "name": ["Alice", "Bob", None, "David", "Eve"],
    })


@pytest.fixture
def sample_contract(tmp_path):
    """Sample contract file."""
    contract = tmp_path / "contract.yml"
    contract.write_text("""
name: test_contract
datasource: placeholder

rules:
  - name: not_null
    params:
      column: id
  - name: min_rows
    params:
      threshold: 3
""")
    return contract


# =============================================================================
# Rules Helpers Tests
# =============================================================================


class TestRulesHelpers:
    """Tests for kontra.rules helper functions."""

    def test_not_null(self):
        """rules.not_null() returns correct dict."""
        rule = rules.not_null("user_id")
        assert rule["name"] == "not_null"
        assert rule["params"]["column"] == "user_id"
        assert rule["severity"] == "blocking"

    def test_not_null_with_severity(self):
        """rules.not_null() accepts severity."""
        rule = rules.not_null("email", severity="warning")
        assert rule["severity"] == "warning"

    def test_unique(self):
        """rules.unique() returns correct dict."""
        rule = rules.unique("id")
        assert rule["name"] == "unique"
        assert rule["params"]["column"] == "id"

    def test_dtype(self):
        """rules.dtype() returns correct dict."""
        rule = rules.dtype("age", "int64")
        assert rule["name"] == "dtype"
        assert rule["params"]["column"] == "age"
        assert rule["params"]["type"] == "int64"

    def test_dtype_with_dtype_param(self):
        """rules.dtype() accepts dtype= parameter as alias for type."""
        # Both should produce the same result
        rule1 = rules.dtype("age", type="int64")
        rule2 = rules.dtype("age", dtype="int64")
        assert rule1 == rule2
        assert rule2["params"]["type"] == "int64"

    def test_range(self):
        """rules.range() returns correct dict."""
        rule = rules.range("age", min=0, max=150)
        assert rule["name"] == "range"
        assert rule["params"]["min"] == 0
        assert rule["params"]["max"] == 150

    def test_range_partial(self):
        """rules.range() works with only min or max."""
        rule_min = rules.range("age", min=0)
        assert "min" in rule_min["params"]
        assert "max" not in rule_min["params"]

        rule_max = rules.range("age", max=100)
        assert "max" in rule_max["params"]
        assert "min" not in rule_max["params"]

    def test_allowed_values(self):
        """rules.allowed_values() returns correct dict."""
        rule = rules.allowed_values("status", ["active", "inactive"])
        assert rule["name"] == "allowed_values"
        assert rule["params"]["values"] == ["active", "inactive"]

    def test_regex(self):
        """rules.regex() returns correct dict."""
        rule = rules.regex("email", r"^[\w.-]+@[\w.-]+\.\w+$")
        assert rule["name"] == "regex"
        assert rule["params"]["pattern"] == r"^[\w.-]+@[\w.-]+\.\w+$"

    def test_regex_invalid_pattern_raises(self):
        """rules.regex() with invalid pattern raises at rule creation time (not validate time)."""
        # Invalid regex: unclosed bracket - raises immediately when creating rule dict
        with pytest.raises(ValueError, match="Invalid regex pattern"):
            rules.regex("email", "[invalid(regex")

    def test_min_rows(self):
        """rules.min_rows() returns correct dict."""
        rule = rules.min_rows(100)
        assert rule["name"] == "min_rows"
        assert rule["params"]["threshold"] == 100

    def test_max_rows(self):
        """rules.max_rows() returns correct dict."""
        rule = rules.max_rows(1000000)
        assert rule["name"] == "max_rows"
        assert rule["params"]["threshold"] == 1000000

    def test_freshness(self):
        """rules.freshness() returns correct dict."""
        rule = rules.freshness("updated_at", max_age="24h")
        assert rule["name"] == "freshness"
        assert rule["params"]["column"] == "updated_at"
        assert rule["params"]["max_age"] == "24h"

    def test_column_validation_rejects_none(self):
        """Rule helpers reject None column names (BUG-002)."""
        with pytest.raises(ValueError, match="requires a column name"):
            rules.unique(None)
        with pytest.raises(ValueError, match="requires a column name"):
            rules.not_null(None)
        with pytest.raises(ValueError, match="requires a column name"):
            rules.range(None, min=0)

    def test_column_validation_rejects_empty_string(self):
        """Rule helpers reject empty column names."""
        with pytest.raises(ValueError, match="cannot be empty"):
            rules.unique("")
        with pytest.raises(ValueError, match="cannot be empty"):
            rules.not_null("  ")

    def test_column_validation_rejects_non_string(self):
        """Rule helpers reject non-string column names."""
        with pytest.raises(ValueError, match="must be a string"):
            rules.unique(123)
        with pytest.raises(ValueError, match="must be a string"):
            rules.allowed_values(["column"], values=[1, 2])

    def test_freshness_validates_max_age(self):
        """rules.freshness() validates max_age format (BUG-003)."""
        with pytest.raises(ValueError, match="invalid max_age"):
            rules.freshness("updated_at", max_age="invalid")
        with pytest.raises(ValueError, match="requires max_age"):
            rules.freshness("updated_at", max_age=None)

        # Valid formats should work
        for fmt in ["24h", "1d", "30m", "7d", "1w", "1h30m"]:
            rule = rules.freshness("updated_at", max_age=fmt)
            assert rule["params"]["max_age"] == fmt


# =============================================================================
# Validate Function Tests
# =============================================================================


class TestValidateFunction:
    """Tests for kontra.validate()."""

    def test_validate_with_contract(self, sample_df, sample_contract):
        """Validate with contract file."""
        result = kontra.validate(sample_df, str(sample_contract), save=False)

        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert result.total_rules == 2

    def test_validate_with_inline_rules(self, sample_df):
        """Validate with inline rules only."""
        result = kontra.validate(sample_df, rules=[
            rules.not_null("id"),
            rules.unique("id"),
            rules.min_rows(3),
        ], save=False)

        assert result.passed is True
        assert result.total_rules == 3

    def test_validate_mixed_contract_and_inline(self, sample_df, sample_contract):
        """Validate with both contract and inline rules."""
        result = kontra.validate(
            sample_df,
            str(sample_contract),
            rules=[rules.unique("id")],
            save=False,
        )

        assert result.passed is True
        assert result.total_rules == 3  # 2 from contract + 1 inline

    def test_validate_failing_rules(self, df_with_nulls):
        """Validate with failing rules."""
        result = kontra.validate(df_with_nulls, rules=[
            rules.not_null("id", severity="blocking"),
            rules.not_null("name", severity="warning"),
        ], save=False)

        assert result.passed is False
        assert result.failed_count == 1  # Only blocking failures
        assert result.warning_count == 1

    def test_validate_requires_contract_or_rules(self, sample_df):
        """Validate raises error without contract or rules."""
        with pytest.raises(ValueError, match="Either contract or rules"):
            kontra.validate(sample_df)

    def test_validate_with_string_path(self, sample_df, tmp_path):
        """Validate with file path as data."""
        # Write sample data to parquet
        parquet = tmp_path / "data.parquet"
        sample_df.write_parquet(parquet)

        result = kontra.validate(
            str(parquet),
            rules=[rules.min_rows(3)],
            save=False,
        )

        assert result.passed is True

    # -------------------------------------------------------------------------
    # list[dict] and dict input tests
    # -------------------------------------------------------------------------

    def test_validate_list_of_dicts(self):
        """Validate with list of dicts (flat tabular JSON)."""
        data = [
            {"id": 1, "email": "alice@example.com"},
            {"id": 2, "email": "bob@example.com"},
            {"id": 3, "email": "charlie@example.com"},
        ]

        result = kontra.validate(data, rules=[
            rules.not_null("id"),
            rules.not_null("email"),
            rules.min_rows(2),
        ], save=False)

        assert result.passed is True
        assert result.total_rules == 3

    def test_validate_list_of_dicts_with_failures(self):
        """Validate list of dicts with failing rules."""
        data = [
            {"id": 1, "email": "valid@example.com"},
            {"id": 2, "email": "bad"},  # Invalid email
            {"id": 3, "email": None},   # Null email
        ]

        result = kontra.validate(data, rules=[
            rules.not_null("email"),
            rules.regex("email", r".+@.+\..+"),
        ], save=False)

        assert result.passed is False
        assert result.failed_count == 2  # not_null fails 1, regex fails 2

    def test_validate_single_dict(self):
        """Validate single dict (single record)."""
        record = {"id": 1, "email": "test@example.com", "status": "active"}

        result = kontra.validate(record, rules=[
            rules.not_null("id"),
            rules.not_null("email"),
            rules.allowed_values("status", ["active", "inactive"]),
        ], save=False)

        assert result.passed is True
        assert result.total_rules == 3

    def test_validate_single_dict_with_failure(self):
        """Validate single dict with failing rule."""
        record = {"id": 1, "email": None}

        result = kontra.validate(record, rules=[
            rules.not_null("email"),
        ], save=False)

        assert result.passed is False
        assert result.failed_count == 1

    def test_validate_empty_list(self):
        """Validate empty list (zero rows)."""
        result = kontra.validate([], rules=[
            rules.min_rows(1),
        ], save=False)

        assert result.passed is False
        assert result.failed_count == 1  # min_rows fails

    def test_validate_empty_list_passes_max_rows(self):
        """Validate empty list passes max_rows check."""
        result = kontra.validate([], rules=[
            rules.max_rows(100),
        ], save=False)

        assert result.passed is True

    def test_validate_list_preserves_types(self):
        """Validate that list input preserves data types."""
        data = [
            {"id": 1, "score": 95.5, "active": True},
            {"id": 2, "score": 87.0, "active": False},
        ]

        result = kontra.validate(data, rules=[
            rules.range("score", min=0, max=100),
        ], save=False)

        assert result.passed is True


# =============================================================================
# ValidationResult Tests
# =============================================================================


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_result_properties(self, sample_df):
        """ValidationResult has correct properties."""
        result = kontra.validate(sample_df, rules=[
            rules.not_null("id"),
            rules.unique("id"),
        ], save=False)

        assert hasattr(result, "passed")
        assert hasattr(result, "dataset")
        assert hasattr(result, "total_rules")
        assert hasattr(result, "passed_count")
        assert hasattr(result, "failed_count")
        assert hasattr(result, "warning_count")
        assert hasattr(result, "rules")

    def test_result_blocking_failures(self, df_with_nulls):
        """blocking_failures returns failed blocking rules."""
        result = kontra.validate(df_with_nulls, rules=[
            rules.not_null("id", severity="blocking"),
            rules.unique("id", severity="warning"),
        ], save=False)

        assert len(result.blocking_failures) == 1
        assert result.blocking_failures[0].severity == "blocking"

    def test_result_warnings(self, df_with_nulls):
        """warnings returns failed warning rules."""
        result = kontra.validate(df_with_nulls, rules=[
            rules.not_null("id", severity="warning"),
        ], save=False)

        assert len(result.warnings) == 1
        assert result.warnings[0].severity == "warning"

    def test_result_to_dict(self, sample_df):
        """to_dict() returns serializable dict."""
        result = kontra.validate(sample_df, rules=[rules.min_rows(1)], save=False)

        d = result.to_dict()
        assert isinstance(d, dict)
        assert "passed" in d
        assert "rules" in d
        assert isinstance(d["rules"], list)

    def test_result_to_json(self, sample_df):
        """to_json() returns JSON string."""
        result = kontra.validate(sample_df, rules=[rules.min_rows(1)], save=False)

        import json
        json_str = result.to_json()
        parsed = json.loads(json_str)
        assert parsed["passed"] is True

    def test_result_to_llm(self, sample_df):
        """to_llm() returns token-optimized string."""
        result = kontra.validate(sample_df, rules=[rules.min_rows(1)], save=False)

        llm = result.to_llm()
        assert isinstance(llm, str)
        assert "VALIDATION" in llm
        assert "PASSED" in llm

    def test_result_repr(self, sample_df):
        """__repr__ returns readable string."""
        result = kontra.validate(sample_df, rules=[rules.min_rows(1)], save=False)

        repr_str = repr(result)
        assert "ValidationResult" in repr_str
        assert "PASSED" in repr_str

    def test_result_total_rows(self, sample_df):
        """total_rows reflects the dataset row count."""
        result = kontra.validate(sample_df, rules=[rules.not_null("id")], save=False)

        assert result.total_rows == 5
        assert result.total_rows == len(sample_df)

    def test_result_total_rows_in_to_dict(self, sample_df):
        """to_dict() includes total_rows."""
        result = kontra.validate(sample_df, rules=[rules.not_null("id")], save=False)

        d = result.to_dict()
        assert "total_rows" in d
        assert d["total_rows"] == 5

    def test_result_total_rows_in_to_llm(self, df_with_nulls):
        """to_llm() shows row count in output."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("id")], save=False)

        llm = result.to_llm()
        # Should show "(5 rows)" in the output
        assert "5 rows" in llm

    def test_result_failure_fraction(self, df_with_nulls):
        """total_rows enables computing failure fractions."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("id")], save=False)

        # df_with_nulls has 5 rows, 1 null in id column
        assert result.total_rows == 5
        assert result.failed_count == 1

        # Consumer can compute failure fraction
        fraction = result.failed_count / result.total_rows
        assert fraction == 0.2  # 20%

    def test_result_total_rows_parquet(self, sample_df, tmp_path):
        """total_rows works for Parquet files (from metadata)."""
        parquet_path = tmp_path / "test.parquet"
        sample_df.write_parquet(parquet_path)

        result = kontra.validate(str(parquet_path), rules=[rules.not_null("id")], save=False)

        assert result.total_rows == 5

    def test_result_total_rows_csv(self, sample_df, tmp_path):
        """total_rows works for CSV files (from DuckDB)."""
        csv_path = tmp_path / "test.csv"
        sample_df.write_csv(csv_path)

        result = kontra.validate(str(csv_path), rules=[rules.not_null("id")], save=False)

        assert result.total_rows == 5

    def test_bool_returns_true_when_passed(self, sample_df):
        """bool(result) returns True when validation passes."""
        result = kontra.validate(sample_df, rules=[rules.not_null("id")], save=False)

        assert result.passed is True
        assert bool(result) is True
        # Should work in if statements
        if result:
            passed = True
        else:
            passed = False
        assert passed is True

    def test_bool_returns_false_when_failed(self, df_with_nulls):
        """bool(result) returns False when validation fails."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("id")], save=False)

        assert result.passed is False
        assert bool(result) is False
        # Should work in if statements
        if result:
            passed = True
        else:
            passed = False
        assert passed is False


# =============================================================================
# RuleResult Tests
# =============================================================================


class TestRuleResult:
    """Tests for RuleResult class."""

    def test_rule_result_from_dict(self):
        """RuleResult.from_dict() works correctly."""
        d = {
            "rule_id": "COL:id:not_null",
            "rule_name": "not_null",
            "passed": True,
            "failed_count": 0,
            "message": "Passed",
            "severity": "blocking",
            "execution_source": "polars",
        }
        rule = RuleResult.from_dict(d)

        assert rule.rule_id == "COL:id:not_null"
        assert rule.name == "not_null"
        assert rule.passed is True
        assert rule.column == "id"

    def test_rule_result_repr(self):
        """RuleResult has readable repr."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=10,
            message="10 nulls found",
        )

        assert "FAIL" in repr(rule)
        assert "10" in repr(rule)


# =============================================================================
# Scout Function Tests
# =============================================================================


class TestScoutFunction:
    """Tests for kontra.scout()."""

    def test_scout_dataframe(self, sample_df):
        """Scout profiles a DataFrame."""
        profile = kontra.scout(sample_df, preset="lite")

        assert profile.row_count == 5
        assert profile.column_count == 4

    def test_scout_with_columns(self, sample_df):
        """Scout profiles specific columns."""
        profile = kontra.scout(sample_df, preset="lite", columns=["id", "name"])

        column_names = [c.name for c in profile.columns]
        assert "id" in column_names
        assert "name" in column_names

    def test_scout_presets(self, sample_df):
        """Scout works with different presets."""
        for preset in ["lite", "standard", "deep"]:
            profile = kontra.scout(sample_df, preset=preset)
            assert profile.row_count == 5

    def test_scout_empty_dataframe(self):
        """Scout handles empty DataFrame (no columns) gracefully."""
        # BUG-001: Empty DataFrame used to fail with DuckDB error
        empty_df = pl.DataFrame()

        profile = kontra.scout(empty_df, preset="lite")

        assert profile.row_count == 0
        assert profile.column_count == 0
        assert profile.columns == []

    def test_scout_empty_rows_dataframe(self):
        """Scout handles DataFrame with columns but no rows."""
        empty_rows_df = pl.DataFrame({"id": [], "name": []}).cast({"id": pl.Int64, "name": pl.Utf8})

        profile = kontra.scout(empty_rows_df, preset="lite")

        assert profile.row_count == 0
        assert profile.column_count == 2
        assert len(profile.columns) == 2

    def test_profile_to_llm(self, sample_df):
        """DatasetProfile.to_llm() returns token-optimized string."""
        profile = kontra.scout(sample_df, preset="standard")

        llm_output = profile.to_llm()

        assert isinstance(llm_output, str)
        assert "PROFILE" in llm_output
        assert "rows=" in llm_output or str(profile.row_count) in llm_output
        # Should include column info
        assert "COLUMNS" in llm_output or "id" in llm_output

    def test_profile_to_llm_with_nulls(self, df_with_nulls):
        """DatasetProfile.to_llm() includes null info."""
        profile = kontra.scout(df_with_nulls, preset="standard")

        llm_output = profile.to_llm()

        assert isinstance(llm_output, str)
        # Should mention nulls for columns that have them
        assert "null" in llm_output.lower()


# =============================================================================
# Suggestions Tests
# =============================================================================


class TestSuggestions:
    """Tests for kontra.suggest_rules()."""

    def test_suggest_from_profile(self, sample_df):
        """suggest_rules generates rules from profile."""
        profile = kontra.scout(sample_df, preset="standard")
        suggestions = kontra.suggest_rules(profile)

        assert isinstance(suggestions, Suggestions)
        assert len(suggestions) > 0

    def test_suggestions_filter(self, sample_df):
        """Suggestions can be filtered."""
        profile = kontra.scout(sample_df, preset="standard")
        suggestions = kontra.suggest_rules(profile)

        high_conf = suggestions.filter(min_confidence=0.9)
        assert all(r.confidence >= 0.9 for r in high_conf)

        not_null_only = suggestions.filter(name="not_null")
        assert all(r.name == "not_null" for r in not_null_only)

    def test_suggestions_to_dict(self, sample_df):
        """to_dict() returns list of rule dicts."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        rules_list = suggestions.to_dict()
        assert isinstance(rules_list, list)
        assert all("name" in r and "params" in r for r in rules_list)

    def test_suggestions_to_yaml(self, sample_df):
        """to_yaml() returns YAML contract string."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        yaml_str = suggestions.to_yaml()
        assert "rules:" in yaml_str
        assert "name:" in yaml_str

    def test_suggestions_save(self, sample_df, tmp_path):
        """save() writes YAML file."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        output = tmp_path / "suggested.yml"
        suggestions.save(output)

        assert output.exists()
        content = output.read_text()
        assert "rules:" in content

    def test_suggestions_usable_in_validate(self, sample_df):
        """Suggestions can be used directly in validate."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        # Use suggestions directly
        result = kontra.validate(sample_df, rules=suggestions.to_dict(), save=False)
        assert isinstance(result, ValidationResult)


# =============================================================================
# Explain Function Tests
# =============================================================================


class TestExplainFunction:
    """Tests for kontra.explain()."""

    def test_explain_returns_plan(self, sample_df, sample_contract):
        """explain() returns execution plan."""
        plan = kontra.explain(sample_df, str(sample_contract))

        assert isinstance(plan, dict)
        assert "required_columns" in plan
        assert "total_rules" in plan


# =============================================================================
# Import Tests
# =============================================================================


class TestImports:
    """Tests for public API imports."""

    def test_import_validate(self):
        """Can import validate."""
        from kontra import validate
        assert callable(validate)

    def test_import_scout(self):
        """Can import scout."""
        from kontra import scout
        assert callable(scout)

    def test_import_rules(self):
        """Can import rules."""
        from kontra import rules
        assert hasattr(rules, "not_null")

    def test_import_result_types(self):
        """Can import result types."""
        from kontra import ValidationResult, RuleResult, Suggestions
        assert ValidationResult is not None

    def test_import_profile_types(self):
        """Can import profile types."""
        from kontra import DatasetProfile, ColumnProfile
        assert DatasetProfile is not None

    def test_import_diff(self):
        """Can import Diff."""
        from kontra import Diff
        assert Diff is not None

    def test_import_config_functions(self):
        """Can import config functions."""
        from kontra import resolve, config, list_datasources
        assert callable(resolve)
        assert callable(config)
        assert callable(list_datasources)

    def test_import_history_functions(self):
        """Can import history functions."""
        from kontra import list_runs, get_run, has_runs
        assert callable(list_runs)
        assert callable(get_run)
        assert callable(has_runs)


# =============================================================================
# History Function Tests
# =============================================================================


class TestHistoryFunctions:
    """Tests for history-related functions."""

    def test_has_runs_no_history(self):
        """has_runs returns False when no history."""
        # Without any state store, should return False
        result = kontra.has_runs("nonexistent_contract")
        assert result is False

    def test_list_runs_no_history(self):
        """list_runs returns empty list when no history."""
        result = kontra.list_runs("nonexistent_contract")
        assert result == []

    def test_get_run_no_history(self):
        """get_run returns None when no history."""
        result = kontra.get_run("nonexistent_contract")
        assert result is None

    def test_get_history_rejects_data_file(self, tmp_path):
        """get_history raises clear error for data files (BUG-014)."""
        # Create a parquet file
        df = pl.DataFrame({"x": [1, 2, 3]})
        parquet_path = tmp_path / "data.parquet"
        df.write_parquet(parquet_path)

        # Should raise ValueError with helpful message, not UnicodeDecodeError
        with pytest.raises(ValueError, match="requires a contract YAML file"):
            kontra.get_history(str(parquet_path))

    def test_diff_rejects_data_file(self, tmp_path):
        """diff raises clear error for data files."""
        df = pl.DataFrame({"x": [1, 2, 3]})
        csv_path = tmp_path / "data.csv"
        df.write_csv(csv_path)

        with pytest.raises(ValueError, match="requires a contract YAML file"):
            kontra.diff(str(csv_path))

    def test_list_profiles_not_implemented(self):
        """list_profiles returns empty (not yet implemented)."""
        result = kontra.list_profiles("nonexistent")
        assert result == []

    def test_get_profile_not_implemented(self):
        """get_profile returns None (not yet implemented)."""
        result = kontra.get_profile("nonexistent")
        assert result is None


# =============================================================================
# Diff Function Tests
# =============================================================================


class TestDiffFunction:
    """Tests for kontra.diff()."""

    def test_diff_no_history(self):
        """diff returns None when no history."""
        result = kontra.diff("nonexistent_contract")
        assert result is None

    def test_diff_with_history(self, sample_df, tmp_path, monkeypatch):
        """diff returns Diff object when history exists."""
        from kontra import rules
        from kontra.api.results import Diff

        # Set up temp directory as cwd for state storage
        monkeypatch.chdir(tmp_path)

        # Create a contract file
        contract_path = tmp_path / "test_contract.yml"
        contract_path.write_text("""
name: test_diff_contract
datasource: inline
rules:
  - name: not_null
    params: { column: id }
  - name: min_rows
    params: { threshold: 1 }
""")

        # Run validation twice to create history
        result1 = kontra.validate(sample_df, str(contract_path), save=True)
        result2 = kontra.validate(sample_df, str(contract_path), save=True)

        # Both should pass
        assert result1.passed
        assert result2.passed

        # Now diff should return a Diff object
        diff = kontra.diff(str(contract_path))

        # Should return a Diff (not None)
        assert diff is not None
        assert isinstance(diff, Diff)

        # Should have expected attributes
        assert hasattr(diff, "has_changes")
        assert hasattr(diff, "regressed")

    def test_diff_to_llm(self, sample_df, tmp_path, monkeypatch):
        """diff().to_llm() returns token-optimized string."""
        # Set up temp directory as cwd for state storage
        monkeypatch.chdir(tmp_path)

        # Create a contract file
        contract_path = tmp_path / "test_contract.yml"
        contract_path.write_text("""
name: test_diff_llm_contract
datasource: inline
rules:
  - name: min_rows
    params: { threshold: 1 }
""")

        # Run validation twice
        kontra.validate(sample_df, str(contract_path), save=True)
        kontra.validate(sample_df, str(contract_path), save=True)

        # Get diff
        diff = kontra.diff(str(contract_path))
        assert diff is not None

        # to_llm should return a string
        llm_output = diff.to_llm()
        assert isinstance(llm_output, str)
        assert len(llm_output) > 0
        # Should contain some expected content
        assert "Diff" in llm_output or "diff" in llm_output.lower()


# =============================================================================
# Scout Diff Tests
# =============================================================================


class TestScoutDiffFunction:
    """Tests for kontra.scout_diff()."""

    def test_scout_diff_not_implemented(self):
        """scout_diff returns None (not yet implemented)."""
        result = kontra.scout_diff("nonexistent")
        assert result is None


# =============================================================================
# Config Function Tests
# =============================================================================


class TestConfigFunctions:
    """Tests for configuration functions."""

    def test_config_returns_config_object(self):
        """config() returns config object with expected attributes."""
        cfg = kontra.config()
        # Should have standard config attributes
        assert hasattr(cfg, "preplan")
        assert hasattr(cfg, "pushdown")
        assert hasattr(cfg, "projection")

    def test_config_with_env(self, tmp_path, monkeypatch):
        """config(env=...) uses environment overlay."""
        # Create config with environment
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".kontra").mkdir()
        (tmp_path / ".kontra" / "config.yml").write_text("""
version: "1"
defaults:
  preplan: "off"
environments:
  production:
    preplan: "on"
""")

        cfg_default = kontra.config()
        assert cfg_default.preplan == "off"

        cfg_prod = kontra.config(env="production")
        assert cfg_prod.preplan == "on"

    def test_list_datasources_empty(self):
        """list_datasources returns empty when no config."""
        result = kontra.list_datasources()
        # May return empty dict or None depending on config state
        assert result is None or isinstance(result, dict)


# =============================================================================
# Service/Agent Support Tests
# =============================================================================


class TestServiceAgentSupport:
    """Tests for service/agent support functions."""

    def test_health_returns_dict(self):
        """health() returns dict with expected keys."""
        result = kontra.health()

        assert isinstance(result, dict)
        assert "version" in result
        assert "status" in result
        assert "rule_count" in result
        assert "rules" in result
        assert "config_found" in result

    def test_health_version_matches(self):
        """health() version matches __version__."""
        result = kontra.health()
        assert result["version"] == kontra.__version__

    def test_health_rules_list(self):
        """health() includes list of available rules."""
        result = kontra.health()

        assert isinstance(result["rules"], list)
        assert "not_null" in result["rules"]
        assert "unique" in result["rules"]
        assert result["rule_count"] == len(result["rules"])

    def test_list_rules_returns_list(self):
        """list_rules() returns list of rule info dicts."""
        result = kontra.list_rules()

        assert isinstance(result, list)
        assert len(result) > 0

        # Each rule should have expected keys
        for rule in result:
            assert "name" in rule
            assert "description" in rule
            assert "params" in rule
            assert "scope" in rule

    def test_list_rules_includes_all_rules(self):
        """list_rules() includes all 18 built-in rules."""
        result = kontra.list_rules()
        rule_names = [r["name"] for r in result]

        expected = [
            "not_null", "unique", "allowed_values", "disallowed_values",
            "range", "length", "regex", "contains", "starts_with", "ends_with",
            "dtype", "min_rows", "max_rows", "freshness", "custom_sql_check",
            "compare", "conditional_not_null", "conditional_range",
        ]

        for expected_rule in expected:
            assert expected_rule in rule_names, f"Missing rule: {expected_rule}"

    def test_list_rules_has_descriptions(self):
        """list_rules() provides meaningful descriptions."""
        result = kontra.list_rules()

        for rule in result:
            # Description should be non-empty
            assert rule["description"], f"Empty description for {rule['name']}"
            # Description should describe what the rule does
            assert len(rule["description"]) > 10, f"Description too short for {rule['name']}"

    def test_list_rules_has_complete_metadata(self):
        """BUG-009: list_rules() returns complete metadata for all rules."""
        result = kontra.list_rules()

        for rule in result:
            # Scope should not be "unknown"
            assert rule["scope"] != "unknown", f"Unknown scope for {rule['name']}"
            # Params should not be empty
            assert rule["params"], f"Empty params for {rule['name']}"
            # Should have valid scope
            assert rule["scope"] in ["column", "dataset", "cross-column"], \
                f"Invalid scope '{rule['scope']}' for {rule['name']}"

    def test_set_config_and_get_config_path(self):
        """set_config() and get_config_path() work together."""
        # Initially should be None
        original = kontra.get_config_path()

        try:
            # Set a custom path
            kontra.set_config("/tmp/test-config.yml")
            assert kontra.get_config_path() == "/tmp/test-config.yml"

            # Reset
            kontra.set_config(None)
            assert kontra.get_config_path() is None
        finally:
            # Restore original state
            kontra.set_config(original)

    def test_set_config_affects_health(self):
        """set_config() affects health() output."""
        original = kontra.get_config_path()

        try:
            # Set a nonexistent config
            kontra.set_config("/nonexistent/config.yml")

            health = kontra.health()
            assert health["config_path"] == "/nonexistent/config.yml"
            assert health["config_found"] is False
            assert health["status"] == "config_not_found"
        finally:
            kontra.set_config(original)


# =============================================================================
# ValidationResult Additional Tests
# =============================================================================


class TestValidationResultExtended:
    """Extended tests for ValidationResult."""

    def test_result_to_json_with_indent(self, sample_df):
        """to_json(indent=2) produces formatted JSON."""
        result = kontra.validate(sample_df, rules=[rules.min_rows(1)], save=False)

        json_str = result.to_json(indent=2)
        assert "\n" in json_str  # Indented JSON has newlines

    def test_result_to_dict_structure(self, sample_df):
        """to_dict() has expected structure."""
        result = kontra.validate(sample_df, rules=[
            rules.not_null("id"),
            rules.min_rows(1),
        ], save=False)

        d = result.to_dict()
        assert "passed" in d
        assert "dataset" in d
        assert "total_rules" in d
        assert "passed_count" in d
        assert "failed_count" in d
        assert "warning_count" in d
        assert "rules" in d
        assert isinstance(d["rules"], list)

    def test_result_rules_iteration(self, sample_df):
        """Can iterate over result.rules."""
        result = kontra.validate(sample_df, rules=[
            rules.not_null("id"),
            rules.unique("id"),
        ], save=False)

        count = 0
        for rule in result.rules:
            assert hasattr(rule, "rule_id")
            assert hasattr(rule, "passed")
            count += 1
        assert count == 2

    def test_result_with_failed_rules(self, df_with_nulls):
        """ValidationResult handles failed rules correctly."""
        result = kontra.validate(df_with_nulls, rules=[
            rules.not_null("id", severity="blocking"),
        ], save=False)

        assert result.passed is False
        assert result.failed_count == 1
        assert len(result.blocking_failures) == 1

        failure = result.blocking_failures[0]
        assert failure.passed is False
        assert failure.failed_count > 0

    def test_result_to_llm_with_failures(self, df_with_nulls):
        """to_llm() includes failure info."""
        result = kontra.validate(df_with_nulls, rules=[
            rules.not_null("id", severity="blocking"),
            rules.not_null("name", severity="warning"),
        ], save=False)

        llm = result.to_llm()
        assert "FAILED" in llm
        assert "BLOCKING" in llm
        assert "WARNING" in llm


# =============================================================================
# RuleResult Extended Tests
# =============================================================================


class TestRuleResultExtended:
    """Extended tests for RuleResult."""

    def test_rule_result_to_dict(self):
        """RuleResult.to_dict() works correctly."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=True,
            failed_count=0,
            message="All values are non-null",
            severity="blocking",
            source="polars",
            column="id",
        )

        d = rule.to_dict()
        assert d["rule_id"] == "COL:id:not_null"
        assert d["name"] == "not_null"
        assert d["passed"] is True
        assert d["column"] == "id"

    def test_rule_result_from_dict_extracts_column(self):
        """from_dict extracts column from rule_id."""
        d = {
            "rule_id": "COL:email:unique",
            "passed": True,
            "failed_count": 0,
            "message": "All unique",
        }
        rule = RuleResult.from_dict(d)

        assert rule.column == "email"
        assert rule.name == "unique"

    def test_rule_result_from_dict_dataset_rule(self):
        """from_dict handles DATASET: rules."""
        d = {
            "rule_id": "DATASET:min_rows",
            "passed": True,
            "failed_count": 0,
            "message": "Row count OK",
        }
        rule = RuleResult.from_dict(d)

        assert rule.column is None
        assert rule.name == "min_rows"

    def test_violation_rate_with_failures(self):
        """violation_rate computes correctly for failed rules."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=50,
            message="50 NULL values",
            _total_rows=1000,
        )

        assert rule.violation_rate == 0.05  # 50/1000 = 5%

    def test_violation_rate_none_when_passed(self):
        """violation_rate is None for passing rules."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=True,
            failed_count=0,
            message="Passed",
            _total_rows=1000,
        )

        assert rule.violation_rate is None

    def test_violation_rate_none_without_total_rows(self):
        """violation_rate is None when total_rows not set."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=50,
            message="50 NULL values",
        )

        assert rule.violation_rate is None

    def test_violation_rate_in_to_llm(self):
        """violation_rate appears in to_llm() output."""
        rule = RuleResult(
            rule_id="COL:id:not_null",
            name="not_null",
            passed=False,
            failed_count=100,
            message="100 NULL values",
            _total_rows=1000,
        )

        llm_output = rule.to_llm()
        assert "10.0%" in llm_output  # 100/1000 = 10%


# =============================================================================
# ValidationResult.data Tests
# =============================================================================


class TestValidationResultData:
    """Tests for ValidationResult.data property."""

    def test_data_with_dataframe_input(self, sample_df):
        """result.data returns input DataFrame when DataFrame passed."""
        from kontra import rules

        result = kontra.validate(sample_df, rules=[rules.not_null("id")], save=False)

        assert result.data is not None
        assert result.data is sample_df

    def test_data_with_polars_execution(self, tmp_path, sample_df):
        """result.data returns loaded DataFrame when Polars executes."""
        from kontra import rules

        path = tmp_path / "test.parquet"
        sample_df.write_parquet(path)

        # Force Polars execution
        result = kontra.validate(
            str(path),
            rules=[rules.not_null("id")],
            preplan="off",
            pushdown="off",
            save=False,
        )

        assert result.data is not None
        assert result.data.shape[0] == sample_df.shape[0]

    def test_data_none_when_preplan_handles(self, tmp_path, sample_df):
        """result.data is None when preplan handles everything."""
        from kontra import rules

        path = tmp_path / "test.parquet"
        sample_df.write_parquet(path)

        # Let preplan handle it (auto defaults)
        result = kontra.validate(
            str(path),
            rules=[rules.not_null("id")],  # Preplan can prove this via metadata
            save=False,
        )

        # Preplan proved pass via metadata - no data loaded
        assert result.data is None
        assert result.passed


# =============================================================================
# Suggestions Extended Tests
# =============================================================================


class TestSuggestionsExtended:
    """Extended tests for Suggestions."""

    def test_suggestions_len(self, sample_df):
        """len(suggestions) works."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        assert len(suggestions) > 0

    def test_suggestions_iteration(self, sample_df):
        """Can iterate over suggestions."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        for s in suggestions:
            assert hasattr(s, "name")
            assert hasattr(s, "params")
            assert hasattr(s, "confidence")

    def test_suggestions_indexing(self, sample_df):
        """Can index suggestions."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        first = suggestions[0]
        assert hasattr(first, "name")

    def test_suggestions_to_json(self, sample_df):
        """to_json() returns valid JSON."""
        import json
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        json_str = suggestions.to_json()
        parsed = json.loads(json_str)
        assert isinstance(parsed, list)

    def test_suggestions_filter_by_name(self, sample_df):
        """filter(name=...) works."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        not_null_only = suggestions.filter(name="not_null")
        assert all(s.name == "not_null" for s in not_null_only)

    def test_suggestions_filter_combined(self, sample_df):
        """filter with multiple criteria."""
        profile = kontra.scout(sample_df, preset="lite")
        suggestions = kontra.suggest_rules(profile)

        filtered = suggestions.filter(min_confidence=0.9, name="not_null")
        assert all(s.name == "not_null" and s.confidence >= 0.9 for s in filtered)

    def test_suggested_rule_to_dict(self):
        """SuggestedRule.to_dict() returns rule dict."""
        rule = SuggestedRule(
            name="not_null",
            params={"column": "id"},
            confidence=1.0,
            reason="Column has no nulls",
        )

        d = rule.to_dict()
        assert d == {"name": "not_null", "params": {"column": "id"}}

    def test_suggested_rule_to_full_dict(self):
        """SuggestedRule.to_full_dict() includes metadata."""
        rule = SuggestedRule(
            name="not_null",
            params={"column": "id"},
            confidence=0.9,
            reason="Column has no nulls",
        )

        d = rule.to_full_dict()
        assert "confidence" in d
        assert "reason" in d
        assert d["confidence"] == 0.9


# =============================================================================
# Explain Extended Tests
# =============================================================================


class TestExplainExtended:
    """Extended tests for kontra.explain()."""

    def test_explain_structure(self, sample_df, sample_contract):
        """explain() returns expected structure."""
        plan = kontra.explain(sample_df, str(sample_contract))

        assert "required_columns" in plan
        assert "total_rules" in plan
        assert "predicates" in plan
        assert "fallback_rules" in plan
        assert "sql_rules" in plan

    def test_explain_columns_list(self, sample_df, tmp_path):
        """explain() returns required columns."""
        contract = tmp_path / "contract.yml"
        contract.write_text("""
name: test
datasource: placeholder
rules:
  - name: not_null
    params:
      column: id
  - name: not_null
    params:
      column: name
""")

        plan = kontra.explain(sample_df, str(contract))
        assert "id" in plan["required_columns"]
        assert "name" in plan["required_columns"]


# =============================================================================
# Rules Helpers Extended Tests
# =============================================================================


class TestRulesHelpersExtended:
    """Extended tests for rules helpers."""

    def test_rules_module_repr(self):
        """rules module has repr."""
        assert "<kontra.rules module>" in repr(rules)

    def test_custom_sql_check(self):
        """rules.custom_sql_check() returns correct dict."""
        rule = rules.custom_sql_check("SELECT COUNT(*) FROM {table} WHERE x < 0")
        assert rule["name"] == "custom_sql_check"
        assert "sql" in rule["params"]

    def test_all_rules_have_severity(self):
        """All rule helpers include severity."""
        rule_funcs = [
            rules.not_null("col"),
            rules.unique("col"),
            rules.dtype("col", "int64"),
            rules.range("col", min=0),
            rules.allowed_values("col", [1, 2]),
            rules.regex("col", ".*"),
            rules.min_rows(1),
            rules.max_rows(100),
            rules.freshness("col", "24h"),
        ]

        for rule in rule_funcs:
            assert "severity" in rule
            assert rule["severity"] == "blocking"  # default


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_validate_empty_rules_list(self, sample_df):
        """validate with empty rules list runs but has 0 rules."""
        # Empty rules list is valid - just returns 0 rules validated
        result = kontra.validate(sample_df, rules=[], save=False)
        assert result.total_rules == 0
        assert result.passed is True  # No rules to fail

    def test_validate_with_stats(self, sample_df):
        """validate with stats option."""
        result = kontra.validate(
            sample_df,
            rules=[rules.min_rows(1)],
            stats="summary",
            save=False,
        )
        # Stats should be included in result
        assert result.stats is not None or result.passed  # Just verify it runs

    def test_scout_with_sample(self, sample_df):
        """scout with sample parameter."""
        profile = kontra.scout(sample_df, preset="lite", sample=3)
        # Should still profile (sample may be >= data size)
        assert profile.row_count <= 5

    def test_validate_pandas_dataframe(self, sample_df):
        """validate works with pandas DataFrame."""
        pytest.importorskip("pandas")
        pdf = sample_df.to_pandas()

        result = kontra.validate(pdf, rules=[rules.min_rows(1)], save=False)
        assert result.passed is True


# =============================================================================
# BYOC (Bring Your Own Connection) Tests
# =============================================================================


class TestConnectionDetection:
    """Tests for connection type detection."""

    def test_parse_table_reference_simple(self):
        """Parse simple table name."""
        from kontra.connectors.detection import parse_table_reference

        db, schema, table = parse_table_reference("users")
        assert db is None
        assert schema is None
        assert table == "users"

    def test_parse_table_reference_schema_table(self):
        """Parse schema.table reference."""
        from kontra.connectors.detection import parse_table_reference

        db, schema, table = parse_table_reference("public.users")
        assert db is None
        assert schema == "public"
        assert table == "users"

    def test_parse_table_reference_db_schema_table(self):
        """Parse database.schema.table reference."""
        from kontra.connectors.detection import parse_table_reference

        db, schema, table = parse_table_reference("mydb.dbo.orders")
        assert db == "mydb"
        assert schema == "dbo"
        assert table == "orders"

    def test_parse_table_reference_too_many_parts(self):
        """Reject table reference with too many parts."""
        from kontra.connectors.detection import parse_table_reference

        with pytest.raises(ValueError, match="Invalid table reference"):
            parse_table_reference("a.b.c.d")

    def test_get_default_schema_postgres(self):
        """Default schema for PostgreSQL is 'public'."""
        from kontra.connectors.detection import get_default_schema, POSTGRESQL

        assert get_default_schema(POSTGRESQL) == "public"

    def test_get_default_schema_sqlserver(self):
        """Default schema for SQL Server is 'dbo'."""
        from kontra.connectors.detection import get_default_schema, SQLSERVER

        assert get_default_schema(SQLSERVER) == "dbo"

    def test_is_database_connection_none(self):
        """None is not a database connection."""
        from kontra.connectors.detection import is_database_connection

        assert is_database_connection(None) is False

    def test_is_database_connection_string(self):
        """Strings are not database connections."""
        from kontra.connectors.detection import is_database_connection

        assert is_database_connection("not a connection") is False

    def test_is_database_connection_has_cursor(self):
        """Objects with callable cursor() are detected as connections."""
        from kontra.connectors.detection import is_database_connection

        class FakeConn:
            def cursor(self):
                pass

        assert is_database_connection(FakeConn()) is True


class TestDatasetHandleBYOC:
    """Tests for DatasetHandle BYOC support."""

    def test_from_connection_creates_handle(self):
        """from_connection creates a valid BYOC handle."""
        from kontra.connectors.handle import DatasetHandle
        from unittest.mock import MagicMock

        # Mock a psycopg connection
        mock_conn = MagicMock()
        mock_conn.__class__.__module__ = "psycopg"
        mock_conn.__class__.__name__ = "Connection"

        handle = DatasetHandle.from_connection(mock_conn, "public.users")

        assert handle.scheme == "byoc"
        assert handle.dialect == "postgresql"
        assert handle.table_ref == "public.users"
        assert handle.external_conn is mock_conn
        assert handle.owned is False  # User manages connection lifecycle

    def test_from_connection_sqlserver_dialect(self):
        """from_connection detects SQL Server dialect."""
        from kontra.connectors.handle import DatasetHandle
        from unittest.mock import MagicMock

        # Mock a pymssql connection
        mock_conn = MagicMock()
        mock_conn.__class__.__module__ = "pymssql"
        mock_conn.__class__.__name__ = "Connection"

        handle = DatasetHandle.from_connection(mock_conn, "dbo.orders")

        assert handle.scheme == "byoc"
        assert handle.dialect == "sqlserver"
        assert handle.table_ref == "dbo.orders"


class TestValidateBYOC:
    """Tests for validate() with BYOC pattern."""

    def test_validate_requires_table_for_connection(self):
        """validate raises error if table missing for connection."""
        from unittest.mock import MagicMock

        # Mock a database connection
        mock_conn = MagicMock()
        mock_conn.__class__.__module__ = "psycopg"
        mock_conn.__class__.__name__ = "Connection"

        with pytest.raises(ValueError, match="table.*parameter is required"):
            kontra.validate(mock_conn, rules=[rules.not_null("id")], save=False)

    def test_validate_rejects_table_for_non_connection(self, sample_df):
        """validate raises error if table provided for non-connection data."""
        with pytest.raises(ValueError, match="table.*only valid"):
            kontra.validate(sample_df, table="users", rules=[rules.not_null("id")], save=False)

    def test_validate_rejects_table_for_string_path(self, tmp_path, sample_df):
        """validate raises error if table provided for file path."""
        parquet = tmp_path / "data.parquet"
        sample_df.write_parquet(parquet)

        with pytest.raises(ValueError, match="table.*only valid"):
            kontra.validate(str(parquet), table="users", rules=[rules.not_null("id")], save=False)


# =============================================================================
# Invalid Data Type Tests (BUG-001 to BUG-005)
# =============================================================================


class TestInvalidDataErrors:
    """Tests for clear error messages on invalid data types."""

    def test_validate_rejects_none_data(self):
        """BUG-001: validate raises InvalidDataError for None data."""
        from kontra.errors import InvalidDataError

        with pytest.raises(InvalidDataError, match="NoneType.*cannot be None"):
            kontra.validate(None, rules=[rules.not_null("col")], save=False)

    def test_validate_rejects_invalid_string(self):
        """BUG-002: validate raises InvalidDataError for invalid string data."""
        from kontra.errors import InvalidDataError

        with pytest.raises(InvalidDataError, match="str.*not a valid file path"):
            kontra.validate("not a dataset", rules=[rules.not_null("col")], save=False)

    def test_validate_rejects_integer_data(self):
        """BUG-003: validate raises InvalidDataError for integer data."""
        from kontra.errors import InvalidDataError

        with pytest.raises(InvalidDataError, match="int"):
            kontra.validate(42, rules=[rules.not_null("col")], save=False)

    def test_validate_rejects_directory_path(self):
        """BUG-004: validate raises InvalidPathError for directory path."""
        from kontra.errors import InvalidPathError

        with pytest.raises(InvalidPathError, match="directory.*not a file"):
            kontra.validate("/tmp/", rules=[rules.not_null("col")], save=False)

    def test_validate_rejects_cursor_object(self):
        """BUG-005: validate raises InvalidDataError for cursor object."""
        from kontra.errors import InvalidDataError
        from unittest.mock import MagicMock

        # Create a mock cursor (has execute/fetchone but NOT cursor method)
        mock_cursor = MagicMock()
        mock_cursor.__class__.__name__ = "Cursor"
        mock_cursor.__class__.__module__ = "psycopg"
        # Cursors have execute and fetchone, but not cursor()
        del mock_cursor.cursor

        with pytest.raises(InvalidDataError, match="Cursor.*Expected database connection.*got cursor"):
            kontra.validate(mock_cursor, table="users", rules=[rules.not_null("id")], save=False)

    def test_validate_rejects_float_data(self):
        """validate raises InvalidDataError for float data."""
        from kontra.errors import InvalidDataError

        with pytest.raises(InvalidDataError, match="float"):
            kontra.validate(3.14, rules=[rules.not_null("col")], save=False)

    def test_validate_rejects_tuple_data(self):
        """validate raises InvalidDataError for tuple data."""
        from kontra.errors import InvalidDataError

        with pytest.raises(InvalidDataError, match="tuple"):
            kontra.validate((1, 2, 3), rules=[rules.not_null("col")], save=False)


# =============================================================================
# Parameter Validation Tests (BUG-006, BUG-007)
# =============================================================================


class TestParameterValidation:
    """Tests for parameter validation in rule helpers."""

    def test_min_rows_rejects_negative_threshold(self):
        """BUG-006: min_rows raises ValueError for negative threshold."""
        with pytest.raises(ValueError, match="non-negative"):
            rules.min_rows(-1)

    def test_min_rows_accepts_zero(self):
        """min_rows accepts zero as threshold."""
        rule = rules.min_rows(0)
        assert rule["params"]["threshold"] == 0

    def test_min_rows_accepts_positive(self):
        """min_rows accepts positive threshold."""
        rule = rules.min_rows(100)
        assert rule["params"]["threshold"] == 100

    def test_range_rejects_min_greater_than_max(self):
        """BUG-007: range raises ValueError when min > max."""
        with pytest.raises(ValueError, match="min.*must be <= max"):
            rules.range("col", min=100, max=10)

    def test_range_accepts_min_equals_max(self):
        """range accepts min == max (exact value check)."""
        rule = rules.range("col", min=50, max=50)
        assert rule["params"]["min"] == 50
        assert rule["params"]["max"] == 50

    def test_range_accepts_valid_bounds(self):
        """range accepts valid min <= max."""
        rule = rules.range("col", min=0, max=100)
        assert rule["params"]["min"] == 0
        assert rule["params"]["max"] == 100

    def test_range_accepts_only_min(self):
        """range accepts only min (no max)."""
        rule = rules.range("col", min=0)
        assert rule["params"]["min"] == 0
        assert "max" not in rule["params"]

    def test_range_accepts_only_max(self):
        """range accepts only max (no min)."""
        rule = rules.range("col", max=100)
        assert rule["params"]["max"] == 100
        assert "min" not in rule["params"]


# =============================================================================
# Dry Run Tests
# =============================================================================


class TestDryRun:
    """Tests for dry_run functionality."""

    def test_dry_run_returns_dry_run_result(self, sample_df, sample_contract):
        """dry_run=True returns DryRunResult, not ValidationResult."""
        from kontra.api.results import DryRunResult

        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        assert isinstance(result, DryRunResult)

    def test_dry_run_valid_contract(self, sample_df, sample_contract):
        """dry_run with valid contract shows valid=True."""
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        assert result.valid is True
        assert result.rules_count == 2
        assert result.errors == []
        assert result.contract_name == "test_contract"

    def test_dry_run_columns_needed(self, sample_df, sample_contract):
        """dry_run extracts required columns."""
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        assert "id" in result.columns_needed

    def test_dry_run_inline_rules(self, sample_df):
        """dry_run works with inline rules."""
        result = kontra.validate(sample_df, rules=[
            rules.not_null("id"),
            rules.unique("name"),
            rules.range("age", min=0, max=150),
        ], dry_run=True)
        assert result.valid is True
        assert result.rules_count == 3
        assert set(result.columns_needed) == {"id", "name", "age"}

    def test_dry_run_invalid_contract_file(self, sample_df):
        """dry_run with missing contract file shows valid=False."""
        result = kontra.validate(sample_df, "/nonexistent/contract.yml", dry_run=True)
        assert result.valid is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_dry_run_invalid_contract_syntax(self, sample_df, tmp_path):
        """dry_run with invalid contract syntax shows valid=False."""
        bad_contract = tmp_path / "bad.yml"
        bad_contract.write_text("- this is not a valid contract\n- just a list")

        result = kontra.validate(sample_df, str(bad_contract), dry_run=True)
        assert result.valid is False
        assert len(result.errors) > 0

    def test_dry_run_to_dict(self, sample_df, sample_contract):
        """DryRunResult.to_dict() works."""
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        d = result.to_dict()
        assert "valid" in d
        assert "rules_count" in d
        assert "columns_needed" in d
        assert "errors" in d

    def test_dry_run_to_json(self, sample_df, sample_contract):
        """DryRunResult.to_json() works."""
        import json
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        j = result.to_json()
        parsed = json.loads(j)
        assert parsed["valid"] is True

    def test_dry_run_to_llm(self, sample_df, sample_contract):
        """DryRunResult.to_llm() works."""
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        llm = result.to_llm()
        assert "DRYRUN" in llm
        assert "VALID" in llm

    def test_dry_run_invalid_to_llm(self, sample_df):
        """DryRunResult.to_llm() for invalid contract."""
        result = kontra.validate(sample_df, "/nonexistent.yml", dry_run=True)
        llm = result.to_llm()
        assert "DRYRUN" in llm
        assert "INVALID" in llm

    def test_dry_run_does_not_execute_validation(self, sample_df, sample_contract):
        """dry_run does not actually run validation - even bad data passes."""
        # This would fail actual validation (df doesn't match contract columns)
        wrong_df = pl.DataFrame({"wrong_col": [1, 2, 3]})

        # But dry_run just checks contract syntax
        result = kontra.validate(wrong_df, str(sample_contract), dry_run=True)
        assert result.valid is True  # Contract syntax is valid

    def test_dry_run_repr(self, sample_df, sample_contract):
        """DryRunResult has readable repr."""
        result = kontra.validate(sample_df, str(sample_contract), dry_run=True)
        repr_str = repr(result)
        assert "DryRunResult" in repr_str
        assert "VALID" in repr_str

    def test_dry_run_accepts_none_data(self, sample_contract):
        """dry_run accepts None as data (just validates contract)."""
        result = kontra.validate(None, str(sample_contract), dry_run=True)
        assert result.valid is True
        assert result.rules_count == 2

    def test_dry_run_none_data_inline_rules(self):
        """dry_run with None data and inline rules."""
        result = kontra.validate(None, rules=[
            rules.not_null("id"),
            rules.unique("email"),
        ], dry_run=True)
        assert result.valid is True
        assert result.rules_count == 2
        assert set(result.columns_needed) == {"id", "email"}
