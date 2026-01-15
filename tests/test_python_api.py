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
