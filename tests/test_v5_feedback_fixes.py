# tests/test_v5_feedback_fixes.py
"""
Tests for fixes from v5 surprise feedback:
1. Missing column returns validation failure, not exception
2. Context field support in contracts
"""

import pytest
import polars as pl
import tempfile
import os

import kontra
from kontra import rules
from kontra.rule_defs.base import BaseRule
from kontra.config.loader import ContractLoader
from kontra.config.models import RuleSpec
from kontra.rule_defs.factory import RuleFactory


class TestMissingColumnValidationFailure:
    """Test that missing columns result in validation failure, not exception."""

    def test_not_null_missing_column(self):
        """not_null on missing column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.not_null("missing_col")])

        assert not result.passed
        assert result.rules[0].passed is False
        assert "missing_col" in result.rules[0].message
        assert "not found" in result.rules[0].message.lower()

    def test_missing_column_has_config_error_failure_mode(self):
        """Missing column should have failure_mode='config_error' (BUG-004)."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.not_null("missing_col")])

        assert not result.passed
        rule_result = result.rules[0]
        assert rule_result.failure_mode == "config_error"
        assert rule_result.details is not None
        assert "missing_columns" in rule_result.details
        assert "missing_col" in rule_result.details["missing_columns"]
        assert "available_columns" in rule_result.details

    def test_unique_missing_column(self):
        """unique on missing column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.unique("missing_col")])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_range_missing_column(self):
        """range on missing column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.range("missing_col", min=0)])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_allowed_values_missing_column(self):
        """allowed_values on missing column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.allowed_values("missing_col", ["x"])])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_regex_missing_column(self):
        """regex on missing column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.regex("missing_col", r".*")])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_compare_missing_left_column(self):
        """compare with missing left column should fail, not throw."""
        df = pl.DataFrame({"b": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.compare("missing", "b", ">=")])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_compare_missing_right_column(self):
        """compare with missing right column should fail, not throw."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[rules.compare("a", "missing", ">=")])

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_conditional_not_null_missing_column(self):
        """conditional_not_null with missing column should fail, not throw."""
        df = pl.DataFrame({"status": ["shipped", "pending"]})
        result = kontra.validate(
            df, rules=[rules.conditional_not_null("missing", when="status == 'shipped'")]
        )

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_conditional_not_null_missing_when_column(self):
        """conditional_not_null with missing when column should fail, not throw."""
        df = pl.DataFrame({"shipping_date": ["2024-01-01", None]})
        result = kontra.validate(
            df, rules=[rules.conditional_not_null("shipping_date", when="missing == 'x'")]
        )

        assert not result.passed
        assert "not found" in result.rules[0].message.lower()

    def test_nested_data_hint(self):
        """When data has single column (possibly nested), hint should appear."""
        df = pl.DataFrame({"data": [{"a": 1}, {"a": 2}]})
        result = kontra.validate(df, rules=[rules.not_null("id")])

        assert not result.passed
        # Should mention nested data
        assert "nested" in result.rules[0].message.lower()

    def test_mixed_valid_invalid_columns(self):
        """Mix of valid and missing columns should handle both correctly."""
        df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        result = kontra.validate(
            df,
            rules=[
                rules.not_null("name"),  # exists
                rules.not_null("missing"),  # doesn't exist
                rules.range("age", min=0),  # exists
            ],
        )

        # Should have 3 rules: 2 pass, 1 fail
        assert len(result.rules) == 3
        passed_rules = [r for r in result.rules if r.passed]
        failed_rules = [r for r in result.rules if not r.passed]
        assert len(passed_rules) == 2
        assert len(failed_rules) == 1
        assert "not found" in failed_rules[0].message.lower()

    def test_missing_column_details(self):
        """Missing column failure should include details with available columns."""
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = kontra.validate(df, rules=[rules.not_null("missing")])

        assert result.rules[0].details is not None
        assert "missing_columns" in result.rules[0].details
        assert "available_columns" in result.rules[0].details
        assert "missing" in result.rules[0].details["missing_columns"]


class TestContextSupport:
    """Test context field support in contracts."""

    def test_context_parsed_from_yaml(self):
        """Context should be parsed from contract YAML."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
    context:
      owner: data_team
      fix_hint: "Ensure email is provided"
      tags:
        - critical
        - pii
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            contract = ContractLoader.from_path(path)
            assert len(contract.rules) == 1
            assert contract.rules[0].context == {
                "owner": "data_team",
                "fix_hint": "Ensure email is provided",
                "tags": ["critical", "pii"],
            }
        finally:
            os.unlink(path)

    def test_context_passed_to_rule(self):
        """Context should be passed to rule instances."""
        spec = RuleSpec(
            name="not_null",
            params={"column": "email"},
            context={"owner": "test_team", "severity_hint": "high"},
        )
        factory = RuleFactory([spec])
        rules_list = factory.build_rules()

        assert len(rules_list) == 1
        assert rules_list[0].context == {"owner": "test_team", "severity_hint": "high"}

    def test_context_in_validation_result(self):
        """Context should be accessible in validation results."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
    context:
      owner: data_team
      fix_hint: "Check email field"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            df = pl.DataFrame({"email": ["a@b.com", None]})
            result = kontra.validate(df, contract=path)

            assert len(result.rules) == 1
            assert result.rules[0].context is not None
            assert result.rules[0].context["owner"] == "data_team"
            assert result.rules[0].context["fix_hint"] == "Check email field"
        finally:
            os.unlink(path)

    def test_context_empty_by_default(self):
        """Rules without context should have empty context dict."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            df = pl.DataFrame({"email": ["a@b.com"]})
            result = kontra.validate(df, contract=path)

            # Context should be None or empty when not specified
            assert result.rules[0].context is None or result.rules[0].context == {}
        finally:
            os.unlink(path)

    def test_context_does_not_affect_validation(self):
        """Context should not affect pass/fail determination."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
    context:
      should_pass: false
      force_fail: true
      override_result: "fail"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            # Data is valid - no nulls
            df = pl.DataFrame({"email": ["a@b.com", "b@c.com"]})
            result = kontra.validate(df, contract=path)

            # Should pass despite context suggesting otherwise
            assert result.passed
            assert result.rules[0].passed
        finally:
            os.unlink(path)

    def test_context_with_arbitrary_fields(self):
        """Context should accept any fields user defines."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
    context:
      custom_field_1: "value1"
      custom_field_2: 123
      nested:
        a: 1
        b: 2
      list_field:
        - item1
        - item2
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            contract = ContractLoader.from_path(path)
            ctx = contract.rules[0].context

            assert ctx["custom_field_1"] == "value1"
            assert ctx["custom_field_2"] == 123
            assert ctx["nested"] == {"a": 1, "b": 2}
            assert ctx["list_field"] == ["item1", "item2"]
        finally:
            os.unlink(path)

    def test_context_in_to_dict(self):
        """Context should be included in to_dict() output."""
        contract_yaml = """
name: test
datasource: inline
rules:
  - name: not_null
    params:
      column: email
    context:
      owner: test_team
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(contract_yaml)
            path = f.name

        try:
            df = pl.DataFrame({"email": [None]})  # Will fail
            result = kontra.validate(df, contract=path)

            rule_dict = result.rules[0].to_dict()
            assert "context" in rule_dict
            assert rule_dict["context"]["owner"] == "test_team"
        finally:
            os.unlink(path)


class TestCheckColumnsHelper:
    """Test the _check_columns helper method."""

    def test_check_columns_all_present(self):
        """_check_columns returns None when all columns exist."""
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})

        # Create a dummy rule to test the helper
        rule = type(
            "DummyRule",
            (BaseRule,),
            {"validate": lambda self, df: {}, "required_columns": lambda self: set()},
        )("dummy", {})

        result = rule._check_columns(df, {"a", "b"})
        assert result is None

    def test_check_columns_single_missing(self):
        """_check_columns returns failure dict for single missing column."""
        df = pl.DataFrame({"a": [1], "b": [2]})

        rule = type(
            "DummyRule",
            (BaseRule,),
            {"validate": lambda self, df: {}, "required_columns": lambda self: set()},
        )("dummy", {})
        rule.rule_id = "test_rule"

        result = rule._check_columns(df, {"a", "missing"})
        assert result is not None
        assert result["passed"] is False
        assert "missing" in result["message"].lower()
        assert "missing" in result["details"]["missing_columns"]

    def test_check_columns_multiple_missing(self):
        """_check_columns returns failure dict for multiple missing columns."""
        df = pl.DataFrame({"a": [1]})

        rule = type(
            "DummyRule",
            (BaseRule,),
            {"validate": lambda self, df: {}, "required_columns": lambda self: set()},
        )("dummy", {})
        rule.rule_id = "test_rule"

        result = rule._check_columns(df, {"missing1", "missing2"})
        assert result is not None
        assert result["passed"] is False
        assert "missing1" in result["details"]["missing_columns"]
        assert "missing2" in result["details"]["missing_columns"]

    def test_check_columns_empty_set(self):
        """_check_columns returns None for empty column set."""
        df = pl.DataFrame({"a": [1]})

        rule = type(
            "DummyRule",
            (BaseRule,),
            {"validate": lambda self, df: {}, "required_columns": lambda self: set()},
        )("dummy", {})

        result = rule._check_columns(df, set())
        assert result is None
