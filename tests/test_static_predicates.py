# tests/test_static_predicates.py
"""Tests for static predicate extraction from rules."""

import pytest

from kontra.rule_defs.static_predicates import (
    extract_static_predicates_from_rules,
    extract_static_predicates,
    _normalize,
    _from_rule_hook,
    _conservative_builtin_mapping,
)
from kontra.rule_defs.registry import get_rule


def make_rule(name: str, params: dict):
    """Helper to create a rule instance."""
    rule_cls = get_rule(name)
    rule = rule_cls(name, params)
    # Set rule_id like the factory would
    col = params.get("column")
    if col:
        rule.rule_id = f"COL:{col}:{name}"
    else:
        rule.rule_id = f"DATASET:{name}"
    return rule


class TestNormalize:
    """Tests for _normalize function."""

    def test_normalize_filters_invalid_rule_id(self):
        """Invalid rule_ids are filtered."""
        pairs = [
            ("", "col", "==", 1),  # Empty rule_id
            (123, "col", "==", 1),  # Non-string rule_id
            ("valid", "col", "==", 1),  # Valid
        ]
        result = _normalize(pairs)
        assert len(result) == 1
        assert result[0][0] == "valid"

    def test_normalize_filters_invalid_column(self):
        """Invalid columns are filtered."""
        pairs = [
            ("rule", "", "==", 1),  # Empty column
            ("rule", None, "==", 1),  # None column
            ("rule", "col", "==", 1),  # Valid
        ]
        result = _normalize(pairs)
        assert len(result) == 1
        assert result[0][1] == "col"

    def test_normalize_filters_invalid_op(self):
        """Invalid operators are filtered."""
        pairs = [
            ("rule", "col", "INVALID", 1),  # Invalid op
            ("rule", "col", "like", 1),  # Not allowed
            ("rule", "col", "==", 1),  # Valid
        ]
        result = _normalize(pairs)
        assert len(result) == 1
        assert result[0][2] == "=="

    def test_normalize_deduplicates(self):
        """Duplicate predicates are removed."""
        pairs = [
            ("rule", "col", "==", 1),
            ("rule", "col", "==", 1),  # Duplicate
            ("rule", "col", "==", 2),  # Different value
        ]
        result = _normalize(pairs)
        assert len(result) == 2


class TestConservativeBuiltinMapping:
    """Tests for _conservative_builtin_mapping."""

    def test_not_null_rule(self):
        """not_null rule produces not_null predicate."""
        rule = make_rule("not_null", {"column": "id"})
        result = _conservative_builtin_mapping(rule)
        assert len(result) == 1
        assert result[0][2] == "not_null"
        assert result[0][1] == "id"

    def test_allowed_values_single_value(self):
        """allowed_values with single value produces == predicate."""
        rule = make_rule("allowed_values", {"column": "status", "values": ["active"]})
        result = _conservative_builtin_mapping(rule)
        assert len(result) == 1
        assert result[0][2] == "=="
        assert result[0][3] == "active"

    def test_allowed_values_multiple_values_skipped(self):
        """allowed_values with multiple values produces no predicate."""
        rule = make_rule("allowed_values", {"column": "status", "values": ["active", "inactive"]})
        result = _conservative_builtin_mapping(rule)
        assert len(result) == 0

    def test_regex_prefix_pattern(self):
        """regex with ^prefix pattern produces ^= predicate."""
        rule = make_rule("regex", {"column": "email", "pattern": "^user"})
        result = _conservative_builtin_mapping(rule)
        assert len(result) == 1
        assert result[0][2] == "^="
        assert result[0][3] == "user"

    def test_regex_complex_pattern_skipped(self):
        """regex with complex pattern produces no predicate."""
        rule = make_rule("regex", {"column": "email", "pattern": "^user.*@test\\.com$"})
        result = _conservative_builtin_mapping(rule)
        assert len(result) == 0  # Contains special chars


class TestFromRuleHook:
    """Tests for _from_rule_hook."""

    def test_rule_without_hook(self):
        """Rule without to_preplan_predicates returns empty."""
        rule = make_rule("min_rows", {"threshold": 10})
        result = _from_rule_hook(rule)
        assert result == []

    def test_rule_with_invalid_hook_return(self):
        """Rule with hook returning invalid tuples is handled."""
        class MockRule:
            rule_id = "test"
            name = "test"

            def to_preplan_predicates(self):
                return [
                    ("rid", "col", "==", 1),  # Valid
                    ("invalid",),  # Invalid tuple length
                    "not a tuple",  # Not a tuple
                ]

        result = _from_rule_hook(MockRule())
        assert len(result) == 1

    def test_rule_hook_fills_missing_rid(self):
        """Rule hook fills in missing rule_id."""
        class MockRule:
            rule_id = "my_rule"
            name = "test"

            def to_preplan_predicates(self):
                return [
                    ("", "col", "==", 1),  # Empty rid should be filled
                ]

        result = _from_rule_hook(MockRule())
        assert len(result) == 1
        assert result[0][0] == "my_rule"


class TestExtractStaticPredicates:
    """Tests for extract_static_predicates_from_rules."""

    def test_extracts_from_multiple_rules(self):
        """Extracts predicates from multiple rules."""
        rules = [
            make_rule("not_null", {"column": "id"}),
            make_rule("not_null", {"column": "name"}),
        ]
        result = extract_static_predicates_from_rules(rules)
        assert len(result) == 2

    def test_backward_compatible_shim(self):
        """extract_static_predicates works as backward-compatible shim."""
        rules = [make_rule("not_null", {"column": "id"})]
        result = extract_static_predicates(rules=rules)
        assert len(result) == 1
