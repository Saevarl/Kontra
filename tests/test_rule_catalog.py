"""Public rule reference stays complete, exact, and cheap to import."""

from __future__ import annotations

import subprocess
import sys
import warnings

import pytest
import yaml

import kontra


EXPECTED_RULES = {
    "allowed_values", "compare", "conditional_not_null", "conditional_range",
    "contains", "custom_sql_check", "disallowed_values", "dtype", "ends_with",
    "freshness", "length", "max_rows", "min_rows", "not_null", "range",
    "regex", "starts_with", "unique",
}


def test_rule_index_is_compact_and_complete():
    rules = kontra.list_rules()

    assert {rule["name"] for rule in rules} == EXPECTED_RULES
    assert all(set(rule) == {"name", "description", "params", "scope"} for rule in rules)


@pytest.mark.parametrize("name", sorted(EXPECTED_RULES))
def test_rule_detail_has_exact_contract_reference(name):
    detail = kontra.describe_rule(name)
    example = yaml.safe_load(detail["example"])

    assert detail["name"] == name
    assert detail["scope"] in {"column", "dataset", "cross-column"}
    assert detail["summary"]
    assert detail["fails_when"]
    assert detail["nulls"]
    assert detail["counting"]
    assert isinstance(detail["supports_tally"], bool)
    assert detail["parameters"]
    assert example[0]["name"] == name
    assert isinstance(example[0]["params"], dict)
    assert detail["contract"]["fields"]["severity"].startswith("blocking")


def test_rule_detail_is_a_copy():
    detail = kontra.describe_rule("range")
    detail["parameters"][0]["name"] = "changed"

    assert kontra.describe_rule("range")["parameters"][0]["name"] == "column"


def test_every_catalog_example_builds_a_real_rule():
    from kontra.config.models import RuleSpec
    from kontra.engine.phases.compilation import _ensure_builtin_rules_registered
    from kontra.rule_defs.factory import RuleFactory

    _ensure_builtin_rules_registered()
    for name in sorted(EXPECTED_RULES):
        entry = yaml.safe_load(kontra.describe_rule(name)["example"])[0]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            built = RuleFactory([RuleSpec(name=entry["name"], params=entry["params"])]).build_rules()
        assert built[0].name == name


def test_unknown_rule_lists_available_names():
    with pytest.raises(ValueError, match="Unknown Kontra rule 'mystery'.*not_null"):
        kontra.describe_rule("mystery")


def test_rule_reference_does_not_load_execution_dependencies():
    code = """
import sys
import kontra
kontra.list_rules()
kontra.describe_rule('unique')
heavy = {'polars', 'duckdb', 'psycopg', 'pymssql'} & set(sys.modules)
if heavy:
    raise SystemExit(','.join(sorted(heavy)))
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
