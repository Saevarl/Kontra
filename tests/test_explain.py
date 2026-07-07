# tests/test_explain.py
"""
Tests for execution plan preview (explain mode).

Verifies that explain mode correctly identifies which tier each rule
will execute on without running validation.
"""
import pytest
import polars as pl

import kontra
from kontra import rules
from kontra.api.results import ExplainResult, RuleExplainEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    """DataFrame for explain tests."""
    return pl.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com", "i@j.com"],
        "status": ["active", "inactive", "active", "pending", "active"],
    })


@pytest.fixture
def sample_parquet(tmp_path, sample_df):
    """Write sample data to Parquet for file-based tests."""
    path = tmp_path / "data.parquet"
    sample_df.write_parquet(str(path))
    return str(path)


@pytest.fixture
def multi_rule_contract(tmp_path, sample_parquet):
    """Contract with rules spanning multiple tiers."""
    contract = tmp_path / "contract.yml"
    contract.write_text(f"""\
name: test_contract
datasource: {sample_parquet}
rules:
  - name: not_null
    params: {{ column: user_id }}
  - name: unique
    params: {{ column: user_id }}
  - name: not_null
    params: {{ column: email }}
  - name: min_rows
    params: {{ threshold: 1 }}
  - name: regex
    params: {{ column: email, pattern: ".*@.*" }}
""")
    return str(contract)


# ---------------------------------------------------------------------------
# Tests: Python API
# ---------------------------------------------------------------------------

class TestExplainAPI:
    """Tests for explain via the Python API."""

    def test_explain_returns_explain_result(self, sample_df, multi_rule_contract):
        """explain=True should return ExplainResult, not ValidationResult."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        assert isinstance(result, ExplainResult)

    def test_explain_has_correct_rule_count(self, sample_df, multi_rule_contract):
        """ExplainResult should have the correct number of rules."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        assert result.total_rules == 5

    def test_explain_all_polars_for_dataframe(self, sample_df, multi_rule_contract):
        """DataFrame input: all rules should be polars tier."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        for entry in result.rules:
            assert entry.tier == "polars", f"{entry.rule_id} expected polars, got {entry.tier}"

    def test_explain_parquet_has_metadata_tier(self, sample_parquet, multi_rule_contract):
        """Parquet file: not_null should be metadata tier (proven by row-group stats)."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract, explain=True,
        )
        tier_map = {e.rule_id: e.tier for e in result.rules}
        # not_null rules should be metadata (Parquet stats)
        assert tier_map["COL:user_id:not_null"] == "metadata"
        assert tier_map["COL:email:not_null"] == "metadata"
        # min_rows is dataset-level — no preplan predicate, falls to SQL pushdown
        assert tier_map["DATASET:min_rows"] == "sql"

    def test_explain_parquet_has_sql_tier(self, sample_parquet, multi_rule_contract):
        """Parquet file: unique should be sql tier (DuckDB)."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract, explain=True,
        )
        tier_map = {e.rule_id: e.tier for e in result.rules}
        assert tier_map["COL:user_id:unique"] == "sql"

    def test_explain_parquet_regex_is_sql_or_polars(self, sample_parquet, multi_rule_contract):
        """Parquet file: regex goes to sql (DuckDB pushdown) or polars."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract, explain=True,
        )
        tier_map = {e.rule_id: e.tier for e in result.rules}
        assert tier_map["COL:email:regex"] in ("sql", "polars")

    def test_explain_summary(self, sample_parquet, multi_rule_contract):
        """Summary should contain correct tier counts."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract, explain=True,
        )
        assert result.summary["metadata"] >= 1
        assert result.summary["sql"] >= 1
        # All rules should be accounted for
        total = sum(result.summary.values())
        assert total == result.total_rules

    def test_explain_preplan_off(self, sample_parquet, multi_rule_contract):
        """With preplan=off, no rules should be metadata tier."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract,
            explain=True, preplan="off",
        )
        for entry in result.rules:
            assert entry.tier != "metadata", f"{entry.rule_id} should not be metadata with preplan=off"

    def test_explain_pushdown_off(self, sample_parquet, multi_rule_contract):
        """With pushdown=off, no rules should be sql tier."""
        result = kontra.validate(
            sample_parquet, multi_rule_contract,
            explain=True, pushdown="off",
        )
        for entry in result.rules:
            assert entry.tier != "sql", f"{entry.rule_id} should not be sql with pushdown=off"

    def test_explain_function(self, sample_parquet, multi_rule_contract):
        """kontra.explain() should work as shorthand for validate(explain=True)."""
        result = kontra.explain(sample_parquet, multi_rule_contract)
        assert isinstance(result, ExplainResult)
        assert result.total_rules == 5

    def test_explain_with_only_filter(self, sample_df, multi_rule_contract):
        """explain respects the `only` filter."""
        result = kontra.validate(
            sample_df, multi_rule_contract,
            explain=True, only=["not_null"],
        )
        assert result.total_rules == 2
        for entry in result.rules:
            assert entry.name == "not_null"


# ---------------------------------------------------------------------------
# Tests: ExplainResult serialization
# ---------------------------------------------------------------------------

class TestExplainSerialization:
    """Tests for ExplainResult serialization methods."""

    def test_to_dict(self, sample_df, multi_rule_contract):
        """to_dict should produce a valid dict."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        d = result.to_dict()
        assert d["total_rules"] == 5
        assert "rules" in d
        assert "summary" in d
        assert all("rule_id" in r for r in d["rules"])
        assert all("tier" in r for r in d["rules"])

    def test_to_llm(self, sample_df, multi_rule_contract):
        """to_llm should produce a compact string."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        llm = result.to_llm()
        assert "EXPLAIN:" in llm
        assert "test_contract" in llm
        assert "TIERS:" in llm

    def test_render(self, sample_df, multi_rule_contract):
        """render should produce a human-readable table."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        rendered = result.render()
        assert "Execution Plan:" in rendered
        assert "Summary:" in rendered
        assert "polars" in rendered

    def test_to_json(self, sample_df, multi_rule_contract):
        """to_json should produce valid JSON."""
        import json
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        j = result.to_json()
        parsed = json.loads(j)
        assert parsed["total_rules"] == 5

    def test_rule_entry_has_column(self, sample_df, multi_rule_contract):
        """RuleExplainEntry should include column for column-level rules."""
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        for entry in result.rules:
            if entry.rule_id.startswith("COL:"):
                assert entry.column is not None, f"{entry.rule_id} missing column"
            else:
                # Dataset-level rules don't have columns
                pass


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------

class TestExplainEdgeCases:
    """Edge cases for explain mode."""

    def test_explain_with_inline_rules(self, sample_df):
        """Explain works with inline rules (no contract file)."""
        result = kontra.validate(
            sample_df,
            rules=[
                rules.not_null("email"),
                rules.unique("user_id"),
            ],
            explain=True,
        )
        assert isinstance(result, ExplainResult)
        assert result.total_rules == 2

    def test_explain_does_not_execute(self, sample_df, multi_rule_contract):
        """Explain mode should not modify data or save state."""
        # Just verify it returns without error and is an ExplainResult
        result = kontra.validate(
            sample_df, multi_rule_contract, explain=True,
        )
        assert isinstance(result, ExplainResult)
        # ExplainResult should NOT have validation result attributes
        assert not hasattr(result, "passed") or not isinstance(getattr(result, "passed", None), bool)

    def test_explain_empty_rules(self, sample_df, tmp_path):
        """Explain with no rules should return empty result."""
        contract = tmp_path / "empty.yml"
        contract.write_text("""\
name: empty
datasource: inline
rules: []
""")
        result = kontra.validate(
            sample_df, str(contract), explain=True,
        )
        assert isinstance(result, ExplainResult)
        assert result.total_rules == 0
