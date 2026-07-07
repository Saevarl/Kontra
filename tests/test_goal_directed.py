# tests/test_goal_directed.py
"""
Tests for goal-directed validation (--only / --columns).

Verifies that the `only` and `columns` parameters correctly filter
which rules are executed during validation.
"""
import pytest
import polars as pl

import kontra
from kontra import rules


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    """Small DataFrame with multiple columns for filtering tests."""
    return pl.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "email": ["a@b.com", None, "c@d.com", "e@f.com", None],
        "status": ["active", "inactive", "active", "pending", "active"],
        "age": [25, 30, None, 45, 50],
    })


@pytest.fixture
def sample_contract(tmp_path):
    """Contract with multiple rule types across different columns."""
    contract = tmp_path / "multi_rule.yml"
    contract.write_text("""\
name: multi_rule_contract
datasource: inline
rules:
  - name: not_null
    params: { column: user_id }
  - name: not_null
    params: { column: email }
  - name: unique
    params: { column: user_id }
  - name: allowed_values
    params: { column: status, values: [active, inactive, pending] }
  - name: min_rows
    params: { threshold: 1 }
  - name: not_null
    params: { column: age }
""")
    return str(contract)


# ---------------------------------------------------------------------------
# Tests: `only` parameter
# ---------------------------------------------------------------------------

class TestOnlyFilter:
    """Tests for filtering by rule name or rule ID."""

    def test_only_by_rule_name(self, sample_df, sample_contract):
        """only=['not_null'] should run only the 3 not_null rules."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["not_null"],
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 3
        assert all(r.name == "not_null" for r in result.rules)

    def test_only_by_rule_id(self, sample_df, sample_contract):
        """only=['COL:email:not_null'] should run exactly 1 rule."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["COL:email:not_null"],
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 1
        assert result.rules[0].rule_id == "COL:email:not_null"

    def test_only_multiple_names(self, sample_df, sample_contract):
        """only=['not_null', 'unique'] should run not_null + unique rules."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["not_null", "unique"],
            preplan="off", pushdown="off",
        )
        rule_names = {r.name for r in result.rules}
        assert rule_names == {"not_null", "unique"}
        assert result.total_rules == 4  # 3 not_null + 1 unique

    def test_only_mix_name_and_id(self, sample_df, sample_contract):
        """only can mix rule names and explicit rule IDs."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["COL:email:not_null", "unique"],
            preplan="off", pushdown="off",
        )
        rule_ids = {r.rule_id for r in result.rules}
        assert "COL:email:not_null" in rule_ids
        assert "COL:user_id:unique" in rule_ids
        assert result.total_rules == 2

    def test_only_no_matches(self, sample_df, sample_contract):
        """only with nonexistent rule name should return 0 rules."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["nonexistent_rule"],
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 0
        assert result.passed is True  # No rules = pass

    def test_only_with_inline_rules(self, sample_df):
        """only works with inline rules too."""
        result = kontra.validate(
            sample_df,
            rules=[
                rules.not_null("email"),
                rules.unique("user_id"),
                rules.not_null("age"),
            ],
            only=["unique"],
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 1
        assert result.rules[0].name == "unique"


# ---------------------------------------------------------------------------
# Tests: `columns` parameter
# ---------------------------------------------------------------------------

class TestColumnsFilter:
    """Tests for filtering by column name."""

    def test_columns_single(self, sample_df, sample_contract):
        """columns=['email'] should include email rules + dataset rules."""
        result = kontra.validate(
            sample_df, sample_contract,
            columns=["email"],
            preplan="off", pushdown="off",
        )
        rule_ids = {r.rule_id for r in result.rules}
        # Should include COL:email:not_null + DATASET:min_rows (dataset-level always included)
        assert "COL:email:not_null" in rule_ids
        assert "DATASET:min_rows" in rule_ids
        # Should NOT include user_id or status rules
        assert "COL:user_id:not_null" not in rule_ids
        assert "COL:status:allowed_values" not in rule_ids

    def test_columns_multiple(self, sample_df, sample_contract):
        """columns=['email', 'user_id'] includes rules for both columns."""
        result = kontra.validate(
            sample_df, sample_contract,
            columns=["email", "user_id"],
            preplan="off", pushdown="off",
        )
        rule_ids = {r.rule_id for r in result.rules}
        assert "COL:email:not_null" in rule_ids
        assert "COL:user_id:not_null" in rule_ids
        assert "COL:user_id:unique" in rule_ids
        assert "DATASET:min_rows" in rule_ids
        # Should NOT include status or age rules
        assert "COL:status:allowed_values" not in rule_ids
        assert "COL:age:not_null" not in rule_ids

    def test_columns_dataset_rules_always_included(self, sample_df, sample_contract):
        """Dataset-level rules (min_rows) should always be included with columns filter."""
        result = kontra.validate(
            sample_df, sample_contract,
            columns=["age"],
            preplan="off", pushdown="off",
        )
        rule_ids = {r.rule_id for r in result.rules}
        assert "DATASET:min_rows" in rule_ids
        assert "COL:age:not_null" in rule_ids

    def test_columns_no_matches(self, sample_df, sample_contract):
        """columns with non-existent column still includes dataset-level rules."""
        result = kontra.validate(
            sample_df, sample_contract,
            columns=["nonexistent_column"],
            preplan="off", pushdown="off",
        )
        # Only dataset-level rules should survive
        rule_ids = {r.rule_id for r in result.rules}
        assert "DATASET:min_rows" in rule_ids
        assert result.total_rules == 1


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------

class TestGoalDirectedEdgeCases:
    """Edge cases for goal-directed validation."""

    def test_only_takes_priority_over_columns(self, sample_df, sample_contract):
        """When both only and columns are provided, only takes priority."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["unique"],
            columns=["email"],  # This should be ignored when only is set
            preplan="off", pushdown="off",
        )
        # only=["unique"] should win
        assert result.total_rules == 1
        assert result.rules[0].name == "unique"

    def test_validation_results_correct_with_filter(self, sample_df, sample_contract):
        """Filtered validation should produce correct pass/fail results."""
        result = kontra.validate(
            sample_df, sample_contract,
            only=["COL:email:not_null"],
            preplan="off", pushdown="off",
        )
        # email has 2 nulls out of 5
        assert result.total_rules == 1
        assert not result.passed
        assert result.rules[0].failed_count >= 1


# ---------------------------------------------------------------------------
# Tests: Filtered runs must NOT be saved (BUG F-015)
# ---------------------------------------------------------------------------

class TestFilteredRunsNotSaved:
    """
    BUG F-015: validate(..., only=["not_null"], save=True) was saving partial
    results, causing diff() to report 'resolved' issues that were just filtered.
    Fix: skip saving when only or columns filters are active.
    """

    def test_only_filter_skips_save_with_warning(self, sample_df, sample_contract):
        """validate() with only= should warn and skip saving."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = kontra.validate(
                sample_df, sample_contract,
                only=["not_null"],
                save=True,
                preplan="off", pushdown="off",
            )

        # Validation itself should succeed
        assert result.total_rules == 3

        # A warning about skipped save should have been emitted
        save_warnings = [
            w for w in caught
            if "Skipping state save" in str(w.message)
        ]
        assert len(save_warnings) == 1, (
            f"Expected 1 save-skip warning, got {len(save_warnings)}. "
            f"All warnings: {[str(w.message) for w in caught]}"
        )
        assert "only=" in str(save_warnings[0].message)

    def test_columns_filter_skips_save_with_warning(self, sample_df, sample_contract):
        """validate() with columns= should warn and skip saving."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = kontra.validate(
                sample_df, sample_contract,
                columns=["email"],
                save=True,
                preplan="off", pushdown="off",
            )

        # Validation itself should succeed
        assert result.total_rules >= 1

        # A warning about skipped save should have been emitted
        save_warnings = [
            w for w in caught
            if "Skipping state save" in str(w.message)
        ]
        assert len(save_warnings) == 1
        assert "columns=" in str(save_warnings[0].message)

    def test_both_filters_skips_save(self, sample_df, sample_contract):
        """validate() with both only= and columns= should skip saving."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = kontra.validate(
                sample_df, sample_contract,
                only=["unique"],
                columns=["email"],
                save=True,
                preplan="off", pushdown="off",
            )

        save_warnings = [
            w for w in caught
            if "Skipping state save" in str(w.message)
        ]
        assert len(save_warnings) == 1
        assert "only=" in str(save_warnings[0].message)

    def test_no_filter_does_not_warn(self, sample_df, sample_contract):
        """validate() without filters should not emit save-skip warning."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = kontra.validate(
                sample_df, sample_contract,
                save=False,  # Don't actually save (no state dir), just verify no warning
                preplan="off", pushdown="off",
            )

        save_warnings = [
            w for w in caught
            if "Skipping state save" in str(w.message)
        ]
        assert len(save_warnings) == 0

    def test_filtered_engine_does_not_save_to_store(self, sample_df, sample_contract, tmp_path):
        """Engine with only= should not write any state files."""
        from kontra.engine.engine import ValidationEngine
        from kontra.state.backends.local import LocalStore

        state_dir = tmp_path / "state"
        store = LocalStore(base_path=str(state_dir))

        import warnings as _warnings
        with _warnings.catch_warnings(record=True):
            _warnings.simplefilter("always")
            engine = ValidationEngine(
                contract_path=sample_contract,
                dataframe=sample_df,
                emit_report=False,
                save_state=True,
                state_store=store,
                preplan="off",
                pushdown="off",
                only_rules=["not_null"],
            )
            engine.run()

        # State directory should not have been created (no save occurred)
        if state_dir.exists():
            # If directory exists, it should have no run files
            all_files = list(state_dir.rglob("*.json"))
            assert len(all_files) == 0, (
                f"Expected no state files when filter active, found: {all_files}"
            )

    def test_unfiltered_engine_saves_to_store(self, sample_df, sample_contract, tmp_path):
        """Engine without filters should save state normally."""
        from kontra.engine.engine import ValidationEngine
        from kontra.state.backends.local import LocalStore

        state_dir = tmp_path / "state"
        store = LocalStore(base_path=str(state_dir))

        engine = ValidationEngine(
            contract_path=sample_contract,
            dataframe=sample_df,
            emit_report=False,
            save_state=True,
            state_store=store,
            preplan="off",
            pushdown="off",
        )
        engine.run()

        # State should have been saved
        all_files = list(state_dir.rglob("*.json"))
        assert len(all_files) == 1, (
            f"Expected 1 state file for unfiltered run, found: {all_files}"
        )
