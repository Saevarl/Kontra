"""
Tier Equivalence Tests

These tests verify that all execution tiers (metadata preplan, SQL pushdown, Polars)
produce semantically equivalent results for the same rule and data.

This is a critical invariant: the tier only affects HOW a measurement is obtained,
not WHAT is measured.
"""

import pytest
import polars as pl
from pathlib import Path
from datetime import datetime


pytestmark = [pytest.mark.integration]


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def tier_test_data(tmp_path) -> str:
    """
    Dataset with known, predictable violations for tier equivalence testing.
    Total rows: 103

    Violations:
    - 5 NULL emails (rows 0-4)
    - 3 duplicate user_ids (rows 100-102 duplicate rows 0-2)
    - 4 out-of-range ages (rows 5-8 have age > 150)
    - 6 invalid statuses (rows 10-15 have status='invalid')
    - 2 regex failures (rows 20-21 have malformed emails)
    """
    n_rows = 103

    # user_id: 0-99 unique, then 0,1,2 duplicated = 3 duplicates
    user_ids = list(range(100)) + [0, 1, 2]

    # email: 5 nulls, 2 bad emails, rest valid
    emails = (
        [None] * 5 +  # rows 0-4: 5 nulls
        ["valid@test.com"] * 15 +  # rows 5-19
        ["not-an-email", "also-bad"] +  # rows 20-21: 2 regex failures
        ["user@domain.com"] * 81  # rows 22-102
    )

    # age: 4 out of range (>150)
    ages = (
        [25] * 5 +  # rows 0-4
        [200, 175, 160, 155] +  # rows 5-8: 4 out of range
        [30] * 94  # rows 9-102
    )

    # status: 6 invalid
    statuses = (
        ["active"] * 10 +  # rows 0-9
        ["invalid"] * 6 +  # rows 10-15: 6 invalid
        ["active", "inactive", "pending"] * 29 +  # rows 16-102 (87 rows)
        []  # total = 10 + 6 + 87 = 103
    )

    # balance: 3 negative
    balances = (
        [100.5] * 50 +  # rows 0-49
        [-50.25, -100.0, -0.01] +  # rows 50-52: 3 negative
        [200.0] * 50  # rows 53-102
    )

    df = pl.DataFrame({
        "user_id": user_ids,
        "email": emails,
        "age": ages,
        "status": statuses,
        "balance": balances,
        "updated_at": [datetime(2024, 1, 15, 12, 0, 0)] * n_rows,
    })

    path = tmp_path / "tier_test.parquet"
    df.write_parquet(str(path))
    return str(path)


@pytest.fixture
def edge_case_data(tmp_path) -> str:
    """
    Edge cases that might differ between tiers:
    - Empty strings vs NULLs
    - Float precision
    - Unicode strings
    - Boundary values
    """
    df = pl.DataFrame({
        "id": list(range(20)),
        "nullable_str": [None, "", "  ", "valid", None] * 4,  # Mix of NULL and empty
        "float_col": [
            0.0, -0.0, 1e-10, 1e10,
            float('inf'), float('-inf'),  # Edge floats
            0.1 + 0.2,  # Classic float precision (0.30000000000000004)
            0.3,
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0
        ],
        "unicode_str": ["hello", "héllo", "日本語", "emoji🎉", ""] * 4,
        "boundary_int": [0, -1, 1, 2147483647, -2147483648] * 4,  # int32 boundaries
    })

    path = tmp_path / "edge_cases.parquet"
    df.write_parquet(str(path))
    return str(path)


# =============================================================================
# Helper Functions
# =============================================================================


def get_violation_count(result: dict, rule_id: str) -> int:
    """Extract violation count for a specific rule from engine result."""
    for r in result.get("results", []):
        if r.get("rule_id") == rule_id:
            return r.get("failed_count", 0)
    raise ValueError(f"Rule {rule_id} not found in results")


def get_rule_result(result: dict, rule_id: str) -> dict:
    """Extract full result dict for a specific rule."""
    for r in result.get("results", []):
        if r.get("rule_id") == rule_id:
            return r
    raise ValueError(f"Rule {rule_id} not found in results")


def run_with_tier(
    contract_path: str,
    tier: str,  # "preplan", "sql", "polars"
) -> dict:
    """Run engine forcing a specific execution tier."""
    from kontra.engine.engine import ValidationEngine

    if tier == "preplan":
        # Metadata only - no SQL, no Polars scan
        eng = ValidationEngine(
            contract_path=contract_path,
            preplan="on",
            pushdown="off",
            emit_report=False,
            stats_mode="summary",
            tally=True,  # Need exact counts for tier equivalence tests
        )
    elif tier == "sql":
        # SQL pushdown - skip preplan
        eng = ValidationEngine(
            contract_path=contract_path,
            preplan="off",
            pushdown="on",
            emit_report=False,
            stats_mode="summary",
            tally=True,  # Need exact counts for tier equivalence tests
        )
    elif tier == "polars":
        # Pure Polars - no preplan, no SQL
        eng = ValidationEngine(
            contract_path=contract_path,
            preplan="off",
            pushdown="off",
            emit_report=False,
            stats_mode="summary",
            tally=True,  # Need exact counts for tier equivalence tests
        )
    else:
        raise ValueError(f"Unknown tier: {tier}")

    return eng.run()


# =============================================================================
# not_null: Preplan vs SQL vs Polars
# =============================================================================


class TestNotNullEquivalence:
    """not_null should report identical NULL counts across all tiers."""

    RULE = {"name": "not_null", "params": {"column": "email"}}
    RULE_ID = "COL:email:not_null"
    EXPECTED_VIOLATIONS = 5  # 5 NULL emails in tier_test_data

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should agree on pass/fail for not_null.

        Note: SQL uses EXISTS for not_null (fast, early termination).
        EXISTS returns failed_count=1 for any failure, not exact counts.
        This is by design - "validate validates" (fast pass/fail).
        Use Scout for exact counts when needed.
        """
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_rule = get_rule_result(sql_result, self.RULE_ID)
        polars_rule = get_rule_result(polars_result, self.RULE_ID)

        # Pass/fail must agree
        sql_passed = sql_rule.get("passed", True)
        polars_passed = polars_rule.get("passed", True)
        assert sql_passed == polars_passed, (
            f"not_null pass/fail mismatch: SQL={sql_passed}, Polars={polars_passed}"
        )

        # Both should detect violations
        polars_count = polars_rule.get("failed_count", 0)
        assert polars_count == self.EXPECTED_VIOLATIONS, (
            f"Polars not_null count mismatch: got={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )

        # SQL returns at least 1 when failing (EXISTS semantics)
        sql_count = sql_rule.get("failed_count", 0)
        if not sql_passed:
            assert sql_count >= 1, f"SQL should report >=1 violation, got {sql_count}"

    def test_preplan_vs_polars_pass_fail_equivalence(self, tier_test_data, write_contract):
        """Preplan and Polars should agree on pass/fail outcome.

        Note: Preplan can only prove "pass" (0 violations) or "fail" (≥1 violation).
        It reports failed_count=1 for any failure, not exact counts.
        """
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        preplan_result = run_with_tier(cpath, "preplan")
        polars_result = run_with_tier(cpath, "polars")

        preplan_rule = get_rule_result(preplan_result, self.RULE_ID)
        polars_rule = get_rule_result(polars_result, self.RULE_ID)

        # If preplan resolved it, pass/fail must agree
        if preplan_rule.get("execution_source") == "metadata":
            preplan_passed = preplan_rule.get("passed", True)
            polars_passed = polars_rule.get("passed", True)
            assert preplan_passed == polars_passed, (
                f"not_null pass/fail mismatch: preplan={preplan_passed}, polars={polars_passed}"
            )

            # Preplan returns 1 for any failure (at least one violation)
            # This is expected behavior - preplan can't count exact violations
            if not preplan_passed:
                assert preplan_rule.get("failed_count") >= 1
                assert polars_rule.get("failed_count") >= 1


# =============================================================================
# unique: Polars only for DuckDB (PostgreSQL supports SQL pushdown)
# =============================================================================


class TestUniqueEquivalence:
    """unique consistency tests.

    Note: DuckDB does NOT support unique pushdown - it always falls back to Polars.
    PostgreSQL DOES support unique via COUNT(*) - COUNT(DISTINCT col).

    ⚠️ SEMANTIC MISMATCH WARNING:
    - PostgreSQL SQL: COUNT(*) - COUNT(DISTINCT col) = "extra rows" = 3
    - Polars: is_duplicated().sum() = "all rows in duplicates" = 6

    These are DIFFERENT semantics for the same data. Currently this only matters
    for PostgreSQL since DuckDB doesn't support unique pushdown.

    TODO: Resolve this semantic difference - either:
    Both Polars and SQL now use consistent semantics:
    COUNT(*) - COUNT(DISTINCT col) = "extra rows beyond unique"

    For user_ids [0-99, 0, 1, 2]: 103 total - 100 distinct = 3 extra rows
    """

    # Use tally=True to get exact counts from SQL (default tally=False uses EXISTS)
    RULE = {"name": "unique", "params": {"column": "user_id"}, "tally": True}
    RULE_ID = "COL:user_id:unique"
    # SQL semantics: COUNT(*) - COUNT(DISTINCT col)
    # 103 total rows - 100 distinct values = 3 extra rows
    EXPECTED_VIOLATIONS = 3

    def test_polars_consistency(self, tier_test_data, write_contract):
        """SQL pushdown and Polars should report same duplicate count (with tally=True)."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        # Both should be identical with SQL semantics (requires tally=True)
        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"unique mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )


# =============================================================================
# range: Preplan vs SQL vs Polars
# =============================================================================


class TestRangeEquivalence:
    """range should report identical out-of-bounds counts across all tiers."""

    # Use tally=True to get exact counts from SQL (default tally=False uses EXISTS)
    RULE = {"name": "range", "params": {"column": "age", "min": 0, "max": 150}, "tally": True}
    RULE_ID = "COL:age:range"
    EXPECTED_VIOLATIONS = 4  # 4 ages > 150 in tier_test_data

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should report same out-of-range count (with tally=True)."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"range mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )

    def test_preplan_vs_polars(self, tier_test_data, write_contract):
        """Preplan and Polars should agree on range violations."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        preplan_result = run_with_tier(cpath, "preplan")
        polars_result = run_with_tier(cpath, "polars")

        preplan_rule = get_rule_result(preplan_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        # Preplan can prove pass (if all row groups in range) or fail (if any out)
        # but may return unknown if it can't determine from metadata
        if preplan_rule.get("execution_source") == "metadata":
            preplan_count = preplan_rule.get("failed_count", 0)
            # Note: preplan might report 0 (pass) or >0 (fail detected from metadata)
            # If it reports violations, it should match or be conservative
            if preplan_count > 0:
                assert polars_count >= preplan_count, (
                    f"range preplan overcounted: preplan={preplan_count}, polars={polars_count}"
                )


# =============================================================================
# allowed_values: SQL vs Polars
# =============================================================================


class TestAllowedValuesEquivalence:
    """allowed_values should report identical invalid value counts."""

    # Use tally=True to get exact counts from SQL (default tally=False uses EXISTS)
    RULE = {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive", "pending"]}, "tally": True}
    RULE_ID = "COL:status:allowed_values"
    EXPECTED_VIOLATIONS = 6  # 6 'invalid' statuses in tier_test_data

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should report same invalid value count (with tally=True)."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"allowed_values mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )


# =============================================================================
# min_rows / max_rows: Preplan vs SQL vs Polars
# =============================================================================


class TestMinRowsEquivalence:
    """min_rows should report identical shortfall across all tiers."""

    # tier_test_data has 103 rows
    RULE = {"name": "min_rows", "params": {"threshold": 200}}
    RULE_ID = "DATASET:min_rows"
    EXPECTED_VIOLATIONS = 97  # 200 - 103 = 97 rows short

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should report same row shortfall."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"min_rows mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )

    def test_preplan_vs_polars(self, tier_test_data, write_contract):
        """Preplan and Polars should report same row shortfall."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        preplan_result = run_with_tier(cpath, "preplan")
        polars_result = run_with_tier(cpath, "polars")

        preplan_rule = get_rule_result(preplan_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        if preplan_rule.get("execution_source") == "metadata":
            preplan_count = preplan_rule.get("failed_count", 0)
            assert preplan_count == polars_count, (
                f"min_rows preplan mismatch: preplan={preplan_count}, polars={polars_count}"
            )


class TestMaxRowsEquivalence:
    """max_rows should report identical excess across all tiers."""

    # tier_test_data has 103 rows
    RULE = {"name": "max_rows", "params": {"threshold": 50}}
    RULE_ID = "DATASET:max_rows"
    EXPECTED_VIOLATIONS = 53  # 103 - 50 = 53 rows excess

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should report same row excess."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"max_rows mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )


# =============================================================================
# regex: SQL vs Polars
# =============================================================================


class TestRegexEquivalence:
    """regex should report identical pattern match failures."""

    # Simple pattern that should work across SQL dialects
    # Use tally=True to get exact counts from SQL (default tally=False uses EXISTS)
    RULE = {"name": "regex", "params": {"column": "email", "pattern": ".*@.*"}, "tally": True}
    RULE_ID = "COL:email:regex"
    # 5 NULLs + 2 malformed = 7 failures (NULLs fail regex)
    EXPECTED_VIOLATIONS = 7

    def test_sql_vs_polars(self, tier_test_data, write_contract):
        """SQL and Polars should report same regex failure count (with tally=True)."""
        cpath = write_contract(dataset=tier_test_data, rules=[self.RULE])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, self.RULE_ID)
        polars_count = get_violation_count(polars_result, self.RULE_ID)

        assert sql_count == polars_count == self.EXPECTED_VIOLATIONS, (
            f"regex mismatch: SQL={sql_count}, Polars={polars_count}, expected={self.EXPECTED_VIOLATIONS}"
        )


# =============================================================================
# Edge Cases
# =============================================================================


class TestNullVsEmptyString:
    """Verify NULL and empty string are handled consistently across tiers."""

    def test_not_null_distinguishes_null_from_empty(self, edge_case_data, write_contract):
        """not_null should count NULLs, not empty strings.

        Note: SQL uses EXISTS for not_null, returning failed_count=1 for any failure.
        Polars returns exact counts. We verify pass/fail equivalence.
        """
        rule = {"name": "not_null", "params": {"column": "nullable_str"}}
        cpath = write_contract(dataset=edge_case_data, rules=[rule])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_rule = get_rule_result(sql_result, "COL:nullable_str:not_null")
        polars_rule = get_rule_result(polars_result, "COL:nullable_str:not_null")

        # Pass/fail must agree
        sql_passed = sql_rule.get("passed", True)
        polars_passed = polars_rule.get("passed", True)
        assert sql_passed == polars_passed, (
            f"NULL vs empty pass/fail mismatch: SQL={sql_passed}, Polars={polars_passed}"
        )

        # Polars counts actual NULLs (8 in edge_case_data), not empty strings
        polars_count = polars_rule.get("failed_count", 0)
        assert polars_count == 8, f"Polars should count 8 NULLs, got {polars_count}"


class TestFloatPrecision:
    """Verify float comparisons are consistent across tiers."""

    def test_range_with_float_boundaries(self, edge_case_data, write_contract):
        """Range checks on floats should be consistent (with tally=True)."""
        # Use tally=True to get exact counts from SQL (default uses EXISTS)
        rule = {"name": "range", "params": {"column": "float_col", "min": 0.0, "max": 100.0}, "tally": True}
        cpath = write_contract(dataset=edge_case_data, rules=[rule])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, "COL:float_col:range")
        polars_count = get_violation_count(polars_result, "COL:float_col:range")

        assert sql_count == polars_count, (
            f"Float range mismatch: SQL={sql_count}, Polars={polars_count}"
        )


class TestIntegerBoundaries:
    """Verify integer boundary values are handled consistently."""

    def test_range_with_int_boundaries(self, edge_case_data, write_contract):
        """Range checks at integer boundaries should be consistent (with tally=True)."""
        # Use tally=True to get exact counts from SQL (default uses EXISTS)
        rule = {"name": "range", "params": {"column": "boundary_int", "min": -100, "max": 100}, "tally": True}
        cpath = write_contract(dataset=edge_case_data, rules=[rule])

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        sql_count = get_violation_count(sql_result, "COL:boundary_int:range")
        polars_count = get_violation_count(polars_result, "COL:boundary_int:range")

        assert sql_count == polars_count, (
            f"Int boundary mismatch: SQL={sql_count}, Polars={polars_count}"
        )


# =============================================================================
# Multi-rule consistency
# =============================================================================


class TestMultiRuleEquivalence:
    """Verify multiple rules execute consistently across tier combinations."""

    # Use tally=True on all rules to get exact counts for comparison
    # not_null is kept at default (tally=False) to test EXISTS behavior separately
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},  # Uses EXISTS by default
        {"name": "unique", "params": {"column": "user_id"}, "tally": True},
        {"name": "range", "params": {"column": "age", "min": 0, "max": 150}, "tally": True},
        {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive", "pending"]}, "tally": True},
    ]

    def test_all_rules_consistent(self, tier_test_data, write_contract):
        """All rules should report consistent violations regardless of tier mix.

        Note: SQL uses EXISTS for not_null (returns failed_count=1 for any failure).
        Other rules use tally=True to get exact counts. We check:
        - not_null: pass/fail equivalence
        - Other rules: exact count equivalence (with tally=True)
        """
        cpath = write_contract(dataset=tier_test_data, rules=self.RULES)

        # Run with different tier configurations
        auto_result = run_with_tier(cpath, "sql")  # Let engine decide
        polars_result = run_with_tier(cpath, "polars")  # Force all Polars

        for rule in self.RULES:
            col = rule["params"].get("column", "")
            name = rule["name"]
            rule_id = f"COL:{col}:{name}" if col else f"DATASET:{name}"

            auto_rule = get_rule_result(auto_result, rule_id)
            polars_rule = get_rule_result(polars_result, rule_id)

            auto_passed = auto_rule.get("passed", True)
            polars_passed = polars_rule.get("passed", True)

            # Pass/fail must always agree
            assert auto_passed == polars_passed, (
                f"{rule_id} pass/fail mismatch: auto={auto_passed}, polars={polars_passed}"
            )

            if name == "not_null":
                # SQL uses EXISTS for not_null - returns 1 for any failure
                # Only verify pass/fail equivalence (already done above)
                pass
            else:
                # Other rules should have exact count equivalence
                auto_count = auto_rule.get("failed_count", 0)
                polars_count = polars_rule.get("failed_count", 0)
                assert auto_count == polars_count, (
                    f"{rule_id} count mismatch: auto={auto_count}, polars={polars_count}"
                )


# =============================================================================
# Zero violations consistency
# =============================================================================


class TestZeroViolationsEquivalence:
    """Verify that passing rules (0 violations) are consistent across tiers."""

    @pytest.fixture
    def clean_data(self, tmp_path) -> str:
        """Dataset that should pass all rules."""
        df = pl.DataFrame({
            "id": list(range(100)),
            "email": [f"user{i}@test.com" for i in range(100)],
            "status": ["active"] * 50 + ["inactive"] * 50,
            "age": [25] * 100,
        })
        path = tmp_path / "clean.parquet"
        df.write_parquet(str(path))
        return str(path)

    def test_passing_rules_all_tiers(self, clean_data, write_contract):
        """Rules that pass should report 0 violations across all tiers."""
        rules = [
            {"name": "not_null", "params": {"column": "email"}},
            {"name": "unique", "params": {"column": "id"}},
            {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive"]}},
            {"name": "range", "params": {"column": "age", "min": 0, "max": 150}},
        ]
        cpath = write_contract(dataset=clean_data, rules=rules)

        sql_result = run_with_tier(cpath, "sql")
        polars_result = run_with_tier(cpath, "polars")

        # All rules should pass (0 violations) in both tiers
        for r in sql_result["results"]:
            rule_id = r["rule_id"]
            sql_count = r["failed_count"]
            polars_r = get_rule_result(polars_result, rule_id)
            polars_count = polars_r["failed_count"]

            assert sql_count == 0 and polars_count == 0, (
                f"{rule_id} should pass but got: SQL={sql_count}, Polars={polars_count}"
            )
