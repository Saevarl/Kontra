"""
Tests for ValidationResult.sample_failures() method.

This file tests the sample_failures() feature which returns sample rows
that failed a specific validation rule.
"""

import pytest
import polars as pl
import tempfile
import os

import kontra
from kontra.api import rules


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def df_with_nulls():
    """DataFrame with null values in 'email' column."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["a@b.com", None, "c@d.com", None, "e@f.com"],
        "status": ["active", "active", "inactive", "active", "active"],
    })


@pytest.fixture
def df_with_duplicates():
    """DataFrame with duplicate values in 'code' column."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "code": ["A", "B", "A", "C", "B"],  # A and B are duplicates
    })


@pytest.fixture
def df_with_invalid_values():
    """DataFrame with values outside allowed set."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "status": ["active", "pending", "active", "deleted", "active"],
    })


@pytest.fixture
def df_with_range_violations():
    """DataFrame with values outside range."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "age": [25, -5, 150, 30, 200],  # -5, 150, 200 are out of range [0, 120]
    })


@pytest.fixture
def parquet_file(df_with_nulls):
    """Temporary parquet file."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name
        df_with_nulls.write_parquet(path)
    yield path
    os.unlink(path)


@pytest.fixture
def csv_file(df_with_nulls):
    """Temporary CSV file."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
        df_with_nulls.write_csv(path)
    yield path
    os.unlink(path)


# =============================================================================
# Basic Tests - Each Rule Type
# =============================================================================


class TestSampleFailuresNotNull:
    """Tests for not_null rule samples."""

    def test_returns_failing_rows(self, df_with_nulls):
        """not_null rule returns rows with null values."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])

        assert not result.passed
        samples = result.sample_failures("COL:email:not_null")

        assert len(samples) == 2
        assert all("row_index" in s for s in samples)
        assert all(s["email"] is None for s in samples)

    def test_row_indices_are_accurate(self, df_with_nulls):
        """Row indices match actual positions in DataFrame."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        # Rows 1 and 3 have nulls (0-indexed)
        indices = [s["row_index"] for s in samples]
        assert sorted(indices) == [1, 3]


class TestSampleFailuresUnique:
    """Tests for unique rule samples."""

    def test_returns_duplicate_rows(self, df_with_duplicates):
        """unique rule returns rows with duplicate values."""
        result = kontra.validate(df_with_duplicates, rules=[rules.unique("code")])

        assert not result.passed
        samples = result.sample_failures("COL:code:unique")

        # A appears at indices 0, 2; B appears at indices 1, 4
        # All 4 rows are duplicates
        assert len(samples) == 4
        duplicate_codes = [s["code"] for s in samples]
        assert set(duplicate_codes) == {"A", "B"}

    def test_includes_duplicate_count(self):
        """unique rule includes _duplicate_count column."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5, 6],
            "user_id": [100, 100, 100, 200, 200, 300],  # 100 appears 3x, 200 appears 2x
        })

        result = kontra.validate(df, rules=[rules.unique("user_id")])
        samples = result.sample_failures("COL:user_id:unique", n=10)

        # Check _duplicate_count is present
        assert all("_duplicate_count" in s for s in samples)

        # Check counts are correct
        counts_by_user = {s["user_id"]: s["_duplicate_count"] for s in samples}
        assert counts_by_user[100] == 3
        assert counts_by_user[200] == 2

    def test_sorted_by_worst_offenders(self):
        """unique rule sorts by duplicate count descending."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "user_id": [100, 100, 200, 200, 200, 200, 300, 300],  # 200=4, 100=2, 300=2
        })

        result = kontra.validate(df, rules=[rules.unique("user_id")])
        samples = result.sample_failures("COL:user_id:unique", n=5)

        # First samples should be from user_id=200 (4 duplicates)
        assert samples[0]["user_id"] == 200
        assert samples[0]["_duplicate_count"] == 4

    def test_to_llm_shows_duplicate_count(self):
        """to_llm() shows duplicate count as 'dupes='."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "user_id": [100, 100, 100],
        })

        result = kontra.validate(df, rules=[rules.unique("user_id")])
        samples = result.sample_failures("COL:user_id:unique")

        llm_str = samples.to_llm()
        assert "dupes=3" in llm_str


class TestSampleFailuresAllowedValues:
    """Tests for allowed_values rule samples."""

    def test_returns_invalid_value_rows(self, df_with_invalid_values):
        """allowed_values rule returns rows with values not in allowed set."""
        result = kontra.validate(
            df_with_invalid_values,
            rules=[rules.allowed_values("status", ["active", "inactive"])],
        )

        assert not result.passed
        samples = result.sample_failures("COL:status:allowed_values")

        # "pending" at index 1, "deleted" at index 3
        assert len(samples) == 2
        invalid_statuses = [s["status"] for s in samples]
        assert set(invalid_statuses) == {"pending", "deleted"}


class TestSampleFailuresRange:
    """Tests for range rule samples."""

    def test_returns_out_of_range_rows(self, df_with_range_violations):
        """range rule returns rows with values outside bounds."""
        result = kontra.validate(
            df_with_range_violations,
            rules=[rules.range("age", min=0, max=120)],
        )

        assert not result.passed
        samples = result.sample_failures("COL:age:range")

        # -5 at index 1, 150 at index 2, 200 at index 4
        assert len(samples) == 3
        out_of_range = [s["age"] for s in samples]
        assert set(out_of_range) == {-5, 150, 200}


class TestSampleFailuresRegex:
    """Tests for regex rule samples."""

    def test_returns_non_matching_rows(self):
        """regex rule returns rows that don't match pattern."""
        df = pl.DataFrame({
            "email": ["test@example.com", "invalid", "user@domain.org", "bad"],
        })

        result = kontra.validate(
            df, rules=[rules.regex("email", pattern=r".+@.+\..+")]
        )

        assert not result.passed
        samples = result.sample_failures("COL:email:regex")

        # "invalid" at index 1, "bad" at index 3
        assert len(samples) == 2
        bad_emails = [s["email"] for s in samples]
        assert set(bad_emails) == {"invalid", "bad"}


class TestSampleFailuresCompare:
    """Tests for compare rule samples."""

    def test_returns_comparison_failures(self):
        """compare rule returns rows where comparison fails."""
        df = pl.DataFrame({
            "start_date": ["2024-01-01", "2024-03-01", "2024-02-01"],
            "end_date": ["2024-02-01", "2024-02-01", "2024-03-01"],  # Row 1 fails
        })

        result = kontra.validate(
            df,
            rules=[{"name": "compare", "params": {
                "left": "end_date", "op": ">=", "right": "start_date"
            }}],
        )

        assert not result.passed
        samples = result.sample_failures("DATASET:compare")

        # Row 1: end_date (2024-02-01) < start_date (2024-03-01)
        assert len(samples) == 1
        assert samples[0]["row_index"] == 1


class TestSampleFailuresConditionalNotNull:
    """Tests for conditional_not_null rule samples."""

    def test_returns_conditional_failures(self):
        """conditional_not_null returns rows matching condition with nulls."""
        df = pl.DataFrame({
            "status": ["active", "active", "inactive", "active"],
            "email": ["a@b.com", None, None, "d@e.com"],
        })

        result = kontra.validate(
            df,
            rules=[{
                "name": "conditional_not_null",
                "params": {
                    "column": "email",
                    "when": "status == 'active'"
                }
            }],
        )

        assert not result.passed
        samples = result.sample_failures("COL:email:conditional_not_null")

        # Row 1: status=active but email=null
        # Row 2: status=inactive so doesn't count
        assert len(samples) == 1
        assert samples[0]["row_index"] == 1
        assert samples[0]["status"] == "active"
        assert samples[0]["email"] is None


# =============================================================================
# Limit and Edge Case Tests
# =============================================================================


class TestSampleFailuresLimits:
    """Tests for n parameter and limits."""

    def test_respects_n_parameter(self):
        """Returns at most n samples."""
        df = pl.DataFrame({
            "value": [None] * 50,  # 50 nulls
        })

        result = kontra.validate(df, rules=[rules.not_null("value")])
        samples = result.sample_failures("COL:value:not_null", n=5)

        assert len(samples) == 5

    def test_caps_at_100(self):
        """Maximum samples is 100 even if n is higher."""
        df = pl.DataFrame({
            "value": [None] * 200,  # 200 nulls
        })

        result = kontra.validate(df, rules=[rules.not_null("value")])
        samples = result.sample_failures("COL:value:not_null", n=500)

        assert len(samples) == 100

    def test_returns_all_if_fewer_than_n(self):
        """Returns all failures if fewer than n exist."""
        df = pl.DataFrame({
            "value": [1, None, 3],  # 1 null
        })

        result = kontra.validate(df, rules=[rules.not_null("value")])
        samples = result.sample_failures("COL:value:not_null", n=10)

        assert len(samples) == 1


class TestSampleFailuresPassingRule:
    """Tests for passing rules."""

    def test_returns_empty_for_passing_rule(self, df_with_nulls):
        """Returns empty FailureSamples for rules that passed."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("id")])

        assert result.passed
        samples = result.sample_failures("COL:id:not_null")

        assert len(samples) == 0
        assert not samples  # Also test bool conversion
        assert samples.to_dict() == []


class TestSampleFailuresErrors:
    """Tests for error cases."""

    def test_raises_for_unknown_rule_id(self, df_with_nulls):
        """Raises ValueError for unknown rule ID."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])

        with pytest.raises(ValueError, match="Rule not found"):
            result.sample_failures("nonexistent_rule")

    def test_raises_for_dataset_level_rules(self):
        """Raises ValueError for dataset-level rules that can't have row samples."""
        df = pl.DataFrame({"id": [1, 2, 3]})

        result = kontra.validate(
            df, rules=[{"name": "min_rows", "params": {"threshold": 100}}]
        )

        with pytest.raises(ValueError, match="does not support row-level samples"):
            result.sample_failures("DATASET:min_rows")

    def test_raises_for_dtype_rule(self):
        """Raises ValueError for dtype rule (schema-level)."""
        df = pl.DataFrame({"value": ["a", "b", "c"]})  # String column

        result = kontra.validate(
            df, rules=[{"name": "dtype", "params": {"column": "value", "type": "int64"}}]
        )

        # dtype rule doesn't have compile_predicate, should fail
        with pytest.raises(ValueError, match="does not support row-level samples"):
            result.sample_failures("COL:value:dtype")


# =============================================================================
# Data Source Tests
# =============================================================================


class TestSampleFailuresDataSources:
    """Tests for different data source types."""

    def test_works_with_parquet_file(self, parquet_file):
        """sample_failures works with parquet file path."""
        result = kontra.validate(parquet_file, rules=[rules.not_null("email")])

        assert not result.passed
        samples = result.sample_failures("COL:email:not_null")

        assert len(samples) == 2
        assert all(s["email"] is None for s in samples)

    def test_works_with_csv_file(self, csv_file):
        """sample_failures works with CSV file path."""
        result = kontra.validate(csv_file, rules=[rules.not_null("email")])

        assert not result.passed
        samples = result.sample_failures("COL:email:not_null")

        assert len(samples) == 2

    def test_works_with_dataframe(self, df_with_nulls):
        """sample_failures works with Polars DataFrame."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])

        assert not result.passed
        samples = result.sample_failures("COL:email:not_null")

        assert len(samples) == 2

    def test_works_with_list_of_dicts(self):
        """sample_failures works with list[dict] input."""
        data = [
            {"id": 1, "email": "a@b.com"},
            {"id": 2, "email": None},
            {"id": 3, "email": "c@d.com"},
        ]

        result = kontra.validate(data, rules=[rules.not_null("email")])

        assert not result.passed
        samples = result.sample_failures("COL:email:not_null")

        assert len(samples) == 1
        assert samples[0]["row_index"] == 1


# =============================================================================
# Row Index Accuracy Tests
# =============================================================================


class TestRowIndexAccuracy:
    """Tests that row_index accurately reflects source data position."""

    def test_indices_match_original_positions(self):
        """Row indices match original DataFrame positions."""
        df = pl.DataFrame({
            "id": list(range(100)),
            "value": [None if i % 17 == 0 else i for i in range(100)],
        })

        result = kontra.validate(df, rules=[rules.not_null("value")])
        samples = result.sample_failures("COL:value:not_null", n=100)

        indices = [s["row_index"] for s in samples]

        # Verify each index is a multiple of 17
        for idx in indices:
            assert idx % 17 == 0, f"Index {idx} should be multiple of 17"

    def test_can_locate_row_by_index(self, df_with_nulls):
        """Can use row_index to locate row in original data."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        for sample in samples:
            idx = sample["row_index"]
            original_row = df_with_nulls.row(idx, named=True)

            # Verify sample matches original
            assert sample["id"] == original_row["id"]
            assert sample["email"] == original_row["email"]
            assert sample["status"] == original_row["status"]


# =============================================================================
# Multiple Rules Tests
# =============================================================================


class TestFailureSamplesMethods:
    """Tests for FailureSamples serialization methods."""

    def test_to_dict(self, df_with_nulls):
        """to_dict() returns list of dicts."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        as_dict = samples.to_dict()
        assert isinstance(as_dict, list)
        assert len(as_dict) == 2
        assert all(isinstance(row, dict) for row in as_dict)

    def test_to_json(self, df_with_nulls):
        """to_json() returns valid JSON string."""
        import json

        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        json_str = samples.to_json()
        parsed = json.loads(json_str)
        assert len(parsed) == 2

    def test_to_llm(self, df_with_nulls):
        """to_llm() returns token-optimized string."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        llm_str = samples.to_llm()
        assert "SAMPLES:" in llm_str
        assert "COL:email:not_null" in llm_str
        assert "2 rows" in llm_str
        assert "row=" in llm_str  # New format: row=1 instead of row_index=1

    def test_to_llm_empty(self, df_with_nulls):
        """to_llm() handles empty samples."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("id")])
        samples = result.sample_failures("COL:id:not_null")

        llm_str = samples.to_llm()
        assert "0 rows" in llm_str

    def test_iterable(self, df_with_nulls):
        """FailureSamples is iterable."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        # Can iterate
        count = 0
        for row in samples:
            assert "row_index" in row
            count += 1
        assert count == 2

    def test_indexable(self, df_with_nulls):
        """FailureSamples supports indexing."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        first = samples[0]
        assert "row_index" in first
        assert isinstance(first, dict)

    def test_count_property(self, df_with_nulls):
        """FailureSamples has count property."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        assert samples.count == 2
        assert samples.count == len(samples)

    def test_rule_id_property(self, df_with_nulls):
        """FailureSamples has rule_id property."""
        result = kontra.validate(df_with_nulls, rules=[rules.not_null("email")])
        samples = result.sample_failures("COL:email:not_null")

        assert samples.rule_id == "COL:email:not_null"


# =============================================================================
# Performance Tests
# =============================================================================


@pytest.mark.slow
class TestSampleFailuresPerformance:
    """Performance tests for sample_failures."""

    def test_perf_1m_rows_few_failures(self):
        """Large dataset with few failures should still be fast."""
        import time

        # 1M rows, 10 nulls (at multiples of 100k)
        df = pl.DataFrame({
            "id": list(range(1_000_000)),
            "value": [None if i % 100_000 == 0 else i for i in range(1_000_000)],
        })

        result = kontra.validate(df, rules=[rules.not_null("value")])

        start = time.time()
        samples = result.sample_failures("COL:value:not_null", n=5)
        elapsed = time.time() - start

        assert len(samples) == 5
        assert elapsed < 2.0, f"sample_failures took {elapsed:.2f}s, expected < 2s"

    def test_perf_repeated_calls(self):
        """Multiple calls to sample_failures should be reasonable."""
        import time

        df = pl.DataFrame({
            "a": [None if i % 100 == 0 else i for i in range(10_000)],
            "b": [None if i % 200 == 0 else i for i in range(10_000)],
        })

        result = kontra.validate(
            df,
            rules=[rules.not_null("a"), rules.not_null("b")],
        )

        start = time.time()
        for _ in range(10):
            result.sample_failures("COL:a:not_null", n=5)
            result.sample_failures("COL:b:not_null", n=5)
        elapsed = time.time() - start

        # 20 calls should complete in reasonable time
        assert elapsed < 5.0, f"20 sample_failures calls took {elapsed:.2f}s"


class TestMultipleRules:
    """Tests for results with multiple rules."""

    def test_samples_for_each_failing_rule(self):
        """Can get samples for each failing rule independently."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": ["a@b.com", None, "c@d.com", None, "e@f.com"],
            "status": ["active", "pending", "active", "deleted", "active"],
        })

        result = kontra.validate(
            df,
            rules=[
                rules.not_null("email"),
                rules.allowed_values("status", ["active", "inactive"]),
            ],
        )

        # Get samples for each rule
        email_samples = result.sample_failures("COL:email:not_null")
        status_samples = result.sample_failures("COL:status:allowed_values")

        assert len(email_samples) == 2  # Rows 1, 3
        assert len(status_samples) == 2  # Rows 1, 3 (pending, deleted)

        # Verify correct failures
        assert all(s["email"] is None for s in email_samples)
        assert set(s["status"] for s in status_samples) == {"pending", "deleted"}

    def test_samples_may_overlap(self):
        """Same row can appear in samples for multiple rules."""
        df = pl.DataFrame({
            "email": [None],  # Fails not_null
            "status": ["invalid"],  # Fails allowed_values
        })

        result = kontra.validate(
            df,
            rules=[
                rules.not_null("email"),
                rules.allowed_values("status", ["active"]),
            ],
        )

        email_samples = result.sample_failures("COL:email:not_null")
        status_samples = result.sample_failures("COL:status:allowed_values")

        # Same row (index 0) appears in both
        assert email_samples[0]["row_index"] == 0
        assert status_samples[0]["row_index"] == 0


# =============================================================================
# Eager Sampling Tests
# =============================================================================


class TestEagerSampling:
    """Tests for eager sampling (samples embedded in RuleResult)."""

    def test_samples_populated_during_validation(self):
        """Samples are populated during validate() call when sample > 0."""
        df = pl.DataFrame({"email": [None, "a@b.com", None]})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=5)

        r = result.rules[0]
        assert r.samples is not None
        assert len(r.samples) == 2
        assert r.samples_source == "polars"

    def test_passing_rule_has_empty_samples(self):
        """Passing rules have samples=[] and reason=rule_passed."""
        df = pl.DataFrame({"name": ["a", "b", "c"]})
        result = kontra.validate(df, rules=[rules.not_null("name")], sample=5)

        r = result.rules[0]
        assert r.passed
        assert r.samples == []
        assert r.samples_reason == "rule_passed"

    def test_sample_parameter_limits_per_rule(self):
        """sample parameter limits samples per rule."""
        df = pl.DataFrame({"email": [None] * 10})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=3, tally=True)

        r = result.rules[0]
        assert len(r.samples) == 3
        assert r.samples_truncated is True
        assert r.samples_reason == "per_rule_limit"

    def test_sample_budget_limits_total_samples(self):
        """sample_budget limits total samples across rules."""
        df = pl.DataFrame({
            "a": [None] * 10,  # 10 failures
            "b": [None] * 10,  # 10 failures
        })
        result = kontra.validate(
            df,
            rules=[rules.not_null("a"), rules.not_null("b")],
            sample=5,
            sample_budget=6,
        )

        # Worst offender should get samples (both have same count, so order matters)
        total_samples = sum(len(r.samples) for r in result.rules if r.samples)
        assert total_samples <= 6

    def test_budget_exhausted_rules_have_truncated_flag(self):
        """Rules that don't get samples due to budget have samples=None."""
        df = pl.DataFrame({
            "a": [None if i < 10 else 1 for i in range(12)],  # 10 failures - worst
            "b": [None if i < 5 else 1 for i in range(12)],   # 5 failures
            "c": [None if i < 2 else 1 for i in range(12)],   # 2 failures - may be cut off
        })
        result = kontra.validate(
            df,
            rules=[
                rules.not_null("a"),
                rules.not_null("b"),
                rules.not_null("c"),
            ],
            sample=3,
            sample_budget=4,  # Only enough for ~1 rule
        )

        # Sort by failed_count to find worst offenders
        sorted_rules = sorted(result.rules, key=lambda r: r.failed_count, reverse=True)

        # First rule(s) should have samples
        has_samples = [r for r in sorted_rules if r.samples]
        assert len(has_samples) >= 1

        # Some rule(s) should be budget exhausted
        budget_exhausted = [r for r in sorted_rules if r.samples_reason == "budget_exhausted"]
        assert len(budget_exhausted) >= 1

    def test_sample_zero_disables_eager_sampling(self):
        """sample=0 disables eager sampling."""
        df = pl.DataFrame({"email": [None]})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=0)

        r = result.rules[0]
        assert r.samples is None
        # sample_failures() should still work (lazy path)
        samples = result.sample_failures("COL:email:not_null")
        assert len(samples) == 1

    def test_unsupported_rule_has_unavailable_samples(self):
        """Dataset-level rules have samples=None with reason."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = kontra.validate(df, rules=[{"name": "min_rows", "params": {"threshold": 10}}], sample=5)

        r = result.rules[0]
        assert not r.passed
        assert r.samples is None
        assert r.samples_reason == "rule_unsupported"

    def test_cached_samples_returned_from_sample_failures(self):
        """sample_failures() returns cached samples instead of re-querying."""
        df = pl.DataFrame({"email": [None] * 5})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=5)

        # First call uses cache
        samples = result.sample_failures("COL:email:not_null", n=3)
        assert len(samples) == 3

        # Verify it's the same data as cached
        cached = result.rules[0].samples
        assert samples[0]["row_index"] == cached[0]["row_index"]

    def test_unique_rule_has_duplicate_count(self):
        """Unique rule samples include _duplicate_count."""
        df = pl.DataFrame({
            "user_id": [1, 1, 1, 2, 2, 3],  # 1 has 3, 2 has 2
            "name": list("ABCDEF"),
        })
        result = kontra.validate(df, rules=[rules.unique("user_id")], sample=5)

        r = result.rules[0]
        assert r.samples is not None
        assert all("_duplicate_count" in s for s in r.samples)

        # Worst offenders first
        counts = [s["_duplicate_count"] for s in r.samples]
        assert counts == sorted(counts, reverse=True)

    def test_to_dict_includes_samples(self):
        """to_dict() includes sample data."""
        df = pl.DataFrame({"email": [None, "a@b.com"]})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=5)

        d = result.to_dict()
        assert "samples" in d["rules"][0]
        assert d["rules"][0]["samples"] is not None

    def test_rule_result_to_llm_includes_samples(self):
        """RuleResult.to_llm() includes sample data."""
        df = pl.DataFrame({"email": [None, "a@b.com"]})
        result = kontra.validate(df, rules=[rules.not_null("email")], sample=5)

        llm = result.rules[0].to_llm()
        assert "Samples" in llm
        assert "row=" in llm


class TestEagerSamplingWithParquet:
    """Tests for eager sampling with Parquet files (preplan tier)."""

    @pytest.fixture
    def parquet_with_nulls(self, tmp_path):
        """Create a parquet file with nulls."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": ["a@b.com", None, "c@d.com", None, "e@f.com"],
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        return str(path)

    def test_parquet_preplan_still_samples(self, parquet_with_nulls):
        """Parquet files with preplan still get samples (file can be read)."""
        result = kontra.validate(parquet_with_nulls, rules=[rules.not_null("email")], sample=5)

        r = result.rules[0]
        # Even if preplan was used, we can still sample from file
        assert r.samples is not None
        assert len(r.samples) == 2

    def test_parquet_samples_source_indicates_upgrade(self, parquet_with_nulls):
        """samples_source reflects how samples were obtained."""
        result = kontra.validate(parquet_with_nulls, rules=[rules.not_null("email")], sample=5)

        r = result.rules[0]
        # If preplan was used for measurement but polars for sampling
        if r.source == "metadata":
            assert r.samples_source == "polars"


# =============================================================================
# Column Projection Tests
# =============================================================================


class TestSampleColumnProjection:
    """Tests for sample_columns parameter (token efficiency)."""

    @pytest.fixture
    def df_many_columns(self):
        """DataFrame with many columns for projection testing."""
        return pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": ["a@x.com", None, "c@x.com", None, "e@x.com"],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "status": ["active", "active", "inactive", "active", "inactive"],
            "score": [85, 90, 75, 60, 95],
            "extra1": [1, 2, 3, 4, 5],
            "extra2": ["a", "b", "c", "d", "e"],
        })

    def test_default_includes_all_columns(self, df_many_columns):
        """Default behavior includes all columns in samples."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        # Should have all columns plus row_index
        assert "row_index" in r.samples[0]
        assert "email" in r.samples[0]
        assert "name" in r.samples[0]
        assert "extra1" in r.samples[0]

    def test_specific_columns_list(self, df_many_columns):
        """sample_columns list restricts output columns."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["id", "email"],
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        # Should only have id, email, and row_index
        sample_keys = set(r.samples[0].keys())
        assert sample_keys == {"row_index", "id", "email"}

    def test_relevant_mode_not_null(self, df_many_columns):
        """'relevant' mode includes only rule's columns."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns="relevant",
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        sample_keys = set(r.samples[0].keys())
        # Should only have email and row_index
        assert sample_keys == {"row_index", "email"}

    def test_relevant_mode_compare_rule(self):
        """'relevant' mode includes both columns for compare rule."""
        df = pl.DataFrame({
            "start_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "end_date": ["2024-01-15", "2024-01-15", "2024-03-15"],
            "name": ["A", "B", "C"],
            "extra": [1, 2, 3],
        })
        result = kontra.validate(
            df,
            rules=[rules.compare("end_date", "start_date", ">=")],
            sample=5,
            sample_columns="relevant",
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        sample_keys = set(r.samples[0].keys())
        # Should have start_date, end_date, and row_index
        assert sample_keys == {"row_index", "start_date", "end_date"}

    def test_relevant_mode_conditional_not_null(self, df_many_columns):
        """'relevant' mode includes condition column and target column."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.conditional_not_null("email", "status == 'active'")],
            sample=5,
            sample_columns="relevant",
            save=False
        )
        r = result.rules[0]
        if r.samples:  # May or may not have failures
            sample_keys = set(r.samples[0].keys())
            assert "email" in sample_keys
            assert "status" in sample_keys
            assert "row_index" in sample_keys

    def test_row_index_always_included(self, df_many_columns):
        """row_index is always included even when not in column list."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["email"],  # Doesn't include row_index
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        assert "row_index" in r.samples[0]

    def test_nonexistent_columns_ignored(self, df_many_columns):
        """Columns not in DataFrame are silently ignored."""
        result = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["id", "nonexistent_column"],
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        # Should only have id and row_index (nonexistent is ignored)
        sample_keys = set(r.samples[0].keys())
        assert "nonexistent_column" not in sample_keys
        assert "id" in sample_keys

    def test_unique_rule_includes_duplicate_count(self):
        """Unique rule samples always include _duplicate_count."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "code": ["A", "B", "A", "C", "B"],
            "name": ["a", "b", "c", "d", "e"],
        })
        result = kontra.validate(
            df,
            rules=[rules.unique("code")],
            sample=5,
            sample_columns=["code"],
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        # Should include _duplicate_count even with column projection
        sample_keys = set(r.samples[0].keys())
        assert "_duplicate_count" in sample_keys
        assert "code" in sample_keys
        assert "row_index" in sample_keys

    def test_token_efficiency(self, df_many_columns):
        """Column projection reduces JSON output size."""
        import json

        result_full = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            save=False
        )
        result_proj = kontra.validate(
            df_many_columns,
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["id"],
            save=False
        )

        full_size = len(json.dumps(result_full.to_dict()))
        proj_size = len(json.dumps(result_proj.to_dict()))

        # Projected should be smaller
        assert proj_size < full_size

    def test_parquet_with_column_projection(self, tmp_path):
        """Column projection works with Parquet files."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "email": [None, "b@x.com", None],
            "name": ["A", "B", "C"],
            "extra": [1, 2, 3],
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        result = kontra.validate(
            str(path),
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["id", "email"],
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        sample_keys = set(r.samples[0].keys())
        assert sample_keys == {"row_index", "id", "email"}

    def test_csv_with_column_projection(self, tmp_path):
        """Column projection works with CSV files."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "email": [None, "b@x.com", None],
            "name": ["A", "B", "C"],
            "extra": [1, 2, 3],
        })
        path = tmp_path / "test.csv"
        df.write_csv(path)

        result = kontra.validate(
            str(path),
            rules=[rules.not_null("email")],
            sample=5,
            sample_columns=["id", "email"],
            save=False
        )
        r = result.rules[0]
        assert r.samples is not None
        sample_keys = set(r.samples[0].keys())
        assert sample_keys == {"row_index", "id", "email"}
