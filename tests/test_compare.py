# tests/test_compare.py
"""Tests for the compare probe."""

import json
import pytest
import polars as pl

import kontra
from kontra import compare, CompareResult


class TestCompareBasic:
    """Basic compare functionality tests."""

    def test_compare_identical_datasets(self):
        """Compare identical datasets should show no changes."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        result = compare(df, df, key="id")

        assert isinstance(result, CompareResult)
        assert result.before_rows == 3
        assert result.after_rows == 3
        assert result.row_delta == 0
        assert result.row_ratio == 1.0
        assert result.unique_before == 3
        assert result.unique_after == 3
        assert result.preserved == 3
        assert result.dropped == 0
        assert result.added == 0
        assert result.duplicated_after == 0
        assert result.unchanged_rows == 3
        assert result.changed_rows == 0
        assert result.columns_added == []
        assert result.columns_removed == []
        assert result.columns_modified == []

    def test_compare_with_changes(self):
        """Compare datasets with value changes."""
        before = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [100, 200, 300],
        })
        after = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [100, 250, 300],  # Row 2 changed
        })

        result = compare(before, after, key="id")

        assert result.before_rows == 3
        assert result.after_rows == 3
        assert result.preserved == 3
        assert result.changed_rows == 1
        assert result.unchanged_rows == 2
        assert "value" in result.columns_modified
        assert result.modified_fraction["value"] == pytest.approx(1/3)

    def test_compare_row_explosion(self):
        """Detect row explosion from JOIN (classic 1:N issue)."""
        before = pl.DataFrame({
            "order_id": [1, 2, 3],
            "amount": [100, 200, 300],
        })
        # After JOIN, order_id 2 appears multiple times
        after = pl.DataFrame({
            "order_id": [1, 2, 2, 2, 3],
            "amount": [100, 200, 200, 200, 300],
        })

        result = compare(before, after, key="order_id")

        assert result.before_rows == 3
        assert result.after_rows == 5
        assert result.row_delta == 2
        assert result.row_ratio == pytest.approx(5/3)
        assert result.unique_before == 3
        assert result.unique_after == 3  # Still 3 unique keys
        assert result.preserved == 3
        assert result.duplicated_after == 1  # order_id 2 is duplicated

    def test_compare_row_loss(self):
        """Detect row loss from filter/dedup."""
        before = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "status": ["active", "active", "inactive", "active", "inactive"],
        })
        # After filtering out inactive
        after = pl.DataFrame({
            "id": [1, 2, 4],
            "status": ["active", "active", "active"],
        })

        result = compare(before, after, key="id")

        assert result.before_rows == 5
        assert result.after_rows == 3
        assert result.row_delta == -2
        assert result.unique_before == 5
        assert result.unique_after == 3
        assert result.preserved == 3
        assert result.dropped == 2
        assert result.added == 0

    def test_compare_key_duplication(self):
        """Detect duplicated keys after transformation."""
        before = pl.DataFrame({
            "order_id": ["A", "B", "C"],
            "amount": [100, 200, 300],
        })
        after = pl.DataFrame({
            "order_id": ["A", "A", "B", "C", "C", "C"],
            "amount": [100, 100, 200, 300, 300, 300],
        })

        result = compare(before, after, key="order_id")

        assert result.duplicated_after == 2  # A and C are duplicated
        assert len(result.samples_duplicated_keys) <= result.sample_limit

    def test_compare_column_changes(self):
        """Detect added/removed/modified columns."""
        before = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        })
        after = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bobby"],  # Modified
            "status": ["active", "active"],  # Added
            # age removed
        })

        result = compare(before, after, key="id")

        assert "status" in result.columns_added
        assert "age" in result.columns_removed
        assert "name" in result.columns_modified

    def test_compare_nullability_delta(self):
        """Track nullability changes per column."""
        before = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com"],
        })
        after = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "email": ["a@b.com", None, None, "g@h.com"],  # 2 nulls introduced
        })

        result = compare(before, after, key="id")

        assert "email" in result.columns_modified
        assert "email" in result.nullability_delta
        assert result.nullability_delta["email"]["before"] == 0.0
        assert result.nullability_delta["email"]["after"] == 0.5


class TestCompareSamples:
    """Tests for sample collection."""

    def test_samples_bounded(self):
        """Samples respect limit."""
        before = pl.DataFrame({
            "id": list(range(100)),
            "value": [1] * 100,
        })
        after = pl.DataFrame({
            "id": list(range(50, 150)),  # 50 dropped, 50 added
            "value": [2] * 100,  # All changed
        })

        result = compare(before, after, key="id", sample_limit=3)

        assert len(result.samples_dropped_keys) <= 3

    def test_samples_duplicated_keys(self):
        """Sample duplicated keys."""
        before = pl.DataFrame({
            "id": ["A", "B", "C"],
            "value": [1, 2, 3],
        })
        after = pl.DataFrame({
            "id": ["A", "A", "A", "B", "C"],
            "value": [1, 1, 1, 2, 3],
        })

        result = compare(before, after, key="id")

        assert result.duplicated_after == 1  # Only A is duplicated
        assert "A" in result.samples_duplicated_keys

    def test_samples_changed_rows(self):
        """Sample changed rows with before/after values."""
        before = pl.DataFrame({
            "id": [1, 2],
            "value": [100, 200],
        })
        after = pl.DataFrame({
            "id": [1, 2],
            "value": [100, 999],
        })

        result = compare(before, after, key="id")

        assert len(result.samples_changed_rows) == 1
        sample = result.samples_changed_rows[0]
        assert sample["key"] == 2
        assert sample["before"]["value"] == 200
        assert sample["after"]["value"] == 999


class TestCompareCompositeKey:
    """Tests with multi-column keys."""

    def test_composite_key(self):
        """Works with multi-column keys."""
        before = pl.DataFrame({
            "customer_id": [1, 1, 2],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
            "amount": [100, 200, 300],
        })
        after = pl.DataFrame({
            "customer_id": [1, 1, 2],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01"],
            "amount": [100, 250, 300],
        })

        result = compare(before, after, key=["customer_id", "date"])

        assert result.key == ["customer_id", "date"]
        assert result.unique_before == 3
        assert result.preserved == 3
        assert result.changed_rows == 1

    def test_composite_key_samples(self):
        """Composite key samples are dicts."""
        before = pl.DataFrame({
            "a": [1, 2],
            "b": ["x", "y"],
            "value": [100, 200],
        })
        after = pl.DataFrame({
            "a": [1],
            "b": ["x"],
            "value": [100],
        })

        result = compare(before, after, key=["a", "b"])

        assert result.dropped == 1
        # Composite key samples should be dicts
        if result.samples_dropped_keys:
            assert isinstance(result.samples_dropped_keys[0], dict)


class TestCompareOutput:
    """Tests for output methods."""

    def test_to_llm(self):
        """to_llm() produces human-readable text format."""
        before = pl.DataFrame({"id": [1, 2], "value": [100, 200]})
        after = pl.DataFrame({"id": [1, 2], "value": [100, 250]})

        result = compare(before, after, key="id")
        llm_output = result.to_llm()

        # Should be human-readable text (not JSON)
        assert isinstance(llm_output, str)
        assert "COMPARE:" in llm_output
        assert "key:" in llm_output
        assert "keys:" in llm_output
        assert "preserved=" in llm_output
        # For JSON output, use to_json() instead
        assert llm_output.strip() == result.to_llm().strip()  # Consistent output

    def test_to_dict_schema(self):
        """to_dict() matches MVP schema."""
        before = pl.DataFrame({"id": [1], "value": [100]})
        after = pl.DataFrame({"id": [1], "value": [200]})

        result = compare(before, after, key="id")
        d = result.to_dict()

        # Check top-level structure
        assert set(d.keys()) == {"meta", "row_stats", "key_stats", "change_stats", "column_stats", "samples"}

        # Check meta
        assert d["meta"]["key"] == ["id"]
        assert d["meta"]["execution_tier"] == "polars"

        # Check row_stats
        assert "delta" in d["row_stats"]
        assert "ratio" in d["row_stats"]

        # Check key_stats
        assert all(k in d["key_stats"] for k in [
            "unique_before", "unique_after", "preserved", "dropped", "added", "duplicated_after"
        ])

    def test_repr(self):
        """__repr__ is informative."""
        before = pl.DataFrame({"id": [1, 2, 3], "value": [1, 2, 3]})
        after = pl.DataFrame({"id": [1, 2], "value": [1, 2]})

        result = compare(before, after, key="id")
        repr_str = repr(result)

        assert "CompareResult" in repr_str
        assert "3" in repr_str  # before rows
        assert "2" in repr_str  # after rows


class TestCompareEdgeCases:
    """Edge case tests."""

    def test_empty_before(self):
        """Handle empty before dataset."""
        before = pl.DataFrame({"id": [], "value": []}).cast({"id": pl.Int64, "value": pl.Int64})
        after = pl.DataFrame({"id": [1, 2], "value": [100, 200]})

        result = compare(before, after, key="id")

        assert result.before_rows == 0
        assert result.after_rows == 2
        assert result.row_ratio == float('inf')
        assert result.added == 2

    def test_empty_after(self):
        """Handle empty after dataset."""
        before = pl.DataFrame({"id": [1, 2], "value": [100, 200]})
        after = pl.DataFrame({"id": [], "value": []}).cast({"id": pl.Int64, "value": pl.Int64})

        result = compare(before, after, key="id")

        assert result.before_rows == 2
        assert result.after_rows == 0
        assert result.dropped == 2

    def test_null_values_in_data(self):
        """Handle NULL values in non-key columns."""
        before = pl.DataFrame({
            "id": [1, 2],
            "value": [100, None],
        })
        after = pl.DataFrame({
            "id": [1, 2],
            "value": [None, 200],  # Both rows changed
        })

        result = compare(before, after, key="id")

        assert result.changed_rows == 2  # NULL -> value and value -> NULL

    def test_missing_key_column_before(self):
        """Error on missing key column in before."""
        before = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        after = pl.DataFrame({"id": [1, 2], "b": [3, 4]})

        with pytest.raises(ValueError, match="not found in before"):
            compare(before, after, key="id")

    def test_missing_key_column_after(self):
        """Error on missing key column in after."""
        before = pl.DataFrame({"id": [1, 2], "b": [3, 4]})
        after = pl.DataFrame({"a": [1, 2], "b": [3, 4]})

        with pytest.raises(ValueError, match="not found in after"):
            compare(before, after, key="id")


class TestCompareImport:
    """Test that compare is importable from kontra."""

    def test_import_from_kontra(self):
        """Can import compare from kontra."""
        from kontra import compare, CompareResult
        assert callable(compare)
        assert CompareResult is not None

    def test_import_via_kontra_namespace(self):
        """Can use compare via kontra namespace."""
        df = pl.DataFrame({"id": [1], "value": [100]})
        result = kontra.compare(df, df, key="id")
        assert isinstance(result, kontra.CompareResult)
