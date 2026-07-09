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


class TestCompareSourceAgnostic:
    """compare() accepts any Kontra source on either side, mixed freely."""

    def _before_after(self, tmp_path):
        before = pl.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]})
        after = pl.DataFrame({"order_id": [1, 2, 3, 5], "amount": [10, 20, 30, 99]})
        pq = str(tmp_path / "before.parquet")
        csv = str(tmp_path / "after.csv")
        before.write_parquet(pq)
        after.write_csv(csv)
        return before, after, pq, csv

    def test_file_vs_dataframe(self, tmp_path):
        before, after, pq, _ = self._before_after(tmp_path)
        r = compare(pq, after, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_dataframe_vs_file(self, tmp_path):
        before, after, _, csv = self._before_after(tmp_path)
        r = compare(before, csv, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_file_vs_file(self, tmp_path):
        _, _, pq, csv = self._before_after(tmp_path)
        r = compare(pq, csv, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_pandas_input(self, tmp_path):
        pd = pytest.importorskip("pandas")
        _, _, pq, _ = self._before_after(tmp_path)
        after_pd = pd.DataFrame({"order_id": [1, 2, 3, 5], "amount": [10, 20, 30, 99]})
        r = compare(pq, after_pd, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_list_of_dicts_input(self):
        before = pl.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]})
        after = [{"order_id": i, "amount": a} for i, a in [(1, 10), (2, 20), (3, 30), (5, 99)]]
        r = compare(before, after, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_named_datasource(self, tmp_path, monkeypatch):
        before = pl.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]})
        after = pl.DataFrame({"order_id": [1, 2, 3, 5], "amount": [10, 20, 30, 99]})
        before.write_parquet(str(tmp_path / "before.parquet"))
        kdir = tmp_path / ".kontra"
        kdir.mkdir()
        (kdir / "config.yml").write_text(
            f'version: "1"\n'
            f"datasources:\n"
            f"  warehouse:\n"
            f"    type: files\n"
            f"    base_path: {tmp_path}\n"
            f"    tables:\n"
            f"      before: before.parquet\n"
        )
        monkeypatch.chdir(tmp_path)
        r = compare("warehouse.before", after, key="order_id")
        assert (r.preserved, r.dropped, r.added) == (3, 1, 1)

    def test_database_uri_no_longer_rejected(self):
        """A database URI routes to the materializer (reaches a real connection
        attempt) instead of the old 'not supported for probes' rejection."""
        after = pl.DataFrame({"order_id": [1], "amount": [10]})
        with pytest.raises(Exception) as exc:
            compare("postgres://u:p@127.0.0.1:1/db/public.t", after, key="order_id")
        msg = str(exc.value).lower()
        assert "not supported for probes" not in msg
        assert "not registered" not in msg

    def test_connection_requires_table(self):
        """A live DB connection object without a table= is a clear error."""
        class FakeConn:  # looks like a DBAPI connection
            def cursor(self):
                raise AssertionError("should fail before use")

        after = pl.DataFrame({"order_id": [1], "amount": [10]})
        with pytest.raises(ValueError, match="table"):
            compare(FakeConn(), after, key="order_id")


class TestCompareAsymmetricKeys:
    """W1: compare() with differently-named before/after keys."""

    def test_different_named_keys(self):
        """before_key/after_key align rows across differently-named columns."""
        # orders.customer_id -> customers.id (FK -> PK shape)
        orders = pl.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "amount": [10, 20, 30, 40],
        })
        customers = pl.DataFrame({
            "id": [2, 3, 4, 5],
            "amount": [20, 30, 30, 50],  # id=4 amount changed 40->30
        })

        result = compare(orders, customers, before_key="customer_id", after_key="id")

        assert isinstance(result, CompareResult)
        # Canonical key name comes from the before side
        assert result.key == ["customer_id"]
        # Overlap {2,3,4}; dropped {1}; added {5}
        assert result.preserved == 3
        assert result.dropped == 1
        assert result.added == 1
        assert result.samples_dropped_keys == [1]
        # amount changed for customer/id 4 only
        assert result.changed_rows == 1
        assert result.unchanged_rows == 2
        assert "amount" in result.columns_modified

    def test_symmetric_path_still_works(self):
        """Regression: same-named key= path unchanged."""
        before = pl.DataFrame({"id": [1, 2, 3], "v": [1, 2, 3]})
        after = pl.DataFrame({"id": [2, 3, 4], "v": [2, 3, 4]})
        result = compare(before, after, key="id")
        assert result.preserved == 2
        assert result.dropped == 1
        assert result.added == 1

    def test_composite_asymmetric_keys(self):
        """Composite keys pair positionally: before_key[i] <-> after_key[i]."""
        before = pl.DataFrame({
            "cust": [1, 1, 2],
            "day": ["a", "b", "a"],
            "amt": [10, 11, 20],
        })
        after = pl.DataFrame({
            "c": [1, 2, 3],
            "d": ["a", "a", "z"],
            "amt": [10, 20, 30],
        })
        result = compare(
            before, after,
            before_key=["cust", "day"], after_key=["c", "d"],
        )
        assert result.key == ["cust", "day"]
        # before keys: (1,a),(1,b),(2,a); after: (1,a),(2,a),(3,z)
        # preserved: (1,a),(2,a) -> 2; dropped: (1,b) -> 1; added: (3,z) -> 1
        assert result.preserved == 2
        assert result.dropped == 1
        assert result.added == 1

    def test_both_symmetric_and_asymmetric_raises(self):
        df = pl.DataFrame({"id": [1], "x": [1]})
        with pytest.raises(ValueError, match="not both"):
            compare(df, df, key="id", before_key="id", after_key="id")

    def test_mismatched_arity_raises(self):
        df = pl.DataFrame({"a": [1], "b": [1]})
        with pytest.raises(ValueError, match="same number"):
            compare(df, df, before_key=["a", "b"], after_key=["a"])

    def test_only_one_side_raises(self):
        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="required"):
            compare(df, df, before_key="id")

    def test_no_key_raises(self):
        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="required"):
            compare(df, df)

    def test_missing_after_key_column_raises(self):
        before = pl.DataFrame({"customer_id": [1], "x": [1]})
        after = pl.DataFrame({"id": [1], "x": [1]})
        with pytest.raises(ValueError, match="not found in after"):
            compare(before, after, before_key="customer_id", after_key="nope")

    def test_asymmetric_source_agnostic(self, tmp_path):
        """Different-named keys work with a file source too."""
        orders = pl.DataFrame({"customer_id": [1, 2, 3], "amt": [1, 2, 3]})
        customers = pl.DataFrame({"id": [2, 3, 4], "amt": [2, 3, 4]})
        p = str(tmp_path / "orders.parquet")
        orders.write_parquet(p)
        result = compare(p, customers, before_key="customer_id", after_key="id")
        assert (result.preserved, result.dropped, result.added) == (2, 1, 1)
