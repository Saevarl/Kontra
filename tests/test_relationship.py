# tests/test_relationship.py
"""Tests for the profile_relationship probe."""

import json
import pytest
import polars as pl

import kontra
from kontra import profile_relationship, RelationshipProfile


class TestRelationshipBasic:
    """Basic profile_relationship functionality tests."""

    def test_relationship_one_to_one(self):
        """Profile 1:1 relationship."""
        left = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        right = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@b.com", "c@d.com", "e@f.com"],
        })

        result = profile_relationship(left, right, on="id")

        assert isinstance(result, RelationshipProfile)
        assert result.left_rows == 3
        assert result.right_rows == 3
        assert result.left_unique_keys == 3
        assert result.right_unique_keys == 3
        assert result.left_duplicate_keys == 0
        assert result.right_duplicate_keys == 0
        assert result.left_key_multiplicity_min == 1
        assert result.left_key_multiplicity_max == 1
        assert result.right_key_multiplicity_min == 1
        assert result.right_key_multiplicity_max == 1
        assert result.left_keys_with_match == 3
        assert result.left_keys_without_match == 0

    def test_relationship_one_to_many(self):
        """Profile 1:N relationship (detect multiplicity)."""
        # Orders (left) - each order has one customer
        orders = pl.DataFrame({
            "customer_id": [1, 2, 1, 1, 3],  # Customer 1 has 3 orders
            "order_id": [101, 102, 103, 104, 105],
        })
        # Customers (right) - each customer appears once
        customers = pl.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
        })

        result = profile_relationship(orders, customers, on="customer_id")

        # Left side (orders): customer_id is not unique
        assert result.left_rows == 5
        assert result.left_unique_keys == 3  # 3 distinct customers in orders
        assert result.left_key_multiplicity_max == 3  # Customer 1 appears 3 times

        # Right side (customers): customer_id is unique
        assert result.right_rows == 4
        assert result.right_unique_keys == 4
        assert result.right_key_multiplicity_max == 1

        # Coverage
        assert result.left_keys_with_match == 3  # All order customers exist
        assert result.right_keys_without_match == 1  # Customer 4 has no orders

    def test_relationship_many_to_many(self):
        """Profile M:N relationship."""
        # Both sides have duplicates
        left = pl.DataFrame({
            "tag_id": [1, 1, 2, 2, 2, 3],
        })
        right = pl.DataFrame({
            "tag_id": [1, 1, 2, 4],
        })

        result = profile_relationship(left, right, on="tag_id")

        assert result.left_unique_keys == 3
        assert result.right_unique_keys == 3
        assert result.left_key_multiplicity_max == 3  # tag_id 2 appears 3 times in left
        assert result.right_key_multiplicity_max == 2  # tag_id 1 appears 2 times in right
        assert result.left_duplicate_keys == 2  # tag_ids 1 and 2 are duplicated in left
        assert result.right_duplicate_keys == 1  # tag_id 1 is duplicated in right

    def test_relationship_coverage(self):
        """Correctly count matched/unmatched keys."""
        left = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
        })
        right = pl.DataFrame({
            "id": [3, 4, 5, 6, 7],
        })

        result = profile_relationship(left, right, on="id")

        # Left keys: 1, 2, 3, 4, 5
        # Right keys: 3, 4, 5, 6, 7
        # Common: 3, 4, 5
        assert result.left_keys_with_match == 3
        assert result.left_keys_without_match == 2  # 1, 2
        assert result.right_keys_with_match == 3
        assert result.right_keys_without_match == 2  # 6, 7


class TestRelationshipNulls:
    """Tests for NULL handling in join keys."""

    def test_null_keys(self):
        """Handle nulls in join keys."""
        left = pl.DataFrame({
            "id": [1, 2, None, 3],
        })
        right = pl.DataFrame({
            "id": [1, None, None, 4],
        })

        result = profile_relationship(left, right, on="id")

        # Left: 1 NULL out of 4 rows
        assert result.left_null_rate == 0.25

        # Right: 2 NULLs out of 4 rows
        assert result.right_null_rate == 0.5

        # Unique keys exclude NULLs
        assert result.left_unique_keys == 3  # 1, 2, 3
        assert result.right_unique_keys == 2  # 1, 4


class TestRelationshipSamples:
    """Tests for sample collection."""

    def test_samples_bounded(self):
        """Samples respect limit."""
        left = pl.DataFrame({"id": list(range(100))})
        right = pl.DataFrame({"id": list(range(50, 150))})

        result = profile_relationship(left, right, on="id", sample_limit=3)

        # Left without match: 0-49 (50 keys), should sample 3
        assert len(result.samples_left_unmatched) <= 3

    def test_samples_left_unmatched(self):
        """Sample left unmatched keys."""
        left = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
        right = pl.DataFrame({"id": [1, 2]})

        result = profile_relationship(left, right, on="id")

        assert result.left_keys_without_match == 3
        assert set(result.samples_left_unmatched).issubset({3, 4, 5})

    def test_samples_right_unmatched(self):
        """Sample right unmatched keys."""
        left = pl.DataFrame({"id": [1, 2]})
        right = pl.DataFrame({"id": [1, 2, 3, 4, 5]})

        result = profile_relationship(left, right, on="id")

        assert result.right_keys_without_match == 3
        assert set(result.samples_right_unmatched).issubset({3, 4, 5})

    def test_samples_right_duplicates(self):
        """Sample right duplicate keys."""
        left = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({
            "id": [1, 1, 1, 2, 3],  # id=1 appears 3 times
        })

        result = profile_relationship(left, right, on="id")

        assert result.right_duplicate_keys == 1
        assert 1 in result.samples_right_duplicates


class TestRelationshipCompositeKey:
    """Tests with multi-column keys."""

    def test_composite_key(self):
        """Works with multi-column keys."""
        left = pl.DataFrame({
            "a": [1, 1, 2],
            "b": ["x", "y", "x"],
            "value": [100, 200, 300],
        })
        right = pl.DataFrame({
            "a": [1, 2],
            "b": ["x", "x"],
            "name": ["Alice", "Bob"],
        })

        result = profile_relationship(left, right, on=["a", "b"])

        assert result.on == ["a", "b"]
        assert result.left_unique_keys == 3
        assert result.right_unique_keys == 2
        # Left keys: (1,x), (1,y), (2,x)
        # Right keys: (1,x), (2,x)
        # Common: (1,x), (2,x)
        assert result.left_keys_with_match == 2
        assert result.left_keys_without_match == 1  # (1,y)

    def test_composite_key_samples(self):
        """Composite key samples are dicts."""
        left = pl.DataFrame({
            "a": [1, 2],
            "b": ["x", "y"],
        })
        right = pl.DataFrame({
            "a": [1],
            "b": ["x"],
        })

        result = profile_relationship(left, right, on=["a", "b"])

        assert result.left_keys_without_match == 1
        # Composite key samples should be dicts
        if result.samples_left_unmatched:
            assert isinstance(result.samples_left_unmatched[0], dict)


class TestRelationshipOutput:
    """Tests for output methods."""

    def test_to_llm(self):
        """to_llm() produces human-readable text format."""
        left = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({"id": [1, 2, 4]})

        result = profile_relationship(left, right, on="id")
        llm_output = result.to_llm()

        # Should be human-readable text (not JSON)
        assert isinstance(llm_output, str)
        assert "RELATIONSHIP:" in llm_output
        assert "left:" in llm_output
        assert "right:" in llm_output
        assert "coverage:" in llm_output
        assert "multiplicity:" in llm_output
        # For JSON output, use to_json() instead
        assert llm_output.strip() == result.to_llm().strip()  # Consistent output

    def test_to_dict_schema(self):
        """to_dict() matches MVP schema."""
        left = pl.DataFrame({"id": [1]})
        right = pl.DataFrame({"id": [1]})

        result = profile_relationship(left, right, on="id")
        d = result.to_dict()

        # Check top-level structure
        assert set(d.keys()) == {"meta", "key_stats", "cardinality", "coverage", "samples"}

        # Check meta
        assert d["meta"]["on"] == ["id"]
        assert d["meta"]["execution_tier"] == "polars"

        # Check key_stats nested structure
        assert "left" in d["key_stats"]
        assert "right" in d["key_stats"]
        assert all(k in d["key_stats"]["left"] for k in [
            "null_rate", "unique_keys", "duplicate_keys", "rows"
        ])

        # Check cardinality nested structure
        assert "left_key_multiplicity" in d["cardinality"]
        assert "right_key_multiplicity" in d["cardinality"]
        assert "min" in d["cardinality"]["left_key_multiplicity"]
        assert "max" in d["cardinality"]["left_key_multiplicity"]

    def test_repr(self):
        """__repr__ is informative."""
        left = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({"id": [1, 2]})

        result = profile_relationship(left, right, on="id")
        repr_str = repr(result)

        assert "RelationshipProfile" in repr_str
        assert "on=" in repr_str


class TestRelationshipEdgeCases:
    """Edge case tests."""

    def test_empty_left(self):
        """Handle empty left dataset."""
        left = pl.DataFrame({"id": []}).cast({"id": pl.Int64})
        right = pl.DataFrame({"id": [1, 2, 3]})

        result = profile_relationship(left, right, on="id")

        assert result.left_rows == 0
        assert result.right_rows == 3
        assert result.left_unique_keys == 0
        assert result.left_keys_with_match == 0
        assert result.right_keys_without_match == 3

    def test_empty_right(self):
        """Handle empty right dataset."""
        left = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({"id": []}).cast({"id": pl.Int64})

        result = profile_relationship(left, right, on="id")

        assert result.left_rows == 3
        assert result.right_rows == 0
        assert result.right_unique_keys == 0
        assert result.left_keys_without_match == 3

    def test_no_overlap(self):
        """Handle datasets with no key overlap."""
        left = pl.DataFrame({"id": [1, 2, 3]})
        right = pl.DataFrame({"id": [4, 5, 6]})

        result = profile_relationship(left, right, on="id")

        assert result.left_keys_with_match == 0
        assert result.right_keys_with_match == 0
        assert result.left_keys_without_match == 3
        assert result.right_keys_without_match == 3

    def test_missing_key_column_left(self):
        """Error on missing key column in left."""
        left = pl.DataFrame({"a": [1, 2]})
        right = pl.DataFrame({"id": [1, 2]})

        with pytest.raises(ValueError, match="not found in left"):
            profile_relationship(left, right, on="id")

    def test_missing_key_column_right(self):
        """Error on missing key column in right."""
        left = pl.DataFrame({"id": [1, 2]})
        right = pl.DataFrame({"a": [1, 2]})

        with pytest.raises(ValueError, match="not found in right"):
            profile_relationship(left, right, on="id")


class TestRelationshipImport:
    """Test that profile_relationship is importable from kontra."""

    def test_import_from_kontra(self):
        """Can import profile_relationship from kontra."""
        from kontra import profile_relationship, RelationshipProfile
        assert callable(profile_relationship)
        assert RelationshipProfile is not None

    def test_import_via_kontra_namespace(self):
        """Can use profile_relationship via kontra namespace."""
        left = pl.DataFrame({"id": [1]})
        right = pl.DataFrame({"id": [1]})
        result = kontra.profile_relationship(left, right, on="id")
        assert isinstance(result, kontra.RelationshipProfile)


class TestRelationshipAgentWalkthrough:
    """Test the agent walkthrough scenario from the MVP doc."""

    def test_orders_customers_join_warning(self):
        """
        Agent scenario: Join orders to customers.

        The agent plans to:
        > "Join orders to customers on customer_id."

        Before writing the transformation, it runs profile_relationship
        to understand the join shape.
        """
        # Orders - many rows per customer
        orders = pl.DataFrame({
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [1, 1, 2, 2, 3],  # Customers 1 and 2 have 2 orders each
            "amount": [100, 200, 150, 250, 300],
        })

        # Customers - some duplicates (historical reactivations)
        customers = pl.DataFrame({
            "customer_id": [1, 1, 2, 3, 4],  # Customer 1 appears twice
            "status": ["active", "inactive", "active", "active", "active"],
        })

        result = profile_relationship(orders, customers, on="customer_id")

        # Agent sees:
        # - Right side is not unique (max multiplicity > 1)
        assert result.right_key_multiplicity_max > 1

        # - Some right keys don't match (customer 4 has no orders)
        assert result.right_keys_without_match > 0

        # Agent now knows:
        # 1. JOIN may explode rows (right side has duplicates)
        # 2. Inner join will exclude customer 4

        # The profile gives all this info without any judgment
        d = result.to_dict()

        # Cardinality warning is clear
        assert d["cardinality"]["right_key_multiplicity"]["max"] == 2

        # Sample shows which key is duplicated
        assert 1 in result.samples_right_duplicates
