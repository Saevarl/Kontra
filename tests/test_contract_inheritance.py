# tests/test_contract_inheritance.py
"""
Tests for contract inheritance (extends:).

Verifies that contracts can inherit rules from base contracts via
the `extends` field, with proper path resolution, cycle detection,
and multi-level inheritance.
"""
import pytest
import polars as pl

import kontra
from kontra.config.loader import ContractLoader
from kontra.errors import ContractNotFoundError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    """Simple DataFrame for validation tests."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com", "i@j.com"],
        "status": ["active", "inactive", "active", "pending", "active"],
        "age": [25, 30, 35, 40, 45],
    })


@pytest.fixture
def contracts_dir(tmp_path):
    """Directory for contract files."""
    return tmp_path


# ---------------------------------------------------------------------------
# Tests: Single inheritance
# ---------------------------------------------------------------------------

class TestSingleInheritance:
    """Tests for basic single-level contract inheritance."""

    def test_child_gets_base_plus_own_rules(self, contracts_dir, sample_df):
        """Child contract inherits base rules and adds its own."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base
rules:
  - name: not_null
    params: { column: id }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: base.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: email }
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 2
        rule_ids = {r.rule_id for r in result.rules}
        assert "COL:id:not_null" in rule_ids
        assert "COL:email:not_null" in rule_ids

    def test_base_rules_come_first(self, contracts_dir):
        """Base rules should appear before child rules in the compiled order."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base
rules:
  - name: not_null
    params: { column: id }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: base.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: email }
""")

        contract = ContractLoader.from_path(str(child))
        # First rule should be from base (id), second from child (email)
        assert contract.rules[0].params["column"] == "id"
        assert contract.rules[1].params["column"] == "email"

    def test_name_not_inherited(self, contracts_dir):
        """Child name should NOT be inherited from base."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base_contract_name
rules:
  - name: not_null
    params: { column: id }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: base.yml
name: child_contract_name
datasource: inline
rules: []
""")

        contract = ContractLoader.from_path(str(child))
        assert contract.name == "child_contract_name"

    def test_datasource_not_inherited(self, contracts_dir):
        """Child datasource should NOT be inherited from base."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base
datasource: base_data.parquet
rules:
  - name: not_null
    params: { column: id }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: base.yml
name: child
datasource: child_data.parquet
rules: []
""")

        contract = ContractLoader.from_path(str(child))
        assert contract.datasource == "child_data.parquet"


# ---------------------------------------------------------------------------
# Tests: Multi-level inheritance
# ---------------------------------------------------------------------------

class TestMultiLevelInheritance:
    """Tests for recursive inheritance (grandparent -> parent -> child)."""

    def test_three_level_inheritance(self, contracts_dir, sample_df):
        """grandparent -> parent -> child should accumulate all rules."""
        grandparent = contracts_dir / "grandparent.yml"
        grandparent.write_text("""\
name: grandparent
rules:
  - name: not_null
    params: { column: id }
""")

        parent = contracts_dir / "parent.yml"
        parent.write_text("""\
extends: grandparent.yml
name: parent
rules:
  - name: not_null
    params: { column: email }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: parent.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: status }
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 3
        rule_ids = {r.rule_id for r in result.rules}
        assert "COL:id:not_null" in rule_ids
        assert "COL:email:not_null" in rule_ids
        assert "COL:status:not_null" in rule_ids


# ---------------------------------------------------------------------------
# Tests: Multiple bases
# ---------------------------------------------------------------------------

class TestMultipleBases:
    """Tests for extending multiple base contracts."""

    def test_extends_list(self, contracts_dir, sample_df):
        """extends: [base1.yml, base2.yml] should include rules from both."""
        base1 = contracts_dir / "base1.yml"
        base1.write_text("""\
name: base1
rules:
  - name: not_null
    params: { column: id }
""")

        base2 = contracts_dir / "base2.yml"
        base2.write_text("""\
name: base2
rules:
  - name: not_null
    params: { column: email }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends:
  - base1.yml
  - base2.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: status }
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 3
        rule_ids = {r.rule_id for r in result.rules}
        assert "COL:id:not_null" in rule_ids
        assert "COL:email:not_null" in rule_ids
        assert "COL:status:not_null" in rule_ids

    def test_multiple_bases_order(self, contracts_dir):
        """Rules from base1 come before base2, then child."""
        base1 = contracts_dir / "base1.yml"
        base1.write_text("""\
rules:
  - name: not_null
    params: { column: id }
""")

        base2 = contracts_dir / "base2.yml"
        base2.write_text("""\
rules:
  - name: not_null
    params: { column: email }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: [base1.yml, base2.yml]
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: status }
""")

        contract = ContractLoader.from_path(str(child))
        cols = [r.params["column"] for r in contract.rules]
        assert cols == ["id", "email", "status"]


# ---------------------------------------------------------------------------
# Tests: Error cases
# ---------------------------------------------------------------------------

class TestInheritanceErrors:
    """Tests for error handling in contract inheritance."""

    def test_circular_dependency(self, contracts_dir):
        """Circular extends should raise ValueError."""
        a = contracts_dir / "a.yml"
        a.write_text("""\
extends: b.yml
name: a
rules: []
""")

        b = contracts_dir / "b.yml"
        b.write_text("""\
extends: a.yml
name: b
rules: []
""")

        with pytest.raises(ValueError, match="[Cc]ircular"):
            ContractLoader.from_path(str(a))

    def test_missing_base_file(self, contracts_dir):
        """Missing base contract should raise ContractNotFoundError."""
        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: nonexistent.yml
name: child
rules: []
""")

        with pytest.raises(ContractNotFoundError):
            ContractLoader.from_path(str(child))

    def test_self_reference(self, contracts_dir):
        """Contract extending itself should raise ValueError (cycle)."""
        self_ref = contracts_dir / "self.yml"
        self_ref.write_text("""\
extends: self.yml
name: self_ref
rules: []
""")

        with pytest.raises(ValueError, match="[Cc]ircular"):
            ContractLoader.from_path(str(self_ref))


# ---------------------------------------------------------------------------
# Tests: No extends (backwards compatibility)
# ---------------------------------------------------------------------------

class TestNoExtends:
    """Verify contracts without extends still work."""

    def test_no_extends_field(self, contracts_dir, sample_df):
        """Contract without extends works as before."""
        contract = contracts_dir / "normal.yml"
        contract.write_text("""\
name: normal
datasource: inline
rules:
  - name: not_null
    params: { column: id }
""")

        result = kontra.validate(
            sample_df, str(contract),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 1
        assert result.passed is True

    def test_child_with_no_own_rules(self, contracts_dir, sample_df):
        """Child can have no rules of its own — gets only base rules."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base
rules:
  - name: not_null
    params: { column: id }
  - name: not_null
    params: { column: email }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: base.yml
name: child
datasource: inline
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 2


# ---------------------------------------------------------------------------
# Tests: Subdirectory path resolution
# ---------------------------------------------------------------------------

class TestPathResolution:
    """Test that relative paths in extends are resolved relative to the child."""

    def test_subdirectory_base(self, contracts_dir, sample_df):
        """extends can reference a contract in a subdirectory."""
        subdir = contracts_dir / "bases"
        subdir.mkdir()

        base = subdir / "base.yml"
        base.write_text("""\
name: base
rules:
  - name: not_null
    params: { column: id }
""")

        child = contracts_dir / "child.yml"
        child.write_text("""\
extends: bases/base.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: email }
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 2

    def test_parent_directory_base(self, contracts_dir, sample_df):
        """extends can use ../ to reference parent directory."""
        base = contracts_dir / "base.yml"
        base.write_text("""\
name: base
rules:
  - name: not_null
    params: { column: id }
""")

        subdir = contracts_dir / "children"
        subdir.mkdir()

        child = subdir / "child.yml"
        child.write_text("""\
extends: ../base.yml
name: child
datasource: inline
rules:
  - name: not_null
    params: { column: email }
""")

        result = kontra.validate(
            sample_df, str(child),
            preplan="off", pushdown="off",
        )
        assert result.total_rules == 2
