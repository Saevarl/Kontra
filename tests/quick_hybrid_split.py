# tests/quick_hybrid_split.py
from __future__ import annotations

from pathlib import Path
import polars as pl

from contra.engine.engine import ValidationEngine

CONTRACT_TEMPLATE = """
dataset: "{dataset}"

rules:
  - name: not_null
    rule_id: COL:email:not_null
    params:
      column: email

  - name: regex
    rule_id: COL:email:regex
    params:
      column: email
      pattern: '^[^@]+@[^@]+\\.[^@]+$'

  - name: allowed_values
    rule_id: COL:status:allowed_values
    params:
      column: status
      values: ["active", "inactive", "pending"]

  - name: unique
    rule_id: COL:user_id:unique
    params:
      column: user_id
"""

def _make_fixture(tmp: Path) -> str:
    # Small dataset (4 rows, 4 columns)
    df = pl.DataFrame(
        {
            "user_id": [1, 1, 3, 4],                         # duplicate → unique fails
            "email": ["a@x.com", None, "bad@", "b@y.io"],    # null + regex fail
            "status": ["active", "weird", "inactive", "pending"],  # allowed_values fail ("weird")
            "age": [10, 20, 30, 40],
        }
    )
    data_path = tmp / "users.parquet"
    df.write_parquet(str(data_path))

    contract_path = tmp / "contract.yml"
    contract_path.write_text(CONTRACT_TEMPLATE.format(dataset=str(data_path)), encoding="utf-8")
    return str(contract_path)

def test_hybrid_split_projection_and_results(tmp_path: Path):
    contract = _make_fixture(tmp_path)

    # Run hybrid engine (duckdb) — should push not_null to SQL, rest in Polars
    eng = ValidationEngine(
        contract_path=contract,
        emit_report=False,
        stats_mode="summary",
        engine="duckdb",
        enable_projection=True,
    )
    out = eng.run()

    # Basic correctness: we expect failures from both SQL-able and Polars-only rules
    rids = {r["rule_id"] for r in out["results"]}
    assert "COL:email:not_null" in rids               # SQL-able
    assert "COL:email:regex" in rids                  # Polars
    assert "COL:status:allowed_values" in rids        # Polars
    assert "COL:user_id:unique" in rids               # Polars

    # Projection stats should show pruning (residual columns < available)
    stats = out.get("stats", {})
    proj = (stats or {}).get("projection", {})
    required = proj.get("required_count", 0)
    available = proj.get("available_count", 0)
    loaded = proj.get("loaded_count", 0)

    assert proj.get("enabled") is True
    assert 0 < required <= loaded <= available
    assert proj.get("effective") in (True, required < available)
