from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal

# A rule can be proven by metadata to pass, proven to fail, or remain unknown.
Decision = Literal["pass_meta", "fail_meta", "unknown"]


@dataclass
class PrePlan:
    """
    Result of the metadata-only pre-planning stage.

    - manifest_columns: union of columns still needed for SQL/Polars after metadata decisions.
    - manifest_row_groups: Parquet row-group indices that *may* affect remaining rules.
                           (Single-file MVP; can evolve to a file list later.)
    - rule_decisions: rule_id -> Decision ("pass_meta" | "fail_meta" | "unknown").
    - stats: small numbers for observability (e.g., {"rg_total": 19, "rg_kept": 7}).
    """
    manifest_columns: List[str]
    manifest_row_groups: List[int]
    rule_decisions: Dict[str, Decision]
    stats: Dict[str, int]
