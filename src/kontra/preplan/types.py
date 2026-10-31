from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal

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
    - fail_details: rule_id -> details dict for fail_meta rules (e.g., dtype mismatch info).
    """
    manifest_columns: List[str]
    manifest_row_groups: List[int]
    rule_decisions: Dict[str, Decision]
    stats: Dict[str, int]
    fail_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
