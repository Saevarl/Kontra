# src/contra/rules/planner/predicates.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:
    import polars as pl


@dataclass(frozen=True)
class Predicate:
    """
    A vectorized rule failure mask.

    rule_id : str
        Stable identifier for the rule instance.
    expr : pl.Expr
        Boolean expression; True for rows that FAIL the rule.
    message : str
        Deterministic, human-readable message when the rule fails.
    columns : set[str]
        Column names referenced by `expr` (used for column pruning).
    """
    rule_id: str
    expr: "pl.Expr"
    message: str
    columns: Set[str]
