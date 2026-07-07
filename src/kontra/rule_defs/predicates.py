# src/contra/rules/planner/predicates.py
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Set

if TYPE_CHECKING:
    import polars as pl


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

    The Polars expression may be supplied eagerly (``expr=``) or lazily
    (``expr_factory=``, a zero-argument callable returning the expression).
    A lazy factory lets callers construct a Predicate — and derive its
    ``columns``/``message`` — without importing polars until the expression
    is actually needed (i.e. when the residual Polars tier reads ``.expr``).
    Behaviour is identical either way: the same expression object is produced.
    """

    __slots__ = ("rule_id", "message", "columns", "_expr", "_expr_factory")

    def __init__(
        self,
        rule_id: str,
        *,
        message: str,
        columns: Set[str],
        expr: "Optional[pl.Expr]" = None,
        expr_factory: "Optional[Callable[[], pl.Expr]]" = None,
    ) -> None:
        # Keyword-only: the old frozen-dataclass field order was
        # (rule_id, expr, message, columns) — positional calls against that
        # order must fail loudly here, not silently swap expr and message.
        self.rule_id = rule_id
        self.message = message
        self.columns = columns
        self._expr = expr
        self._expr_factory = expr_factory

    @property
    def expr(self) -> "pl.Expr":
        """The Polars failure-mask expression (built on first access if lazy)."""
        if self._expr is None:
            if self._expr_factory is None:
                raise ValueError(
                    "Predicate has neither an eager 'expr' nor an 'expr_factory'"
                )
            self._expr = self._expr_factory()
        return self._expr

    @property
    def has_eager_expr(self) -> bool:
        """True if the expression has already been materialized."""
        return self._expr is not None

    def __repr__(self) -> str:
        state = "materialized" if self._expr is not None else "lazy"
        return (
            f"Predicate(rule_id={self.rule_id!r}, message={self.message!r}, "
            f"columns={self.columns!r}, expr={state})"
        )
