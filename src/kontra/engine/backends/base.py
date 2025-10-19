from __future__ import annotations

"""
Backend interface (protocols)

A backend turns a compiled plan into results. Different engines (Polars, DuckDB)
can implement this without the ValidationEngine caring about details.

Design goals:
- Tiny surface: compile() → artifact, execute() → {"results": [...]}
- Deterministic results: identical inputs → identical outputs
- Introspection hook for observability (optional)
"""

from typing import Protocol, Any, Dict


class ValidationBackend(Protocol):
    """
    Minimal interface a validation backend must implement.
    """

    # Human-readable identifier (e.g., "polars", "duckdb", "hybrid")
    name: str

    def supports(self, connector_caps: int) -> bool:
        """
        Return True if this backend is viable given connector capabilities
        (e.g., pushdown / remote partial reads). Purely advisory; the engine
        may still select a different backend.
        """
        ...

    def compile(self, compiled_plan: Any) -> Any:
        """
        Prepare a backend-specific artifact from the planner’s CompiledPlan.

        Examples:
          - Polars: just pass CompiledPlan through
          - DuckDB: build a single-row aggregate SQL string
        """
        ...

    def execute(self, source_or_df: Any, compiled_artifact: Any) -> Dict[str, Any]:
        """
        Execute the compiled artifact.

        Args:
          source_or_df:
            - Polars backend: a Polars DataFrame
            - DuckDB backend: a dataset URI (e.g., s3://...), not a dataframe
          compiled_artifact:
            - Polars: CompiledPlan
            - DuckDB: SQL string

        Returns:
          {"results": List[rule_result_dict]}
        """
        ...

    def introspect(self, source_or_df: Any) -> Dict[str, Any]:
        """
        Optional, best-effort stats for observability:
          - {"row_count": int|None, "available_cols": List[str]|None}

        Default is a no-op implementation (override if useful).
        """
        return {"row_count": None, "available_cols": None}
