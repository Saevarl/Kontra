# src/kontra/engine/phases/__init__.py
"""
Validation engine phases.

This package contains the extracted phase implementations for the
validation engine pipeline:
- Compilation: Rule building and plan compilation
- Preplan: Metadata-only optimization (Parquet, PostgreSQL, SQL Server)
- Pushdown: SQL execution (DuckDB, PostgreSQL, SQL Server)
- Residual: Polars execution of remaining rules
- Merge: Result combination and summary building
"""

from kontra.engine.phases.compilation import compile_rules
from kontra.engine.phases.preplan import execute_preplan
from kontra.engine.phases.pushdown import execute_pushdown
from kontra.engine.phases.residual import execute_residual
from kontra.engine.phases.merge import merge_results, build_summary

__all__ = [
    "compile_rules",
    "execute_preplan",
    "execute_pushdown",
    "execute_residual",
    "merge_results",
    "build_summary",
]
