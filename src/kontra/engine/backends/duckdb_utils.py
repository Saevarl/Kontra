# src/kontra/engine/backends/duckdb_utils.py
from __future__ import annotations


def esc_ident(name: str) -> str:
    """
    Quote an identifier for DuckDB (double quotes, escape internal quotes).
    This is a centralized helper used by executors and materializers.
    """
    return '"' + name.replace('"', '""') + '"'


def lit_str(s: str) -> str:
    """
    Return a single-quoted SQL string literal with internal quotes escaped.
    This is a centralized helper used by executors and materializers.
    """
    return "'" + s.replace("'", "''") + "'"