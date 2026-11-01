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

# How Kontra reads CSV through DuckDB, so every route sees the file as Polars
# does: a header row always (DuckDB would guess, and a header-only file whose
# names look like values, such as t/f, loses its column names); a quoted empty
# field as an empty string, not NULL (only an unquoted empty field is NULL in
# Polars); and '"' as the quote and escape character (DuckDB may sniff "'" from
# fields like a lone apostrophe and then merge rows). The delimiter is still
# sniffed. Keys are read_csv's Python keyword arguments.
CSV_READ_OPTIONS = {
    "header": True,
    "allow_quoted_nulls": False,
    "quotechar": '"',
    "escapechar": '"',
}

# The same options under their SQL read_csv_auto parameter names.
_CSV_SQL_OPTIONS = "header = true, allow_quoted_nulls = false, quote = '\"', escape = '\"'"


def csv_read_sql(source: str) -> str:
    """``read_csv_auto(...)`` for ``source`` with CSV_READ_OPTIONS."""
    return f"read_csv_auto({lit_str(source)}, {_CSV_SQL_OPTIONS})"
