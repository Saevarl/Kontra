# src/kontra/scout/backends/trino_backend.py
"""
Trino backend for Scout profiler.

Every statistic is computed by Trino and only scalars come back. Trino has no
cheap exact metadata (table statistics are estimates), so the row count is an
exact COUNT(*) and the profile never reports estimates unless sampling is on.

The profiler emits ANSI-style aggregates; a few shapes are rewritten here:

  - ``PERCENTILE_CONT(...) WITHIN GROUP (ORDER BY c)``: Trino only has the
    approximate ``approx_percentile``. The exact continuous percentile is
    computed from the sorted non-null values with the same linear
    interpolation PostgreSQL uses. Like ClickHouse's ``quantileExact``, this
    holds a column's values in memory; use ``sample`` on very large tables.
  - ``AVG(c)``: on DECIMAL columns Trino rounds the mean to the column's scale,
    so the mean is computed in DOUBLE.
  - ``CAST(... AS FLOAT)``: Trino has no FLOAT type; DOUBLE is used.
  - ``LENGTH(c)`` and ``c = ''``: JSON and UUID columns profile as strings, but
    Trino defines neither on them, so they are read as their text form.
"""

from __future__ import annotations

import re
from typing import Any

from kontra.connectors.detection import parse_table_reference
from kontra.connectors.handle import DatasetHandle
from kontra.engine.sql_ir import esc_ident as _esc_ident
from kontra.engine.sql_ir import lit_str as _lit_str
from kontra.logging import get_logger

_logger = get_logger(__name__)

# Lazy-loaded trino error classes (the driver must not load on `import kontra`).
_trino_errors: tuple[type, ...] | None = None


def _get_db_errors() -> tuple[type, ...]:
    """Return the trino client's base error classes, lazy-loaded."""
    global _trino_errors
    if _trino_errors is None:
        try:
            from trino.exceptions import Error, HttpError

            _trino_errors = (Error, HttpError)
        except ImportError:
            _trino_errors = (Exception,)
    return _trino_errors


# --------------------------------------------------------------------------- #
# SQL dialect adaptation
# --------------------------------------------------------------------------- #

# A double-quoted identifier, as produced by esc_ident(..., "trino").
_IDENT = r'"(?:[^"]|"")*"'

_PERCENTILE_RE = re.compile(
    r"PERCENTILE_CONT\(\s*(ARRAY\[[^\]]*\]|[0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*"
    rf"ORDER\s+BY\s+({_IDENT})\s*\)",
    re.IGNORECASE,
)
_AVG_RE = re.compile(rf"\bAVG\(\s*({_IDENT})\s*\)", re.IGNORECASE)
_CAST_FLOAT_RE = re.compile(r"\bAS\s+FLOAT\s*\)", re.IGNORECASE)
_LENGTH_RE = re.compile(rf"\bLENGTH\(\s*({_IDENT})\s*\)", re.IGNORECASE)
_EMPTY_RE = re.compile(rf"WHEN\s+({_IDENT})\s*=\s*''", re.IGNORECASE)


def _percentile_sql(levels: str, col: str) -> str:
    """Exact PERCENTILE_CONT over the non-null values of ``col``.

    For n sorted values and fraction f, the position is p = f * (n - 1); the
    result interpolates between the values at floor(p) and ceil(p). NULL when
    the column has no non-null values. ``levels`` is a number or ``ARRAY[...]``
    of numbers; the result has the same shape.
    """
    scalar = not levels.upper().startswith("ARRAY")
    fractions = f"ARRAY[{levels}]" if scalar else levels
    lo = "a[CAST(floor(p) AS BIGINT) + 1]"
    hi = "a[CAST(ceil(p) AS BIGINT) + 1]"
    value = f"IF(floor(p) = p, {lo}, {lo} + (p - floor(p)) * ({hi} - {lo}))"
    per_fraction = (
        f"transform({fractions}, f -> element_at(transform("
        f"ARRAY[CAST(f AS DOUBLE) * (cardinality(a) - 1)], p -> {value}), 1))"
    )
    values = f"array_sort(array_agg(CAST({col} AS DOUBLE)) FILTER (WHERE {col} IS NOT NULL))"
    expr = (
        f"element_at(transform(ARRAY[{values}], a -> "
        f"IF(a IS NULL OR cardinality(a) = 0, NULL, {per_fraction})), 1)"
    )
    return f"element_at({expr}, 1)" if scalar else expr


def _adapt_expr(expr: str, text_of: dict[str, str], padded: frozenset[str] = frozenset()) -> str:
    """Rewrite one profiler aggregate into equivalent Trino SQL.

    ``text_of`` maps a quoted identifier to the SQL that reads it as text, for
    columns whose type has no string functions (JSON, UUID). ``padded`` holds
    the CHAR(n) columns: Trino compares them with padding, so a blank value
    equals '', but the driver returns it as n spaces, which is not empty.
    """

    def text(match: re.Match[str]) -> str:
        return text_of.get(match.group(1), match.group(1))

    expr = _PERCENTILE_RE.sub(lambda m: _percentile_sql(m.group(1), m.group(2)), expr)
    expr = _AVG_RE.sub(lambda m: f"AVG(CAST({m.group(1)} AS DOUBLE))", expr)
    expr = _CAST_FLOAT_RE.sub("AS DOUBLE)", expr)
    expr = _LENGTH_RE.sub(lambda m: f"LENGTH({text(m)})", expr)
    expr = _EMPTY_RE.sub(
        lambda m: (
            f"WHEN LENGTH({m.group(1)}) = 0" if m.group(1) in padded else f"WHEN {text(m)} = ''"
        ),
        expr,
    )
    return expr


def _text_sql(ident: str, raw_type: str) -> str | None:
    """SQL reading a JSON or UUID column as text; None for other types."""
    base = raw_type.strip().lower().split("(")[0].strip()
    if base == "json":
        return f"json_format({ident})"
    if base == "uuid":
        return f"CAST({ident} AS VARCHAR)"
    return None


def _is_padded(raw_type: str) -> bool:
    """True for CHAR(n), whose values the driver returns blank-padded."""
    return raw_type.strip().lower().split("(")[0].strip() in ("char", "character")


class TrinoBackend:
    """
    Trino-based profiler backend.

    Features:
    - Schema from information_schema (no scan)
    - Exact row count (COUNT(*))
    - Single aggregate query for all column stats, computed by Trino
    - Exact percentiles (no approx_percentile)
    """

    # Counts are exact full-table aggregates (unless sampling), so a column
    # proven unique or constant needs no GROUP BY for its frequencies.
    exact_value_frequency = True

    def __init__(
        self,
        handle: DatasetHandle,
        *,
        sample_size: int | None = None,
    ):
        self.handle = handle
        self.sample_size = sample_size
        self._conn = None
        self._conn_ctx = None
        self._schema: list[tuple[str, str]] | None = None
        self._text_of: dict[str, str] = {}
        self._padded: frozenset[str] = frozenset()
        # COUNT(*) is exact. Read by the profiler for provenance flagging.
        self.row_count_estimated: bool = False

        self._catalog, self._schema_name, self._table = self._resolve_parts(handle)

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        from kontra.connectors.db_utils import get_connection_ctx

        self._conn_ctx = get_connection_ctx(self.handle, "trino")
        self._conn = self._conn_ctx.__enter__()

    def close(self) -> None:
        if self._conn is not None:
            self._conn_ctx.__exit__(None, None, None)
            self._conn = None
            self._conn_ctx = None

    # ------------------------------------------------------------------ #
    # Schema / row count / size
    # ------------------------------------------------------------------ #

    def get_schema(self) -> list[tuple[str, str]]:
        """Return [(column_name, data_type), ...] from information_schema."""
        if self._schema is not None:
            return self._schema

        source = (
            f"{self.esc_ident(self._catalog)}.information_schema.columns"
            if self._catalog
            else "information_schema.columns"
        )
        rows = self._fetchall(
            f"SELECT column_name, data_type FROM {source} "
            f"WHERE table_schema = {_lit_str(self._schema_name, 'trino')} "
            f"AND table_name = {_lit_str(self._table, 'trino')} "
            "ORDER BY ordinal_position"
        )
        if not rows:
            raise ValueError(f"Trino table not found: {self._qualified_table()}")

        self._schema = [(r[0], r[1]) for r in rows]
        for name, raw in self._schema:
            ident = self.esc_ident(name)
            text = _text_sql(ident, raw)
            if text is not None:
                self._text_of[ident] = text
        self._padded = frozenset(
            self.esc_ident(name) for name, raw in self._schema if _is_padded(raw)
        )
        return self._schema

    def get_row_count(self) -> int:
        rows = self._fetchall(f"SELECT COUNT(*) FROM {self._qualified_table()}")
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

    def get_estimated_size_bytes(self) -> int | None:
        """Trino has no connector-independent size metadata."""
        return None

    # ------------------------------------------------------------------ #
    # Aggregate profiling
    # ------------------------------------------------------------------ #

    def execute_stats_query(self, exprs: list[str]) -> dict[str, Any]:
        """Run all column aggregates in a single query and return {alias: value}."""
        if not exprs:
            return {}

        select = ", ".join(_adapt_expr(e, self._text_of, self._padded) for e in exprs)
        if self.sample_size:
            # A bounded head sample, as for ClickHouse. The profiler flags the
            # results as estimates.
            source = (
                f"(SELECT * FROM {self._qualified_table()} "
                f"LIMIT {int(self.sample_size)}) AS _kontra_sample"
            )
        else:
            source = self._qualified_table()

        cur = self._conn.cursor()
        try:
            cur.execute(f"SELECT {select} FROM {source}")
            row = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]
            return dict(zip(col_names, row)) if row else {}
        finally:
            cur.close()

    def fetch_top_values(self, column: str, limit: int) -> list[tuple[Any, int]]:
        """Top N most frequent non-null values, computed by Trino."""
        col = self.esc_ident(column)
        try:
            rows = self._fetchall(
                f"SELECT {col} AS val, COUNT(*) AS cnt "
                f"FROM {self._qualified_table()} "
                f"WHERE {col} IS NOT NULL "
                f"GROUP BY {col} ORDER BY cnt DESC LIMIT {int(limit)}"
            )
        except _get_db_errors() as e:
            _logger.debug(f"Query error fetching top values for {column}: {e}")
            return []
        return [(r[0], int(r[1])) for r in rows]

    def fetch_distinct_values(self, column: str) -> list[Any]:
        """All distinct non-null values (used for low-cardinality columns)."""
        col = self.esc_ident(column)
        try:
            rows = self._fetchall(
                f"SELECT DISTINCT {col} FROM {self._qualified_table()} "
                f"WHERE {col} IS NOT NULL ORDER BY {col}"
            )
        except _get_db_errors() as e:
            # e.g. MAP columns, which Trino cannot order.
            _logger.debug(f"Query error fetching distinct values for {column}: {e}")
            return []
        return [r[0] for r in rows]

    def fetch_sample_values(self, column: str, limit: int) -> list[Any]:
        """A bounded sample of non-null values (used for pattern detection)."""
        col = self.esc_ident(column)
        try:
            rows = self._fetchall(
                f"SELECT {col} FROM {self._qualified_table()} "
                f"WHERE {col} IS NOT NULL LIMIT {int(limit)}"
            )
        except _get_db_errors() as e:
            _logger.debug(f"Query error fetching sample values for {column}: {e}")
            return []
        return [r[0] for r in rows if r[0] is not None]

    def esc_ident(self, name: str) -> str:
        return _esc_ident(name, "trino")

    @property
    def source_format(self) -> str:
        return "trino"

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _fetchall(self, sql: str) -> list[Any]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def _qualified_table(self) -> str:
        parts = [self._catalog, self._schema_name, self._table]
        return ".".join(self.esc_ident(p) for p in parts if p)

    @staticmethod
    def _resolve_parts(handle: DatasetHandle) -> tuple[str | None, str, str]:
        """(catalog, schema, table) for URI and BYOC handles."""
        if handle.scheme == "byoc" and handle.external_conn is not None:
            if not handle.table_ref:
                raise ValueError("BYOC Trino handle missing table_ref")
            catalog, schema, table = parse_table_reference(handle.table_ref)
            if not schema:
                raise ValueError(
                    f"Trino table reference needs a schema: 'schema.table' or "
                    f"'catalog.schema.table' (got: {handle.table_ref!r})"
                )
            return catalog, schema, table
        if handle.db_params is not None:
            params = handle.db_params
            return params.catalog, params.schema, params.table
        raise ValueError("Trino handle missing db_params or external_conn")
