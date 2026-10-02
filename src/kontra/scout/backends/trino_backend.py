# src/kontra/scout/backends/trino_backend.py
"""
Trino backend for Scout profiler.

Every statistic is computed by Trino and only scalars come back. Trino has no
cheap exact metadata (table statistics are estimates), so the row count is an
exact COUNT(*). Distinct counts are estimates in ``scout`` (``approx_distinct``,
marked as estimated, as the preset promises); everything else is exact unless
sampling is on.

How each statistic is read:

  - ``scout``: one aggregate with exact row and null counts and
    ``approx_distinct`` per column. Exact ``COUNT(DISTINCT)`` costs about 17
    times as much on 11 columns; the null counts are free in the same scan.
  - ``scan``/``interrogate``: one aggregate of exact statistics. Trino's default
    distinct strategy is kept: with many columns it beats ``mark_distinct``,
    which the validation scan sets for its few ``unique`` columns.
  - Value frequencies (top values, low-cardinality value lists): exact
    ``GROUP BY`` per column, run concurrently on cursors of the profile's one
    connection. In a caller's transaction they run one at a time, as before:
    a failed query there aborts the caller's transaction.
  - Sampling (``sample=``): ``TABLESAMPLE SYSTEM`` on an Iceberg table with at
    least 100 data files, sized from ``$files``. It reads whole files, so it
    saves reading the rest; with fewer files it would read all or nothing. Any
    other table takes the first rows (``LIMIT``), as before. Both are labelled
    estimates.

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
from concurrent.futures import ThreadPoolExecutor
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

# The sampled-row count added to a SYSTEM-sampled stats query.
_SAMPLED = '"__kontra_sampled_rows__"'

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
    - Value frequencies grouped concurrently
    """

    # Counts are exact full-table aggregates (unless sampling), so a column
    # proven unique or constant needs no GROUP BY for its frequencies.
    exact_value_frequency = True
    # Complete value lists are prefetched only where the profiler lists them.
    value_counts_batch_limit = 0
    # Value queries in flight at once, on cursors of the one connection.
    value_query_concurrency = 4
    # TABLESAMPLE SYSTEM picks whole data files: below this many it reads
    # all or nothing.
    system_sample_min_files = 100
    # Files a SYSTEM sample aims to pick at least, so an empty sample is
    # improbable (about e**-10). An empty one still falls back to LIMIT.
    system_sample_target_files = 10

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
        # A caller's transaction: run queries one at a time (set on connect).
        self._sequential = False
        # Prefetched exact frequencies: complete lists, and top-N by (column, n).
        self._value_counts: dict[str, list[tuple[Any, int]] | None] = {}
        self._top_values: dict[tuple[str, int], list[tuple[Any, int]]] = {}
        # The sampling percentage for TABLESAMPLE SYSTEM, once decided.
        self._system_percent: float | None = None
        self._system_decided = False

        self._catalog, self._schema_name, self._table = self._resolve_parts(handle)

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        from kontra.connectors import trino_read
        from kontra.connectors.db_utils import get_connection_ctx

        self._conn_ctx = get_connection_ctx(self.handle, "trino")
        self._conn = self._conn_ctx.__enter__()
        self._sequential = trino_read.caller_transaction(self.handle)

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
    # scout
    # ------------------------------------------------------------------ #

    def supports_metadata_only(self) -> bool:
        """scout's one-aggregate path; a sampled scout aggregates the sample instead."""
        return self.sample_size is None

    def profile_metadata_only(
        self, schema: list[tuple[str, str]], row_count: int
    ) -> dict[str, dict[str, Any]]:
        """
        The ``scout`` preset: one aggregate, with exact row and null counts and
        ``approx_distinct`` for each column's distinct count, flagged as an
        estimate (Trino's error is about 2.3%). Trino has no exact metadata to
        answer distinct counts, and an exact ``COUNT(DISTINCT)`` per column
        costs far more than this preset is for. The null counts come free in
        the same scan, and are exact.
        """
        exprs = ["COUNT(*)"]
        for name, _ in schema:
            col = self.esc_ident(name)
            exprs += [f"count_if({col} IS NULL)", f"approx_distinct({col})"]
        rows = self._fetchall(f"SELECT {', '.join(exprs)} FROM {self._qualified_table()}")
        row = rows[0]
        exact_rows = int(row[0])
        return {
            name: {
                "null_count": int(row[1 + 2 * i]),
                "distinct_count": int(row[2 + 2 * i]),
                "null_count_estimated": False,
                "distinct_count_estimated": True,
                "exact_row_count": exact_rows,
            }
            for i, (name, _) in enumerate(schema)
        }

    # ------------------------------------------------------------------ #
    # Aggregate profiling
    # ------------------------------------------------------------------ #

    def execute_stats_query(self, exprs: list[str]) -> dict[str, Any]:
        """Run all column aggregates in a single query and return {alias: value}."""
        if not exprs:
            return {}

        select = ", ".join(_adapt_expr(e, self._text_of, self._padded) for e in exprs)
        if not self.sample_size:
            return self._stats_row(f"SELECT {select} FROM {self._qualified_table()}")

        # The profiler flags aggregates over a sample as estimates.
        limit = int(self.sample_size)
        head = f"(SELECT * FROM {self._qualified_table()} LIMIT {limit}) AS _kontra_sample"
        percent = self._sample_percent()
        if percent is None:
            return self._stats_row(f"SELECT {select} FROM {head}")
        # Whole data files, picked at random; capped at the sample size.
        system = (
            f"(SELECT * FROM {self._qualified_table()} TABLESAMPLE SYSTEM ({percent!r}) "
            f"LIMIT {limit}) AS _kontra_sample"
        )
        result = self._stats_row(f"SELECT {select}, count(*) AS {_SAMPLED} FROM {system}")
        if result.pop(_SAMPLED.strip('"'), None):
            return result
        # No file was picked: take the head sample instead.
        _logger.debug("TABLESAMPLE SYSTEM (%s) picked no rows; sampling the first rows", percent)
        return self._stats_row(f"SELECT {select} FROM {head}")

    def _stats_row(self, sql: str) -> dict[str, Any]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]
            return dict(zip(col_names, row)) if row else {}
        finally:
            cur.close()

    def _sample_percent(self) -> float | None:
        """The TABLESAMPLE SYSTEM percentage, or None to sample the first rows.

        Only an Iceberg table with at least ``system_sample_min_files`` data
        files is sampled by file. The percentage aims at the sample size, and at
        no fewer than ``system_sample_target_files`` files.
        """
        if self._system_decided:
            return self._system_percent
        self._system_decided = True
        from kontra.connectors import trino_read

        try:
            files = trino_read.data_files(self._conn, self._held_parts())
        except _get_db_errors() as e:
            if self._sequential:
                raise  # the caller's transaction is aborted; don't hide why
            _logger.debug("Could not read $files for sampling: %s", e)
            files = None
        if files is None:
            return None
        count, records = files
        if count < self.system_sample_min_files or records <= 0:
            return None
        fraction = max(self.sample_size / records, self.system_sample_target_files / count)
        if fraction >= 1:
            return None  # the sample would be the whole table
        self._system_percent = round(fraction * 100, 6) or None
        return self._system_percent

    def prefetch_value_counts(self, requests: list[tuple[str, int | None]]) -> None:
        """Run the profile's value queries concurrently, each an exact GROUP BY.

        ``None`` asks for a column's complete distribution (``fetch_value_counts``),
        a number for its top N (``fetch_top_values``). Each query and its result,
        failures included, are what the individual call would give; the calls
        then read them from here. In a caller's transaction nothing is
        prefetched: the calls run one at a time, as before.
        """
        if self._sequential or len(requests) < 2:
            return

        def run(request: tuple[str, int | None]) -> None:
            column, limit = request
            if limit is None:
                self._value_counts[column] = self._query_value_counts(column)
            else:
                self._top_values[column, int(limit)] = self._query_top_values(column, limit)

        with ThreadPoolExecutor(max_workers=self.value_query_concurrency) as pool:
            list(pool.map(run, requests))

    def fetch_value_counts(self, column: str) -> list[tuple[Any, int]] | None:
        """All non-null values and exact frequencies, in database value order.

        Used only where the profiler lists a column's values: one GROUP BY
        replaces its separate top-value and DISTINCT queries. None when Trino
        can't order the type (MAP): the profiler then runs those two queries.
        """
        if column in self._value_counts:
            return self._value_counts[column]
        return self._query_value_counts(column)

    def fetch_top_values(self, column: str, limit: int) -> list[tuple[Any, int]]:
        """Top N most frequent non-null values, computed by Trino."""
        cached = self._top_values.get((column, int(limit)))
        if cached is not None:
            return cached
        return self._query_top_values(column, limit)

    def _query_value_counts(self, column: str) -> list[tuple[Any, int]] | None:
        col = self.esc_ident(column)
        try:
            rows = self._fetchall(
                f"SELECT {col}, COUNT(*) FROM {self._qualified_table()} "
                f"WHERE {col} IS NOT NULL GROUP BY {col} ORDER BY {col}"
            )
        except _get_db_errors() as e:
            _logger.debug("Combined value query failed for %s: %s", column, e)
            return None
        return [(r[0], int(r[1])) for r in rows]

    def _query_top_values(self, column: str, limit: int) -> list[tuple[Any, int]]:
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

    def _held_parts(self) -> tuple[str | None, str, str]:
        """(catalog, schema, table), with a caller's session catalog filled in."""
        catalog = self._catalog
        if catalog is None and self.handle.external_conn is not None:
            catalog = getattr(self.handle.external_conn, "catalog", None)
        return catalog, self._schema_name, self._table

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
