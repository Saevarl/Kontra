# tests/test_sqlserver_scout_percentiles.py
"""SQL Server median and percentiles: generated SQL and result mapping.

SQL Server has PERCENTILE_CONT only as a window function, so the backend takes
the profiler's aggregate form out of the stats query and computes every
column's percentiles in one separate query. Live values are checked in
tests/sqlserver/test_sqlserver_scout_percentiles.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kontra.scout.backends.sqlserver_backend import SqlServerBackend
from kontra.scout.profiler import ScoutProfiler

URI = "mssql://sa:pw@localhost:1433/db/dbo.t"


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self._rows = []

    def execute(self, sql):
        self.conn.sql.append(sql)
        if "ROW_NUMBER()" in sql:
            self.description = [("col",)] + [(f"q{i}",) for i in range(8)]
            self._rows = list(self.conn.percentile_rows)
        else:
            self.description = [(name,) for name in self.conn.aggregate_row]
            self._rows = [tuple(self.conn.aggregate_row.values())]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _Conn:
    def __init__(self, aggregate_row=None, percentile_rows=()):
        self.sql = []
        self.aggregate_row = aggregate_row or {"__total_rows__": 5}
        self.percentile_rows = percentile_rows

    def cursor(self):
        return _Cursor(self)


def _backend(conn, sample_size=None):
    handle = MagicMock()
    handle.db_params.schema = "dbo"
    handle.db_params.table = "t"
    backend = SqlServerBackend(handle, sample_size=sample_size)
    backend._conn = conn
    return backend


def _profiler(preset, percentiles=None):
    profiler = ScoutProfiler(URI, preset=preset, percentiles=percentiles)
    profiler.backend = _backend(_Conn())
    return profiler


class TestProfilerExpressions:
    def test_interrogate_requests_median_and_percentiles(self):
        exprs = _profiler("interrogate")._build_column_agg_exprs("amount", "float")
        assert (
            "PERCENTILE_CONT(ARRAY[0.5, 0.25, 0.75, 0.99]) WITHIN GROUP "
            "(ORDER BY [amount]) AS [__quantiles__amount]"
        ) in exprs
        assert "STDEV([amount]) AS [__std__amount]" in exprs
        assert not any(e.startswith("STDDEV") for e in exprs)

    def test_scan_aggregate_path_requests_median_only(self):
        exprs = _profiler("scan")._build_column_agg_exprs("amount", "int")
        assert (
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [amount]) AS [__median__amount]"
        ) in exprs
        assert not any("ARRAY[" in e for e in exprs)

    def test_non_numeric_columns_request_no_percentiles(self):
        profiler = _profiler("interrogate")
        for dtype in ("bool", "string", "datetime"):
            exprs = profiler._build_column_agg_exprs("c", dtype)
            assert not any("PERCENTILE_CONT" in e for e in exprs)


class TestPercentileQuery:
    def test_percentiles_leave_the_aggregate_query(self):
        conn = _Conn()
        _backend(conn).execute_stats_query([
            "COUNT(*) AS [__total_rows__]",
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [a]) AS [__quantiles__a]",
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [b]) AS [__median__b]",
        ])
        aggregate, percentile = conn.sql
        assert aggregate == "SELECT COUNT(*) AS [__total_rows__] FROM [dbo].[t]"
        assert "PERCENTILE_CONT" not in percentile

    def test_one_query_for_all_columns(self):
        conn = _Conn()
        _backend(conn).execute_stats_query([
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [a]) AS [__quantiles__a]",
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [b]) AS [__median__b]",
        ])
        # No aggregate left to run: only the percentile query.
        assert len(conn.sql) == 1
        sql = conn.sql[0]
        # One unpivot, so one sort covers every column.
        assert sql.count("FROM [dbo].[t]") == 1
        assert "(VALUES (0, CAST([a] AS FLOAT)), (1, CAST([b] AS FLOAT)))" in sql
        assert "ROW_NUMBER() OVER (PARTITION BY v.col ORDER BY v.val)" in sql
        assert "COUNT_BIG(*) OVER (PARTITION BY v.col)" in sql
        assert "WHERE v.val IS NOT NULL" in sql

    def test_linear_interpolation_between_ranks(self):
        conn = _Conn()
        _backend(conn).execute_stats_query([
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [a]) AS [__quantiles__a]",
        ])
        sql = conn.sql[0]
        # Position p = f * (n - 1); value = v[floor p] + (p - floor p) * (v[ceil p] - v[floor p]).
        # The fraction is FLOAT so DECIMAL precision rules can't round the weight.
        p = "CAST(0.25 AS FLOAT) * (q.n - 1)"
        lo = f"MAX(CASE WHEN q.rn = FLOOR({p}) THEN q.val END)"
        hi = f"MAX(CASE WHEN q.rn = CEILING({p}) THEN q.val END)"
        assert f"{lo} + ({p} - FLOOR({p})) * ({hi} - {lo})" in sql
        assert "GROUP BY q.col, q.n" in sql

    def test_results_map_back_to_aliases(self):
        # Rows are (column index, value per requested fraction in first-seen order).
        conn = _Conn(percentile_rows=[(0, 3.0, 2.0, None), (1, 7.5, None, 9.0)])
        result = _backend(conn).execute_stats_query([
            "COUNT(*) AS [__total_rows__]",
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [a]) AS [__quantiles__a]",
            "PERCENTILE_CONT(ARRAY[0.5, 0.99]) WITHIN GROUP (ORDER BY [b]) AS [__quantiles__b]",
        ])
        assert result == {
            "__total_rows__": 5,
            "__quantiles__a": [3.0, 2.0],
            "__quantiles__b": [7.5, 9.0],
        }

    def test_scalar_median_is_a_number(self):
        conn = _Conn(percentile_rows=[(0, 4.5)])
        result = _backend(conn).execute_stats_query([
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [a]) AS [__median__a]",
        ])
        assert result == {"__median__a": 4.5}

    def test_column_without_values_is_null(self):
        # Empty table or all-NULL column: no unpivoted rows, so no result row.
        conn = _Conn(percentile_rows=[])
        result = _backend(conn).execute_stats_query([
            "PERCENTILE_CONT(ARRAY[0.5, 0.25]) WITHIN GROUP (ORDER BY [a]) AS [__quantiles__a]",
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [b]) AS [__median__b]",
        ])
        assert result == {"__quantiles__a": None, "__median__b": None}

    def test_bracketed_identifiers(self):
        conn = _Conn(percentile_rows=[(0, 1.0)])
        result = _backend(conn).execute_stats_query([
            (
                "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [odd]]name (x)]) "
                "AS [__median__odd]]name (x)]"
            ),
        ])
        assert "(VALUES (0, CAST([odd]]name (x)] AS FLOAT)))" in conn.sql[0]
        assert result == {"__median__odd]name (x)": 1.0}

    def test_fraction_outside_unit_interval_is_rejected(self):
        with pytest.raises(ValueError, match="between 0 and 1"):
            _backend(_Conn()).execute_stats_query([
                "PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY [a]) AS [__median__a]",
            ])

    def test_sampled_queries_read_the_same_sample(self):
        conn = _Conn()
        backend = _backend(conn, sample_size=1000)
        backend.execute_stats_query([
            "MIN([a]) AS [__min__a]",
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [a]) AS [__median__a]",
        ])
        sample = f"TABLESAMPLE (1000 ROWS) REPEATABLE ({backend._sample_seed})"
        assert all(sample in sql for sql in conn.sql) and len(conn.sql) == 2

    def test_end_to_end_profile_shape(self):
        conn = _Conn(
            aggregate_row={
                "__total_rows__": 4,
                "__null__x": 1,
                "__distinct__x": 3,
                "__min__x": 1,
                "__max__x": 9,
                "__mean__x": 4.0,
                "__std__x": 4.36,
            },
            percentile_rows=[(0, 2.0, 1.5, 5.5, 8.86)],
        )
        profiler = _profiler("interrogate")
        profiler.backend = _backend(conn)
        exprs = ["COUNT(*) AS [__total_rows__]"] + profiler._build_column_agg_exprs("x", "int")
        results = profiler.backend.execute_stats_query(exprs)
        profile = profiler._build_column_profile("x", "int", "int", results, 4)
        assert profile.numeric.median == 2.0
        assert profile.numeric.percentiles == {"p25": 1.5, "p75": 5.5, "p99": 8.86}
