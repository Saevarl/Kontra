"""Unit tests for SqlServerBackend aggregate-arg widening (no live DB).

Widening casts INT aggregate arguments so SQL Server's INT accumulator does not
overflow on wide-range columns. These tests exercise the string parser directly.
"""
from __future__ import annotations

import pytest

from kontra.scout.backends.sqlserver_backend import SqlServerBackend


def widen(expr: str) -> str:
    # Method only touches string state; call it unbound with a bare instance.
    b = SqlServerBackend.__new__(SqlServerBackend)
    return SqlServerBackend._widen_aggregate_arg(b, expr)


class TestWidening:
    def test_avg_int_widened_to_float(self):
        assert widen("AVG([n]) AS [a]") == "AVG(CAST([n] AS FLOAT)) AS [a]"

    def test_sum_int_widened_to_bigint(self):
        assert widen("SUM([n]) AS [s]") == "SUM(CAST([n] AS BIGINT)) AS [s]"

    def test_real_existing_cast_skipped(self):
        expr = "AVG(CAST([n] AS FLOAT)) AS [a]"
        assert widen(expr) == expr

    def test_min_max_not_touched(self):
        assert widen("MIN([n]) AS [m]") == "MIN([n]) AS [m]"
        assert widen("MAX([n]) AS [m]") == "MAX([n]) AS [m]"

    def test_paren_inside_bracketed_identifier(self):
        # Column literally named with a paren must not split the arg early.
        assert widen("STDEV([amount (usd)]) AS [z]") == \
            "STDEV(CAST([amount (usd)] AS FLOAT)) AS [z]"

    def test_bracketed_identifier_named_like_a_cast_is_still_widened(self):
        # #3: a column literally named [amount AS BIGINT] must NOT be mistaken
        # for an existing cast; it must still be widened (or it can overflow).
        assert widen("AVG([amount AS BIGINT]) AS [a]") == \
            "AVG(CAST([amount AS BIGINT] AS FLOAT)) AS [a]"
