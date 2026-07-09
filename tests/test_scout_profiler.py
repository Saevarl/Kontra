"""Unit tests for ScoutProfiler post-processing fixes (no live DB required):

Bug 8: constant (distinct_count == 1) columns must surface their single value
       in top_values / values on every backend and preset.
Bug 7: ESTIMATED distinct counts on identifier-like columns on small tables are
       replaced by an exact COUNT(DISTINCT) so uniqueness_ratio is trustworthy.

These use a fake backend so they exercise the profiler logic deterministically.
"""

from __future__ import annotations

import pytest

from kontra.scout.profiler import ScoutProfiler, _is_identifier_name
from kontra.scout.types import ColumnProfile, TopValue


class FakeBackend:
    """Minimal backend exposing only the methods the fixes call."""

    def __init__(self, top_values_map=None, stats_result=None):
        self.top_values_map = top_values_map or {}
        self.stats_result = stats_result or {}
        self.top_values_calls = []
        self.stats_calls = []
        self.source_format = "parquet"

    def esc_ident(self, name):
        return '"' + name.replace('"', '""') + '"'

    def fetch_top_values(self, column, limit):
        self.top_values_calls.append((column, limit))
        return self.top_values_map.get(column, [])

    def execute_stats_query(self, exprs):
        self.stats_calls.append(exprs)
        return dict(self.stats_result)


def _profiler(preset="scan", backend=None):
    p = ScoutProfiler("dummy.parquet", preset=preset)
    p.backend = backend if backend is not None else FakeBackend()
    p._effective_row_count = 0
    p._row_count_estimated = False
    return p


def _col(name, **kw):
    base = dict(name=name, dtype="string", dtype_raw="text", row_count=100)
    base.update(kw)
    return ColumnProfile(**base)


# --------------------------------------------------------------------------
# _is_identifier_name
# --------------------------------------------------------------------------

class TestIdentifierNameHeuristic:
    @pytest.mark.parametrize("name", [
        "id", "ID", "uuid", "guid", "user_id", "order_uuid", "tenant_key",
        "customerId", "orderID", "session_guid", "rowid",
    ])
    def test_positive(self, name):
        assert _is_identifier_name(name) is True

    @pytest.mark.parametrize("name", [
        "amount", "status", "name", "valid", "paid", "grid", "void", "price",
        "created_at", "customerid", "",
    ])
    def test_negative(self, name):
        assert _is_identifier_name(name) is False


# --------------------------------------------------------------------------
# Bug 8: constant columns surface their value
# --------------------------------------------------------------------------

class TestConstantValueSurfacing:
    def test_synthesizes_top_values_from_known_values_no_query(self):
        """When the single value is already known (e.g. pg_stats MCV), top_values
        is synthesized for free -- no table scan."""
        backend = FakeBackend()
        p = _profiler("scout", backend)  # metadata-only preset
        col = _col("status", distinct_count=1, null_count=0,
                   values=["ACTIVE"], distinct_count_estimated=True)
        p._ensure_constant_values_surfaced([col], row_count=100)

        assert [(t.value, t.count) for t in col.top_values] == [("ACTIVE", 100)]
        assert col.is_low_cardinality is True
        assert backend.top_values_calls == []  # no scan

    def test_count_is_non_null_rows(self):
        backend = FakeBackend()
        p = _profiler("scout", backend)
        col = _col("status", distinct_count=1, null_count=10, values=["X"])
        p._ensure_constant_values_surfaced([col], row_count=100)
        assert col.top_values[0].count == 90  # 100 - 10 nulls

    def test_fetches_value_when_unknown(self):
        """When the value is not known from metadata, a single targeted GROUP BY
        surfaces it (SQL Server has no MCV metadata)."""
        backend = FakeBackend(top_values_map={"status": [("ACTIVE", 100)]})
        p = _profiler("scout", backend)
        col = _col("status", distinct_count=1, null_count=0, values=None)
        p._ensure_constant_values_surfaced([col], row_count=100)

        assert [(t.value, t.count) for t in col.top_values] == [("ACTIVE", 100)]
        assert col.values == ["ACTIVE"]
        assert backend.top_values_calls == [("status", 1)]

    def test_existing_top_values_mirrored_into_values(self):
        backend = FakeBackend()
        p = _profiler("scan", backend)
        col = _col("status", distinct_count=1, null_count=0,
                   top_values=[TopValue("ACTIVE", 100, 100.0)], values=None)
        p._ensure_constant_values_surfaced([col], row_count=100)
        assert col.values == ["ACTIVE"]
        assert backend.top_values_calls == []

    def test_non_constant_columns_untouched(self):
        backend = FakeBackend(top_values_map={"x": [("a", 1)]})
        p = _profiler("scan", backend)
        col = _col("x", distinct_count=5, null_count=0, values=None)
        p._ensure_constant_values_surfaced([col], row_count=100)
        assert col.top_values == []
        assert backend.top_values_calls == []


# --------------------------------------------------------------------------
# Bug 7: exact distinct fallback for identifier columns
# --------------------------------------------------------------------------

class TestIdentifierExactDistinct:
    def _stats(self, distinct, nulls, rows):
        return {"__exd__id": distinct, "__exn__id": nulls, "__exrows__": rows}

    def test_estimated_id_refined_to_exact_ratio_one(self):
        """A truly-unique id reported with a fake < 1 ratio is corrected to
        an exact distinct count and uniqueness_ratio == 1.0."""
        backend = FakeBackend(stats_result=self._stats(500, 0, 500))
        p = _profiler("scan", backend)
        p._row_count_estimated = True
        col = _col("id", dtype="int", dtype_raw="bigint", row_count=500,
                   distinct_count=400, null_count=0,
                   uniqueness_ratio=0.8, distinct_count_estimated=True)

        p._refine_identifier_distinct_counts([col], row_count=500)

        assert col.distinct_count == 500
        assert col.uniqueness_ratio == 1.0
        assert col.distinct_count_estimated is False
        assert len(backend.stats_calls) == 1
        # Same-moment exact COUNT(*) adopted as row count
        assert p._effective_row_count == 500
        assert p._row_count_estimated is False

    def test_non_estimated_untouched(self):
        """Exact distinct counts (parquet scan / full scan) are never re-queried."""
        backend = FakeBackend(stats_result=self._stats(500, 0, 500))
        p = _profiler("scan", backend)
        col = _col("id", dtype="int", distinct_count=500, null_count=0,
                   uniqueness_ratio=1.0, distinct_count_estimated=False)
        p._refine_identifier_distinct_counts([col], row_count=500)
        assert backend.stats_calls == []

    def test_non_identifier_untouched(self):
        backend = FakeBackend(stats_result={"__exd__amount": 400})
        p = _profiler("scan", backend)
        col = _col("amount", dtype="float", distinct_count=400, null_count=0,
                   distinct_count_estimated=True)
        p._refine_identifier_distinct_counts([col], row_count=500)
        assert backend.stats_calls == []

    def test_large_table_skipped(self):
        backend = FakeBackend(stats_result=self._stats(1, 0, 1))
        p = _profiler("scan", backend)
        col = _col("id", dtype="int", distinct_count=400, null_count=0,
                   distinct_count_estimated=True)
        p._refine_identifier_distinct_counts(
            [col], row_count=ScoutProfiler._EXACT_DISTINCT_ROW_THRESHOLD + 1)
        assert backend.stats_calls == []

    def test_sampled_skipped(self):
        backend = FakeBackend(stats_result=self._stats(1, 0, 1))
        p = _profiler("scan", backend)
        p.sample_size = 100
        col = _col("id", dtype="int", distinct_count=400, null_count=0,
                   distinct_count_estimated=True)
        p._refine_identifier_distinct_counts([col], row_count=500)
        assert backend.stats_calls == []

    def test_metadata_only_preset_skipped(self):
        """scout (metadata-only) must stay scan-free; the estimate is kept."""
        backend = FakeBackend(stats_result=self._stats(500, 0, 500))
        p = _profiler("scout", backend)
        col = _col("id", dtype="int", distinct_count=400, null_count=0,
                   uniqueness_ratio=0.8, distinct_count_estimated=True)
        p._refine_identifier_distinct_counts([col], row_count=500)
        assert backend.stats_calls == []
        assert col.distinct_count_estimated is True  # still flagged
