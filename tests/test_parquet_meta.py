# tests/test_parquet_meta.py
"""
Tests for the pure-Python Parquet footer reader used by the metadata preplan.

Covers the adversarial findings from review:
- nanosecond timestamp stats must widen (floor min / ceil max), never truncate
  toward a false pass_meta
- hostile footers must raise ParquetMetaError quickly (no hangs, no leaks)
- nested schemas must match pyarrow's name/type surface so both readers make
  the same planner decisions
- value/type parity with pyarrow across the common type matrix
"""

import struct
import time as time_mod
from datetime import date, datetime, time, timezone

import pytest

from kontra.preplan.parquet_meta import (
    ParquetMetaError,
    read_parquet_meta,
    _ns_to_us,
)

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402


def _write(tmp_path, table, name="f.parquet", **kw):
    path = str(tmp_path / name)
    pq.write_table(table, path, **kw)
    return path


# ---------------------------------------------------------------------------
# Parity with pyarrow
# ---------------------------------------------------------------------------


def test_typed_matrix_matches_pyarrow(tmp_path):
    n = 10
    table = pa.table({
        "i64": pa.array(range(-5, 5), pa.int64()),
        "u64": pa.array([2**64 - 5 + i % 5 for i in range(n)], pa.uint64()),
        "f64": pa.array([i * 2.5 for i in range(n)], pa.float64()),
        "s": pa.array([f"név_{i}" for i in range(n)], pa.string()),
        "b": pa.array([i % 2 == 0 for i in range(n)], pa.bool_()),
        "d": pa.array([date(2020, 1, 1 + i) for i in range(n)], pa.date32()),
        "ts": pa.array([datetime(2021, 5, 1, 12, 0, i) for i in range(n)], pa.timestamp("us")),
        "ts_utc": pa.array(
            [datetime(2021, 5, 1, 12, 0, i, tzinfo=timezone.utc) for i in range(n)],
            pa.timestamp("us", tz="UTC"),
        ),
        "nulls": pa.array([None if i % 3 == 0 else i for i in range(n)], pa.int64()),
    })
    path = _write(tmp_path, table, row_group_size=4)

    mine = read_parquet_meta(path)
    md = pq.ParquetFile(path).metadata

    assert mine.num_rows == md.num_rows
    assert mine.num_row_groups == md.num_row_groups
    assert mine.schema_names == list(md.schema.names)
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for j in range(rg.num_columns):
            col = rg.column(j)
            st = col.statistics
            entry = mine.row_groups[i][str(col.path_in_schema)]
            if st.has_min_max:
                assert entry["min"] == st.min, (i, col.path_in_schema)
                assert entry["max"] == st.max, (i, col.path_in_schema)
            assert entry.get("null_count") == (st.null_count if st.has_null_count else None)


def test_nested_schema_matches_pyarrow_surface(tmp_path):
    table = pa.table({
        "flat": pa.array([1, 2, 3], pa.int64()),
        "s": pa.array([{"a": 1, "b": "x"}] * 3, pa.struct([("a", pa.int64()), ("b", pa.string())])),
    })
    path = _write(tmp_path, table)
    mine = read_parquet_meta(path)
    md = pq.ParquetFile(path).metadata

    # Bare leaf names, exactly as pyarrow reports them: nested leaves must not
    # satisfy the planner's dotted-column existence check in one path only.
    assert mine.schema_names == list(md.schema.names)
    # Nested top-level columns get no dtype entry (planner returns "unknown").
    assert "s" not in mine.schema_types
    assert mine.schema_types["flat"] == "int64"


# ---------------------------------------------------------------------------
# Nanosecond widening (wrong-pass_meta regression)
# ---------------------------------------------------------------------------


def test_ns_to_us_floor_and_ceil():
    assert _ns_to_us(2500, ceil=False) == 2
    assert _ns_to_us(2500, ceil=True) == 3
    assert _ns_to_us(2000, ceil=True) == 2  # exact values don't widen
    assert _ns_to_us(-2500, ceil=False) == -3  # floor toward -inf
    assert _ns_to_us(-2500, ceil=True) == -2


def test_ns_timestamp_stats_widen_not_truncate(tmp_path):
    # max is 2500ns; truncation would report 2us (below the true max) and
    # let the planner "prove" col <= 2us passes when a violation exists.
    vals = pa.array([1500, 2500, 999], pa.timestamp("ns"))
    path = _write(tmp_path, pa.table({"t": vals}))
    mine = read_parquet_meta(path)
    entry = mine.row_groups[0]["t"]
    assert entry["min"] == datetime(1970, 1, 1, 0, 0, 0, 0)  # floor(999ns)
    assert entry["max"] == datetime(1970, 1, 1, 0, 0, 0, 3)  # ceil(2500ns)


def test_ns_timestamp_preplan_defers_instead_of_false_pass(tmp_path):
    from kontra.preplan.planner import preplan_single_parquet

    vals = pa.array([1500, 2500, 999], pa.timestamp("ns"))
    path = _write(tmp_path, pa.table({"t": vals}))
    pre = preplan_single_parquet(
        path=path,
        required_columns=["t"],
        predicates=[("r1", "t", "<=", "1970-01-01T00:00:00.000002")],
    )
    # 2500ns > 2us: a violation exists, so pass_meta would be a wrong PASS.
    assert pre.rule_decisions["r1"] != "pass_meta"


# ---------------------------------------------------------------------------
# Hostile footers
# ---------------------------------------------------------------------------


def _hostile_file(tmp_path, footer: bytes, name: str) -> str:
    path = tmp_path / name
    payload = b"PAR1" + footer + struct.pack("<I", len(footer)) + b"PAR1"
    path.write_bytes(payload)
    return str(path)


def test_bad_magic_raises(tmp_path):
    path = tmp_path / "bad.parquet"
    path.write_bytes(b"PAR1" + b"\x00" * 16 + b"NOPE")
    with pytest.raises(ParquetMetaError):
        read_parquet_meta(str(path))


def test_truncated_and_lying_footer_len_raise(tmp_path):
    path = tmp_path / "tiny.parquet"
    path.write_bytes(b"PAR1")
    with pytest.raises(ParquetMetaError):
        read_parquet_meta(str(path))

    path2 = tmp_path / "lie.parquet"
    path2.write_bytes(b"PAR1" + b"\x00" * 8 + struct.pack("<I", 10_000) + b"PAR1")
    with pytest.raises(ParquetMetaError):
        read_parquet_meta(str(path2))


def test_hostile_list_size_fails_fast(tmp_path):
    # FileMetaData field 2 (schema): LIST header claiming 50M byte elements
    # in a 10-byte footer. Must raise ParquetMetaError in well under a second.
    hostile = bytes([0x19, 0xF3]) + b"\x80\xd0\xac\xf3\x2e" + b"\x00"
    path = _hostile_file(tmp_path, hostile, "dos.parquet")
    t0 = time_mod.perf_counter()
    with pytest.raises(ParquetMetaError):
        read_parquet_meta(path)
    assert time_mod.perf_counter() - t0 < 0.5


def test_garbage_footer_raises_parquet_meta_error(tmp_path):
    for i, junk in enumerate([b"\xff" * 64, b"\x19", b"\x0c" * 32]):
        path = _hostile_file(tmp_path, junk, f"junk{i}.parquet")
        with pytest.raises(ParquetMetaError):
            read_parquet_meta(path)


# ---------------------------------------------------------------------------
# Conservative declines
# ---------------------------------------------------------------------------


def test_decimal_int96_binary_decline_to_none(tmp_path):
    from decimal import Decimal

    table = pa.table({
        "dec": pa.array([Decimal("1.25"), Decimal("2.50")], pa.decimal128(10, 2)),
        "bin": pa.array([b"\x01", b"\x02"], pa.binary()),
    })
    path = _write(tmp_path, table)
    mine = read_parquet_meta(path)
    for colname in ("dec", "bin"):
        entry = mine.row_groups[0].get(colname)
        if entry is not None:
            assert entry["min"] is None and entry["max"] is None


def test_all_null_column(tmp_path):
    table = pa.table({"x": pa.array([None, None, None], pa.float64())})
    path = _write(tmp_path, table)
    mine = read_parquet_meta(path)
    entry = mine.row_groups[0]["x"]
    assert entry["min"] is None and entry["max"] is None
    assert entry["null_count"] == 3
