"""
Live ClickHouse Scout profiler tests.

Exercises the ClickHouseBackend end-to-end against the seeded ``ch_test_users``
table (see conftest.py's ``clickhouse_seed``):

    user_id UInt32          1, 2, 3, 4                 -> null=0 distinct=4
    email   String          a@b.com,bad,c@d.com,e@f.com-> null=0 distinct=4
    status  Nullable(String) active,inactive,NULL,active-> null=1 distinct=2
    age     Nullable(Int32) 25, NULL, 120, -1          -> null=1 distinct=3
    note    String          'abc   ',x,yy,zzz          -> null=0 distinct=4

The whole directory is skipped by conftest if the container is unreachable.
"""

from __future__ import annotations

import pytest

import kontra
from kontra.scout.backends.clickhouse_backend import normalize_clickhouse_type
from kontra.scout.dtype_mapping import normalize_dtype


def _by_name(profile):
    return {c.name: c for c in profile.columns}


# --------------------------------------------------------------------------- #
# Row count
# --------------------------------------------------------------------------- #

def test_row_count(clickhouse_uri):
    profile = kontra.profile(clickhouse_uri, save=False)
    assert profile.row_count == 4
    assert profile.row_count_estimated is False
    assert profile.column_count == 5
    assert profile.source_format == "clickhouse"


# --------------------------------------------------------------------------- #
# Null counts (scan preset — exact aggregate path)
# --------------------------------------------------------------------------- #

def test_null_counts(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    assert cols["status"].null_count == 1
    assert cols["age"].null_count == 1
    assert cols["email"].null_count == 0
    assert cols["user_id"].null_count == 0
    assert cols["note"].null_count == 0


def test_distinct_counts(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    assert cols["user_id"].distinct_count == 4
    assert cols["email"].distinct_count == 4
    assert cols["status"].distinct_count == 2
    assert cols["age"].distinct_count == 3
    assert cols["note"].distinct_count == 4


# --------------------------------------------------------------------------- #
# Dtype normalization
# --------------------------------------------------------------------------- #

def test_column_dtypes(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    assert cols["user_id"].dtype == "int"   # UInt32
    assert cols["email"].dtype == "string"  # String
    assert cols["status"].dtype == "string"  # Nullable(String)
    assert cols["age"].dtype == "int"       # Nullable(Int32)
    assert cols["note"].dtype == "string"


def test_normalize_clickhouse_type_unit():
    assert normalize_clickhouse_type("UInt32") == "int"
    assert normalize_clickhouse_type("Int64") == "int"
    assert normalize_clickhouse_type("Nullable(Int32)") == "int"
    assert normalize_clickhouse_type("Float64") == "float"
    assert normalize_clickhouse_type("Decimal(10, 2)") == "float"
    assert normalize_clickhouse_type("String") == "string"
    assert normalize_clickhouse_type("FixedString(4)") == "string"
    assert normalize_clickhouse_type("LowCardinality(String)") == "string"
    assert normalize_clickhouse_type("LowCardinality(Nullable(String))") == "string"
    assert normalize_clickhouse_type("Bool") == "bool"
    assert normalize_clickhouse_type("Date") == "date"
    assert normalize_clickhouse_type("Date32") == "date"
    assert normalize_clickhouse_type("DateTime") == "datetime"
    assert normalize_clickhouse_type("DateTime64(3)") == "datetime"
    assert normalize_clickhouse_type("SomethingExotic") == "unknown"


def test_shared_normalize_dtype_handles_clickhouse_wrappers():
    # The profiler calls normalize_dtype directly, so it must unwrap the
    # ClickHouse Nullable(...) / LowCardinality(...) wrappers.
    assert normalize_dtype("Nullable(Int32)") == "int"
    assert normalize_dtype("LowCardinality(String)") == "string"
    assert normalize_dtype("Float64") == "float"


# --------------------------------------------------------------------------- #
# Type-specific stats (pushed down as aggregates)
# --------------------------------------------------------------------------- #

def test_numeric_stats(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    age = cols["age"]
    assert age.numeric is not None
    assert age.numeric.min == -1.0
    assert age.numeric.max == 120.0
    # mean of (25, 120, -1) ignoring NULL = 48.0
    assert age.numeric.mean == pytest.approx(48.0)


def test_string_stats(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    email = cols["email"]
    assert email.string is not None
    # 'bad' (3) .. 'a@b.com'/'c@d.com'/'e@f.com' (7)
    assert email.string.min_length == 3
    assert email.string.max_length == 7


def test_top_values(clickhouse_uri):
    cols = _by_name(kontra.profile(clickhouse_uri, save=False))
    status = cols["status"]
    assert status.top_values is not None
    top = {tv.value: tv.count for tv in status.top_values}
    assert top.get("active") == 2


# --------------------------------------------------------------------------- #
# Preset coverage
# --------------------------------------------------------------------------- #

def test_scout_preset_metadata_path(clickhouse_uri):
    # scout uses the metadata-only fast path (non-Nullable => null=0 from schema).
    profile = kontra.profile(clickhouse_uri, preset="scout", save=False)
    assert profile.row_count == 4
    cols = _by_name(profile)
    assert cols["status"].null_count == 1
    assert cols["age"].null_count == 1
    assert cols["email"].null_count == 0
    assert cols["user_id"].null_count == 0
    # distinct counts are exact even on the scout path
    assert cols["status"].distinct_count == 2
    assert cols["age"].distinct_count == 3
    # exact, not estimated
    assert cols["status"].null_count_estimated is False
    assert cols["status"].distinct_count_estimated is False


def test_interrogate_preset(clickhouse_uri):
    profile = kontra.profile(clickhouse_uri, preset="interrogate", save=False)
    assert profile.row_count == 4
    cols = _by_name(profile)
    assert cols["age"].dtype == "int"
    assert cols["age"].numeric is not None
    assert cols["status"].null_count == 1


def test_to_llm_smoke(clickhouse_uri):
    text = kontra.profile(clickhouse_uri, save=False).to_llm()
    assert "rows=4" in text
    assert "user_id" in text
    # credentials must be masked
    assert "kontra_test@" not in text
