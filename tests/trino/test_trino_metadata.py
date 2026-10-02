"""Rules Trino settles from metadata must give the Polars tier's answer.

Every test compares a metadata decision with the same rule run without
preplan and without pushdown, on the frame the Trino materializer builds.
"""

from __future__ import annotations

import subprocess

import pytest
import trino

import kontra
from kontra.rule_defs.builtin.dtype import DtypeRule

_URI = "trino://kontra@localhost:8095/{catalog}/kontra.{table}"

# One column per Trino type, NULL in one row, so an empty or all-NULL column
# isn't what decides the dtype. JSON exists in the memory connector only.
_TYPES = {
    "c_tinyint": ("tinyint", "TINYINT '7'"),
    "c_smallint": ("smallint", "SMALLINT '7'"),
    "c_integer": ("integer", "7"),
    "c_bigint": ("bigint", "BIGINT '7'"),
    "c_real": ("real", "REAL '1.5'"),
    "c_double": ("double", "DOUBLE '1.5'"),
    "c_decimal": ("decimal(12,2)", "DECIMAL '12.50'"),
    "c_decimal38": ("decimal(38,0)", "DECIMAL '12'"),
    "c_varchar": ("varchar", "'x'"),
    "c_varchar30": ("varchar(30)", "'x'"),
    "c_boolean": ("boolean", "TRUE"),
    "c_date": ("date", "DATE '2026-01-01'"),
    "c_ts6": ("timestamp(6)", "TIMESTAMP '2026-01-01 00:00:00.123456'"),
    "c_tstz6": ("timestamp(6) with time zone", "TIMESTAMP '2026-01-01 00:00:00.123456 UTC'"),
    "c_time6": ("time(6)", "TIME '01:02:03.123456'"),
    "c_varbinary": ("varbinary", "X'0102'"),
    "c_uuid": ("uuid", "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"),
    "c_array": ("array(integer)", "ARRAY[1, 2]"),
}
_MEMORY_ONLY = {
    "c_char": ("char(3)", "CAST('ab' AS char(3))"),
    "c_ts3": ("timestamp(3)", "TIMESTAMP '2026-01-01 00:00:00.123'"),
    "c_ts9": ("timestamp(9)", "TIMESTAMP '2026-01-01 00:00:00.123456789'"),
    "c_json": ("json", "JSON '{\"a\": 1}'"),
}


def _query(*statements: str) -> list:
    """Run statements on a fresh connection; return the last one's rows."""
    with trino.dbapi.connect(host="localhost", port=8095, user="kontra") as conn:
        cur = conn.cursor()
        rows: list = []
        for sql in statements:
            cur.execute(sql)
            rows = cur.fetchall()
        return rows


def _create(catalog: str, table: str, columns: dict) -> None:
    ddl = ", ".join(f"{name} {data_type}" for name, (data_type, _) in columns.items())
    values = ", ".join(value for _, value in columns.values())
    nulls = ", ".join("NULL" for _ in columns)
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.kontra",
        f"DROP TABLE IF EXISTS {catalog}.kontra.{table}",
        f"CREATE TABLE {catalog}.kontra.{table} ({ddl})",
        f"INSERT INTO {catalog}.kontra.{table} VALUES ({values}), ({nulls})",
    )


@pytest.fixture(scope="module", params=["iceberg", "memory"])
def typed_table(request, trino_container):
    catalog = request.param
    columns = dict(_TYPES)
    if catalog == "memory":
        columns.update(_MEMORY_ONLY)
    _create(catalog, "dtypes", columns)
    yield catalog, _URI.format(catalog=catalog, table="dtypes"), columns
    _query(f"DROP TABLE IF EXISTS {catalog}.kontra.dtypes")


def _dtype_rules(columns) -> list[dict]:
    return [
        {
            "name": "dtype",
            "id": f"{column}:{requested}",
            "params": {"column": column, "type": requested},
        }
        for column in columns
        for requested in DtypeRule._VALID_TYPES
    ]


@pytest.mark.integration
def test_dtype_from_metadata_matches_the_polars_frame(typed_table):
    """Every Trino type × every dtype name: metadata gives the Polars tier's answer."""
    from kontra.connectors.trino_types import polars_dtype

    catalog, uri, columns = typed_table
    rules = _dtype_rules(columns)
    meta = kontra.validate(uri, rules=rules, save=False)
    polars = kontra.validate(uri, rules=rules, save=False, preplan="off", pushdown="off")

    by_polars = {r.rule_id: r for r in polars.rules}
    assert len(meta.rules) == len(rules) == len(by_polars)
    for rule in meta.rules:
        column = rule.rule_id.split(":", 1)[0]
        assert by_polars[rule.rule_id].source == "polars", rule.rule_id
        assert rule.passed == by_polars[rule.rule_id].passed, (catalog, rule.rule_id)
        # Mapped types never reach the Polars tier; unmapped ones always do.
        mapped = polars_dtype(columns[column][0]) is not None
        assert (rule.source == "metadata") == mapped, (catalog, rule.rule_id, rule.source)


@pytest.mark.integration
def test_integer_column_is_int32_as_in_its_parquet_file(tmp_path, trino_container):
    """The intended change: an Iceberg ``integer`` reads as Int32, as its data file does."""
    _create("iceberg", "int32", {"n": ("integer", "7")})
    try:
        uri = _URI.format(catalog="iceberg", table="int32")
        rules = [
            {"name": "dtype", "id": t, "params": {"column": "n", "type": t}}
            for t in ("int32", "int64", "int")
        ]
        rows = _query('SELECT file_path FROM iceberg.kontra."int32$files" WHERE content = 0')
        # The catalog's local:// file system is rooted at /tmp in the container.
        in_container = "/tmp" + rows[0][0].removeprefix("local://")
        local = tmp_path / "int32.parquet"
        subprocess.run(
            ["docker", "cp", f"kontra-trino-test:{in_container}", str(local)],
            check=True,
            capture_output=True,
        )
        parquet = {
            r.rule_id: r.passed for r in kontra.validate(str(local), rules=rules, save=False).rules
        }
        for kwargs in ({}, {"preplan": "off", "pushdown": "off"}):
            got = {
                r.rule_id: r.passed
                for r in kontra.validate(uri, rules=rules, save=False, **kwargs).rules
            }
            assert got == parquet == {"int32": True, "int64": False, "int": True}, kwargs
    finally:
        _query("DROP TABLE IF EXISTS iceberg.kontra.int32")
