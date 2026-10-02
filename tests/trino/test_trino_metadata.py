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


# --------------------------------------------------------------------------- #
# $files: counts and bounds
# --------------------------------------------------------------------------- #

FILES_CATALOGS = ["iceberg", "iceberg_jdbc"]
_LONG = "prefix-common-" + "x" * 20  # longer than Iceberg's 16-character string bounds

# Four inserts, four data files; the last holds two rows (nn 4 and 6).
# n has one NULL; f has NaN and no NULL.
_FILES_DDL = (
    "id bigint, n integer, nn integer, amt decimal(12,2), d date, f double, s varchar, "
    "cat varchar, ts timestamp(6)"
)
_FILES_ROWS = [
    "(1, 5, 1, DECIMAL '1.50', DATE '2020-01-01', 1.0, '{long}a', 'a', TIMESTAMP '2026-01-05 00:00:00')",
    (
        "(2, NULL, 2, DECIMAL '2.00', DATE '2020-02-01', nan(), '{long}b', 'b', "
        "TIMESTAMP '2026-02-05 00:00:00')"
    ),
    (
        "(3, 30, 3, DECIMAL '-4.00', DATE '2021-01-01', 2.5, '{long}c', 'a', "
        "TIMESTAMP '2026-03-05 00:00:00')"
    ),
    (
        "(4, 7, 4, DECIMAL '3.00', DATE '2020-06-01', 3.0, '{long}d', 'b', "
        "TIMESTAMP '2026-04-05 00:00:00'), "
        "(5, 8, 6, DECIMAL '4.00', DATE '2020-07-01', 3.5, '{long}e', 'a', "
        "TIMESTAMP '2026-04-06 00:00:00')"
    ),
]


def _files_rules() -> list[dict]:
    """Rules $files can settle, and rules it must leave to the scan."""
    r = []

    def add(rule_id, name, **params):
        r.append({"name": name, "id": rule_id, "params": params})

    for column in ("id", "n", "nn", "amt", "d", "f", "s"):
        add(f"not_null:{column}", "not_null", column=column)
    add("range:nn:in", "range", column="nn", min=1, max=6)
    add("range:nn:straddle", "range", column="nn", min=1, max=5)  # bounds can't tell
    add("range:nn:above", "range", column="nn", min=10)  # every file is below 10
    add("range:id:max", "range", column="id", max=1)  # the files from id 2 lie above
    add("range:n:nulls", "range", column="n", min=0, max=100)
    add("range:amt:in", "range", column="amt", min=-10, max=10)
    add("range:amt:out", "range", column="amt", max=-5)
    add("range:amt:frac", "range", column="amt", min=-4.5)  # fractional: left to the scan
    add("range:d:in", "range", column="d", min="2020-01-01", max="2021-12-31")
    add("range:d:out", "range", column="d", min="2022-01-01")
    add("range:f", "range", column="f", min=0, max=10)  # floats: never from bounds
    add("cnn:nn", "conditional_not_null", column="nn", when="cat == 'a'")
    add("cnn:n", "conditional_not_null", column="n", when="cat == 'a'")  # NULL is at cat 'b'
    add("min_rows:ok", "min_rows", threshold=5)
    add("min_rows:short", "min_rows", threshold=6)
    add("max_rows:ok", "max_rows", threshold=5)
    add("max_rows:over", "max_rows", threshold=4)
    add("length:s", "length", column="s", max=40)  # string bounds are truncated
    return r


# rule -> source on a table without deletes. Anything not listed must scan.
_FROM_METADATA = {
    "not_null:id",
    "not_null:n",
    "not_null:nn",
    "not_null:amt",
    "not_null:d",
    "not_null:f",
    "not_null:s",
    "range:nn:in",
    "range:nn:above",
    "range:id:max",
    "range:n:nulls",
    "range:amt:in",
    "range:amt:out",
    "range:d:in",
    "range:d:out",
    "cnn:nn",
    "min_rows:ok",
    "max_rows:ok",
}


def _make_files_table(catalog: str, name: str, extra: str = "") -> str:
    fq = f"{catalog}.kontra_meta.{name}"
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.kontra_meta",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} ({_FILES_DDL}){extra}",
        *(f"INSERT INTO {fq} VALUES {row.format(long=_LONG)}" for row in _FILES_ROWS),
    )
    return fq


def _three_ways(uri: str, rules: list[dict], tally: bool = False):
    """(metadata on, scan without metadata, Polars tier), each by rule id."""
    runs = (
        {},
        {"preplan": "off"},
        {"preplan": "off", "pushdown": "off"},
    )
    return [
        {
            r.rule_id: r
            for r in kontra.validate(uri, rules=rules, tally=tally, save=False, **kw).rules
        }
        for kw in runs
    ]


def _assert_same_answers(meta, scan, polars):
    assert set(meta) == set(scan) == set(polars)
    for rule_id, rule in meta.items():
        assert rule.passed == scan[rule_id].passed == polars[rule_id].passed, rule_id
        if rule.source == "metadata":
            # A metadata FAIL reports 1, as Kontra's other preplan sources do in
            # fail-fast mode; the scan may count more (study Q6).
            assert rule.failed_count == (0 if rule.passed else 1), rule_id


@pytest.fixture(params=FILES_CATALOGS)
def files_catalog(request, trino_container):
    yield request.param
    _query(
        *(f"DROP TABLE IF EXISTS {request.param}.kontra_meta.{t}" for t in _FILES_TABLES),
    )


_FILES_TABLES = ("plain", "deleted", "parted", "wide", "nostats", "allowed")


def _uri_of(fq: str) -> str:
    catalog, schema, table = fq.split(".")
    return f"trino://kontra@localhost:8095/{catalog}/{schema}.{table}"


@pytest.mark.integration
def test_files_answers_match_the_scan(files_catalog):
    """No deletes: counts and bounds settle these rules, with the scan's answers."""
    fq = _make_files_table(files_catalog, "plain")
    meta, scan, polars = _three_ways(_uri_of(fq), _files_rules())
    _assert_same_answers(meta, scan, polars)
    from_metadata = {rid for rid, r in meta.items() if r.source == "metadata"}
    assert from_metadata == _FROM_METADATA
    assert not any(r.source == "metadata" for r in scan.values())


@pytest.mark.integration
def test_deletes_allow_only_pass_proofs(files_catalog):
    """
    Data-file statistics keep deleted rows. After deleting n's only NULL and
    the rows outside some ranges, metadata still sees them: it may prove a
    PASS but never a FAIL or a row count.
    """
    fq = _make_files_table(files_catalog, "deleted")
    _query(f"DELETE FROM {fq} WHERE id IN (2, 3, 5)")
    deletes = f'SELECT count(*) FROM {files_catalog}.kontra_meta."deleted$files" WHERE content <> 0'
    assert _query(deletes)[0][0] > 0
    meta, scan, polars = _three_ways(_uri_of(fq), _files_rules())
    _assert_same_answers(meta, scan, polars)
    from_metadata = {rid for rid, r in meta.items() if r.source == "metadata"}
    # Only PASS proofs: n's only NULL is deleted, which the statistics still count.
    assert all(meta[rid].passed for rid in from_metadata)
    assert "not_null:n" not in from_metadata and meta["not_null:n"].passed
    assert not any(rid.endswith(("rows:ok", "rows:short", "rows:over")) for rid in from_metadata)
    for rid in ("not_null:id", "range:nn:in", "range:amt:in", "cnn:nn"):
        assert rid in from_metadata, rid


@pytest.mark.integration
def test_partitioned_table(files_catalog):
    fq = _make_files_table(
        files_catalog, "parted", " WITH (partitioning = ARRAY['cat', 'month(ts)'])"
    )
    meta, scan, polars = _three_ways(_uri_of(fq), _files_rules())
    _assert_same_answers(meta, scan, polars)
    assert {rid for rid, r in meta.items() if r.source == "metadata"} == _FROM_METADATA | {
        # Partitioning splits nn 4 and 6 into two files; the file of 6 lies above 5.
        "range:nn:straddle"
    }


@pytest.mark.integration
def test_more_than_100_columns(files_catalog):
    """Iceberg's default keeps full metrics for the first 100 columns only."""
    fq = f"{files_catalog}.kontra_meta.wide"
    columns = ", ".join(f"c{i} integer" for i in range(1, 106))
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {files_catalog}.kontra_meta",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} ({columns})",
        f"INSERT INTO {fq} VALUES ({', '.join(str(i) for i in range(1, 106))})",
        f"INSERT INTO {fq} (c1) VALUES (0)",
    )
    rules = [
        {"name": "not_null", "id": f"nn:c{i}", "params": {"column": f"c{i}"}} for i in (1, 100, 105)
    ] + [
        {"name": "range", "id": f"range:c{i}", "params": {"column": f"c{i}", "min": 0, "max": 200}}
        for i in (1, 100, 105)
    ]
    meta, scan, polars = _three_ways(_uri_of(fq), rules)
    _assert_same_answers(meta, scan, polars)
    # Trino writes full metrics for every column, so all six are settled.
    assert all(r.source == "metadata" for r in meta.values())


@pytest.mark.integration
def test_files_without_metrics_leave_rules_to_the_scan(files_catalog, tmp_path):
    """A data file with no null count and no bounds for a column (written outside Trino)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    fq = f"{files_catalog}.kontra_meta.nostats"
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {files_catalog}.kontra_meta",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (id bigint, v integer)",
        f"INSERT INTO {fq} VALUES (1, 5)",
    )
    # Statistics for id only: Iceberg records no null count or bounds for v.
    local = tmp_path / "nostats.parquet"
    pq.write_table(
        pa.table({"id": pa.array([2, 3], pa.int64()), "v": pa.array([7, 8], pa.int32())}),
        local,
        write_statistics=["id"],
    )
    folder = f"/ext/{files_catalog}-nostats"
    subprocess.run(
        [
            "docker",
            "exec",
            "kontra-trino-test",
            "sh",
            "-c",
            f"rm -rf /tmp{folder} && mkdir -p /tmp{folder}",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["docker", "cp", str(local), f"kontra-trino-test:/tmp{folder}/f.parquet"],
        check=True,
        capture_output=True,
    )
    _query(
        f"ALTER TABLE {fq} EXECUTE add_files(location => 'local://{folder}', format => 'PARQUET')"
    )
    rules = [
        {"name": "not_null", "id": "nn:id", "params": {"column": "id"}},
        {"name": "not_null", "id": "nn:v", "params": {"column": "v"}},
        {"name": "range", "id": "range:id", "params": {"column": "id", "min": 0, "max": 9}},
        {"name": "range", "id": "range:v", "params": {"column": "v", "min": 0, "max": 9}},
        {
            "name": "conditional_not_null",
            "id": "cnn:v",
            "params": {"column": "v", "when": "id == 2"},
        },
    ]
    meta, scan, polars = _three_ways(_uri_of(fq), rules)
    _assert_same_answers(meta, scan, polars)
    assert {rid for rid, r in meta.items() if r.source == "metadata"} == {"nn:id", "range:id"}


@pytest.mark.integration
def test_tally_reads_no_files(files_catalog, monkeypatch):
    """tally=True rules need exact counts and skip preplan; $files isn't read for them."""
    import kontra.preplan.trino as trino_preplan

    fq = _make_files_table(files_catalog, "plain")
    reads = []
    original = trino_preplan._read_files
    monkeypatch.setattr(
        trino_preplan, "_read_files", lambda *a, **k: reads.append(1) or original(*a, **k)
    )
    rules = [r for r in _files_rules() if not r["name"].endswith("_rows")]
    meta, scan, polars = _three_ways(_uri_of(fq), rules, tally=True)
    assert reads == []
    for rule_id, rule in meta.items():
        assert rule.source != "metadata", rule_id
        assert rule.failed_count == scan[rule_id].failed_count == polars[rule_id].failed_count, (
            rule_id
        )


@pytest.mark.integration
def test_files_failure_reports_its_cause_in_a_transaction(monkeypatch, trino_container):
    """
    A failed query aborts a Trino transaction. In one, a $files failure is
    raised as itself; outside one (a caller's autocommit connection), preplan
    is skipped and the scan answers.
    """
    import kontra.preplan.trino as trino_preplan

    fq = _make_files_table("iceberg", "plain")

    def broken(handle, columns, partitions=()):
        with kontra.connectors.db_utils.get_connection_ctx(handle, "trino") as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM iceberg.kontra_meta."no_such_table$files"')
            cur.fetchall()

    monkeypatch.setattr(trino_preplan, "_read_files", broken)
    rules = [{"name": "not_null", "id": "nn", "params": {"column": "n"}}]
    with pytest.raises(Exception, match="no_such_table") as raised:
        kontra.validate(_uri_of(fq), rules=rules, save=False)
    assert "TRANSACTION_ALREADY_ABORTED" not in str(raised.value)

    conn = trino.dbapi.connect(host="localhost", port=8095, user="kontra")
    try:
        result = kontra.validate(conn, table=fq, rules=rules, save=False)
    finally:
        conn.close()
    assert [(r.passed, r.source) for r in result.rules] == [(False, "sql")]


@pytest.mark.integration
@pytest.mark.parametrize("declared", ["", " NOT NULL"])
def test_conditional_on_a_missing_column_is_not_settled(files_catalog, declared):
    """
    A conditional_not_null whose condition names a missing column gets the
    generic not_null predicate too. Neither that proof nor a declared NOT NULL
    may settle it: like the scan and the Polars tier, it reports the missing
    column.
    """
    fq = f"{files_catalog}.kontra_meta.plain"
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {files_catalog}.kontra_meta",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (x integer{declared})",
        f"INSERT INTO {fq} VALUES 10",
    )
    rules = [
        {
            "name": "conditional_not_null",
            "id": "cnn",
            "params": {"column": "x", "when": "missing == 1"},
        }
    ]
    for kw in ({}, {"preplan": "off"}, {"preplan": "off", "pushdown": "off"}):
        with pytest.raises(trino.exceptions.TrinoUserError, match="COLUMN_NOT_FOUND"):
            kontra.validate(_uri_of(fq), rules=rules, save=False, **kw)


# --------------------------------------------------------------------------- #
# allowed_values from identity-partition values
# --------------------------------------------------------------------------- #


def _allowed_rules() -> list[dict]:
    def rule(rule_id, column, values):
        return {
            "name": "allowed_values",
            "id": rule_id,
            "params": {"column": column, "values": values},
        }

    return [
        rule("cat:all", "cat", ["a", "b"]),
        rule("cat:a", "cat", ["a"]),
        rule("cat:null", "cat", ["a", "b", None]),
        rule("cat:nonull", "cat", ["a"]),
        rule("k:all", "k", [1, 2]),
        rule("k:one", "k", [1]),
        rule("s:all", "s", ["x", "y"]),  # not a partition column: scan
        rule("k:float", "k", [1.0, 2.0]),  # floats against integers: scan
    ]


def _make_allowed_table(catalog: str, partitioning: str = "ARRAY['cat', 'k']") -> str:
    fq = f"{catalog}.kontra_meta.allowed"
    _query(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.kontra_meta",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (id bigint, cat varchar, k integer, s varchar, ts timestamp(6)) "
        f"WITH (partitioning = {partitioning})",
        f"INSERT INTO {fq} VALUES (1, 'a', 1, 'x', TIMESTAMP '2026-01-05 00:00:00'), "
        "(2, 'b', 2, 'y', TIMESTAMP '2026-02-05 00:00:00'), "
        "(3, 'a', 2, 'x', TIMESTAMP '2026-02-06 00:00:00')",
    )
    return fq


def _metadata_ids(meta) -> set[str]:
    return {rid for rid, r in meta.items() if r.source == "metadata"}


@pytest.mark.integration
def test_allowed_values_from_partition_values(files_catalog):
    """Identity partitions on a varchar and an integer column settle allowed_values."""
    fq = _make_allowed_table(files_catalog)
    meta, scan, polars = _three_ways(_uri_of(fq), _allowed_rules())
    _assert_same_answers(meta, scan, polars)
    assert _metadata_ids(meta) == {"cat:all", "cat:a", "cat:null", "cat:nonull", "k:all", "k:one"}
    assert [meta[r].passed for r in ("cat:all", "cat:a", "k:all", "k:one")] == [
        True,
        False,
        True,
        False,
    ]


@pytest.mark.integration
def test_allowed_values_null_partition(files_catalog):
    """A NULL partition value holds NULL in every row of its file."""
    fq = _make_allowed_table(files_catalog)
    _query(f"INSERT INTO {fq} VALUES (4, NULL, 1, 'x', TIMESTAMP '2026-03-01 00:00:00')")
    meta, scan, polars = _three_ways(_uri_of(fq), _allowed_rules())
    _assert_same_answers(meta, scan, polars)
    assert meta["cat:null"].passed and meta["cat:null"].source == "metadata"
    assert not meta["cat:all"].passed and meta["cat:all"].source == "metadata"


@pytest.mark.integration
def test_allowed_values_with_deletes_takes_only_passes(files_catalog):
    """
    A row delete keeps its data file: a value that is gone still shows as a
    partition, so only PASS proofs hold.
    """
    fq = _make_allowed_table(files_catalog, "ARRAY['cat']")
    _query(f"DELETE FROM {fq} WHERE id = 2")  # the only 'b' row, in a file with no other
    _query(f"INSERT INTO {fq} VALUES (5, 'b', 1, 'x', TIMESTAMP '2026-03-01 00:00:00')")
    _query(f"DELETE FROM {fq} WHERE id = 5")
    deletes = f'SELECT count(*) FROM {files_catalog}.kontra_meta."allowed$files" WHERE content <> 0'
    if _query(deletes)[0][0] == 0:
        pytest.skip("the catalog removed whole files instead of writing delete files")
    meta, scan, polars = _three_ways(_uri_of(fq), _allowed_rules())
    _assert_same_answers(meta, scan, polars)
    # 'b' is gone from the data, but its files remain: cat:a passes, by the scan.
    assert meta["cat:a"].passed and meta["cat:a"].source != "metadata"
    assert meta["cat:all"].passed and meta["cat:all"].source == "metadata"


@pytest.mark.integration
def test_allowed_values_after_partition_evolution(files_catalog):
    """
    Files written after cat leaves the spec read NULL for it. Their null
    count says they hold values, so their partition value isn't trusted.
    """
    fq = _make_allowed_table(files_catalog, "ARRAY['cat']")
    _query(
        f"ALTER TABLE {fq} SET PROPERTIES partitioning = ARRAY['month(ts)']",
        f"INSERT INTO {fq} VALUES (4, 'c', 1, 'x', TIMESTAMP '2026-03-01 00:00:00')",
    )
    meta, scan, polars = _three_ways(_uri_of(fq), _allowed_rules())
    _assert_same_answers(meta, scan, polars)
    # 'c' sits in a file whose spec has no cat: only the scan sees it.
    assert not meta["cat:all"].passed and meta["cat:all"].source != "metadata"
    assert not meta["cat:null"].passed and meta["cat:null"].source != "metadata"
    # 'b' is in a trusted file, so the FAIL is still proven.
    assert not meta["cat:a"].passed and meta["cat:a"].source == "metadata"


@pytest.mark.integration
def test_allowed_values_after_renaming_the_partition_column(files_catalog):
    """The partition field keeps the old name; the renamed column is left to the scan."""
    fq = _make_allowed_table(files_catalog, "ARRAY['cat']")
    _query(f"ALTER TABLE {fq} RENAME COLUMN cat TO category")
    rules = [
        {
            "name": "allowed_values",
            "id": "all",
            "params": {"column": "category", "values": ["a", "b"]},
        },
        {"name": "allowed_values", "id": "a", "params": {"column": "category", "values": ["a"]}},
    ]
    meta, scan, polars = _three_ways(_uri_of(fq), rules)
    _assert_same_answers(meta, scan, polars)
    assert _metadata_ids(meta) == set()
    assert [meta["all"].passed, meta["a"].passed] == [True, False]


@pytest.mark.integration
def test_allowed_values_on_an_unpartitioned_table(files_catalog):
    fq = _make_files_table(files_catalog, "plain")
    rules = [
        {"name": "allowed_values", "id": "cat", "params": {"column": "cat", "values": ["a", "b"]}}
    ]
    meta, scan, polars = _three_ways(_uri_of(fq), rules)
    _assert_same_answers(meta, scan, polars)
    assert meta["cat"].passed and meta["cat"].source != "metadata"
