from kontra.connectors.handle import DatasetHandle
from kontra.engine.materializers.duckdb import DuckDBMaterializer
from kontra.scout.backends import duckdb_backend
from kontra.scout.backends.duckdb_backend import DuckDBBackend


def test_duckdb_materializer_selects_json_reader_for_json_files():
    for path in ("events.json", "events.jsonl", "events.ndjson"):
        materializer = DuckDBMaterializer(DatasetHandle.from_uri(path))

        assert materializer._get_read_function() == "read_json_auto"


def test_scout_reads_json_lines(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"id":1,"kind":"open"}\n{"id":2,"kind":"close"}\n')
    backend = DuckDBBackend(DatasetHandle.from_uri(str(path)))

    backend.connect()
    try:
        assert backend.get_schema() == [("id", "BIGINT"), ("kind", "VARCHAR")]
        assert backend.get_row_count() == 2
    finally:
        backend.close()


def test_scout_parquet_metadata_honors_non_tls_s3_endpoint(monkeypatch):
    handle = DatasetHandle(
        uri="s3://lab/events.parquet",
        scheme="s3",
        path="s3://lab/events.parquet",
        format="parquet",
        fs_opts={
            "s3_access_key_id": "test",
            "s3_secret_access_key": "test",
            "s3_endpoint": "127.0.0.1:9000",
            "s3_use_ssl": "false",
            "s3_url_style": "path",
        },
    )
    filesystem_options = {}

    def fake_filesystem(**options):
        filesystem_options.update(options)
        return object()

    class FakeParquetFile:
        metadata = object()

    monkeypatch.setattr(duckdb_backend.pafs, "S3FileSystem", fake_filesystem)
    monkeypatch.setattr(
        duckdb_backend.pq,
        "ParquetFile",
        lambda uri, filesystem: FakeParquetFile(),
    )

    assert DuckDBBackend(handle)._get_parquet_metadata() is FakeParquetFile.metadata
    assert filesystem_options["scheme"] == "http"
    assert filesystem_options["endpoint_override"] == "127.0.0.1:9000"
