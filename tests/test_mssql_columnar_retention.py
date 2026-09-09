from __future__ import annotations

import json
from pathlib import Path

from dpone.config.load_config import LoadConfig
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import build_columnar_snapshot_request
from dpone.storage import LocalObjectStorageClient


def test_mssql_columnar_provider_writes_run_marker_for_object_storage_cleanup(tmp_path: Path) -> None:
    provider = MssqlColumnarSnapshotProvider(
        connector=_FakeMssqlConnector([[(1, "alpha")]]),
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    provider.snapshot(_request())

    marker_path = tmp_path / "store" / "s3" / "dpone-stage" / "msql" / "run-1" / "__dpone_run_marker.json"
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    assert marker["schema_version"] == "dpone.object_storage.run_marker.v1"
    assert marker["run_id"] == "run-1"
    assert marker["table"] == "orders"
    assert marker["status"] == "running"


def test_mssql_columnar_chunked_object_storage_cleans_root_marker_after_success(tmp_path: Path) -> None:
    client = LocalObjectStorageClient(tmp_path / "store")
    provider = MssqlColumnarSnapshotProvider(
        connector=_FakeMssqlConnector([[(1, "alpha")], [(2, "beta")]]),
        object_client=client,
        parquet_writer=_FakeParquetWriter(),
    )

    windows = list(provider.iter_object_storage_windows(_request()))
    for window in windows:
        window.cleanup()

    assert windows
    assert not client.exists(_uri("s3://dpone-stage/msql/run-1/__dpone_run_marker.json"))
    assert not client.list_prefix(_uri("s3://dpone-stage/msql/run-1/"))


def test_columnar_snapshot_request_carries_retention_options() -> None:
    request = build_columnar_snapshot_request(
        load_config=_load_config(),
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
    )

    assert request.options["retention"]["bucket_limit_bytes"] == "200GiB"
    assert request.options["retention"]["failed_run_ttl_hours"] == 24


def _request() -> ColumnarSnapshotRequest:
    return ColumnarSnapshotRequest(
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        uri_prefix="s3://dpone-stage/msql/run-1/",
        run_id="run-1",
        target_chunk_bytes=512 * 1024 * 1024,
        max_chunk_bytes=1024 * 1024,
        options={
            "batch_size": 1,
            "target_chunk_rows": 1,
            "clickhouse_read_access": {"mode": "named_collection", "named_collection": "dpone_stage"},
        },
    )


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        options={
            "run_id": "run-1",
            "native_transfer": {
                "snapshot": {
                    "columnar_fast_path": {
                        "mode": "required",
                        "object_storage": {
                            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
                            "retention": {
                                "bucket_limit_bytes": "200GiB",
                                "failed_run_ttl_hours": 24,
                            },
                            "clickhouse_read_access": {
                                "mode": "named_collection",
                                "named_collection": "dpone_stage",
                            },
                        },
                    }
                }
            },
        },
    )


class _FakeMssqlConnector:
    def __init__(self, batches: list[list[tuple[object, ...]]]) -> None:
        self.batches = batches

    def get_records_streaming(self, query: str, *, batch_size: int, as_dict: bool = False):
        del query, batch_size
        assert as_dict is False
        yield from self.batches


class _FakeParquetWriter:
    def is_available(self) -> bool:
        return True

    def write_chunk(self, *, local_path: Path, schema, rows, compression: str) -> int:
        del schema, compression
        materialized = list(rows)
        local_path.write_bytes(b"PAR1 fake parquet PAR1")
        return len(materialized)


def _uri(value: str):
    from dpone.storage import ObjectStorageUri

    return ObjectStorageUri.parse(value)
