from __future__ import annotations

from datetime import datetime, time
from pathlib import Path
from threading import Barrier

import pytest

from dpone.config.load_config import LoadConfig
from dpone.runtime.columnar_parquet_writer import PyArrowParquetChunkWriter, mssql_source_type_supported
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import build_columnar_snapshot_request
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


@pytest.mark.parametrize(
    "source_type",
    [
        "int",
        "numeric(38, 9)",
        "money",
        "float",
        "real",
        "date",
        "datetime2(7)",
        "datetimeoffset(7)",
        "timestamp",
        "rowversion",
        "nvarchar(max)",
        "uniqueidentifier",
    ],
)
def test_mssql_columnar_type_taxonomy_supports_certified_types(source_type: str) -> None:
    assert mssql_source_type_supported(source_type) is True


@pytest.mark.parametrize("source_type", ["sql_variant", "geometry"])
def test_mssql_columnar_type_taxonomy_blocks_unsafe_types(source_type: str) -> None:
    assert mssql_source_type_supported(source_type) is False


def test_mssql_columnar_provider_blocks_without_parquet_writer_before_source_io(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector([])
    writer = _FakeParquetWriter(available=False)
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=writer,
    )
    request = _request()

    capability = provider.capabilities(request)

    assert capability.certified is False
    assert capability.blockers == ("parquet_writer_unavailable",)
    with pytest.raises(RuntimeError, match="parquet_writer_unavailable"):
        provider.snapshot(request)
    assert connector.stream_calls == []


def test_mssql_columnar_provider_blocks_unsupported_type_before_source_io(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector([])
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    capability = provider.capabilities(_request(schema=[("payload", "sql_variant")]))

    assert capability.certified is False
    assert capability.blockers == ("mssql_columnar_type_unsupported:payload:sql_variant",)
    with pytest.raises(RuntimeError, match="mssql_columnar_type_unsupported"):
        provider.snapshot(_request(schema=[("payload", "sql_variant")]))
    assert connector.stream_calls == []


def test_mssql_columnar_provider_writes_and_uploads_parquet_chunks(tmp_path: Path) -> None:
    object_client = LocalObjectStorageClient(tmp_path / "store")
    writer = _FakeParquetWriter()
    connector = _FakeMssqlConnector(
        [
            [(1, "alpha"), (2, "beta")],
            [(3, "gamma")],
        ]
    )
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=object_client,
        parquet_writer=writer,
    )

    manifest = provider.snapshot(_request(batch_size=1, target_chunk_rows=2))

    assert provider.provider_id == "mssql_odbc_arrow_parquet"
    assert connector.stream_calls == [("SELECT id, name FROM dbo.orders", 1)]
    assert [chunk.row_count for chunk in manifest.chunks] == [2, 1]
    assert [chunk.index for chunk in manifest.chunks] == [0, 1]
    assert manifest.row_count == 3
    assert manifest.schema_hash.startswith("sha256:")
    assert manifest.read_contract.mode == "named_collection"
    assert object_client.exists(ObjectStorageUri.parse("s3://dpone-stage/msql/run-1/chunk-00000.parquet"))
    assert object_client.exists(ObjectStorageUri.parse("s3://dpone-stage/msql/run-1/chunk-00001.parquet"))
    assert writer.calls[0].compression == "zstd"


def test_mssql_columnar_provider_projects_submicrosecond_temporals_before_odbc(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(
        [[("2026-06-22 12:34:56.1234567", "12:34:56.1234567", "2026-06-22 09:34:56.1234567")]]
    )
    writer = _FakeParquetWriter()
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=writer,
    )
    request = _request(
        schema=[
            ("created_at", "datetime2 nullable"),
            ("business_time", "time(7) nullable"),
            ("offset_at", "datetimeoffset(7) nullable"),
        ]
    )

    provider.snapshot(request)

    query, _ = connector.stream_calls[0]
    assert "CONVERT(VARCHAR(33), dpone_columnar_src.[created_at], 121) AS [created_at]" in query
    assert "CONVERT(VARCHAR(16), dpone_columnar_src.[business_time]) AS [business_time]" in query
    assert "SWITCHOFFSET(CAST(dpone_columnar_src.[offset_at] AS datetimeoffset), '+00:00')" in query
    assert writer.calls[0].rows == [("2026-06-22 12:34:56.1234567", "12:34:56.1234567", "2026-06-22 09:34:56.1234567")]


def test_mssql_columnar_provider_aggregates_source_batches_before_chunk_flush(tmp_path: Path) -> None:
    writer = _FakeParquetWriter()
    connector = _FakeMssqlConnector(
        [
            [(1, "alpha")],
            [(2, "beta")],
            [(3, "gamma")],
        ]
    )
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=writer,
    )

    manifest = provider.snapshot(_request(batch_size=1, target_chunk_rows=3))

    assert [chunk.row_count for chunk in manifest.chunks] == [3]
    assert [call.rows for call in writer.calls] == [[(1, "alpha"), (2, "beta"), (3, "gamma")]]


def test_mssql_columnar_provider_iter_local_chunks_cleans_previous_chunk_eagerly(tmp_path: Path) -> None:
    seen_paths: list[Path] = []

    def assert_previous_chunk_removed_before_next_write(path: Path) -> None:
        if seen_paths:
            assert not seen_paths[-1].exists()
        seen_paths.append(path)

    writer = _FakeParquetWriter(on_write=assert_previous_chunk_removed_before_next_write)
    connector = _FakeMssqlConnector(
        [
            [(1, "alpha")],
            [(2, "beta")],
        ]
    )
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=None,
        parquet_writer=writer,
        temp_dir=tmp_path,
    )

    iterator = provider.iter_local_chunks(_request(batch_size=1, target_chunk_rows=1))
    first = next(iterator)

    assert first.path.exists()
    base_dir = first.path.parent

    second = next(iterator)

    assert not first.path.exists()
    assert second.path.exists()

    with pytest.raises(StopIteration):
        next(iterator)

    assert not second.path.exists()
    assert not base_dir.exists()


def test_mssql_columnar_provider_iter_local_chunks_respects_max_inflight_window(tmp_path: Path) -> None:
    writer = _FakeParquetWriter()
    provider = MssqlColumnarSnapshotProvider(
        connector=_FakeMssqlConnector(
            [
                [(1, "alpha")],
                [(2, "beta")],
                [(3, "gamma")],
            ]
        ),
        object_client=None,
        parquet_writer=writer,
        temp_dir=tmp_path,
    )

    iterator = provider.iter_local_chunks(
        _request(batch_size=1, target_chunk_rows=1, target_chunk_bytes=1, direct_push={"max_inflight_chunks": 2})
    )
    first = next(iterator)
    second = next(iterator)

    assert first.path.exists()
    assert second.path.exists()

    third = next(iterator)

    assert not first.path.exists()
    assert second.path.exists()
    assert third.path.exists()

    with pytest.raises(StopIteration):
        next(iterator)

    assert not second.path.exists()
    assert not third.path.exists()


def test_pyarrow_parquet_writer_writes_readable_table_with_declared_columns(tmp_path: Path) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "chunk.parquet"

    rows = [(1, "alpha"), (2, "beta")]
    row_count = PyArrowParquetChunkWriter().write_chunk(
        local_path=path,
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        rows=rows,
        compression="zstd",
    )

    table = pq.read_table(path)
    assert row_count == 2
    assert table.column_names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]


def test_pyarrow_parquet_writer_preserves_mssql_max_datetime_as_exact_text(tmp_path: Path) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "chunk.parquet"
    value = datetime(9999, 12, 31, 23, 59, 59, 999999)

    row_count = PyArrowParquetChunkWriter().write_chunk(
        local_path=path,
        schema=[("period_end_datetime", "datetime2(7)")],
        rows=[(value,)],
        compression="zstd",
    )

    table = pq.read_table(path)
    assert row_count == 1
    assert table.to_pylist() == [{"period_end_datetime": "9999-12-31 23:59:59.9999990"}]


def test_pyarrow_parquet_writer_preserves_datetime2_seventh_fractional_digit(tmp_path: Path) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "chunk.parquet"

    row_count = PyArrowParquetChunkWriter().write_chunk(
        local_path=path,
        schema=[("created_at", "datetime2(7) nullable")],
        rows=[("2026-06-22 12:34:56.1234567",), (None,)],
        compression="zstd",
    )

    table = pq.read_table(path)
    assert row_count == 2
    assert table.to_pylist() == [
        {"created_at": "2026-06-22 12:34:56.1234567"},
        {"created_at": None},
    ]


def test_pyarrow_parquet_writer_serializes_mssql_time_without_synthetic_epoch_date(tmp_path: Path) -> None:
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "chunk.parquet"

    row_count = PyArrowParquetChunkWriter().write_chunk(
        local_path=path,
        schema=[("business_time", "time(7) nullable")],
        rows=[(time(12, 34, 56, 123456),), (None,)],
        compression="zstd",
    )

    table = pq.read_table(path)
    assert row_count == 2
    assert table.to_pylist() == [
        {"business_time": "12:34:56.1234560"},
        {"business_time": None},
    ]


def test_mssql_columnar_provider_rejects_oversized_chunk_before_manifest_success(tmp_path: Path) -> None:
    object_client = LocalObjectStorageClient(tmp_path / "store")
    writer = _FakeParquetWriter(payload=b"x" * 128)
    provider = MssqlColumnarSnapshotProvider(
        connector=_FakeMssqlConnector([[(1, "alpha")]]),
        object_client=object_client,
        parquet_writer=writer,
    )

    with pytest.raises(RuntimeError, match="columnar_chunk_exceeds_max_bytes"):
        provider.snapshot(_request(max_chunk_bytes=16))

    assert object_client.list_prefix(ObjectStorageUri.parse("s3://dpone-stage/msql/run-1/")) == ()


def test_mssql_queryout_factory_uses_injected_columnar_provider_when_required() -> None:
    provider = _FakeColumnarSnapshotProvider(artifact={"kind": "manifest"})
    factory = MSSQLQueryoutArtifactFactory(
        connector=object(),
        logger=_FakeLogger(),
        columnar_snapshot_provider=provider,
    )
    config = _load_config()

    artifact = factory.artifact_for_query(config, "SELECT id FROM dbo.orders", [("id", "int")])

    assert artifact == {"kind": "manifest"}
    assert len(provider.requests) == 1
    assert provider.requests[0].query == "SELECT id FROM dbo.orders"
    assert provider.requests[0].uri_prefix == "s3://dpone-stage/msql/{run_id}/"
    assert provider.requests[0].run_id == "run-1"
    assert provider.requests[0].target_chunk_bytes == 64 * 1024 * 1024


def test_columnar_snapshot_request_uses_canonical_execution_options() -> None:
    config = _load_config()
    config.options["native_transfer"]["snapshot"]["columnar_fast_path"]["execution"] = {
        "target_chunk_bytes": "32MiB",
        "max_chunk_bytes": "96MiB",
        "max_inflight_chunks": 2,
        "cleanup_policy": "on_success",
    }

    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        run_id="run-1",
    )

    assert request.target_chunk_bytes == 32 * 1024 * 1024
    assert request.max_chunk_bytes == 96 * 1024 * 1024
    assert request.options["execution"]["max_inflight_chunks"] == 2
    assert request.options["cleanup_policy"] == "on_success"


def test_columnar_snapshot_request_carries_canonical_range_plan(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(
        config,
        {
            "column": "id",
            "num_partitions": 2,
            "export_workers": 2,
            "load_workers": 1,
            "bounds": {"lower": 0, "upper": 20},
            "range_parallelism": {
                "mode": "required",
                "upload_workers": 1,
                "max_inflight_ranges": 2,
                "max_inflight_rows": 10,
                "max_inflight_bytes": 1024,
                "consistency": "immutable",
            },
        },
    )

    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-ranges",
    )

    assert request.range_partitioner is not None
    assert request.range_plan is not None
    assert len(request.range_plan.ranges) == 2
    assert request.range_plan.policy.reader_workers == 2

    provider = MssqlColumnarSnapshotProvider(
        connector=_ParallelMssqlConnector(partitions=2),
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )
    with pytest.raises(RuntimeError, match="requires_chunked_execution"):
        provider.snapshot(request)


def test_parallel_windows_use_independent_typed_ranges_and_wait_for_all_eof(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-ranges",
    )
    connector = _ParallelMssqlConnector(partitions=2)
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    iterator = provider.iter_object_storage_windows(request)
    first = next(iterator)

    assert all(session.eof for session in connector.sessions)
    assert len(connector.sessions) == 2
    assert first.range_ordinal == 0
    assert first.uri_prefix.endswith("range-000000/window-000001")
    queries = [session.stream_calls[0][0] for session in connector.sessions]
    assert any("[id] >= 0 AND [id] < 10" in query for query in queries)
    assert any("[id] >= 10 AND [id] <= 20" in query for query in queries)

    windows = [first, *list(iterator)]
    evidence = provider.range_execution_evidence(request)
    assert [window.range_ordinal for window in windows] == [0, 1]
    assert evidence is not None
    assert evidence["all_ranges_confirmed"] is True
    assert evidence["observed_reader_concurrency"] == 2
    assert evidence["requested_upload_concurrency"] == 1
    assert evidence["byte_measurement_scope"] == "preencode_max_chunk_reservation_and_postencode_actual"
    assert evidence["bytes_high_water"] == 1024
    assert evidence["byte_reservation_per_item"] == 1024
    assert evidence["bytes_high_water_kind"] == "configured_max_chunk_reservation"
    assert evidence["rows_high_water_kind"] == "declared_batch_reservation"
    assert evidence["row_reservation_per_item"] == 1
    assert evidence["actual_rows"] == 2
    assert evidence["actual_encoded_bytes"] < evidence["bytes_high_water"] * 2
    assert evidence["rows_high_water"] <= 10
    assert evidence["rss_bounded"] is False
    assert all(session.closed for session in connector.sessions)


def test_parallel_window_failure_cleans_owned_prefix_and_never_yields(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-ranges-failed",
    )
    connector = _ParallelMssqlConnector(partitions=2, fail_ordinal=0)
    object_client = LocalObjectStorageClient(tmp_path / "store")
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=object_client,
        parquet_writer=_FakeParquetWriter(),
    )

    with pytest.raises(RuntimeError, match="synthetic range failure"):
        list(provider.iter_object_storage_windows(request))

    prefix = ObjectStorageUri.parse("s3://dpone-stage/msql/run-ranges-failed/")
    assert object_client.list_prefix(prefix) == ()
    assert connector.sessions
    assert all(session.closed for session in connector.sessions)


def test_parallel_window_query_is_blocked_before_source_io(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    connector = _ParallelMssqlConnector(partitions=2)

    with pytest.raises(ValueError, match="machine-checkable"):
        build_columnar_snapshot_request(
            load_config=config,
            query="SELECT id, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id) AS rank FROM dbo.orders",
            schema=[("id", "int"), ("rank", "bigint")],
            run_id="run-window",
        )

    assert connector.sessions == []


def test_parallel_window_budget_contract_blocks_unsafe_declared_items_before_io(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    execution = config.options["native_transfer"]["snapshot"]["columnar_fast_path"]["execution"]
    execution["max_chunk_bytes"] = 2048
    config.options["batch_size"] = 11
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-budget-blocked",
    )
    connector = _ParallelMssqlConnector(partitions=2)
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    capability = provider.capabilities(request)

    assert "columnar_range_max_chunk_exceeds_inflight_byte_budget" in capability.blockers
    assert "columnar_range_batch_size_exceeds_inflight_row_budget" in capability.blockers
    assert connector.sessions == []


def test_parallel_window_oversized_encoded_item_fails_without_success_evidence(tmp_path: Path) -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning["range_parallelism"]["max_inflight_bytes"] = 32
    _configure_parallel(config, partitioning)
    execution = config.options["native_transfer"]["snapshot"]["columnar_fast_path"]["execution"]
    execution["max_chunk_bytes"] = 32
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-oversized-item",
    )
    connector = _ParallelMssqlConnector(partitions=2)
    object_client = LocalObjectStorageClient(tmp_path / "store")
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=object_client,
        parquet_writer=_FakeParquetWriter(payload=b"x" * 128),
    )

    with pytest.raises(RuntimeError, match="columnar_chunk_exceeds_max_bytes"):
        list(provider.iter_object_storage_windows(request))

    assert provider.range_execution_evidence(request) is None
    assert object_client.list_prefix(ObjectStorageUri.parse("s3://dpone-stage/msql/run-oversized-item/")) == ()


def test_parallel_window_source_batch_cannot_exceed_row_reservation(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-oversized-batch",
    )
    connector = _ParallelMssqlConnector(partitions=2, rows_per_batch=2)
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    with pytest.raises(ValueError, match="declared row reservation"):
        list(provider.iter_object_storage_windows(request))

    assert provider.range_execution_evidence(request) is None


def test_mssql_queryout_factory_blocks_required_columnar_without_provider() -> None:
    factory = MSSQLQueryoutArtifactFactory(connector=object(), logger=_FakeLogger())

    with pytest.raises(RuntimeError, match="columnar_snapshot_provider_missing"):
        factory.artifact_for_query(_load_config(), "SELECT id FROM dbo.orders", [("id", "int")])


def _request(
    *,
    max_chunk_bytes: int = 1024 * 1024,
    target_chunk_bytes: int | None = None,
    batch_size: int = 10000,
    target_chunk_rows: int | None = None,
    schema: list[tuple[str, str]] | None = None,
    direct_push: dict[str, object] | None = None,
) -> ColumnarSnapshotRequest:
    options = {
        "batch_size": batch_size,
        "clickhouse_read_access": {"mode": "named_collection", "named_collection": "dpone_stage"},
    }
    if target_chunk_rows is not None:
        options["target_chunk_rows"] = target_chunk_rows
    if direct_push is not None:
        options["direct_push"] = direct_push
    return ColumnarSnapshotRequest(
        query="SELECT id, name FROM dbo.orders",
        schema=schema or [("id", "int"), ("name", "nvarchar(50)")],
        uri_prefix="s3://dpone-stage/msql/run-1/",
        run_id="run-1",
        target_chunk_bytes=target_chunk_bytes or 512 * 1024 * 1024,
        max_chunk_bytes=max_chunk_bytes,
        options=options,
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
                        "execution": {
                            "target_chunk_bytes": "64MiB",
                            "max_chunk_bytes": "128MiB",
                        },
                        "object_storage": {
                            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
                            "format": "parquet",
                            "compression": "zstd",
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
        self.stream_calls: list[tuple[str, int]] = []

    def get_records_streaming(self, query: str, *, batch_size: int, as_dict: bool = False):
        assert as_dict is False
        self.stream_calls.append((query, batch_size))
        yield from self.batches


def _parallel_partitioning() -> dict[str, object]:
    return {
        "column": "id",
        "num_partitions": 2,
        "export_workers": 2,
        "load_workers": 1,
        "bounds": {"lower": 0, "upper": 20},
        "range_parallelism": {
            "mode": "required",
            "reader_workers": 2,
            "upload_workers": 1,
            "max_inflight_ranges": 2,
            "max_inflight_rows": 10,
            "max_inflight_bytes": 1024,
            "consistency": "immutable",
        },
    }


def _configure_parallel(config: LoadConfig, partitioning: dict[str, object] | None = None) -> None:
    config.options["partitioning"] = partitioning or _parallel_partitioning()
    config.options["batch_size"] = 1
    execution = config.options["native_transfer"]["snapshot"]["columnar_fast_path"]["execution"]
    execution["max_chunk_bytes"] = 1024


class _ParallelMssqlConnector:
    def __init__(self, *, partitions: int, fail_ordinal: int | None = None, rows_per_batch: int = 1) -> None:
        self._barrier = Barrier(partitions)
        self.fail_ordinal = fail_ordinal
        self.rows_per_batch = rows_per_batch
        self.sessions: list[_ParallelMssqlSession] = []

    def get_records_streaming(self, *args, **kwargs):
        raise AssertionError(f"Root connector must not read range data: {args}, {kwargs}")

    def open_session(self, *, application_name: str):
        ordinal = int(application_name.rsplit("-", 1)[-1])
        session = _ParallelMssqlSession(self, ordinal)
        self.sessions.append(session)
        return session


class _ParallelMssqlSession:
    def __init__(self, owner: _ParallelMssqlConnector, ordinal: int) -> None:
        self.owner = owner
        self.ordinal = ordinal
        self.stream_calls: list[tuple[str, int]] = []
        self.eof = False
        self.closed = False
        self.cancelled = False

    def get_records_streaming(self, query: str, *, batch_size: int, as_dict: bool = False):
        assert as_dict is False
        self.stream_calls.append((query, batch_size))
        self.owner._barrier.wait(timeout=2)
        if self.ordinal == self.owner.fail_ordinal:
            raise RuntimeError("synthetic range failure")
        yield [(self.ordinal * 10 + offset + 1, f"range-{self.ordinal}") for offset in range(self.owner.rows_per_batch)]
        self.eof = True

    def cancel(self) -> None:
        self.cancelled = True

    def close(self) -> None:
        self.closed = True


class _WriterCall:
    def __init__(self, rows: list[tuple[object, ...]], compression: str) -> None:
        self.rows = rows
        self.compression = compression


class _FakeParquetWriter:
    def __init__(
        self,
        *,
        available: bool = True,
        payload: bytes = b"PAR1 fake parquet PAR1",
        on_write=None,
    ) -> None:
        self.available = available
        self.payload = payload
        self.on_write = on_write
        self.calls: list[_WriterCall] = []

    def is_available(self) -> bool:
        return self.available

    def write_chunk(self, *, local_path: Path, schema, rows, compression: str) -> int:
        materialized = list(rows)
        self.calls.append(_WriterCall(materialized, compression))
        if self.on_write is not None:
            self.on_write(local_path)
        local_path.write_bytes(self.payload)
        return len(materialized)


class _FakeColumnarSnapshotProvider:
    provider_id = "fake_columnar"

    def __init__(self, *, artifact) -> None:
        self.artifact = artifact
        self.requests: list[ColumnarSnapshotRequest] = []

    def capabilities(self, request: ColumnarSnapshotRequest):
        self.requests.append(request)
        return _FakeCapability()

    def snapshot(self, request: ColumnarSnapshotRequest):
        if not self.requests or self.requests[-1] is not request:
            self.requests.append(request)
        return self.artifact


class _FakeCapability:
    blockers: tuple[str, ...] = ()

    def supports(self, request: ColumnarSnapshotRequest) -> bool:
        return True


class _FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def log_etl_progress(self, event: str, details: dict[str, object]) -> None:
        self.events.append((event, details))
