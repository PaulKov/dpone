from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
from pathlib import Path
from threading import Barrier, current_thread
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.runtime.columnar_object_storage_windows import ObjectStorageColumnarChunkedArtifact
from dpone.runtime.columnar_parquet_writer import PyArrowParquetChunkWriter, mssql_source_type_supported
from dpone.runtime.columnar_runtime_assembly import MssqlColumnarSnapshotRequestFactory
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import build_columnar_snapshot_request
from dpone.runtime.sources.strategies.mssql.mssql_columnar_windows import build_parallel_object_windows
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


@pytest.fixture
def enabled_range_byte_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the dormant target implementation without enabling production."""

    from dpone.runtime.sources.strategies.mssql import mssql_columnar_range_admission

    monkeypatch.setattr(mssql_columnar_range_admission, "range_byte_admission_available", lambda: True)


def test_parallel_window_builder_has_terminal_admission_guard(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="columnar_range_pre_read_byte_admission_unavailable"):
        build_parallel_object_windows(
            connector=object(),
            request=object(),
            prefix=object(),
            schema_hash="unused",
            object_client=object(),
            write_chunk_file=lambda *_args: (0, 0, ""),
            read_contract=object(),
            tmp_dir=tmp_path,
        )

    assert list(tmp_path.iterdir()) == []


def test_direct_queryout_auto_fallback_records_inactive_consistency() -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning["range_parallelism"].update(
        {
            "mode": "auto",
            "consistency": "write_exclusion",
            "consistency_authority": {"write_exclusion_ref": "parallel-only-lease"},
        }
    )
    _configure_parallel(config, partitioning)

    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-direct-auto-fallback",
    )

    assert request.range_plan is None
    assert request.options["range_parallelism_fallback_reason"] == (
        "columnar_range_pre_read_byte_admission_unavailable"
    )
    assert request.options["range_consistency_binding"] == {
        "mode": "inactive",
        "requested_consistency": "write_exclusion",
        "reason": "columnar_range_pre_read_byte_admission_unavailable",
    }


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


@pytest.mark.usefixtures("enabled_range_byte_admission")
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
    assert request.range_plan.execution_identity is not None
    assert request.range_plan.execution_plan_fingerprint is not None

    provider = MssqlColumnarSnapshotProvider(
        connector=_ParallelMssqlConnector(partitions=2),
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )
    with pytest.raises(RuntimeError, match="requires_chunked_execution"):
        provider.snapshot(request)


def test_columnar_range_request_requires_explicit_consistency() -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    del partitioning["range_parallelism"]["consistency"]
    _configure_parallel(config, partitioning)

    with pytest.raises(ValueError, match="requires_explicit_consistency"):
        build_columnar_snapshot_request(
            load_config=config,
            query="SELECT id, name FROM dbo.orders",
            schema=[("id", "int"), ("name", "nvarchar(50)")],
            run_id="run-missing-consistency",
        )


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_database_snapshot_consistency_binds_schema_and_query_to_snapshot_database() -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning["range_parallelism"].update(
        {
            "consistency": "database_snapshot",
            "consistency_authority": {"database_snapshot": "SyntheticSnapshot"},
        }
    )
    _configure_parallel(config, partitioning)
    connector = _AuthorityConnector()

    request = MssqlColumnarSnapshotRequestFactory()(
        load_config=config,
        source=SimpleNamespace(connector=connector),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-snapshot"),
    )

    assert connector.schema_databases == ["SyntheticSnapshot"]
    assert connector.query_databases == ["SyntheticSnapshot"]
    assert "SyntheticSnapshot.dbo.orders" in request.query
    assert request.options["range_consistency_binding"] == {
        "mode": "database_snapshot",
        "database_snapshot": "SyntheticSnapshot",
    }


@pytest.mark.usefixtures("enabled_range_byte_admission")
@pytest.mark.parametrize("consistency", ["temporal_as_of", "write_exclusion"])
def test_unverifiable_mutable_consistency_fails_before_schema_io(consistency: str) -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    authority = {"as_of": "2026-01-01T00:00:00.1234567Z"}
    if consistency == "write_exclusion":
        authority = {"write_exclusion_ref": "synthetic-lease"}
    partitioning["range_parallelism"].update({"consistency": consistency, "consistency_authority": authority})
    _configure_parallel(config, partitioning)
    connector = _AuthorityConnector()

    with pytest.raises(RuntimeError, match="query_builder_missing|verifier_missing"):
        MssqlColumnarSnapshotRequestFactory()(
            load_config=config,
            source=SimpleNamespace(connector=connector),
            sink=object(),
            state=None,
            load_record=SimpleNamespace(run_id="run-unverified"),
        )

    assert connector.schema_databases == []


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_auto_bounds_fail_before_odbc_truncates_datetime2_seven_precision() -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning.update(
        {
            "column": "event_ts",
            "bounds": "auto",
            "planner": {"boundary_type": "datetime2"},
        }
    )
    _configure_parallel(config, partitioning)
    connector = _AuthorityConnector(schema=[("event_ts", "datetime2(7)")])

    with pytest.raises(ValueError, match="100ns_requires_explicit_ranges"):
        MssqlColumnarSnapshotRequestFactory()(
            load_config=config,
            source=SimpleNamespace(connector=connector),
            sink=object(),
            state=None,
            load_record=SimpleNamespace(run_id="run-precise-auto"),
        )

    assert connector.bounds_queries == []


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_auto_bounds_project_datetime2_text_before_odbc() -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning.update(
        {
            "column": "event_ts",
            "bounds": "auto",
            "planner": {"boundary_type": "datetime2"},
        }
    )
    _configure_parallel(config, partitioning)
    connector = _AuthorityConnector(
        schema=[("event_ts", "datetime2(6)")],
        bounds_row=("2026-01-01 00:00:00.123456", "2026-01-02 00:00:00.123456", 2, 0),
    )

    request = MssqlColumnarSnapshotRequestFactory()(
        load_config=config,
        source=SimpleNamespace(connector=connector),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-safe-auto"),
    )

    assert request.range_plan is not None
    assert "MIN(CONVERT(varchar(33), [event_ts], 121))" in connector.bounds_queries[0]


@pytest.mark.usefixtures("enabled_range_byte_admission")
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
    object_client = LocalObjectStorageClient(tmp_path / "store")
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=object_client,
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
    payload = evidence.to_dict()
    assert payload["all_ranges_eof_confirmed"] is True
    assert payload["outcome"]["status"] == "extracted"
    assert payload["concurrency"] == {
        "requested": {"reader": 2, "upload": 1, "load": 1},
        "observed": {"reader": 2, "upload": 1, "load": 0},
    }
    assert payload["resource_high_water"] == {"rows": 2, "bytes": 1024}
    assert sum(item["rows"] for item in payload["ranges"]) == 2
    assert all(item["chunks"] for item in payload["ranges"])
    assert all(chunk["checksum_sha256"].startswith("sha256:") for item in payload["ranges"] for chunk in item["chunks"])
    assert all(session.closed for session in connector.sessions)
    run_prefix = ObjectStorageUri.parse("s3://dpone-stage/msql/run-ranges/")
    assert object_client.list_prefix(run_prefix)
    for window in windows:
        window.cleanup()
    assert object_client.list_prefix(run_prefix) == ("s3://dpone-stage/msql/run-ranges/__dpone_run_marker.json",)
    provider.cleanup_object_storage_run(request)
    assert object_client.list_prefix(run_prefix) == ()


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_parallel_upload_workers_run_in_independent_bounded_executor(tmp_path: Path) -> None:
    config = _load_config()
    partitioning = _parallel_partitioning()
    partitioning["range_parallelism"]["upload_workers"] = 2
    partitioning["range_parallelism"]["max_inflight_bytes"] = 2048
    _configure_parallel(config, partitioning)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-upload-workers",
    )
    object_client = _ConcurrentUploadClient(LocalObjectStorageClient(tmp_path / "store"), workers=2)
    provider = MssqlColumnarSnapshotProvider(
        connector=_ParallelMssqlConnector(partitions=2),
        object_client=object_client,
        parquet_writer=_FakeParquetWriter(),
    )

    assert len(list(provider.iter_object_storage_windows(request))) == 2

    evidence = provider.range_execution_evidence(request)
    assert evidence is not None
    assert evidence.observed_upload_concurrency == 2
    assert len(set(object_client.thread_names)) == 2
    assert all(name.startswith("dpone-columnar-upload") for name in object_client.thread_names)


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_parallel_upload_failure_cancels_readers_and_cleans_owned_prefix(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-upload-failed",
    )
    connector = _ParallelMssqlConnector(partitions=2)
    object_client = _FailingUploadClient(LocalObjectStorageClient(tmp_path / "store"))
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=object_client,
        parquet_writer=_FakeParquetWriter(),
    )

    with pytest.raises(RuntimeError, match="synthetic upload failure"):
        list(provider.iter_object_storage_windows(request))

    _assert_failed_range_evidence(provider, request)
    assert all(session.closed for session in connector.sessions)
    assert object_client.list_prefix(ObjectStorageUri.parse("s3://dpone-stage/msql/run-upload-failed/")) == ()


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_range_evidence_handoff_is_consumed_and_cleared_before_retry(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-evidence-retry",
    )
    writer = _FakeParquetWriter()
    provider = MssqlColumnarSnapshotProvider(
        connector=_ParallelMssqlConnector(partitions=2),
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=writer,
    )

    assert len(list(provider.iter_object_storage_windows(request))) == 2
    assert provider.range_execution_evidence(request) is not None
    assert provider.range_execution_evidence(request) is None

    assert len(list(provider.iter_object_storage_windows(request))) == 2
    writer.available = False
    with pytest.raises(RuntimeError, match="parquet_writer_unavailable"):
        list(provider.iter_object_storage_windows(request))
    assert provider.range_execution_evidence(request) is None


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_parallel_failure_is_bound_to_consumed_artifact(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-artifact-failed",
    )
    provider = MssqlColumnarSnapshotProvider(
        connector=_ParallelMssqlConnector(partitions=2, fail_ordinal=0),
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=provider,
        request=request,
        columns=("id", "name"),
        schema_hash="schema",
        cleanup_policy="on_success",
    )

    with pytest.raises(RuntimeError, match="synthetic range failure"):
        list(artifact.iter_windows())

    assert artifact.range_execution_evidence is not None
    assert artifact.range_execution_evidence.outcome_status == "failed"
    assert artifact.to_evidence()["range_execution"]["outcome"]["status"] == "failed"


@pytest.mark.usefixtures("enabled_range_byte_admission")
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


@pytest.mark.usefixtures("enabled_range_byte_admission")
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


@pytest.mark.usefixtures("enabled_range_byte_admission")
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


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_parallel_window_requires_run_owned_object_prefix_before_io(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = replace(
        build_columnar_snapshot_request(
            load_config=config,
            query="SELECT id, name FROM dbo.orders",
            schema=[("id", "int"), ("name", "nvarchar(50)")],
            run_id="run-shared-prefix",
        ),
        uri_prefix="s3://dpone-stage/msql/shared/",
    )
    connector = _ParallelMssqlConnector(partitions=2)
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    capability = provider.capabilities(request)

    assert "columnar_range_uri_prefix_must_include_run_id" in capability.blockers
    assert connector.sessions == []


@pytest.mark.usefixtures("enabled_range_byte_admission")
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

    _assert_failed_range_evidence(provider, request)
    assert object_client.list_prefix(ObjectStorageUri.parse("s3://dpone-stage/msql/run-oversized-item/")) == ()


@pytest.mark.usefixtures("enabled_range_byte_admission")
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

    _assert_failed_range_evidence(provider, request)


@pytest.mark.usefixtures("enabled_range_byte_admission")
def test_parallel_window_rejects_same_connector_session_before_range_query(tmp_path: Path) -> None:
    config = _load_config()
    _configure_parallel(config)
    request = build_columnar_snapshot_request(
        load_config=config,
        query="SELECT id, name FROM dbo.orders",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        run_id="run-reused-session",
    )
    connector = _SameSessionConnector()
    provider = MssqlColumnarSnapshotProvider(
        connector=connector,
        object_client=LocalObjectStorageClient(tmp_path / "store"),
        parquet_writer=_FakeParquetWriter(),
    )

    with pytest.raises(RuntimeError, match="session_not_independent"):
        list(provider.iter_object_storage_windows(request))

    assert connector.stream_calls == []


def _assert_failed_range_evidence(provider: MssqlColumnarSnapshotProvider, request: ColumnarSnapshotRequest) -> None:
    evidence = provider.range_execution_evidence(request)
    assert evidence is not None
    assert evidence.outcome_status == "failed"
    assert evidence.failure_code == "columnar_range_extraction_failed"
    assert evidence.cleanup_status == "completed"
    assert evidence.publication_receipt_sha256 is None


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


class _SameSessionConnector:
    def __init__(self) -> None:
        self.stream_calls: list[str] = []

    def open_session(self, *, application_name: str):
        del application_name
        return self

    def get_records_streaming(self, query: str, *, batch_size: int, as_dict: bool = False):
        del batch_size, as_dict
        self.stream_calls.append(query)
        yield [(1, "unsafe")]

    def close(self) -> None:
        return


class _AuthorityConnector:
    def __init__(
        self,
        *,
        schema: list[tuple[str, str]] | None = None,
        bounds_row: tuple[object, object, int, int] | None = None,
    ) -> None:
        self.schema_databases: list[str | None] = []
        self.query_databases: list[str | None] = []
        self.schema = schema or [("id", "int"), ("name", "nvarchar(50)")]
        self.bounds_row = bounds_row
        self.bounds_queries: list[str] = []

    def fetch_schema(self, schema: str, table: str, *, database: str | None = None):
        del schema, table
        self.schema_databases.append(database)
        return self.schema

    def build_select_query(self, schema: str, table: str, columns: list[str], *, database: str | None = None) -> str:
        self.query_databases.append(database)
        prefix = f"{database}." if database else ""
        return f"SELECT {', '.join(columns)} FROM {prefix}{schema}.{table}"

    def quote_identifier(self, value: str) -> str:
        return f"[{value}]"

    def get_records(self, query: str):
        self.bounds_queries.append(query)
        return [self.bounds_row] if self.bounds_row is not None else []

    def open_session(self, *, application_name: str):
        return SimpleNamespace(application_name=application_name)


class _ConcurrentUploadClient:
    def __init__(self, delegate: LocalObjectStorageClient, *, workers: int) -> None:
        self._delegate = delegate
        self._barrier = Barrier(workers)
        self.thread_names: list[str] = []

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    def put_file(self, *args, **kwargs):
        if current_thread().name.startswith("dpone-columnar-upload"):
            self.thread_names.append(current_thread().name)
            self._barrier.wait(timeout=2)
        return self._delegate.put_file(*args, **kwargs)


class _FailingUploadClient:
    def __init__(self, delegate: LocalObjectStorageClient) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    def put_file(self, *args, **kwargs):
        if current_thread().name.startswith("dpone-columnar-upload"):
            raise RuntimeError("synthetic upload failure")
        return self._delegate.put_file(*args, **kwargs)


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
