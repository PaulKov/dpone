from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig
from dpone.runtime.columnar_fast_path import (
    ColumnarFastPathPlanner,
    LocalColumnarChunk,
    LocalColumnarStagingManifest,
    ObjectStorageChunk,
    ObjectStorageChunkWindow,
    ObjectStorageColumnarChunkedArtifact,
    ObjectStorageStagingManifest,
)
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability, ColumnarSnapshotRequest
from dpone.runtime.object_storage_access import (
    ObjectStorageAccessEvidence,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.route_capabilities import (
    CapabilityEvidence,
    CapabilityRequirement,
    RouteCandidate,
    RouteCapabilityPlanner,
    RuntimeCapability,
)
from dpone.runtime.sinks.clickhouse_columnar_pull import ClickHouseColumnarPullLoader
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.storage import LocalObjectStorageClient


def test_required_columnar_fast_path_blocks_before_source_io_on_failed_preflight() -> None:
    preflight = _access_evidence(passed=False, blockers=("clickhouse_named_collection_missing",))

    decision = ColumnarFastPathPlanner().decide(
        columnar_options={"mode": "required", "provider": "object_storage_pull"},
        preflight=preflight,
        schema_supported=True,
        parquet_writer_available=True,
        clickhouse_pull_available=True,
    )

    assert decision.selected_provider == "blocked"
    assert decision.should_start_source_io is False
    assert decision.blockers == ("clickhouse_named_collection_missing",)
    assert decision.to_evidence()["schema_version"] == "dpone.native_transfer.columnar_fast_path.v1"


def test_auto_columnar_fast_path_falls_back_with_auditable_reason() -> None:
    preflight = _access_evidence(passed=False, blockers=("object_storage_runtime_put_denied",))

    decision = ColumnarFastPathPlanner().decide(
        columnar_options={"mode": "auto", "provider": "auto"},
        preflight=preflight,
        schema_supported=True,
        parquet_writer_available=True,
        clickhouse_pull_available=True,
        current_provider="typed_binary_streaming",
    )

    assert decision.selected_provider == "typed_binary_streaming"
    assert decision.fallback_allowed is True
    assert decision.fallback_reason == "object_storage_runtime_put_denied"
    assert decision.should_start_source_io is True


def test_columnar_fast_path_decision_embeds_generic_route_capability_evidence() -> None:
    route_decision = RouteCapabilityPlanner().decide(
        candidates=(
            RouteCandidate(
                route_id="object_storage_pull_s3cluster",
                requirements=(
                    CapabilityRequirement(
                        id="sink.object_storage_pull.s3_cluster",
                        domain=RuntimeCapability.CLUSTER,
                        required_for="object_storage_pull_s3cluster",
                        blocker_code="sink.cluster_pull_unsupported",
                    ),
                ),
                priority=10,
            ),
            RouteCandidate(
                route_id="object_storage_pull_s3",
                requirements=(
                    CapabilityRequirement(
                        id="sink.object_storage_pull.s3",
                        domain=RuntimeCapability.TRANSPORT,
                        required_for="object_storage_pull_s3",
                    ),
                ),
                priority=20,
            ),
        ),
        evidence={
            "sink.object_storage_pull.s3_cluster": CapabilityEvidence.failure(
                requirement_id="sink.object_storage_pull.s3_cluster",
                domain=RuntimeCapability.CLUSTER,
                blockers=("sink.cluster_pull_unsupported",),
            ),
            "sink.object_storage_pull.s3": CapabilityEvidence.success(
                requirement_id="sink.object_storage_pull.s3",
                domain=RuntimeCapability.TRANSPORT,
            ),
        },
        mode="auto",
    )

    decision = ColumnarFastPathPlanner().decide(
        columnar_options={"mode": "auto", "provider": "auto"},
        preflight=_access_evidence(passed=True, blockers=()),
        schema_supported=True,
        parquet_writer_available=True,
        clickhouse_pull_available=True,
        route_decision=route_decision,
    )

    route_evidence = decision.to_evidence()["details"]["route_capabilities"]
    assert route_evidence["selected_route_id"] == "object_storage_pull_s3"
    assert route_evidence["fallback_reason"] == "sink.cluster_pull_unsupported"


def test_object_storage_manifest_records_chunk_hash_and_schema_hash() -> None:
    manifest = ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/msql/run-1/",
        columns=("id", "amount"),
        chunks=(
            ObjectStorageChunk(
                uri="s3://dpone-stage/msql/run-1/chunk-00000.parquet",
                index=0,
                row_count=10,
                size_bytes=1024,
                sha256="a" * 64,
                schema_hash="schema-1",
            ),
        ),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-1",
        estimated_rows=10,
    )

    evidence = manifest.to_evidence()

    assert evidence["schema_version"] == "dpone.native_transfer.columnar_chunks.v1"
    assert evidence["row_count"] == 10
    assert evidence["chunks"][0]["sha256"] == "a" * 64
    assert evidence["schema_hash"] == "schema-1"


def test_columnar_snapshot_capability_requires_certified_format_and_compression() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="s3://dpone-stage/msql/run-1/",
        run_id="run-1",
        format="parquet",
        compression="zstd",
    )

    assert ColumnarSnapshotCapability(provider_id="mssql_bcp_native_parquet", certified=True).supports(request)
    assert not ColumnarSnapshotCapability(provider_id="mssql_bcp_native_parquet", certified=False).supports(request)
    assert not ColumnarSnapshotCapability(
        provider_id="mssql_bcp_native_parquet",
        certified=True,
        supported_compression=("snappy",),
    ).supports(request)


def test_clickhouse_columnar_pull_loader_renders_s3cluster_named_collection_without_secrets() -> None:
    connector = _RecordingClickHouseConnector(count=42)
    manifest = ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/msql/run-1/",
        columns=("id", "amount"),
        chunks=(
            ObjectStorageChunk(
                uri="s3://dpone-stage/msql/run-1/chunk-00000.parquet",
                index=0,
                row_count=42,
                size_bytes=2048,
                sha256="b" * 64,
                schema_hash="schema-2",
            ),
        ),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-2",
        estimated_rows=42,
    )
    loader = ClickHouseColumnarPullLoader(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        count_rows=lambda _cfg: 42,
    )

    inserted = loader.load(_load_config(), manifest, [("id", "int"), ("amount", "decimal(18,2)")])

    assert inserted == 42
    sql = connector.queries[-1]
    assert "INSERT INTO `DWH_Raw`.`orders__staging` (`id`, `amount`)" in sql
    assert "FROM s3Cluster('dwh', dpone_stage, filename='msql/run-1/*.parquet')" in sql
    assert "input_format_parquet_allow_missing_columns = 0" in sql
    assert "secret" not in sql.lower()
    assert "password" not in sql.lower()


def test_object_storage_manifest_materializes_through_sink_loader() -> None:
    manifest = ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/msql/run-1/",
        columns=("id",),
        chunks=(),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-3",
        estimated_rows=7,
    )
    sink = _FakeColumnarSink()

    handle = manifest.materialize(sink, _load_config(), [("id", "int")])

    assert handle.row_count == 7
    assert sink.loaded == [(manifest, [("id", "int")])]


def test_object_storage_chunked_artifact_materializes_through_window_loader() -> None:
    window = ObjectStorageChunkWindow(
        uri_prefix="s3://dpone-stage/msql/run-1/window-000001/",
        columns=("id",),
        chunks=(),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-window",
        estimated_rows=3,
    )
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider([window]),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=3,
    )
    sink = _FakeColumnarSink()

    handle = artifact.materialize(sink, _load_config(), [("id", "int")])

    assert handle.row_count == 3
    assert sink.windowed_loaded == [(artifact, [("id", "int")])]


def test_clickhouse_columnar_pull_loader_loads_object_windows_and_cleans_each_prefix() -> None:
    connector = _RecordingClickHouseConnector(count=8)
    cleanup = _CleanupRecorder()
    clock = _StepClock([0.0, 2.0, 2.5, 3.0, 5.0, 5.25])
    windows = [
        ObjectStorageChunkWindow(
            uri_prefix="s3://dpone-stage/msql/run-1/window-000001/",
            columns=("id",),
            chunks=(
                ObjectStorageChunk(
                    uri="s3://dpone-stage/msql/run-1/window-000001/chunk-00000.parquet",
                    index=0,
                    row_count=4,
                    size_bytes=0,
                    sha256="a" * 64,
                    schema_hash="schema-window",
                ),
            ),
            read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
            schema_hash="schema-window",
            object_client=cleanup,
            estimated_rows=4,
        ),
        ObjectStorageChunkWindow(
            uri_prefix="s3://dpone-stage/msql/run-1/window-000002/",
            columns=("id",),
            chunks=(
                ObjectStorageChunk(
                    uri="s3://dpone-stage/msql/run-1/window-000002/chunk-00000.parquet",
                    index=0,
                    row_count=4,
                    size_bytes=0,
                    sha256="b" * 64,
                    schema_hash="schema-window",
                ),
            ),
            read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
            schema_hash="schema-window",
            object_client=cleanup,
            estimated_rows=4,
        ),
    ]
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(windows),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=8,
    )
    loader = ClickHouseColumnarPullLoader(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        count_rows=lambda _cfg: 8,
        clock=clock,
    )

    inserted = loader.load_windowed(_load_config(), artifact, [("id", "int")])

    assert inserted == 8
    assert [
        "FROM s3Cluster('dwh', dpone_stage, filename='msql/run-1/window-000001/*.parquet')" in query
        for query in connector.queries
    ] == [True, False]
    assert [
        "FROM s3Cluster('dwh', dpone_stage, filename='msql/run-1/window-000002/*.parquet')" in query
        for query in connector.queries
    ] == [False, True]
    assert cleanup.deleted_prefixes == [
        "s3://dpone-stage/msql/run-1/window-000001",
        "s3://dpone-stage/msql/run-1/window-000002",
    ]
    assert artifact.to_evidence()["window_metrics"] == [
        {
            "schema_version": "dpone.native_transfer.columnar_window_metrics.v1",
            "window_index": 1,
            "row_count": 4,
            "size_bytes": 0,
            "uri_prefix": "s3://dpone-stage/msql/run-1/window-000001/",
            "producer_metrics": {},
            "clickhouse_pull_seconds": 2.0,
            "window_cleanup_seconds": 0.5,
            "rows_per_second": 2.0,
        },
        {
            "schema_version": "dpone.native_transfer.columnar_window_metrics.v1",
            "window_index": 2,
            "row_count": 4,
            "size_bytes": 0,
            "uri_prefix": "s3://dpone-stage/msql/run-1/window-000002/",
            "producer_metrics": {},
            "clickhouse_pull_seconds": 2.0,
            "window_cleanup_seconds": 0.25,
            "rows_per_second": 2.0,
        },
    ]


def test_clickhouse_payload_ingestion_routes_object_storage_manifest_to_columnar_pull() -> None:
    connector = _RecordingClickHouseConnector(count=5)
    sink = _FakeClickHouseSink(connector)
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    manifest = ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/msql/run-1/",
        columns=("id",),
        chunks=(),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-5",
        estimated_rows=5,
    )

    inserted = service.insert_payload(_load_config(), LoadPayload(artifact=manifest, schema=[("id", "int")]))

    assert inserted == 5
    assert "FROM s3Cluster('dwh', dpone_stage, filename='msql/run-1/*.parquet')" in connector.queries[-1]


def test_clickhouse_payload_ingestion_routes_object_storage_chunked_to_windowed_pull() -> None:
    connector = _RecordingClickHouseConnector(count=6)
    sink = _FakeClickHouseSink(connector)
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(
            [
                ObjectStorageChunkWindow(
                    uri_prefix="s3://dpone-stage/msql/run-1/window-000001/",
                    columns=("id",),
                    chunks=(),
                    read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
                    schema_hash="schema-window",
                    estimated_rows=6,
                )
            ]
        ),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=6,
    )

    inserted = service.insert_payload(_load_config(), LoadPayload(artifact=artifact, schema=[("id", "int")]))

    assert inserted == 6
    assert "FROM s3Cluster('dwh', dpone_stage, filename='msql/run-1/window-000001/*.parquet')" in connector.queries[-1]


def test_clickhouse_staged_load_metadata_exposes_columnar_window_microsteps() -> None:
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider([]),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=8,
    )
    artifact.record_window_metric(
        {
            "row_count": 8,
            "producer_metrics": {
                "source_read_seconds": 10.0,
                "parquet_write_seconds": 2.0,
                "object_upload_seconds": 1.0,
            },
            "clickhouse_pull_seconds": 0.5,
            "window_cleanup_seconds": 0.25,
        }
    )
    service = ClickHouseStagedLoadService(_FakeStagedSink(staged_rows=8))

    handle = service.stage(_load_config(), LoadPayload(artifact=artifact, schema=[("id", "int")]))

    assert handle.metadata["staged_microsteps"] == [
        {"step_id": "source_read", "duration_seconds": 10.0, "rows": 8, "rows_per_second": 0.8},
        {"step_id": "parquet_write", "duration_seconds": 2.0, "rows": 8, "rows_per_second": 4.0},
        {"step_id": "object_upload", "duration_seconds": 1.0, "rows": 8, "rows_per_second": 8.0},
        {"step_id": "clickhouse_pull", "duration_seconds": 0.5, "rows": 8, "rows_per_second": 16.0},
        {"step_id": "window_cleanup", "duration_seconds": 0.25, "rows": 8, "rows_per_second": 32.0},
    ]


def test_clickhouse_payload_ingestion_routes_local_columnar_manifest_to_direct_push(tmp_path: Path) -> None:
    parquet_file = tmp_path / "chunk-00000.parquet"
    parquet_file.write_bytes(b"PAR1 fake parquet PAR1")
    sink = _FakeClickHouseSink(_RecordingClickHouseConnector(count=5))
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    manifest = LocalColumnarStagingManifest(
        base_dir=tmp_path,
        columns=("id",),
        chunks=(
            LocalColumnarChunk(
                path=parquet_file,
                index=0,
                row_count=5,
                size_bytes=parquet_file.stat().st_size,
                sha256="c" * 64,
                schema_hash="schema-local",
            ),
        ),
        schema_hash="schema-local",
        estimated_rows=5,
    )

    inserted = service.insert_payload(_load_config(), LoadPayload(artifact=manifest, schema=[("id", "int")]))

    assert inserted == 5
    assert sink.client_runner.inserts == [("`DWH_Raw`.`orders__staging`", ["id"], str(parquet_file))]
    assert sink.client_runner.options.input_format == "Parquet"


def test_object_storage_manifest_cleanup_deletes_only_run_prefix(tmp_path: Path) -> None:
    client = LocalObjectStorageClient(root_dir=tmp_path)
    prefix = "s3://dpone-stage/msql/run-1/"
    keep_file = tmp_path / "keep.parquet"
    delete_file = tmp_path / "delete.parquet"
    keep_file.write_text("keep", encoding="utf-8")
    delete_file.write_text("delete", encoding="utf-8")
    client.put_file(keep_file, destination=_uri("s3://dpone-stage/msql/other-run/chunk.parquet"))
    client.put_file(delete_file, destination=_uri("s3://dpone-stage/msql/run-1/chunk.parquet"))
    manifest = ObjectStorageStagingManifest(
        uri_prefix=prefix,
        columns=("id",),
        chunks=(),
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        schema_hash="schema-4",
        object_client=client,
        cleanup_policy="eager",
    )

    manifest.cleanup()

    assert client.exists(_uri("s3://dpone-stage/msql/other-run/chunk.parquet"))
    assert not client.exists(_uri("s3://dpone-stage/msql/run-1/chunk.parquet"))


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="DWH_Raw",
        target_table="orders__staging",
        options={
            "clickhouse_bulk": {
                "mode": "client",
                "columnar_pull": {
                    "use_cluster_function": "s3Cluster",
                    "cluster": "dwh",
                    "auth_mode": "named_collection",
                    "settings": {"input_format_parquet_allow_missing_columns": False},
                },
            }
        },
    )


def _access_evidence(*, passed: bool, blockers: tuple[str, ...]) -> ObjectStorageAccessEvidence:
    return ObjectStorageAccessEvidence(
        passed=passed,
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="airflow", connection_id="s3_writer")
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        uri_prefix="s3://dpone-stage/msql/{run_id}/",
        sentinel_uri="s3://dpone-stage/msql/run-1/__dpone_sentinel.parquet",
        clickhouse_probe_sql=None,
        checks=(),
        warnings=(),
        blockers=blockers,
    )


def _uri(value: str):
    from dpone.storage import ObjectStorageUri

    return ObjectStorageUri.parse(value)


class _RecordingClickHouseConnector:
    host = "localhost"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    secure = False

    def __init__(self, *, count: int) -> None:
        self.count = count
        self.queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return self.count


class _FakeColumnarSink:
    def __init__(self) -> None:
        self.loaded: list[tuple[ObjectStorageStagingManifest, list[tuple[str, str]]]] = []
        self.windowed_loaded: list[tuple[ObjectStorageColumnarChunkedArtifact, list[tuple[str, str]]]] = []

    def load_from_object_storage_manifest(
        self,
        load_config: LoadConfig,
        manifest: ObjectStorageStagingManifest,
        schema: list[tuple[str, str]],
    ) -> int:
        del load_config
        self.loaded.append((manifest, schema))
        return 7

    def load_from_object_storage_chunked(
        self,
        load_config: LoadConfig,
        artifact: ObjectStorageColumnarChunkedArtifact,
        schema: list[tuple[str, str]],
    ) -> int:
        del load_config
        self.windowed_loaded.append((artifact, schema))
        return 3


class _FakeClickHouseSink:
    def __init__(self, connector: _RecordingClickHouseConnector) -> None:
        self.connector = connector
        self.client_runner = None

    @staticmethod
    def _table(load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    def _count(self, load_config: LoadConfig) -> int:
        del load_config
        return self.connector.count

    def _build_client_runner(self, load_config: LoadConfig, *, input_format: str | None = None):
        del load_config
        self.client_runner = _RecordingClientRunner(input_format=input_format or "TabSeparated")
        return self.client_runner

    def _build_http_runner(self, load_config: LoadConfig, *, input_format: str | None = None):
        raise AssertionError(f"HTTP runner is not expected in this test: {load_config}, {input_format}")


class _FakeStagingDecoder:
    @staticmethod
    def prepare(load_config: LoadConfig, staging_config: LoadConfig, payload: LoadPayload):
        del load_config, payload
        return staging_config, None


class _FakeStagedSink:
    def __init__(self, *, staged_rows: int) -> None:
        self.staged_rows = staged_rows
        self._staging_decoder = _FakeStagingDecoder()

    def _create_payload_staging_table(self, load_config: LoadConfig, payload: LoadPayload) -> LoadConfig:
        del payload
        return load_config

    def _insert_payload(self, load_config: LoadConfig, payload: LoadPayload) -> int:
        del load_config, payload
        return self.staged_rows


class _RecordingClientOptions:
    def __init__(self, *, input_format: str) -> None:
        self.input_format = input_format


class _RecordingClientRunner:
    def __init__(self, *, input_format: str) -> None:
        self.options = _RecordingClientOptions(input_format=input_format)
        self.inserts: list[tuple[str, list[str], str]] = []

    def insert_file(self, table: str, columns: list[str], input_path: str) -> None:
        self.inserts.append((table, columns, input_path))


class _WindowProvider:
    def __init__(self, windows: list[ObjectStorageChunkWindow]) -> None:
        self.windows = windows
        self.requests: list[object] = []

    def iter_object_storage_windows(self, request: object):
        self.requests.append(request)
        yield from self.windows


class _CleanupRecorder:
    def __init__(self) -> None:
        self.deleted_prefixes: list[str] = []

    def delete_prefix(self, prefix) -> int:
        self.deleted_prefixes.append(str(prefix))
        return 1


class _StepClock:
    def __init__(self, values: list[float]) -> None:
        self.values = values
        self.index = 0

    def __call__(self) -> float:
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        return value
