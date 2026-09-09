from __future__ import annotations

from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.columnar_execution_mode import resolve_columnar_execution_policy
from dpone.runtime.columnar_route_runtime import (
    ColumnarCapabilityProbeRunner,
    ColumnarDirectPushExecutor,
    ColumnarObjectStoragePullExecutor,
    ColumnarRouteCandidateProvider,
)
from dpone.runtime.columnar_runtime_assembly import _object_storage_options, _SourceCapabilityProbe
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability, ColumnarSnapshotRequest
from dpone.runtime.object_storage_access import (
    ObjectStorageAccessEvidence,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.route_capabilities import CapabilityEvidence, RuntimeCapability


def test_columnar_candidate_provider_returns_pull_direct_and_streaming_routes() -> None:
    provider = ColumnarRouteCandidateProvider()

    route_ids = [
        candidate.route_id for candidate in provider.candidates(load_config=_cfg(), source=object(), sink=object())
    ]

    assert route_ids == [
        "object_storage_pull_s3cluster",
        "object_storage_pull_s3",
        "direct_push_columnar",
        "typed_binary_streaming",
    ]


def test_columnar_probe_runner_maps_source_storage_and_sink_evidence() -> None:
    runner = ColumnarCapabilityProbeRunner(
        source_capability=lambda **_: ColumnarSnapshotCapability(
            provider_id="mssql_odbc_arrow_parquet", certified=True
        ),
        object_storage_access=lambda **_: _access_evidence(passed=True),
        sink_evidence=lambda **_: {
            "sink.object_storage_pull.s3": CapabilityEvidence.success(
                requirement_id="sink.object_storage_pull.s3",
                domain=RuntimeCapability.TRANSPORT,
            ),
            "format.parquet.read": CapabilityEvidence.success(
                requirement_id="format.parquet.read",
                domain=RuntimeCapability.FILE_FORMAT,
            ),
        },
    )

    evidence = runner.probe(
        candidates=(),
        load_config=_cfg(),
        source=object(),
        sink=object(),
    )

    assert evidence["source.columnar_snapshot.parquet"].passed is True
    assert evidence["storage.object.prefix_access"].passed is True
    assert evidence["transport.push_stream"].passed is True
    assert evidence["sink.object_storage_pull.s3"].passed is True


def test_columnar_probe_runner_passes_object_storage_evidence_to_sink_probe() -> None:
    access = _access_evidence(passed=True)
    seen: dict[str, object] = {}
    runner = ColumnarCapabilityProbeRunner(
        source_capability=lambda **_: ColumnarSnapshotCapability(provider_id="provider", certified=True),
        object_storage_access=lambda **_: access,
        sink_evidence=lambda **context: (
            seen.setdefault("object_storage_access", context.get("object_storage_access")) and {}
        ),
    )

    runner.probe(candidates=(), load_config=_cfg(), source=object(), sink=object())

    assert seen["object_storage_access"] is access


def test_source_capability_probe_records_redacted_exception_details() -> None:
    def fail_factory(**_):
        raise RuntimeError("PWD=secret; certificate verify failed")

    capability = _SourceCapabilityProbe(fail_factory, SimpleNamespace(provider_id="provider"))(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        load_record=SimpleNamespace(run_id="run-1"),
    )

    assert capability.blockers == ("source.columnar_request_failed:RuntimeError",)
    assert capability.details == {
        "exception": "RuntimeError",
        "message": "***REDACTED***",
    }


def test_columnar_object_storage_pull_executor_returns_extract_result_from_manifest() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="s3://bucket/run-1/",
        run_id="run-1",
        options={"execution": {"mode": "file"}},
    )
    snapshot_provider = _SnapshotProvider(manifest=SimpleNamespace(kind="manifest"))
    executor = ColumnarObjectStoragePullExecutor(
        route_id="object_storage_pull_s3",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.kind == "manifest"
    assert result.schema == [("id", "int")]
    assert snapshot_provider.requests == [request]


def test_columnar_object_storage_pull_executor_defaults_to_lazy_chunked_artifact() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="s3://bucket/run-1/",
        run_id="run-1",
    )
    window = SimpleNamespace(index=0, object_key_pattern=lambda: "run-1/window-000001/*.parquet")
    snapshot_provider = _SnapshotProvider(object_windows=[window])
    executor = ColumnarObjectStoragePullExecutor(
        route_id="object_storage_pull_s3",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.__class__.__name__ == "ObjectStorageColumnarChunkedArtifact"
    assert result.schema == [("id", "int")]
    assert snapshot_provider.requests == []
    assert snapshot_provider.object_window_requests == []
    assert list(result.artifact.iter_windows()) == [window]
    assert snapshot_provider.object_window_requests == [request]


def test_columnar_object_storage_pull_executor_keeps_explicit_file_mode() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="s3://bucket/run-1/",
        run_id="run-1",
        options={"execution": {"mode": "file"}},
    )
    snapshot_provider = _SnapshotProvider(manifest=SimpleNamespace(kind="object_manifest"))
    executor = ColumnarObjectStoragePullExecutor(
        route_id="object_storage_pull_s3",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.kind == "object_manifest"
    assert snapshot_provider.requests == [request]


def test_columnar_execution_policy_rejects_conflicting_legacy_direct_push_mode() -> None:
    import pytest

    with pytest.raises(RuntimeError, match="columnar_execution_mode_conflict"):
        resolve_columnar_execution_policy(
            {
                "execution": {"mode": "chunked"},
                "direct_push": {"mode": "file"},
            }
        )


def test_columnar_runtime_reads_object_storage_from_source_option_namespace() -> None:
    load_config = _cfg()
    load_config.options = {
        "source_options": {
            "native_transfer": {
                "snapshot": {
                    "columnar_fast_path": {
                        "mode": "required",
                        "object_storage": {"runtime_access": {"connection_id": "s3_writer"}},
                    }
                }
            }
        },
        "sink_options": {
            "clickhouse_bulk": {
                "columnar_pull": {"cluster": "dwh"},
            }
        },
    }

    assert _object_storage_options(load_config)["runtime_access"]["connection_id"] == "s3_writer"


def test_columnar_direct_push_executor_defaults_to_lazy_chunked_artifact() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="local://dpone/run-1/",
        run_id="run-1",
    )
    chunk = SimpleNamespace(path="/tmp/chunk-00000.parquet", index=0)
    snapshot_provider = _SnapshotProvider(local_chunks=[chunk])
    executor = ColumnarDirectPushExecutor(
        route_id="direct_push_columnar",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.__class__.__name__ == "LocalColumnarChunkedArtifact"
    assert result.schema == [("id", "int")]
    assert snapshot_provider.local_requests == []
    assert snapshot_provider.chunked_requests == []
    assert list(result.artifact.iter_chunks()) == [chunk]
    assert snapshot_provider.chunked_requests == [request]


def test_columnar_direct_push_executor_keeps_explicit_file_mode() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="local://dpone/run-1/",
        run_id="run-1",
        options={"direct_push": {"mode": "file"}},
    )
    snapshot_provider = _SnapshotProvider(local_manifest=SimpleNamespace(kind="local_columnar_manifest"))
    executor = ColumnarDirectPushExecutor(
        route_id="direct_push_columnar",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.kind == "local_columnar_manifest"
    assert result.schema == [("id", "int")]
    assert snapshot_provider.local_requests == [request]


def test_columnar_direct_push_executor_accepts_deprecated_two_phase_alias() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="local://dpone/run-1/",
        run_id="run-1",
        options={"direct_push": {"mode": "two_phase"}},
    )
    snapshot_provider = _SnapshotProvider(local_manifest=SimpleNamespace(kind="local_columnar_manifest"))
    executor = ColumnarDirectPushExecutor(
        route_id="direct_push_columnar",
        request_factory=lambda **_: request,
        snapshot_provider=snapshot_provider,
    )

    result = executor.extract(
        load_config=_cfg(),
        source=object(),
        sink=object(),
        state=None,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )

    assert result.artifact.kind == "local_columnar_manifest"
    assert snapshot_provider.local_requests == [request]


def test_columnar_direct_push_executor_blocks_unimplemented_streaming_mode() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT id FROM dbo.orders",
        schema=[("id", "int")],
        uri_prefix="local://dpone/run-1/",
        run_id="run-1",
        options={"direct_push": {"mode": "streaming"}},
    )
    executor = ColumnarDirectPushExecutor(
        route_id="direct_push_columnar",
        request_factory=lambda **_: request,
        snapshot_provider=_SnapshotProvider(local_chunks=[]),
    )

    import pytest

    with pytest.raises(RuntimeError, match="columnar_streaming_provider_not_certified"):
        executor.extract(
            load_config=_cfg(),
            source=object(),
            sink=object(),
            state=None,
            load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
        )


def _cfg() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={},
    )


def _access_evidence(*, passed: bool) -> ObjectStorageAccessEvidence:
    return ObjectStorageAccessEvidence(
        passed=passed,
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="writer"),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        uri_prefix="s3://bucket/prefix/{run_id}/",
        sentinel_uri="s3://bucket/prefix/run/__dpone_sentinel.parquet",
        clickhouse_probe_sql=None,
        checks=("runtime_put_object",) if passed else (),
        warnings=(),
        blockers=(),
    )


class _SnapshotProvider:
    def __init__(self, manifest=None, local_manifest=None, local_chunks=None, object_windows=None) -> None:
        self.manifest = manifest
        self.local_manifest = local_manifest
        self.local_chunks = tuple(local_chunks or ())
        self.object_windows = tuple(object_windows or ())
        self.requests: list[ColumnarSnapshotRequest] = []
        self.local_requests: list[ColumnarSnapshotRequest] = []
        self.chunked_requests: list[ColumnarSnapshotRequest] = []
        self.object_window_requests: list[ColumnarSnapshotRequest] = []

    def snapshot(self, request: ColumnarSnapshotRequest):
        self.requests.append(request)
        return self.manifest

    def snapshot_local(self, request: ColumnarSnapshotRequest):
        self.local_requests.append(request)
        return self.local_manifest

    def iter_local_chunks(self, request: ColumnarSnapshotRequest):
        self.chunked_requests.append(request)
        yield from self.local_chunks

    def iter_object_storage_windows(self, request: ColumnarSnapshotRequest):
        self.object_window_requests.append(request)
        yield from self.object_windows
