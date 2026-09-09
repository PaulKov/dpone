from __future__ import annotations

from dpone.runtime.columnar_route_capabilities import ColumnarRouteCapabilityPlanner
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability
from dpone.runtime.object_storage_access import (
    ObjectStorageAccessEvidence,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.route_capabilities import CapabilityEvidence, RuntimeCapability


def test_columnar_route_selects_s3_pull_when_cluster_pull_is_blocked() -> None:
    decision = ColumnarRouteCapabilityPlanner().decide(
        mode="auto",
        requested_provider="object_storage_pull",
        source_capability=_source_capability(certified=True),
        object_storage_access=_access_evidence(passed=True),
        sink_evidence={
            "sink.object_storage_pull.s3_cluster": _sink_failure(
                "sink.object_storage_pull.s3_cluster",
                RuntimeCapability.CLUSTER,
                "sink.cluster_pull_unsupported",
            ),
            "sink.object_storage_pull.s3": _sink_success("sink.object_storage_pull.s3", RuntimeCapability.TRANSPORT),
            "sink.auth.named_collection": _sink_success("sink.auth.named_collection", RuntimeCapability.AUTH),
            "format.parquet.read": _sink_success("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        },
    )

    assert decision.selected_route_id == "object_storage_pull_s3"
    assert decision.fallback_reason == "sink.cluster_pull_unsupported"
    assert decision.should_start_source_io is True


def test_columnar_route_falls_back_when_columnar_writer_is_missing() -> None:
    decision = ColumnarRouteCapabilityPlanner().decide(
        mode="auto",
        requested_provider="object_storage_pull",
        source_capability=_source_capability(certified=False, blockers=("source.columnar_writer_missing",)),
        object_storage_access=_access_evidence(passed=True),
        sink_evidence={
            "sink.object_storage_pull.s3_cluster": _sink_success(
                "sink.object_storage_pull.s3_cluster", RuntimeCapability.CLUSTER
            ),
            "sink.object_storage_pull.s3": _sink_success("sink.object_storage_pull.s3", RuntimeCapability.TRANSPORT),
            "sink.auth.named_collection": _sink_success("sink.auth.named_collection", RuntimeCapability.AUTH),
            "format.parquet.read": _sink_success("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        },
    )

    assert decision.selected_route_id == "typed_binary_streaming"
    assert decision.fallback_reason == "source.columnar_writer_missing"
    assert decision.recommendations == ("Install dpone[columnar] or use a streaming fallback.",)


def test_required_columnar_route_blocks_before_source_io() -> None:
    decision = ColumnarRouteCapabilityPlanner().decide(
        mode="required",
        requested_provider="object_storage_pull",
        source_capability=_source_capability(certified=True),
        object_storage_access=_access_evidence(passed=False, blockers=("storage.runtime_put_denied",)),
        sink_evidence={
            "sink.object_storage_pull.s3_cluster": _sink_success(
                "sink.object_storage_pull.s3_cluster", RuntimeCapability.CLUSTER
            ),
            "sink.object_storage_pull.s3": _sink_success("sink.object_storage_pull.s3", RuntimeCapability.TRANSPORT),
            "sink.auth.named_collection": _sink_success("sink.auth.named_collection", RuntimeCapability.AUTH),
            "format.parquet.read": _sink_success("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        },
    )

    assert decision.selected_route_id == "blocked"
    assert decision.should_start_source_io is False
    assert decision.blockers == ("storage.runtime_put_denied",)


def test_source_capability_details_are_preserved_in_route_evidence() -> None:
    decision = ColumnarRouteCapabilityPlanner().decide(
        mode="required",
        requested_provider="direct_push_columnar",
        source_capability=_source_capability(
            certified=False,
            blockers=("source.columnar_request_failed:OperationalError",),
            details={"exception": "OperationalError", "message": "certificate verify failed"},
        ),
        object_storage_access=None,
        sink_evidence={
            "format.parquet.read": _sink_success("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        },
    )

    source_evidence = next(
        item for item in decision.evidence if item.requirement_id == "source.columnar_snapshot.parquet"
    )
    assert source_evidence.details == {
        "exception": "OperationalError",
        "message": "certificate verify failed",
    }


def test_direct_push_columnar_provider_is_a_first_class_candidate() -> None:
    decision = ColumnarRouteCapabilityPlanner().decide(
        mode="required",
        requested_provider="direct_push_columnar",
        source_capability=_source_capability(certified=True),
        object_storage_access=None,
        sink_evidence={
            "format.parquet.read": _sink_success("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        },
    )

    assert decision.selected_route_id == "direct_push_columnar"
    assert decision.should_start_source_io is True


def _source_capability(
    *,
    certified: bool,
    blockers: tuple[str, ...] = (),
    details: dict[str, object] | None = None,
) -> ColumnarSnapshotCapability:
    return ColumnarSnapshotCapability(
        provider_id="mssql_odbc_arrow_parquet",
        certified=certified,
        blockers=blockers,
        details=details or {},
    )


def _access_evidence(*, passed: bool, blockers: tuple[str, ...] = ()) -> ObjectStorageAccessEvidence:
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
        blockers=blockers,
    )


def _sink_success(requirement_id: str, domain: RuntimeCapability) -> CapabilityEvidence:
    return CapabilityEvidence.success(requirement_id=requirement_id, domain=domain)


def _sink_failure(requirement_id: str, domain: RuntimeCapability, blocker: str) -> CapabilityEvidence:
    return CapabilityEvidence.failure(requirement_id=requirement_id, domain=domain, blockers=(blocker,))
