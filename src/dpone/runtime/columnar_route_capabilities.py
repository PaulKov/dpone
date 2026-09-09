"""Columnar fast-path route capability adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability
from dpone.runtime.route_capabilities import (
    CapabilityEvidence,
    CapabilityRequirement,
    RouteCandidate,
    RouteCapabilityDecision,
    RouteCapabilityPlanner,
    RuntimeCapability,
)

REQ_SOURCE_COLUMNAR = "source.columnar_snapshot.parquet"
REQ_STORAGE_ACCESS = "storage.object.prefix_access"
REQ_S3 = "sink.object_storage_pull.s3"
REQ_S3_CLUSTER = "sink.object_storage_pull.s3_cluster"
REQ_NAMED_COLLECTION = "sink.auth.named_collection"
REQ_PARQUET = "format.parquet.read"
REQ_PUSH_STREAM = "transport.push_stream"


class ColumnarRouteCapabilityPlanner:
    """Map columnar source/sink/storage evidence into generic route decisions."""

    def __init__(self, *, planner: RouteCapabilityPlanner | None = None) -> None:
        self._planner = planner or RouteCapabilityPlanner()

    def decide(
        self,
        *,
        mode: str,
        requested_provider: str,
        source_capability: ColumnarSnapshotCapability,
        object_storage_access: Any | None,
        sink_evidence: Mapping[str, CapabilityEvidence],
        include_streaming_fallback: bool = True,
    ) -> RouteCapabilityDecision:
        evidence = columnar_route_evidence(
            source_capability=source_capability,
            object_storage_access=object_storage_access,
            sink_evidence=sink_evidence,
        )
        return self._planner.decide(
            candidates=columnar_route_candidates(include_streaming_fallback),
            evidence=evidence,
            mode=mode,
            requested_route_id=_requested_route(requested_provider),
        )


def columnar_route_candidates(include_streaming_fallback: bool = True) -> tuple[RouteCandidate, ...]:
    routes = [
        RouteCandidate(
            route_id="object_storage_pull_s3cluster",
            priority=10,
            expected_performance_class="very_high",
            requirements=(
                _requirement(REQ_SOURCE_COLUMNAR, RuntimeCapability.SOURCE_READ, "source.columnar_writer_missing"),
                _requirement(REQ_STORAGE_ACCESS, RuntimeCapability.INTERMEDIATE_STORAGE, "storage.runtime_put_denied"),
                _requirement(REQ_NAMED_COLLECTION, RuntimeCapability.AUTH, "sink.auth.named_collection_missing"),
                _requirement(REQ_S3_CLUSTER, RuntimeCapability.CLUSTER, "sink.cluster_pull_unsupported"),
                _requirement(REQ_PARQUET, RuntimeCapability.FILE_FORMAT, "format.parquet_read_unsupported"),
            ),
        ),
        RouteCandidate(
            route_id="object_storage_pull_s3",
            priority=20,
            expected_performance_class="high",
            requirements=(
                _requirement(REQ_SOURCE_COLUMNAR, RuntimeCapability.SOURCE_READ, "source.columnar_writer_missing"),
                _requirement(REQ_STORAGE_ACCESS, RuntimeCapability.INTERMEDIATE_STORAGE, "storage.runtime_put_denied"),
                _requirement(REQ_NAMED_COLLECTION, RuntimeCapability.AUTH, "sink.auth.named_collection_missing"),
                _requirement(REQ_S3, RuntimeCapability.TRANSPORT, "sink.object_storage_pull_unsupported"),
                _requirement(REQ_PARQUET, RuntimeCapability.FILE_FORMAT, "format.parquet_read_unsupported"),
            ),
        ),
        RouteCandidate(
            route_id="direct_push_columnar",
            priority=70,
            expected_performance_class="medium_high",
            requirements=(
                _requirement(REQ_SOURCE_COLUMNAR, RuntimeCapability.SOURCE_READ, "source.columnar_writer_missing"),
                _requirement(REQ_PARQUET, RuntimeCapability.FILE_FORMAT, "format.parquet_read_unsupported"),
                _requirement(REQ_PUSH_STREAM, RuntimeCapability.TRANSPORT, "transport.push_stream_missing"),
            ),
        ),
    ]
    if include_streaming_fallback:
        routes.append(
            RouteCandidate(
                route_id="typed_binary_streaming",
                priority=90,
                expected_performance_class="standard",
                requirements=(
                    _requirement(REQ_PUSH_STREAM, RuntimeCapability.TRANSPORT, "transport.push_stream_missing"),
                ),
            )
        )
    return tuple(routes)


def columnar_route_evidence(
    *,
    source_capability: ColumnarSnapshotCapability,
    object_storage_access: Any | None,
    sink_evidence: Mapping[str, CapabilityEvidence],
) -> dict[str, CapabilityEvidence]:
    evidence = {
        REQ_SOURCE_COLUMNAR: _source_evidence(source_capability),
        REQ_STORAGE_ACCESS: _storage_evidence(object_storage_access),
        REQ_PUSH_STREAM: CapabilityEvidence.success(
            requirement_id=REQ_PUSH_STREAM,
            domain=RuntimeCapability.TRANSPORT,
            checks=("existing_streaming_route",),
        ),
    }
    evidence.update(sink_evidence)
    return evidence


def _requirement(requirement_id: str, domain: RuntimeCapability, blocker: str) -> CapabilityRequirement:
    return CapabilityRequirement(
        id=requirement_id, domain=domain, required_for="columnar_fast_path", blocker_code=blocker
    )


def _requested_route(requested_provider: str) -> str | None:
    if requested_provider == "object_storage_pull":
        return "object_storage_pull_s3cluster"
    if requested_provider == "direct_push_columnar":
        return "direct_push_columnar"
    return None


def _source_evidence(capability: ColumnarSnapshotCapability) -> CapabilityEvidence:
    if capability.certified and not capability.blockers:
        return CapabilityEvidence.success(
            requirement_id=REQ_SOURCE_COLUMNAR,
            domain=RuntimeCapability.SOURCE_READ,
            connector_id=capability.provider_id,
            checks=("source_columnar_provider_certified",),
            warnings=capability.warnings,
            details=capability.details,
        )
    return CapabilityEvidence.failure(
        requirement_id=REQ_SOURCE_COLUMNAR,
        domain=RuntimeCapability.SOURCE_READ,
        connector_id=capability.provider_id,
        blockers=_source_blockers(capability.blockers),
        warnings=capability.warnings,
        details=capability.details,
    )


def _source_blockers(blockers: tuple[str, ...]) -> tuple[str, ...]:
    if not blockers:
        return ("source.columnar_writer_missing",)
    if any(blocker in {"parquet_writer_unavailable", "mssql_streaming_reader_unavailable"} for blocker in blockers):
        return ("source.columnar_writer_missing",)
    return blockers


def _storage_evidence(access: Any | None) -> CapabilityEvidence:
    if access is None:
        return CapabilityEvidence.failure(
            requirement_id=REQ_STORAGE_ACCESS,
            domain=RuntimeCapability.INTERMEDIATE_STORAGE,
            blockers=("object_storage_preflight_missing",),
        )
    if bool(getattr(access, "passed", False)):
        return CapabilityEvidence.success(
            requirement_id=REQ_STORAGE_ACCESS,
            domain=RuntimeCapability.INTERMEDIATE_STORAGE,
            checks=tuple(getattr(access, "checks", ())),
            warnings=tuple(getattr(access, "warnings", ())),
            details=_access_details(access),
        )
    return CapabilityEvidence.failure(
        requirement_id=REQ_STORAGE_ACCESS,
        domain=RuntimeCapability.INTERMEDIATE_STORAGE,
        blockers=tuple(getattr(access, "blockers", ())) or ("object_storage_preflight_failed",),
        warnings=tuple(getattr(access, "warnings", ())),
        details=_access_details(access),
    )


def _access_details(access: Any) -> dict[str, object]:
    return {"preflight_schema": str(getattr(access, "schema_version", "unknown"))}


__all__ = [
    "ColumnarRouteCapabilityPlanner",
    "REQ_NAMED_COLLECTION",
    "REQ_PARQUET",
    "REQ_S3",
    "REQ_S3_CLUSTER",
    "REQ_SOURCE_COLUMNAR",
    "REQ_STORAGE_ACCESS",
    "columnar_route_candidates",
    "columnar_route_evidence",
]
