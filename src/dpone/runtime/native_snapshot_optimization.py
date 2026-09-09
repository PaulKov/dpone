"""Connector-neutral adaptive snapshot route optimization.

The module is intentionally pure: it does not import MSSQL or ClickHouse
connectors and performs no I/O. Runtime adapters consume the produced decision.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.native_snapshot_evidence import (
    export_optimizer_blocked,
    export_optimizer_evidence,
    is_typed_native_wire,
    source_materialization_evidence,
    source_scan_evidence,
)
from dpone.runtime.native_snapshot_models import (
    SNAPSHOT_FALLBACK_CHAIN,
    SNAPSHOT_OPTIMIZATION_SCHEMA_VERSION,
    GovernorTelemetry,
    SnapshotOptimizationPolicy,
    SnapshotRouteDecision,
    SnapshotRouteRequest,
)


class SnapshotRouteOptimizer:
    """Side-effect-free route optimizer for native bulk snapshots."""

    def plan(self, request: SnapshotRouteRequest) -> SnapshotRouteDecision:
        policy = SnapshotOptimizationPolicy.from_source_options(request.source_options)
        source_scan = source_scan_evidence(request.source_options)
        source_export_optimizer = export_optimizer_evidence(request)
        source_materialization = source_materialization_evidence(request, source_scan)
        bulk = ClickHouseBulkOptionsResolver.resolve(request.sink_options)
        requested = bulk.mode.strip().lower() or "auto"
        requested_native_tcp_backend = bulk.native_tcp.backend
        wire_supported = is_typed_native_wire(request)
        backends = tuple(str(item).strip().lower() for item in request.available_ingest_backends)
        blockers: list[str] = []
        reasons: list[str] = []

        if export_optimizer_blocked(source_export_optimizer):
            blocked_export = source_export_optimizer or {}
            blockers.extend(str(item) for item in blocked_export.get("blockers", []) or ())
            return _decision(
                policy,
                requested,
                None,
                request,
                blockers=blockers,
                source_scan=source_scan,
                source_export_optimizer=source_export_optimizer,
                source_materialization=source_materialization,
            )

        if requested == "native_tcp" and not wire_supported:
            blockers.append("snapshot_native_tcp_requires_typed_binary_native_wire")
            return _decision(
                policy,
                requested,
                None,
                request,
                blockers=blockers,
                source_scan=source_scan,
                source_export_optimizer=source_export_optimizer,
                source_materialization=source_materialization,
            )

        selected = _select_backend(
            requested=requested,
            wire_supported=wire_supported,
            available_backends=backends,
            reasons=reasons,
        )
        native_tcp_backend = None
        if selected is None:
            blockers.append("snapshot_no_eligible_ingest_backend")
            return _decision(
                policy,
                requested,
                None,
                request,
                blockers=blockers,
                reasons=reasons,
                source_scan=source_scan,
                source_export_optimizer=source_export_optimizer,
                source_materialization=source_materialization,
            )
        if selected == "native_tcp":
            native_tcp_backend = _select_native_tcp_backend(
                requested=requested_native_tcp_backend,
                available_backends=tuple(str(item).strip().lower() for item in request.available_native_tcp_backends),
                direct_certified=request.direct_ingest_certified,
                blockers=blockers,
                reasons=reasons,
            )
            if native_tcp_backend is None:
                return _decision(
                    policy,
                    requested,
                    None,
                    request,
                    blockers=blockers,
                    reasons=reasons,
                    fallback_reason=blockers[0] if blockers else None,
                    source_scan=source_scan,
                    source_export_optimizer=source_export_optimizer,
                    source_materialization=source_materialization,
                )

        warnings = []
        if _is_release_policy(request.runner_policy) and not request.route_certified:
            blockers.append("snapshot_route_certification_missing")
        elif not request.route_certified:
            warnings.append("snapshot_route_uncertified_advisory")

        export_limit = policy.execution.effective_max_parallel_exports
        load_limit = policy.execution.effective_max_parallel_loads
        if _source_pressure(policy, request.source_telemetry):
            export_limit = 1
            reasons.append("snapshot_source_governor_throttled")
        if _target_pressure(policy, request.target_telemetry):
            load_limit = 1
            reasons.append("snapshot_target_merge_pressure_throttled")

        return _decision(
            policy,
            requested,
            selected,
            request,
            native_tcp_backend=native_tcp_backend,
            blockers=blockers,
            warnings=warnings,
            reasons=reasons,
            export_limit=export_limit,
            load_limit=load_limit,
            source_scan=source_scan,
            source_export_optimizer=source_export_optimizer,
            source_materialization=source_materialization,
        )


def _decision(
    policy: SnapshotOptimizationPolicy,
    requested: str,
    selected: str | None,
    request: SnapshotRouteRequest,
    *,
    native_tcp_backend: str | None = None,
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    reasons: Sequence[str] = (),
    fallback_reason: str | None = None,
    export_limit: int | None = None,
    load_limit: int | None = None,
    source_scan: dict[str, Any] | None = None,
    source_export_optimizer: dict[str, Any] | None = None,
    source_materialization: dict[str, Any] | None = None,
) -> SnapshotRouteDecision:
    partition_planner = request.partition_planner
    stats_confidence = request.stats_confidence
    if source_scan:
        partition_planner = str(source_scan.get("selected_scan") or partition_planner)
        stats_confidence = str(source_scan.get("stats_confidence") or stats_confidence)
    return SnapshotRouteDecision(
        requested_backend=requested,
        selected_backend=selected,
        native_tcp_backend=native_tcp_backend,
        compression=_compression(policy, selected),
        packet_size=policy.tuning.packet_size,
        block_rows=policy.tuning.block_rows,
        block_bytes=policy.tuning.block_bytes,
        max_parallel_exports=export_limit or policy.execution.effective_max_parallel_exports,
        max_parallel_loads=load_limit or policy.execution.effective_max_parallel_loads,
        release_gate=_release_gate(blockers, warnings),
        route_certified=request.route_certified,
        fallback_chain=SNAPSHOT_FALLBACK_CHAIN,
        partition_planner=partition_planner,
        stats_confidence=stats_confidence,
        fallback_reason=fallback_reason,
        source_scan=source_scan,
        source_export_optimizer=source_export_optimizer,
        source_materialization=source_materialization,
        blockers=tuple(blockers),
        warnings=tuple(warnings),
        reasons=tuple(dict.fromkeys(item for item in reasons if item)),
    )


def _select_backend(
    *,
    requested: str,
    wire_supported: bool,
    available_backends: tuple[str, ...],
    reasons: list[str],
) -> str | None:
    if requested != "auto":
        return requested if requested in available_backends else None
    if not wire_supported:
        reasons.append("snapshot_typed_native_wire_unavailable")
        return "python" if "python" in available_backends else None
    for backend in SNAPSHOT_FALLBACK_CHAIN:
        if backend in available_backends:
            if backend != "native_tcp":
                reasons.append("snapshot_native_tcp_backend_unavailable")
            return backend
    return None


def _select_native_tcp_backend(
    *,
    requested: str,
    available_backends: tuple[str, ...],
    direct_certified: bool,
    blockers: list[str],
    reasons: list[str],
) -> str | None:
    if requested == "client":
        return "client" if "client" in available_backends else None
    if requested == "direct":
        if "direct" not in available_backends or not direct_certified:
            blockers.append("snapshot_direct_native_tcp_backend_unavailable")
            return None
        return "direct"
    if "direct" in available_backends and direct_certified:
        return "direct"
    if "client" in available_backends:
        reasons.append("snapshot_direct_native_tcp_fallback_client")
        return "client"
    blockers.append("snapshot_native_tcp_backend_unavailable")
    return None


def _source_pressure(policy: SnapshotOptimizationPolicy, telemetry: GovernorTelemetry | None) -> bool:
    if not policy.source_governor.enabled or telemetry is None:
        return False
    cpu_trip = telemetry.cpu_pct is not None and telemetry.cpu_pct > policy.source_governor.max_source_cpu_pct
    time_trip = (
        telemetry.duration_seconds is not None and telemetry.duration_seconds > policy.source_governor.max_query_seconds
    )
    return bool(cpu_trip or time_trip)


def _target_pressure(policy: SnapshotOptimizationPolicy, telemetry: GovernorTelemetry | None) -> bool:
    if not policy.target_governor.enabled or telemetry is None:
        return False
    parts_trip = (
        telemetry.parts_per_partition is not None
        and telemetry.parts_per_partition > policy.target_governor.max_parts_per_partition
    )
    merges_trip = (
        telemetry.active_merges is not None and telemetry.active_merges > policy.target_governor.max_inflight_blocks
    )
    return bool(parts_trip or merges_trip)


def _compression(policy: SnapshotOptimizationPolicy, selected: str | None) -> str:
    requested = policy.tuning.compression
    if requested != "auto":
        return requested
    return "lz4" if selected in {"native_tcp", "client"} else "none"


def _release_gate(blockers: Sequence[str], warnings: Sequence[str]) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "green"


def _is_release_policy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"release", "production", "prod"}


__all__ = [
    "GovernorTelemetry",
    "SNAPSHOT_OPTIMIZATION_SCHEMA_VERSION",
    "SnapshotOptimizationPolicy",
    "SnapshotRouteDecision",
    "SnapshotRouteOptimizer",
    "SnapshotRouteRequest",
]
