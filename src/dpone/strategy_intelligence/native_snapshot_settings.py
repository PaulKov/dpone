from __future__ import annotations

from typing import Any

from dpone.runtime.native_snapshot_capabilities import resolve_native_tcp_backend_availability
from dpone.runtime.native_snapshot_optimization import SnapshotRouteOptimizer, SnapshotRouteRequest


def build_snapshot_optimization_evidence(
    *,
    source_type: str,
    sink_type: str,
    source_options: dict[str, Any],
    sink_options: dict[str, Any],
) -> dict[str, Any]:
    native_tcp = resolve_native_tcp_backend_availability(sink_options)
    return (
        SnapshotRouteOptimizer()
        .plan(
            SnapshotRouteRequest(
                source_type=source_type,
                sink_type=sink_type,
                source_options=source_options,
                sink_options=sink_options,
                available_native_tcp_backends=native_tcp.available_backends,
                direct_ingest_certified=native_tcp.direct_ingest_certified,
            )
        )
        .to_evidence()
    )


__all__ = ["build_snapshot_optimization_evidence"]
