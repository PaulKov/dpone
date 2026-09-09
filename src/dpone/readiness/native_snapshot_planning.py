from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.native_snapshot_capabilities import resolve_native_tcp_backend_availability
from dpone.runtime.native_snapshot_optimization import SnapshotRouteOptimizer, SnapshotRouteRequest


def build_native_transfer_snapshot_optimization(
    raw: Mapping[str, Any],
    *,
    source_type: str,
    sink_type: str,
) -> dict[str, Any]:
    source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
    sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
    source_options = source.get("options", {}) if isinstance(source.get("options"), Mapping) else {}
    sink_options = sink.get("options", {}) if isinstance(sink.get("options"), Mapping) else {}
    native_tcp = resolve_native_tcp_backend_availability(sink_options if isinstance(sink_options, Mapping) else {})
    return (
        SnapshotRouteOptimizer()
        .plan(
            SnapshotRouteRequest(
                source_type=source_type,
                sink_type=sink_type,
                source_options=source_options if isinstance(source_options, Mapping) else {},
                sink_options=sink_options if isinstance(sink_options, Mapping) else {},
                available_native_tcp_backends=native_tcp.available_backends,
                direct_ingest_certified=native_tcp.direct_ingest_certified,
            )
        )
        .to_evidence()
    )


__all__ = ["build_native_transfer_snapshot_optimization"]
