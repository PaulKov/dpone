"""Capability bridge for native snapshot route planning."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.direct_ingest import DirectIngestResolver, DirectIngestRouteRequest


@dataclass(frozen=True, slots=True)
class NativeTcpBackendAvailability:
    """Resolved Native TCP backends for snapshot optimization."""

    available_backends: tuple[str, ...] = ("client",)
    direct_ingest_certified: bool = False
    direct_ingest_fallback_reason: str | None = None


def resolve_native_tcp_backend_availability(
    sink_options: Mapping[str, Any] | None,
    *,
    provider_loader: Callable[[], Any] | None = None,
) -> NativeTcpBackendAvailability:
    """Resolve direct Native TCP support from the optional provider contract."""

    bulk = ClickHouseBulkOptionsResolver.resolve(sink_options or {})
    decision = DirectIngestResolver(provider_loader=provider_loader).decide(
        DirectIngestRouteRequest(
            bulk_options=bulk,
            input_format="Native",
            route_certified=True,
        )
    )
    if decision.selected_backend == "direct" and decision.certified:
        return NativeTcpBackendAvailability(
            available_backends=("direct", "client"),
            direct_ingest_certified=True,
        )
    return NativeTcpBackendAvailability(
        available_backends=("client",),
        direct_ingest_certified=False,
        direct_ingest_fallback_reason=decision.fallback_reason,
    )


__all__ = [
    "NativeTcpBackendAvailability",
    "resolve_native_tcp_backend_availability",
]
