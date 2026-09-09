"""Route authority shared by Studio draft, plan, and static-check services."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.manifest.models import ProcessSpec
from dpone.readiness.capability_discovery_protocols import (
    CapabilitySnapshotProtocol,
    CapabilitySnapshotProvider,
)
from dpone.readiness.capability_discovery_service import (
    CapabilityDiscoveryError,
    normalize_connector_ref,
    require_authoring_authority,
)
from dpone.readiness.resolved_process_route import resolve_process_route
from dpone.readiness.studio_errors import StudioError


class StudioRouteValidator:
    """Reject complete manifest routes absent from the canonical capability snapshot."""

    def __init__(self, capabilities: CapabilitySnapshotProvider) -> None:
        self._capabilities = capabilities

    def require_supported_route(self, source: str, sink: str, strategy: str) -> None:
        snapshot = self._snapshot()
        self._require_supported_route(snapshot, source, sink, strategy)

    def _require_supported_route(
        self,
        snapshot: CapabilitySnapshotProtocol,
        source: str,
        sink: str,
        strategy: str,
    ) -> None:
        route_id = ":".join(
            (
                normalize_connector_ref(source),
                normalize_connector_ref(sink),
                _normalize_strategy(strategy),
            )
        )
        routes = getattr(snapshot, "routes", ())
        route = next((item for item in routes if item.id == route_id), None)
        if route is None or route.support.status == "not_supported":
            raise StudioError(
                "DPONE_ROUTE_NOT_SUPPORTED",
                "The requested source, sink, and strategy route is not supported.",
            )

    def validate_processes(self, processes: Sequence[ProcessSpec]) -> None:
        snapshot = self._snapshot()
        for process in processes:
            route = resolve_process_route(process)
            self._require_supported_route(snapshot, route.source, route.sink, route.strategy)

    def _snapshot(self) -> CapabilitySnapshotProtocol:
        snapshot = self._capabilities()
        try:
            require_authoring_authority(snapshot)
        except CapabilityDiscoveryError as exc:
            raise StudioError(
                exc.code,
                "The configured authoring authority is invalid; fix it before using Studio authoring.",
            ) from None
        return snapshot


def _normalize_strategy(value: str) -> str:
    return value.strip().lower().replace("-", "_")


__all__ = ["StudioRouteValidator"]
