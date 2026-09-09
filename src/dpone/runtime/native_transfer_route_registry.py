from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.certification_trust import certification_trust
from dpone.runtime.native_transfer_capabilities import _codec_capability, _sink_capability, _source_capability
from dpone.runtime.native_transfer_route_models import (
    FALLBACK_CHAIN,
    ROUTE_CERTIFICATION_SCHEMA_VERSION,
    RouteCapabilityMatrix,
    RouteCertificationPolicy,
    RouteTransportCandidate,
)


class NativeTransferRouteCapabilityRegistry:
    """Aggregates built-in capabilities and route certification artifacts."""

    def build_matrix(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        source_options: Mapping[str, Any],
        sink_options: Mapping[str, Any],
        requested_transport: str,
        certification_policy: RouteCertificationPolicy,
    ) -> RouteCapabilityMatrix:
        artifact = _load_artifact(certification_policy.artifact)
        certification = _certification_state(
            artifact.payload,
            source=source_type,
            sink=sink_type,
            strategy=strategy,
        )
        candidates = tuple(
            self._candidate(
                transport,
                source_type=source_type,
                sink_type=sink_type,
                source_options=source_options,
                sink_options=sink_options,
                certified=transport in certification.transports,
                artifact_path=certification_policy.artifact if transport in certification.transports else None,
            )
            for transport in FALLBACK_CHAIN
        )
        return RouteCapabilityMatrix(
            source=source_type,
            sink=sink_type,
            strategy=strategy,
            requested_transport=requested_transport,
            candidates=candidates,
            certification_artifact_status=certification.status if artifact.status == "present" else artifact.status,
        )

    def _candidate(
        self,
        transport: str,
        *,
        source_type: str,
        sink_type: str,
        source_options: Mapping[str, Any],
        sink_options: Mapping[str, Any],
        certified: bool,
        artifact_path: str | None,
    ) -> RouteTransportCandidate:
        if transport == "stream":
            source = _source_capability(source_type, source_options)
            sink = _sink_capability(sink_type, sink_options)
            codec = _codec_capability(source_type, sink_type, source_options)
            reasons = tuple(item.reason for item in (source, sink, codec) if not item.is_supported)
            return RouteTransportCandidate(
                transport="stream",
                source=source.is_supported,
                sink=sink.is_supported,
                codec=codec.is_supported,
                staging_safe=sink.is_supported,
                certified=certified,
                reasons=reasons,
                certification_artifact=artifact_path,
            )
        if transport == "object":
            return RouteTransportCandidate(
                transport="object",
                source=False,
                sink=False,
                codec=True,
                staging_safe=True,
                certified=certified,
                reasons=("native_transfer_object_transport_not_configured",),
                certification_artifact=artifact_path,
            )
        return RouteTransportCandidate(
            transport="file",
            source=True,
            sink=True,
            codec=True,
            staging_safe=True,
            certified=certified,
            certification_artifact=artifact_path,
        )


class _CertificationArtifact:
    def __init__(self, *, status: str, payload: Mapping[str, Any] | None = None) -> None:
        self.status = status
        self.payload = payload or {}


class _CertificationState:
    def __init__(self, *, status: str, transports: set[str] | None = None) -> None:
        self.status = status
        self.transports = transports or set()


def _load_artifact(path_value: str | None) -> _CertificationArtifact:
    if not path_value:
        return _CertificationArtifact(status="not_configured")
    path = Path(path_value)
    if not path.is_file():
        return _CertificationArtifact(status="missing")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _CertificationArtifact(status="invalid_json")
    if not isinstance(payload, Mapping):
        return _CertificationArtifact(status="invalid_shape")
    if payload.get("schema_version") != ROUTE_CERTIFICATION_SCHEMA_VERSION:
        return _CertificationArtifact(status="unsupported_schema", payload=payload)
    return _CertificationArtifact(status="present", payload=payload)


def _certification_state(
    payload: Mapping[str, Any],
    *,
    source: str,
    sink: str,
    strategy: str,
) -> _CertificationState:
    if not certification_trust(payload).passed or payload.get("status") not in {None, "certified"}:
        return _CertificationState(status="blocked")
    route = payload.get("route")
    if not isinstance(route, Mapping):
        return _CertificationState(status="route_missing")
    if (
        str(route.get("source", "")).lower() != source.lower()
        or str(route.get("sink", "")).lower() != sink.lower()
        or str(route.get("strategy", "")).lower() != strategy.lower()
    ):
        return _CertificationState(status="route_mismatch")
    values = payload.get("certified_transports")
    if not isinstance(values, list | tuple):
        return _CertificationState(status="transports_missing")
    transports = {str(item).strip().lower() for item in values if str(item).strip()}
    expected_hash = route_capability_hash(source=source, sink=sink, strategy=strategy, transports=sorted(transports))
    if payload.get("capability_hash") != expected_hash:
        return _CertificationState(status="stale_hash")
    return _CertificationState(status="present", transports=transports)


def route_capability_hash(
    *,
    source: str,
    sink: str,
    strategy: str,
    transports: list[str] | tuple[str, ...],
    codec: str = "tabseparated",
) -> str:
    payload = {
        "codec": codec,
        "route": {"source": source.lower(), "sink": sink.lower(), "strategy": strategy.lower()},
        "transports": sorted(str(item).lower() for item in transports),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = ["NativeTransferRouteCapabilityRegistry", "route_capability_hash"]
