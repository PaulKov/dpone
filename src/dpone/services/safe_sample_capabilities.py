"""Credential-free sampling capability detection for safe sample planning."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.services.sample_route_certifications import (
    CertifiedSamplingRoute,
    certified_sampling_route_index,
)

_CERTIFIED_SAMPLING_ROUTES = certified_sampling_route_index()


@dataclass(frozen=True, slots=True)
class SampleSourceCapabilities:
    """Public source capability payload plus a non-serialized trust decision."""

    supports_pushdown_sampling: bool | None = None
    full_scan_required: bool | None = None
    estimated_read_bytes: int | None = None
    proof: str | None = None
    mode: str | None = None
    _production_proof_verified: bool = field(default=False, repr=False, compare=False)

    @classmethod
    def unknown(cls) -> SampleSourceCapabilities:
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {
            "supports_pushdown_sampling": self.supports_pushdown_sampling,
            "full_scan_required": self.full_scan_required,
            "estimated_read_bytes": self.estimated_read_bytes,
            "proof": self.proof,
            "mode": self.mode,
        }

    @property
    def production_proof_verified(self) -> bool:
        """Whether a trusted composition root verified external route evidence."""

        return self._production_proof_verified


class SourceSamplingCapabilityDetector:
    """Detect sampling candidates without touching source systems.

    ``verified_route_ids`` is an injected trust decision. Callers may populate
    it only after validating content-addressed certification evidence outside
    the authoring source. A manifest can describe a route, but cannot set this
    internal production authorization bit.
    """

    def __init__(self, *, verified_route_ids: Iterable[str] = ()) -> None:
        self._verified_route_ids = frozenset(str(value).strip() for value in verified_route_ids if str(value).strip())

    def detect(self, pipeline_source: dict[str, Any]) -> SampleSourceCapabilities:
        process = _first_process(pipeline_source)
        source = process.get("source")
        if not isinstance(source, dict):
            return SampleSourceCapabilities.unknown()
        sampling = _sampling_config(source)
        if sampling is None:
            return self._from_route(process)
        return _from_declared_sampling(sampling)

    def _from_route(self, process: dict[str, Any]) -> SampleSourceCapabilities:
        route = _find_certified_sampling_route(process)
        if route is None:
            return SampleSourceCapabilities.unknown()
        return SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            estimated_read_bytes=route.estimated_read_bytes,
            proof=route.proof,
            mode="pushdown",
            _production_proof_verified=route.certification_id in self._verified_route_ids,
        )


def _sampling_config(source: dict[str, Any]) -> dict[str, Any] | None:
    sampling = source.get("sampling")
    if isinstance(sampling, dict):
        return sampling
    capabilities = source.get("capabilities")
    if isinstance(capabilities, dict) and isinstance(capabilities.get("sampling"), dict):
        return capabilities["sampling"]
    return None


def _from_declared_sampling(sampling: dict[str, Any]) -> SampleSourceCapabilities:
    mode = str(sampling.get("mode") or "").strip().lower() or None
    proof = str(sampling.get("proof") or "").strip() or None
    estimated_read_bytes = _optional_int(sampling.get("estimated_read_bytes"))
    if mode == "pushdown":
        proven = bool(proof and _is_declared_pushdown_candidate(proof))
        return SampleSourceCapabilities(
            supports_pushdown_sampling=proven,
            full_scan_required=False if proven else None,
            estimated_read_bytes=estimated_read_bytes,
            proof=proof,
            mode=mode,
        )
    if mode == "full_scan":
        return SampleSourceCapabilities(
            supports_pushdown_sampling=False,
            full_scan_required=True,
            estimated_read_bytes=estimated_read_bytes,
            proof=proof,
            mode=mode,
        )
    if mode:
        return SampleSourceCapabilities(
            supports_pushdown_sampling=False,
            full_scan_required=None,
            estimated_read_bytes=estimated_read_bytes,
            proof=proof,
            mode=mode,
        )
    return SampleSourceCapabilities.unknown()


def _first_process(source: dict[str, Any]) -> dict[str, Any]:
    processes = source.get("processes")
    if not isinstance(processes, list) or not processes or not isinstance(processes[0], dict):
        return {}
    return processes[0]


def _find_certified_sampling_route(process: dict[str, Any]) -> CertifiedSamplingRoute | None:
    source = process.get("source")
    sink = process.get("sink")
    if not isinstance(source, dict) or not isinstance(sink, dict):
        return None
    key = (
        canonical_endpoint_type(str(source.get("type") or "")),
        canonical_endpoint_type(str(sink.get("type") or "")),
        _strategy_mode(sink),
    )
    return _CERTIFIED_SAMPLING_ROUTES.get(key)


def _is_declared_pushdown_candidate(proof: str) -> bool:
    normalized = proof.strip().lower()
    return normalized == "connector_capability" or normalized.startswith(
        ("connector_capability:", "route_certification:")
    )


def _strategy_mode(sink: dict[str, Any]) -> str:
    strategy = sink.get("strategy")
    if isinstance(strategy, dict):
        return _normalized_string(strategy.get("mode"))
    return _normalized_string(strategy)


def _normalized_string(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _optional_int(value: Any) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


__all__ = ["SampleSourceCapabilities", "SourceSamplingCapabilityDetector"]
