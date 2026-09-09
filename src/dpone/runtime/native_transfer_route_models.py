from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.native_transfer_transport import SliceTransportPlan, StreamEligibility

ROUTE_DECISION_SCHEMA_VERSION = "dpone.native_transfer.route_decision.v1"
ROUTE_CERTIFICATION_SCHEMA_VERSION = "dpone.native_transfer.route_certification.v1"
FALLBACK_CHAIN: tuple[str, ...] = ("stream", "object", "file")


@dataclass(frozen=True, slots=True)
class RouteCertificationPolicy:
    mode: str = "auto"
    artifact: str | None = None

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None = None) -> RouteCertificationPolicy:
        raw = dict(value or {})
        return cls(mode=str(raw.get("mode") or "auto"), artifact=_path(raw.get("artifact")))

    def effective_mode(self, *, runner_policy: str | None = None) -> str:
        normalized = self.mode.strip().lower()
        if normalized in {"advisory", "certified_only"}:
            return normalized
        runner = str(runner_policy or "").strip().lower()
        if runner in {"release", "production", "prod"}:
            return "certified_only"
        return "advisory"

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "artifact": self.artifact}


@dataclass(frozen=True, slots=True)
class RouteTransportCandidate:
    transport: str
    source: bool
    sink: bool
    codec: bool
    staging_safe: bool
    certified: bool
    reasons: tuple[str, ...] = ()
    certification_artifact: str | None = None

    @property
    def technically_eligible(self) -> bool:
        return self.source and self.sink and self.codec and self.staging_safe

    def to_dict(self) -> dict[str, Any]:
        return {
            "transport": self.transport,
            "source": self.source,
            "sink": self.sink,
            "codec": self.codec,
            "staging_safe": self.staging_safe,
            "certified": self.certified,
            "technically_eligible": self.technically_eligible,
            "reasons": list(self.reasons),
            "certification_artifact": self.certification_artifact,
        }


@dataclass(frozen=True, slots=True)
class RouteCapabilityMatrix:
    source: str
    sink: str
    strategy: str
    requested_transport: str
    candidates: tuple[RouteTransportCandidate, ...]
    certification_artifact_status: str = "not_configured"

    def candidate(self, transport: str) -> RouteTransportCandidate | None:
        return next((item for item in self.candidates if item.transport == transport), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": {"source": self.source, "sink": self.sink, "strategy": self.strategy},
            "requested_transport": self.requested_transport,
            "certification_artifact_status": self.certification_artifact_status,
            "candidates": [item.to_dict() for item in self.candidates],
        }


@dataclass(frozen=True, slots=True)
class RouteTransportDecision:
    matrix: RouteCapabilityMatrix
    requested_transport: str
    selected_transport: str | None
    certification_mode: str
    certification_status: str
    release_gate: str
    fallback_chain: tuple[str, ...]
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": ROUTE_DECISION_SCHEMA_VERSION,
            "route": {
                "source": self.matrix.source,
                "sink": self.matrix.sink,
                "strategy": self.matrix.strategy,
            },
            "requested_transport": self.requested_transport,
            "selected_transport": self.selected_transport,
            "certification_mode": self.certification_mode,
            "certification_status": self.certification_status,
            "release_gate": self.release_gate,
            "fallback_chain": list(self.fallback_chain),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
            "matrix": {"candidates": [item.to_dict() for item in self.matrix.candidates]},
        }

    def to_transport_plan(self) -> SliceTransportPlan:
        stream = self.matrix.candidate("stream")
        eligibility = StreamEligibility(
            source=bool(stream and stream.source),
            sink=bool(stream and stream.sink),
            codec=bool(stream and stream.codec),
            reasons=tuple(stream.reasons if stream else ()),
        )
        return SliceTransportPlan(
            self.selected_transport or "blocked",
            eligibility,
            fallback_allowed=True,
            fallback_reason=_fallback_reason(eligibility) if self.selected_transport == "file" else None,
        )


def _fallback_reason(eligibility: StreamEligibility) -> str | None:
    if eligibility.supported:
        return None
    if not eligibility.source:
        return "native_transfer_stream_fallback_file_only_source"
    if not eligibility.sink:
        return "native_transfer_stream_fallback_file_only_sink"
    return "native_transfer_codec_not_stream_safe"


def _path(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return str(Path(text)) if text else None


__all__ = [
    "FALLBACK_CHAIN",
    "ROUTE_CERTIFICATION_SCHEMA_VERSION",
    "ROUTE_DECISION_SCHEMA_VERSION",
    "RouteCapabilityMatrix",
    "RouteCertificationPolicy",
    "RouteTransportCandidate",
    "RouteTransportDecision",
]
