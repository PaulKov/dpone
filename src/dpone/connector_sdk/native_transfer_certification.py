from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_STREAM_CASES = {
    "static": ("manifest_schema", "secret_redaction", "docs_contract"),
    "local_live": (
        "stream_success",
        "file_fallback",
        "source_failure",
        "sink_failure",
        "timeout",
        "checksum_mismatch",
    ),
    "vendor_live": (
        "stream_success",
        "file_fallback",
        "source_failure",
        "sink_failure",
        "timeout",
        "checksum_mismatch",
    ),
}


@dataclass(frozen=True, slots=True)
class NativeTransferCapability:
    """Declared native-transfer role for one connector package."""

    name: str
    transport: str
    role: str
    formats: tuple[str, ...] = ()
    bounded: bool = False
    staging_safe: bool = False
    supports_checksum: bool = False
    supports_cleanup: bool = False
    supports_abort: bool = False
    supports_idempotency_key: bool = False

    @classmethod
    def stream_export(cls, *, formats: tuple[str, ...] = ("tabseparated", "jsonl")) -> NativeTransferCapability:
        return cls(
            name="stream_export",
            transport="stream",
            role="export",
            formats=formats,
            bounded=True,
            supports_checksum=True,
            supports_cleanup=True,
        )

    @classmethod
    def file_export(cls) -> NativeTransferCapability:
        return cls(name="file_export", transport="file", role="export", supports_cleanup=True)

    @classmethod
    def stream_staging_load(cls, *, formats: tuple[str, ...] = ("tabseparated", "jsonl")) -> NativeTransferCapability:
        return cls(
            name="stream_staging_load",
            transport="stream",
            role="staging_load",
            formats=formats,
            staging_safe=True,
            supports_abort=True,
            supports_idempotency_key=True,
        )

    @classmethod
    def from_manifest(cls, name: str, raw: dict[str, Any]) -> NativeTransferCapability:
        transport = name.split("_", maxsplit=1)[0]
        role = "staging_load" if name.endswith("_staging_load") else "export"
        return cls(
            name=name,
            transport=transport,
            role=role,
            formats=tuple(str(item) for item in raw.get("formats", ())),
            bounded=bool(raw.get("bounded", False)),
            staging_safe=bool(raw.get("staging_safe", False)),
            supports_checksum=bool(raw.get("supports_checksum", False)),
            supports_cleanup=bool(raw.get("supports_cleanup", False)),
            supports_abort=bool(raw.get("supports_abort", False)),
            supports_idempotency_key=bool(raw.get("supports_idempotency_key", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "transport": self.transport,
            "role": self.role,
            "formats": list(self.formats),
            "bounded": self.bounded,
            "staging_safe": self.staging_safe,
            "supports_checksum": self.supports_checksum,
            "supports_cleanup": self.supports_cleanup,
            "supports_abort": self.supports_abort,
            "supports_idempotency_key": self.supports_idempotency_key,
        }


@dataclass(frozen=True, slots=True)
class TransportCapabilityMatrix:
    selected_transport: str
    source_supported: bool
    sink_supported: bool
    codec_supported: bool
    fallback_reason: str | None
    reasons: tuple[str, ...] = ()

    @classmethod
    def from_capabilities(
        cls,
        *,
        source: NativeTransferCapability,
        sink: NativeTransferCapability,
        codec_format: str,
    ) -> TransportCapabilityMatrix:
        source_stream = source.name == "stream_export"
        sink_stream = sink.name == "stream_staging_load"
        codec_supported = codec_format in source.formats and codec_format in sink.formats
        if source_stream and sink_stream and codec_supported:
            return cls("stream", True, True, True, None)
        reasons = _matrix_reasons(source_stream, sink_stream, codec_supported)
        selected = "object" if source.transport == "object" else "file"
        return cls(selected, source_stream, sink_stream, codec_supported, _fallback_reason(reasons), reasons)

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected_transport": self.selected_transport,
            "source_supported": self.source_supported,
            "sink_supported": self.sink_supported,
            "codec_supported": self.codec_supported,
            "fallback_reason": self.fallback_reason,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class TransportConformanceCase:
    name: str
    status: str
    failure_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status, "failure_code": self.failure_code}


@dataclass(frozen=True, slots=True)
class CapabilityCertificationResult:
    name: str
    status: str
    cases: tuple[TransportConformanceCase, ...]
    blockers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "cases": [case.to_dict() for case in self.cases],
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class ConnectorCapabilityCertificationReport:
    connector: str
    connector_type: str
    profile: str
    status: str
    capabilities: dict[str, CapabilityCertificationResult]
    blockers: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.status == "certified"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "dpone.connector_capability_certification.v1",
            "passed": self.passed,
            "connector": self.connector,
            "connector_type": self.connector_type,
            "profile": self.profile,
            "status": self.status,
            "blockers": list(self.blockers),
            "capabilities": {name: result.to_dict() for name, result in self.capabilities.items()},
        }


class ConnectorCapabilityCertificationService:
    """Score connector-declared native-transfer capabilities without runtime IO."""

    def certify(
        self,
        *,
        manifest: dict[str, Any],
        profile: str,
        requested_capabilities: tuple[str, ...],
    ) -> ConnectorCapabilityCertificationReport:
        if profile not in _STREAM_CASES:
            return ConnectorCapabilityCertificationReport(
                connector=str(manifest.get("connector") or "unknown"),
                connector_type=str(manifest.get("connector_type") or "unknown"),
                profile=profile,
                status="blocked",
                capabilities={},
                blockers=(f"unsupported_certification_profile:{profile}",),
            )
        if not requested_capabilities:
            return ConnectorCapabilityCertificationReport(
                connector=str(manifest.get("connector") or "unknown"),
                connector_type=str(manifest.get("connector_type") or "unknown"),
                profile=profile,
                status="unverified",
                capabilities={},
                blockers=("requested_capabilities_missing",),
            )
        capabilities = _native_capabilities(manifest)
        results = {
            capability: self._certify_capability(capability, profile, capabilities)
            for capability in requested_capabilities
        }
        blockers = tuple(blocker for result in results.values() for blocker in result.blockers)
        statuses = {result.status for result in results.values()}
        status = "blocked" if "blocked" in statuses else "unverified" if "unverified" in statuses else "certified"
        return ConnectorCapabilityCertificationReport(
            connector=str(manifest.get("connector") or "unknown"),
            connector_type=str(manifest.get("connector_type") or "unknown"),
            profile=profile,
            status=status,
            capabilities=results,
            blockers=blockers,
        )

    def _certify_capability(
        self,
        capability: str,
        profile: str,
        native_capabilities: dict[str, NativeTransferCapability],
    ) -> CapabilityCertificationResult:
        if capability != "native_transfer.stream":
            blocker = f"unsupported_capability:{capability}"
            return CapabilityCertificationResult(capability, "blocked", (), (blocker,))
        stream_roles = tuple(item for item in native_capabilities.values() if item.transport == "stream")
        blockers = _stream_blockers(stream_roles)
        if blockers:
            return CapabilityCertificationResult(capability, "blocked", (), blockers)
        blocker = "static_profile_is_plan_only" if profile == "static" else f"live_evidence_not_provided:{profile}"
        cases = tuple(TransportConformanceCase(name, "unverified") for name in _STREAM_CASES[profile])
        return CapabilityCertificationResult(capability, "unverified", cases, (blocker,))


def _native_capabilities(manifest: dict[str, Any]) -> dict[str, NativeTransferCapability]:
    raw = ((manifest.get("native_transfer") or {}).get("capabilities") or {}) if isinstance(manifest, dict) else {}
    return {
        str(name): NativeTransferCapability.from_manifest(str(name), dict(value or {}))
        for name, value in raw.items()
        if isinstance(value, dict)
    }


def _stream_blockers(capabilities: tuple[NativeTransferCapability, ...]) -> tuple[str, ...]:
    if not capabilities:
        return ("native_transfer_stream_capability_missing",)
    blockers: list[str] = []
    for capability in capabilities:
        if capability.role == "export" and not (capability.bounded and capability.supports_cleanup):
            blockers.append(f"{capability.name}_contract_incomplete")
        if capability.role == "staging_load" and not (capability.staging_safe and capability.supports_abort):
            blockers.append(f"{capability.name}_contract_incomplete")
    return tuple(blockers)


def _matrix_reasons(source_stream: bool, sink_stream: bool, codec_supported: bool) -> tuple[str, ...]:
    reasons: list[str] = []
    if not source_stream:
        reasons.append("source lacks stream_export")
    if not sink_stream:
        reasons.append("sink lacks stream_staging_load")
    if not codec_supported:
        reasons.append("codec is not stream safe for declared formats")
    return tuple(reasons)


def _fallback_reason(reasons: tuple[str, ...]) -> str:
    if any(reason.startswith("source ") for reason in reasons):
        return "native_transfer_stream_fallback_file_only_source"
    if any(reason.startswith("sink ") for reason in reasons):
        return "native_transfer_stream_fallback_file_only_sink"
    return "native_transfer_codec_not_stream_safe"


__all__ = [
    "CapabilityCertificationResult",
    "ConnectorCapabilityCertificationReport",
    "ConnectorCapabilityCertificationService",
    "NativeTransferCapability",
    "TransportCapabilityMatrix",
    "TransportConformanceCase",
]
