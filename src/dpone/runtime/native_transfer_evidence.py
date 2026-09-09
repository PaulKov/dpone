from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.native_transfer_transport import SliceTransportPlan

_SCHEMA_VERSION = "dpone.native_transfer.transport_evidence.v1"
_SECRET_MARKERS = ("password", "secret", "token", "credential", "key")


@dataclass(frozen=True, slots=True)
class NativeTransferTransportEvidence:
    selected_transport: str
    requested_transport: str
    fallback_allowed: bool
    fallback_reason: str | None
    eligibility: dict[str, Any]
    metrics: dict[str, Any]
    staging_session_id: str | None
    cleanup_status: str
    failure_code: str | None
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": _SCHEMA_VERSION,
            "selected_transport": self.selected_transport,
            "requested_transport": self.requested_transport,
            "fallback_allowed": self.fallback_allowed,
            "fallback_reason": self.fallback_reason,
            "eligibility": self.eligibility,
            "metrics": self.metrics,
            "staging_session_id": self.staging_session_id,
            "cleanup_status": self.cleanup_status,
            "failure_code": self.failure_code,
            "diagnostics": self.diagnostics,
        }


class NativeTransferTransportEvidenceBuilder:
    """Build stable transport evidence without exposing connector secrets."""

    def build(
        self,
        plan: SliceTransportPlan,
        *,
        requested_transport: str,
        rows: int | None,
        bytes_count: int | None,
        checksum: str | None,
        duration_seconds: float | None,
        staging_session_id: str | None,
        cleanup_status: str,
        failure_code: str | None,
        diagnostics: dict[str, Any] | None = None,
    ) -> NativeTransferTransportEvidence:
        return NativeTransferTransportEvidence(
            selected_transport=plan.transport,
            requested_transport=requested_transport,
            fallback_allowed=plan.fallback_allowed,
            fallback_reason=plan.fallback_reason,
            eligibility=plan.eligibility.to_dict(),
            metrics={
                "rows": rows,
                "bytes": bytes_count,
                "checksum": checksum,
                "duration_seconds": duration_seconds,
            },
            staging_session_id=staging_session_id,
            cleanup_status=cleanup_status,
            failure_code=failure_code,
            diagnostics=_redact(diagnostics or {}),
        )


def _redact(value: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, item in value.items():
        if _is_secret_key(key):
            redacted[key] = "***"
        elif isinstance(item, dict):
            redacted[key] = _redact(item)
        else:
            redacted[key] = item
    return redacted


def _is_secret_key(key: str) -> bool:
    normalized = key.lower()
    return any(marker in normalized for marker in _SECRET_MARKERS)


__all__ = ["NativeTransferTransportEvidence", "NativeTransferTransportEvidenceBuilder"]
