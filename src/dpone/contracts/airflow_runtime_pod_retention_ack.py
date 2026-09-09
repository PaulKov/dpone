"""Acknowledgement contract for crash-durable runtime Pod evidence sinks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass


class AirflowRuntimePodRetentionAcknowledgementError(ValueError):
    """Raised when a durable sink acknowledgement is malformed or mismatched."""


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionEvidenceAcknowledgement:
    operation_id: str
    sequence: int
    event_sha256: str
    sink_ref: str

    def __post_init__(self) -> None:
        if not _is_sha256(self.operation_id):
            raise AirflowRuntimePodRetentionAcknowledgementError("acknowledgement operation_id is invalid")
        if isinstance(self.sequence, bool) or not 1 <= self.sequence <= 10_000:
            raise AirflowRuntimePodRetentionAcknowledgementError("acknowledgement sequence is invalid")
        if not _is_sha256(self.event_sha256):
            raise AirflowRuntimePodRetentionAcknowledgementError("acknowledgement event_sha256 is invalid")
        if (
            not self.sink_ref
            or self.sink_ref != self.sink_ref.strip()
            or len(self.sink_ref) > 512
            or any(char in self.sink_ref for char in "\r\n\0")
        ):
            raise AirflowRuntimePodRetentionAcknowledgementError("acknowledgement sink_ref is invalid")

    def require_matches(self, event: Mapping[str, object]) -> None:
        if self.operation_id != event.get("operation_id") or self.sequence != event.get("sequence"):
            raise AirflowRuntimePodRetentionAcknowledgementError(
                "durable acknowledgement does not match the event occurrence"
            )
        if self.event_sha256 != runtime_pod_retention_event_sha256(event):
            raise AirflowRuntimePodRetentionAcknowledgementError(
                "durable acknowledgement does not match the exact event bytes"
            )


def runtime_pod_retention_event_sha256(event: Mapping[str, object]) -> str:
    try:
        payload = json.dumps(
            dict(event),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise AirflowRuntimePodRetentionAcknowledgementError(
            "runtime Pod evidence event is not canonical JSON"
        ) from None
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:"):
        return False
    return all(char in "0123456789abcdef" for char in value[7:])


__all__ = [
    "AirflowRuntimePodRetentionAcknowledgementError",
    "AirflowRuntimePodRetentionEvidenceAcknowledgement",
    "runtime_pod_retention_event_sha256",
]
