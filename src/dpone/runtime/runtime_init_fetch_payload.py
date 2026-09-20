"""Exact external payload descriptors for runtime init-fetch plans."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from dpone.runtime.init_fetch_contract import cache_relative_path
from dpone.runtime.runtime_init_fetch_execution import require_execution_token

MAX_SELECTED_RUNTIME_PAYLOADS = 16
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class RuntimePayloadDescriptor:
    """Exact external runtime payload fetched from the pinned release."""

    id: str
    kind: str
    artifact_ref: str
    sha256: str
    bytes: int
    media_type: str

    def __post_init__(self) -> None:
        require_execution_token("runtime_payload.id", self.id)
        if self.kind not in {
            "dbt_project_bundle",
            "dbt_manifest",
            "dbt_selection_lock",
        }:
            raise ValueError("runtime_payload.kind is unsupported")
        cache_relative_path(self.artifact_ref)
        _require_digest(self.sha256)
        if isinstance(self.bytes, bool) or not isinstance(self.bytes, int) or self.bytes <= 0:
            raise ValueError("runtime_payload.bytes must be a positive integer")
        if not isinstance(self.media_type, str) or not self.media_type or len(self.media_type) > 200:
            raise ValueError("runtime_payload.media_type must be bounded text")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "media_type": self.media_type,
        }


def _require_digest(value: object) -> None:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError("runtime_payload.sha256 must be a canonical sha256 digest")


__all__ = ["MAX_SELECTED_RUNTIME_PAYLOADS", "RuntimePayloadDescriptor"]
