"""Shared evidence artifact validation helpers for ops services."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    artifact_payload_passed,
    artifact_requires_certification_trust,
)
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class EvidenceArtifactStatus:
    """Validation result for one evidence artifact."""

    name: str
    path: str
    required: bool
    exists: bool
    passed: bool
    sha256: str
    summary: str
    blocker: str | None
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["payload"] = dict(self.payload)
        return data


class EvidenceArtifactReader:
    """Reads and validates JSON evidence artifacts without knowing their schema."""

    def read_many(
        self,
        *,
        artifacts: Mapping[str, str | Path],
        required: Sequence[str],
    ) -> tuple[EvidenceArtifactStatus, ...]:
        required_names = {str(item) for item in required}
        names = tuple(sorted({*artifacts.keys(), *required_names}))
        return tuple(self.read(name=name, path=artifacts.get(name), required=name in required_names) for name in names)

    def read(self, *, name: str, path: str | Path | None, required: bool) -> EvidenceArtifactStatus:
        if path is None:
            return EvidenceArtifactStatus(
                name=name,
                path="",
                required=required,
                exists=False,
                passed=not required,
                sha256="0" * 64,
                summary="optional artifact not provided" if not required else "required artifact missing",
                blocker=f"{name}.missing" if required else None,
                payload={},
            )
        artifact_path = Path(path)
        if not artifact_path.is_file():
            return EvidenceArtifactStatus(
                name=name,
                path=str(artifact_path),
                required=required,
                exists=False,
                passed=False,
                sha256="0" * 64,
                summary="artifact missing",
                blocker=f"{name}.missing",
                payload={},
            )
        payload = _read_json(artifact_path)
        passed = artifact_payload_passed(
            payload,
            require_certification_status=artifact_requires_certification_trust(name, payload),
        )
        return EvidenceArtifactStatus(
            name=name,
            path=str(artifact_path),
            required=required,
            exists=True,
            passed=passed,
            sha256=sha256_file(artifact_path),
            summary=_payload_summary(payload),
            blocker=None if passed else f"{name}.not_passed",
            payload=payload,
        )


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"passed": False, "summary": "invalid JSON artifact"}
    return payload if isinstance(payload, Mapping) else {"passed": False, "summary": "non-object JSON artifact"}


def _payload_passed(payload: Mapping[str, Any]) -> bool:
    return artifact_payload_passed(payload)


def _payload_summary(payload: Mapping[str, Any]) -> str:
    for key in ("summary", "status", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    for key in ("total_cases", "passed_cases", "failed_cases"):
        if key in payload:
            return f"{key}={payload[key]}"
    return "artifact passed" if _payload_passed(payload) else "artifact failed"


__all__ = ["EvidenceArtifactReader", "EvidenceArtifactStatus"]
