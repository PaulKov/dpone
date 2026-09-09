"""Evidence artifact normalization for route readiness."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    artifact_payload_passed,
    artifact_requires_certification_trust,
    certification_trust,
)
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.models import RouteEvidenceItem


class RouteEvidenceReader:
    """Read route evidence artifacts without route-specific decisions."""

    def read(self, *, name: str, path: str | Path | None, required: bool) -> RouteEvidenceItem:
        evidence_name = str(name).strip()
        kind = _kind(evidence_name)
        if path is None:
            return _missing_item(evidence_name, kind=kind, path="", required=required)
        artifact_path = Path(path)
        if not artifact_path.is_file():
            return _missing_item(evidence_name, kind=kind, path=str(artifact_path), required=required)
        payload = _read_payload(artifact_path)
        requires_trust = artifact_requires_certification_trust(evidence_name, payload)
        trust = certification_trust(payload) if requires_trust else None
        return RouteEvidenceItem(
            name=evidence_name,
            kind=kind,
            path=str(artifact_path),
            required=required,
            passed=_passed(evidence_name, payload),
            sha256=sha256_file(artifact_path),
            summary=_summary(payload),
            blockers=_blockers(evidence_name, payload),
            missing=False,
            evidence_status=trust.evidence_status if trust is not None else None,
        )


def _missing_item(name: str, *, kind: str, path: str, required: bool) -> RouteEvidenceItem:
    return RouteEvidenceItem(
        name=name,
        kind=kind,
        path=path,
        required=required,
        passed=not required,
        sha256="0" * 64,
        summary="required evidence artifact is missing" if required else "optional evidence artifact is missing",
        blockers=(f"{name}.missing",) if required else (),
        missing=True,
    )


def _kind(name: str) -> str:
    if "benchmark" in name or "slo" in name or "performance" in name:
        return "performance"
    if "type" in name or "schema" in name:
        return "schema"
    if "docs" in name or "runbook" in name:
        return "docs"
    if "matrix" in name or "strategy" in name:
        return "certification"
    if "resume" in name or "run" in name:
        return "runtime"
    return "evidence"


def _read_payload(path: Path) -> Mapping[str, Any]:
    if path.suffix.lower() != ".json":
        return {"passed": False, "blockers": ["unsupported_artifact_type"]}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _passed(name: str, payload: Mapping[str, Any]) -> bool:
    return bool(payload) and _has_success_signal(payload) and artifact_payload_passed(payload, name=name)


def _has_success_signal(payload: Mapping[str, Any]) -> bool:
    return any(key in payload for key in ("passed", "status", "decision", "evidence_status"))


def _summary(payload: Mapping[str, Any]) -> str:
    for key in ("summary", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("blockers", "violations", "findings", "results", "items", "cases", "evidence"):
        value = payload.get(key)
        if isinstance(value, list | tuple):
            return f"{key}={len(value)}"
    if "status" in payload:
        return f"status={payload['status']}"
    return "passed" if artifact_payload_passed(payload) else "failed"


def _blockers(name: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = payload.get("blockers")
    if isinstance(blockers, list | tuple):
        values = tuple(str(item) for item in blockers if str(item))
        return values or (() if _passed(name, payload) else (f"{name}.not_passed",))
    if artifact_payload_passed(payload, name=name):
        return tuple()
    return (f"{name}.not_passed",)
