"""Integrity checks for route certification bundles consumed by release gates."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed, certification_trust
from dpone.ops.checksums import sha256_file

_MAX_ARTIFACT_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class RouteBundleEvidenceVerification:
    """One release-consumer decision over bundle trust and immutable stages."""

    passed: bool
    blockers: tuple[str, ...]
    sha256: str


def verify_route_bundle_evidence(
    payload: Mapping[str, Any],
    *,
    bundle_path: Path,
) -> RouteBundleEvidenceVerification:
    """Verify top-level certification trust and every referenced stage."""

    blockers = route_bundle_integrity_blockers(payload, bundle_path=bundle_path)
    return RouteBundleEvidenceVerification(
        passed=certification_trust(payload).passed and not blockers,
        blockers=blockers,
        sha256=sha256_file(bundle_path),
    )


def route_evidence_sha256(path: Path) -> str:
    """Hash one already-confined release evidence file."""

    return sha256_file(path)


def route_bundle_integrity_blockers(
    payload: Mapping[str, Any],
    *,
    bundle_path: Path,
) -> tuple[str, ...]:
    """Verify that a bundle still points to the immutable artifacts it certified."""

    blockers: list[str] = []
    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages:
        blockers.append("route_certification_bundle.stages_missing")
    else:
        for stage in stages:
            blockers.extend(_stage_blockers(stage, bundle_path=bundle_path))

    artifact_index = payload.get("artifact_index")
    if not isinstance(artifact_index, Mapping) or not artifact_index:
        blockers.append("route_certification_bundle.artifact_index_missing")
    else:
        for name, value in artifact_index.items():
            path = _artifact_path(bundle_path, value)
            if not _safe_file(path):
                blockers.append(f"route_certification_bundle.artifact_invalid:{name}")
    return tuple(dict.fromkeys(blockers))


def _stage_blockers(stage: object, *, bundle_path: Path) -> tuple[str, ...]:
    if not isinstance(stage, Mapping):
        return ("route_certification_bundle.stage_invalid",)
    name = str(stage.get("name") or "unknown")
    if stage.get("required") is not True or stage.get("passed") is not True or bool(stage.get("blockers")):
        return (f"route_certification_bundle.stage_not_passed:{name}",)
    path = _artifact_path(bundle_path, stage.get("path"))
    if not _safe_file(path):
        return (f"route_certification_bundle.stage_artifact_invalid:{name}",)
    expected = stage.get("sha256")
    if not isinstance(expected, str) or sha256_file(path) != expected:
        return (f"route_certification_bundle.stage_digest_mismatch:{name}",)
    payload = _read_mapping(path)
    if payload is None or not artifact_payload_passed(payload, name=name):
        return (f"route_certification_bundle.stage_evidence_not_passed:{name}",)
    return ()


def _artifact_path(bundle_path: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        return Path()
    path = Path(value)
    return path if path.is_absolute() else bundle_path.parent / path


def _safe_file(path: Path) -> bool:
    try:
        return path.is_file() and not path.is_symlink() and path.stat().st_size <= _MAX_ARTIFACT_BYTES
    except OSError:
        return False


def _read_mapping(path: Path) -> Mapping[str, Any] | None:
    if path.suffix.lower() != ".json":
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


__all__ = [
    "RouteBundleEvidenceVerification",
    "route_bundle_integrity_blockers",
    "route_evidence_sha256",
    "verify_route_bundle_evidence",
]
