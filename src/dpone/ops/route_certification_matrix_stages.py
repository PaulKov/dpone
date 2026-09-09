"""On-disk stage verification for route certification matrix evidence."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file
from dpone.readiness.route_attestation_files import RouteAttestationFileError, read_strict_json_mapping

_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024


def validate_bundle_stages(
    payload: Mapping[str, Any],
    *,
    bundle_directory: Path,
) -> tuple[str, ...]:
    """Re-read every required stage and verify its bound content digest."""

    required_names = payload.get("required_evidence")
    stages = payload.get("stages")
    if not isinstance(required_names, list) or not isinstance(stages, list):
        return ("route_matrix.bundle_stages_invalid",)
    stage_by_name = {
        str(stage.get("name")): stage
        for stage in stages
        if isinstance(stage, Mapping) and isinstance(stage.get("name"), str)
    }
    blockers: list[str] = []
    for name in (str(item) for item in required_names):
        stage = stage_by_name.get(name)
        blockers.extend(_stage_blockers(name=name, stage=stage, bundle_directory=bundle_directory))
    return tuple(dict.fromkeys(blockers))


def _stage_blockers(
    *,
    name: str,
    stage: Mapping[str, Any] | None,
    bundle_directory: Path,
) -> tuple[str, ...]:
    if stage is None:
        return (f"route_matrix.stage_missing:{name}",)
    if stage.get("required") is not True or stage.get("passed") is not True or bool(stage.get("blockers")):
        return (f"route_matrix.stage_not_passed:{name}",)
    stage_path = _stage_path(bundle_directory, stage.get("path"))
    if not _safe_file(stage_path):
        return (f"route_matrix.stage_artifact_invalid:{name}",)
    expected_sha = stage.get("sha256")
    if not isinstance(expected_sha, str) or sha256_file(stage_path) != expected_sha:
        return (f"route_matrix.stage_digest_mismatch:{name}",)
    try:
        stage_payload, _ = read_strict_json_mapping(
            stage_path,
            max_bytes=_MAX_EVIDENCE_BYTES,
            label=f"route stage {name}",
        )
    except RouteAttestationFileError:
        return (f"route_matrix.stage_artifact_invalid:{name}",)
    if not artifact_payload_passed(stage_payload, name=name):
        return (f"route_matrix.stage_evidence_not_passed:{name}",)
    return ()


def _stage_path(bundle_directory: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        return Path()
    path = Path(value)
    return path if path.is_absolute() else bundle_directory / path


def _safe_file(path: Path) -> bool:
    try:
        return path.is_file() and not path.is_symlink() and path.stat().st_size <= _MAX_EVIDENCE_BYTES
    except OSError:
        return False


__all__ = ["validate_bundle_stages"]
