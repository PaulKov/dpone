"""Parse and validate agent governance artifact content."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

GOVERNANCE_GATE_FILENAME = "agent_governance_gate.json"
CONTROL_SURFACE_RED_TEAM_CHECK = "changed_control_surface_red_team"


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling policy module when this file is executed by path."""

    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


artifact_limits = load_sibling("dpone_agent_artifact_resource_limits", "artifact_resource_limits.py")


@dataclass(frozen=True)
class GovernanceArtifactContentEvidence:
    """Compact evidence parsed from agent_governance_gate.json."""

    schema_version: int | None
    status: str | None
    control_surface_changed: bool | None
    changed_paths: list[str]
    head_commit: str | None
    checks: list[dict[str, str | None]]
    subject_sha256: str | None = None
    errors: list[str] = field(default_factory=list)

    def check_status(self, name: str) -> str | None:
        """Return the status for a named governance check."""

        for check in self.checks:
            if check.get("name") == name:
                return check.get("status")
        return None


@dataclass(frozen=True)
class GovernanceArtifactFileEvidence:
    """Exact governance JSON subject bytes extracted from an artifact archive."""

    content: bytes | None
    errors: list[str] = field(default_factory=list)


def content_from_archive_bytes(content: bytes) -> GovernanceArtifactContentEvidence:
    """Parse governance content from a GitHub Actions artifact zip archive."""

    file_evidence = governance_gate_file_bytes_from_archive_bytes(content)
    if file_evidence.content is None:
        return _empty_error("; ".join(file_evidence.errors))

    try:
        raw = file_evidence.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        return _empty_error(f"{GOVERNANCE_GATE_FILENAME} is not valid UTF-8: {exc}.")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return _empty_error(f"{GOVERNANCE_GATE_FILENAME} contains invalid JSON: {exc.msg}.")
    return content_from_payload(
        payload,
        subject_sha256=hashlib.sha256(file_evidence.content).hexdigest(),
    )


def governance_gate_file_bytes_from_archive_bytes(content: bytes) -> GovernanceArtifactFileEvidence:
    """Extract the exact governance JSON subject bytes from an artifact archive."""

    try:
        members = artifact_limits.read_bounded_zip(content, resource="Governance artifact archive")
    except ValueError as exc:
        return _file_error(str(exc))
    matches = [
        member
        for member in members
        if not member.is_dir and PurePosixPath(member.filename).name == GOVERNANCE_GATE_FILENAME
    ]
    if not matches:
        return _file_error(f"{GOVERNANCE_GATE_FILENAME} is missing from governance artifact archive.")
    if len(matches) > 1:
        return _file_error(f"Governance artifact archive contains multiple {GOVERNANCE_GATE_FILENAME} files.")
    return GovernanceArtifactFileEvidence(content=matches[0].content, errors=[])


def content_from_payload(
    payload: Any,
    *,
    subject_sha256: str | None = None,
) -> GovernanceArtifactContentEvidence:
    """Build compact content evidence from a governance gate JSON object."""

    if not isinstance(payload, dict):
        return _empty_error(f"{GOVERNANCE_GATE_FILENAME} must be a JSON object.")
    return GovernanceArtifactContentEvidence(
        schema_version=_optional_int(payload.get("schema_version")),
        status=_optional_string(payload.get("status")),
        control_surface_changed=_optional_bool(payload.get("control_surface_changed")),
        changed_paths=_string_list(payload.get("changed_paths")),
        head_commit=_optional_string(payload.get("head_commit")),
        checks=_checks(payload.get("checks")),
        subject_sha256=subject_sha256,
        errors=[],
    )


def validate_content_for_receipt(
    *,
    content: GovernanceArtifactContentEvidence | None,
    expected_changed_paths: list[str],
    control_surface_changed: bool,
    governance_artifact_name: str,
    head_sha: str,
) -> list[str]:
    """Return fail-closed errors for governance artifact content."""

    if content is None:
        return [f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} has no content evidence."]
    errors = [
        f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} content is invalid: {error}"
        for error in content.errors
    ]
    if content.schema_version != 1:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} must contain schema_version 1."
        )
    if content.status != "PASS":
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"must contain status PASS (observed {content.status or 'missing'})."
        )
    if content.head_commit != head_sha:
        errors.append(
            f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
            f"must contain head_commit {head_sha} (observed {content.head_commit or 'missing'})."
        )
    errors.extend(
        _changed_path_errors(
            content.changed_paths,
            expected_changed_paths,
            governance_artifact_name=governance_artifact_name,
            head_sha=head_sha,
        )
    )
    if control_surface_changed:
        if content.control_surface_changed is not True:
            errors.append(
                f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
                "must contain control_surface_changed true."
            )
        red_team_status = content.check_status(CONTROL_SURFACE_RED_TEAM_CHECK)
        if red_team_status != "PASS":
            errors.append(
                f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
                f"must contain {CONTROL_SURFACE_RED_TEAM_CHECK} PASS (observed {red_team_status or 'missing'})."
            )
    return errors


def _changed_path_errors(
    observed: list[str],
    expected: list[str],
    *,
    governance_artifact_name: str,
    head_sha: str,
) -> list[str]:
    observed_set = set(observed)
    expected_set = set(expected)
    missing = sorted(expected_set - observed_set)
    extra = sorted(observed_set - expected_set)
    if not missing and not extra:
        return []
    parts: list[str] = []
    if missing:
        parts.append(f"missing {missing}")
    if extra:
        parts.append(f"extra {extra}")
    return [
        f"Required GitHub artifact '{governance_artifact_name}' for head {head_sha} "
        f"changed_paths do not match PR changed paths ({'; '.join(parts)})."
    ]


def _empty_error(error: str) -> GovernanceArtifactContentEvidence:
    return GovernanceArtifactContentEvidence(
        schema_version=None,
        status=None,
        control_surface_changed=None,
        changed_paths=[],
        head_commit=None,
        checks=[],
        subject_sha256=None,
        errors=[error],
    )


def _file_error(error: str) -> GovernanceArtifactFileEvidence:
    return GovernanceArtifactFileEvidence(content=None, errors=[error])


def _checks(value: Any) -> list[dict[str, str | None]]:
    if not isinstance(value, list):
        return []
    checks: list[dict[str, str | None]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        checks.append(
            {
                "name": _optional_string(item.get("name")),
                "status": _optional_string(item.get("status")),
                "artifact": _optional_string(item.get("artifact")),
                "details": _optional_string(item.get("details")),
            }
        )
    return checks


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(item for item in value if isinstance(item, str)))


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None
