"""Read-only migration plans for legacy Airflow authoring connection fields."""

from __future__ import annotations

import difflib
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.airflow_secret_redaction import is_secret_key, redacted_mapping, without_secret_keys
from dpone.security_redaction import REDACTION_TOKEN

_LEGACY_CONNECTION_KEYS = frozenset({"connection_type", "vault_path", "vault_mount", "kv_version"})


def airflow_authoring_migration_plan(
    *,
    payload: dict[str, Any],
    source_path: Path,
    root: Path,
) -> dict[str, Any] | None:
    """Build a secret-free plan for legacy ``connection_type`` / ``vault_path`` sections."""

    changes = _legacy_section_changes(payload.get("processes"), source_path=source_path, root=root)
    if not changes:
        return None
    return {
        "kind": "dpone.airflow-authoring-migration-plan.v1",
        "schema": "dpone.airflow-authoring-migration-plan.v1",
        "mode": "plan",
        "apply": False,
        "source_path": _relative(source_path, root),
        "summary": {
            "legacy_sections": len(changes),
            "manual_review_required": True,
            "apply_supported": False,
        },
        "changes": changes,
    }


def suggest_connection_ref(section: dict[str, Any], side: str) -> str:
    current = section.get("connection_ref")
    if isinstance(current, str) and current:
        return current
    source_type = str(section.get("type") or side).strip().lower().replace("-", "_")
    return f"{source_type}_dev"


def _legacy_section_changes(processes: object, *, source_path: Path, root: Path) -> list[dict[str, Any]]:
    if not isinstance(processes, list):
        return []
    changes: list[dict[str, Any]] = []
    for index, process in enumerate(processes):
        if not isinstance(process, dict):
            continue
        for side in ("source", "sink"):
            section = process.get(side)
            if not isinstance(section, dict) or not _is_legacy_section(section):
                continue
            changes.append(
                _legacy_section_change(
                    section=section,
                    section_path=f"processes[{index}].{side}",
                    side=side,
                    source_path=source_path,
                    root=root,
                )
            )
    return changes


def _legacy_section_change(
    *,
    section: dict[str, Any],
    section_path: str,
    side: str,
    source_path: Path,
    root: Path,
) -> dict[str, Any]:
    proposed_section = _proposed_section(section, side)
    relative_path = _relative(source_path, root)
    return {
        "action": "replace_legacy_connection_config",
        "path": section_path,
        "suggested_connection_ref": proposed_section["connection_ref"],
        "detected": _detected_legacy_fields(section),
        "proposed_section": proposed_section,
        "manual_review_required": True,
        "unified_diff": _section_diff(
            before=_redacted_legacy_section(section),
            after=proposed_section,
            before_label=f"before/{relative_path}#{section_path}",
            after_label=f"after/{relative_path}#{section_path}",
        ),
    }


def _proposed_section(section: dict[str, Any], side: str) -> dict[str, Any]:
    proposed: dict[str, Any] = {}
    for key, value in section.items():
        key_text = str(key)
        if key_text in _LEGACY_CONNECTION_KEYS or is_secret_key(key_text):
            continue
        cleaned = without_secret_keys(value)
        if cleaned in ({}, []):
            continue
        proposed[key_text] = cleaned
    proposed["connection_ref"] = suggest_connection_ref(section, side)
    return proposed


def _detected_legacy_fields(section: dict[str, Any]) -> dict[str, str]:
    detected: dict[str, str] = {}
    if section.get("connection_type") is not None:
        detected["connection_type"] = str(section["connection_type"])
    if section.get("vault_path") is not None:
        detected["vault_path"] = REDACTION_TOKEN
    return detected


def _redacted_legacy_section(section: dict[str, Any]) -> dict[str, Any]:
    redacted = redacted_mapping(section)
    if "vault_path" in redacted:
        redacted["vault_path"] = REDACTION_TOKEN
    return redacted


def _is_legacy_section(section: dict[str, Any]) -> bool:
    return "connection_type" in section or "vault_path" in section


def _section_diff(*, before: dict[str, Any], after: dict[str, Any], before_label: str, after_label: str) -> str:
    return "".join(
        difflib.unified_diff(
            _yaml_lines(before),
            _yaml_lines(after),
            fromfile=before_label,
            tofile=after_label,
            lineterm="",
        )
    )


def _yaml_lines(payload: dict[str, Any]) -> list[str]:
    return yaml.safe_dump(payload, sort_keys=True).splitlines(keepends=True)


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


__all__ = ["airflow_authoring_migration_plan", "suggest_connection_ref"]
