"""Topology-light text rendering for Airflow artifact delivery reports."""

from __future__ import annotations

from collections.abc import Mapping


def airflow_artifact_publish_text(payload: Mapping[str, object]) -> str:
    status = str(payload.get("status") or "failed").upper()
    lines = [f"dpone Airflow artifact publish: {status}"]
    lines.extend(_identity_lines(payload))
    if status in {"PUBLISHED", "NO_OP"}:
        lines.append(f"- created objects: {payload.get('created_objects', 0)}")
        lines.append(f"- existing equal objects: {payload.get('existing_equal_objects', 0)}")
        lines.append("Next: materialize these exact release/deployment IDs in the Airflow cache.")
    else:
        lines.extend(_error_lines(payload))
    return "\n".join(lines) + "\n"


def airflow_cache_materialize_text(payload: Mapping[str, object]) -> str:
    status = str(payload.get("status") or "failed").upper()
    lines = [f"dpone Airflow cache materialize: {status}"]
    lines.extend(_identity_lines(payload))
    if status in {"MATERIALIZED", "NO_OP"}:
        lines.append(f"- downloaded objects: {payload.get('downloaded_objects', 0)}")
        lines.append("- activated: no")
        lines.append("Next: review current state, then run dpone airflow cache-sync with a reviewed CAS guard.")
    else:
        lines.extend(_error_lines(payload))
    return "\n".join(lines) + "\n"


def _identity_lines(payload: Mapping[str, object]) -> list[str]:
    return [
        f"- release: {payload.get('release_id') or 'unknown'}",
        f"- deployment: {payload.get('deployment_id') or 'unknown'}",
        f"- environment: {payload.get('environment') or 'unknown'}",
        f"- registry ref: {payload.get('artifact_registry_ref') or 'unknown'}",
    ]


def _error_lines(payload: Mapping[str, object]) -> list[str]:
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return []
    lines: list[str] = []
    for error in errors:
        if not isinstance(error, Mapping):
            continue
        lines.append(f"- {error.get('code') or 'DPONE_ERROR'}: {error.get('message') or 'Artifact delivery failed'}")
        fixes = error.get("fixes")
        if not isinstance(fixes, list):
            continue
        for fix in fixes:
            if not isinstance(fix, Mapping):
                continue
            command = fix.get("command")
            if isinstance(command, str) and command:
                lines.append(f"  fix {fix.get('safety') or 'manual'}: {command}")
    return lines


__all__ = ["airflow_artifact_publish_text", "airflow_cache_materialize_text"]
