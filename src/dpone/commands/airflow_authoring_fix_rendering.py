"""Human-readable rendering for safe Airflow authoring fixes."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence


def self_service_authoring_fix_text(payload: Mapping[str, object], *, target: str) -> str | None:
    """Render ``dpone fix`` results with safe next actions and redacted diffs."""

    if payload.get("kind") != "dpone.airflow-authoring-fix.v1":
        return None

    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    assert isinstance(summary, Mapping)
    status = _status(payload, summary)
    target_path = _optional_text(payload.get("target")) or target
    mode = _optional_text(payload.get("mode")) or "unknown"
    legacy_sections = _int_text(summary.get("legacy_sections"))
    applied_sections = _int_text(summary.get("applied_sections"))
    lines = [
        f"dpone fix: {status}",
        f"- target: {target_path}",
        f"- mode: {mode}",
        f"- legacy sections: {legacy_sections}",
        f"- applied sections: {applied_sections}",
    ]
    lines.extend(_planned_ref_lines(payload.get("migration_plan")))
    lines.extend(_change_lines(payload.get("changes")))
    lines.append(_action_line(payload, target=target))
    lines.append("- details: rerun with --format json for the full authoring migration plan")
    return "\n".join(lines) + "\n"


def _status(payload: Mapping[str, object], summary: Mapping[str, object]) -> str:
    if bool(summary.get("no_op")):
        return "NO_OP"
    if bool(payload.get("applied")):
        return "APPLIED"
    return "PLAN"


def _planned_ref_lines(value: object) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    changes = value.get("changes")
    if not isinstance(changes, Sequence) or isinstance(changes, (str, bytes)):
        return []
    lines: list[str] = []
    for change in changes:
        if not isinstance(change, Mapping):
            continue
        path = _optional_text(change.get("path"))
        connection_ref = _optional_text(change.get("suggested_connection_ref"))
        if path and connection_ref:
            lines.append(f"- planned ref: {path} -> {connection_ref}")
    return lines


def _change_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for change in value:
        if not isinstance(change, Mapping):
            continue
        action = _optional_text(change.get("action")) or "change"
        path = _optional_text(change.get("path")) or "<unknown>"
        lines.append(f"- {action}: {path}")
        diff = change.get("diff")
        if isinstance(diff, str) and diff.strip():
            lines.extend(f"  {line}" if line else "" for line in diff.rstrip("\n").splitlines())
    return lines


def _action_line(payload: Mapping[str, object], *, target: str) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    assert isinstance(summary, Mapping)
    quoted_target = shlex.quote(_command_target(payload, fallback=target))
    if bool(summary.get("no_op")):
        return f"- action: run dpone check {quoted_target} to verify the pipeline"
    if bool(payload.get("applied")):
        return f"- action: run dpone check {quoted_target} to verify the migrated authoring source"
    return f"- action: review the redacted diff, then run dpone fix {quoted_target} --apply"


def _command_target(payload: Mapping[str, object], *, fallback: str) -> str:
    target = _optional_text(payload.get("target")) or fallback
    return target.removesuffix("/pipeline.yaml") or target


def _optional_text(value: object) -> str:
    return str(value or "").strip()


def _int_text(value: object) -> str:
    if isinstance(value, bool):
        return "0"
    if isinstance(value, int):
        return str(value)
    return _optional_text(value) or "0"


__all__ = ["self_service_authoring_fix_text"]
