"""Renderers for schema migration bundle diff artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

BUNDLE_DIFF_SCHEMA = "dpone.schema_migration_bundle_diff.v1"


def render_bundle_diff_text(payload: Mapping[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", BUNDLE_DIFF_SCHEMA)),
        f"- status: {payload.get('status')}",
        f"- diff_id: {payload.get('diff_id')}",
        f"- base_pack_id: {payload.get('base_pack_id')}",
        f"- head_pack_id: {payload.get('head_pack_id')}",
    ]
    lines.extend(_list("blockers", payload.get("blockers", [])))
    lines.extend(_list("warnings", payload.get("warnings", [])))
    return "\n".join(lines) + "\n"


def render_bundle_diff_markdown(payload: Mapping[str, Any]) -> str:
    target = payload.get("target", {}) if isinstance(payload.get("target"), Mapping) else {}
    lines = [
        "# Schema Migration Bundle Diff",
        "",
        f"- status: {payload.get('status')}",
        f"- base_pack_id: {payload.get('base_pack_id')}",
        f"- head_pack_id: {payload.get('head_pack_id')}",
        f"- target: {target.get('sink_type')}.{target.get('table')}",
        "",
        "## Reviewer Focus",
        "",
    ]
    focus = _reviewer_focus(payload)
    lines.extend(f"{index}. {item}" for index, item in enumerate(focus, start=1)) if focus else lines.append("- none")
    lines.extend(
        [
            "",
            "## Changes",
            "",
            "| Kind | Severity | Path | Reviewer action |",
            "| --- | --- | --- | --- |",
        ]
    )
    changes = payload.get("changes", [])
    if isinstance(changes, list) and changes:
        for change in changes:
            if isinstance(change, Mapping):
                lines.append(
                    f"| {change.get('kind')} | {change.get('severity')} | "
                    f"{change.get('path')} | {change.get('reviewer_action')} |"
                )
    else:
        lines.append("| none | info | none | No material bundle delta detected. |")
    return "\n".join(lines) + "\n"


def render_bundle_diff_table(payload: Mapping[str, Any]) -> str:
    rows = [
        "Schema Migration Bundle Diff",
        f"status: {payload.get('status')}",
        "",
        "kind | severity | path | reviewer_action",
        "--- | --- | --- | ---",
    ]
    changes = payload.get("changes", [])
    if isinstance(changes, list):
        for change in changes:
            if isinstance(change, Mapping):
                rows.append(
                    f"{change.get('kind')} | {change.get('severity')} | "
                    f"{change.get('path')} | {change.get('reviewer_action')}"
                )
    return "\n".join(rows) + "\n"


def _reviewer_focus(payload: Mapping[str, Any]) -> list[str]:
    changes = payload.get("changes", [])
    if not isinstance(changes, list):
        return []
    focus: list[str] = []
    for change in changes:
        if not isinstance(change, Mapping):
            continue
        if change.get("severity") in {"high", "critical"}:
            focus.append(str(change.get("reviewer_action")))
    return list(dict.fromkeys(focus[:5]))


def _list(title: str, raw: object) -> list[str]:
    values = [str(item) for item in raw] if isinstance(raw, list) else []
    if not values:
        return []
    return [f"- {title}:", *(f"  - {item}" for item in values)]


__all__ = ["render_bundle_diff_markdown", "render_bundle_diff_table", "render_bundle_diff_text"]
