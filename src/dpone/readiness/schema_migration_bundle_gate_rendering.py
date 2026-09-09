"""Renderers for schema migration bundle gate receipts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.schema_migration_bundle_policy import BUNDLE_GATE_SCHEMA


def render_gate_text(payload: Mapping[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", BUNDLE_GATE_SCHEMA)),
        f"- status: {payload.get('status')}",
        f"- profile: {payload.get('profile')}",
    ]
    for key in ("gate_id", "bundle_id", "pack_id"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def render_gate_markdown(payload: Mapping[str, Any]) -> str:
    return (
        "# Schema Migration Bundle Gate\n\n"
        f"- status: {payload.get('status')}\n"
        f"- profile: {payload.get('profile')}\n"
        f"- pack_id: {payload.get('pack_id')}\n\n"
        "## Blockers\n\n"
        + _markdown_items(payload.get("blockers", []))
        + "\n## Warnings\n\n"
        + _markdown_items(payload.get("warnings", []))
    )


def render_gate_table(payload: Mapping[str, Any]) -> str:
    rows = [
        "Schema Migration Bundle Gate",
        f"status: {payload.get('status')}",
        "",
        "check | status | details",
        "--- | --- | ---",
    ]
    for check in payload.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        rows.append(f"{check.get('name')} | {check.get('status')} | {', '.join(check.get('details', []))}")
    return "\n".join(rows) + "\n"


def _markdown_items(raw: object) -> str:
    values = [str(item) for item in raw] if isinstance(raw, list) else []
    return "".join(f"- {item}\n" for item in values) if values else "- none\n"


__all__ = ["render_gate_markdown", "render_gate_table", "render_gate_text"]
