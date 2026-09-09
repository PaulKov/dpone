"""Renderers for schema migration backup/restore evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def render_backup_markdown(payload: Mapping[str, Any]) -> str:
    title = _title(payload)
    lines = [
        f"# {title}",
        "",
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    for key in ("environment", "backup_destination", "restore_table"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", "", *[f"- {item}" for item in payload.get("blockers", [])]])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", "", *[f"- {item}" for item in payload.get("warnings", [])]])
    return "\n".join(lines) + "\n"


def render_backup_table(payload: Mapping[str, Any]) -> str:
    return (
        "\n".join(
            [
                "schema migration backup",
                "field | value",
                "--- | ---",
                f"status | {payload.get('status')}",
                f"pack_id | {payload.get('pack_id')}",
                f"environment | {payload.get('environment')}",
                f"backup_destination | {payload.get('backup_destination')}",
            ]
        )
        + "\n"
    )


def _title(payload: Mapping[str, Any]) -> str:
    schema = str(payload.get("schema_version") or "")
    if "restore" in schema:
        return "Schema Migration Restore"
    if "certificate" in schema:
        return "Schema Migration Backup Certificate"
    return "Schema Migration Backup"


__all__ = ["render_backup_markdown", "render_backup_table"]
