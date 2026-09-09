"""Renderers for schema migration recovery evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def render_recovery_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {_title(payload)}",
        "",
        f"- status: {payload.get('status')}",
    ]
    for key in ("pack_id", "restore_point_id", "chain_verification_id", "environment", "destination"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", "", *[f"- {item}" for item in payload.get("blockers", [])]])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", "", *[f"- {item}" for item in payload.get("warnings", [])]])
    if payload.get("actions"):
        lines.extend(["", "## Actions", "", *[f"- {item.get('action')}" for item in payload.get("actions", [])]])
    return "\n".join(lines) + "\n"


def render_recovery_table(payload: Mapping[str, Any]) -> str:
    if isinstance(payload.get("restore_points"), list):
        lines = ["restore_point_id | status | kind | environment | destination", "--- | --- | --- | --- | ---"]
        for point in payload["restore_points"]:
            lines.append(
                f"{point.get('restore_point_id')} | {point.get('status')} | {point.get('kind')} | "
                f"{point.get('environment')} | {point.get('destination')}"
            )
        return "\n".join(lines) + "\n"
    return (
        "\n".join(
            [
                "schema migration recovery",
                "field | value",
                "--- | ---",
                f"status | {payload.get('status')}",
                f"pack_id | {payload.get('pack_id')}",
                f"restore_point_id | {payload.get('restore_point_id')}",
                f"environment | {payload.get('environment')}",
            ]
        )
        + "\n"
    )


def _title(payload: Mapping[str, Any]) -> str:
    schema = str(payload.get("schema_version") or "")
    if "chain" in schema:
        return "Schema Migration Recovery Chain Verification"
    if "retention" in schema:
        return "Schema Migration Recovery Retention Plan"
    if "restore_certificate" in schema:
        return "Schema Migration Recovery Restore Certificate"
    if "restore_run" in schema:
        return "Schema Migration Recovery Restore Run"
    if "restore_plan" in schema:
        return "Schema Migration Recovery Restore Plan"
    return "Schema Migration Recovery Point"


__all__ = ["render_recovery_markdown", "render_recovery_table"]
