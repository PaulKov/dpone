"""Renderers for schema migration remediation artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def render_remediation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Migration Remediation",
        "",
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    if payload.get("watch_certificate_id"):
        lines.append(f"- watch_certificate_id: {payload.get('watch_certificate_id')}")
    if payload.get("environment"):
        lines.append(f"- environment: {payload.get('environment')}")
    if payload.get("capability"):
        lines.append(f"- capability: {payload.get('capability')}")
    operations = payload.get("operations", [])
    if isinstance(operations, list) and operations:
        lines.extend(["", "## Operations", ""])
        for operation in operations:
            if isinstance(operation, Mapping):
                lines.append(f"- `{operation.get('name')}`: `{operation.get('sql')}`")
    _append_list(lines, "Blockers", payload.get("blockers", []))
    _append_list(lines, "Warnings", payload.get("warnings", []))
    return "\n".join(lines) + "\n"


def render_remediation_table(payload: Mapping[str, Any]) -> str:
    return (
        "\n".join(
            [
                "schema migration remediation",
                "field | value",
                "--- | ---",
                f"status | {payload.get('status')}",
                f"pack_id | {payload.get('pack_id')}",
                f"watch_certificate_id | {payload.get('watch_certificate_id')}",
                f"environment | {payload.get('environment')}",
                f"capability | {payload.get('capability')}",
            ]
        )
        + "\n"
    )


def _append_list(lines: list[str], title: str, values: object) -> None:
    if isinstance(values, list) and values:
        lines.extend(["", f"## {title}", "", *[f"- {item}" for item in values]])


__all__ = ["render_remediation_markdown", "render_remediation_table"]
