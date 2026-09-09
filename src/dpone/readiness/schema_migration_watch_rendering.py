"""Renderers for schema migration release watch artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.schema_migration_watch_support import mapping, strings, target_key


def render_watch_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Migration Watch Certificate",
        "",
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
        f"- post_apply_certificate_id: {payload.get('post_apply_certificate_id')}",
        f"- environment: {payload.get('environment')}",
        f"- target: {target_key(payload.get('target'))}",
        "",
        "## Samples",
    ]
    samples = mapping(payload.get("samples"))
    lines.extend(f"- {key}: {value}" for key, value in samples.items())
    remediation = mapping(payload.get("remediation"))
    if remediation:
        lines.extend(["", "## Remediation", f"- decision: {remediation.get('decision')}"])
    for key in ("blockers", "warnings"):
        values = strings(payload.get(key, []))
        if values:
            lines.extend(["", f"## {key.title()}"])
            lines.extend(f"- {item}" for item in values)
    return "\n".join(lines) + "\n"


def render_watch_table(payload: Mapping[str, Any]) -> str:
    lines = ["schema migration watch", "field | value", "--- | ---"]
    for key in ("status", "pack_id", "post_apply_certificate_id", "environment"):
        lines.append(f"{key} | {payload.get(key)}")
    samples = mapping(payload.get("samples"))
    lines.append(f"samples | {samples.get('passed', 0)}/{samples.get('executed', 0)}")
    lines.append(f"target | {target_key(payload.get('target'))}")
    return "\n".join(lines) + "\n"


__all__ = ["render_watch_markdown", "render_watch_table"]
