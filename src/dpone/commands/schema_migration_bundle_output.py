"""Output rendering for schema migration bundle CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text
from dpone.readiness.schema_migration_bundle_diff_rendering import (
    render_bundle_diff_markdown,
    render_bundle_diff_table,
    render_bundle_diff_text,
)
from dpone.readiness.schema_migration_bundle_gate_rendering import (
    render_gate_markdown,
    render_gate_table,
    render_gate_text,
)


def emit_bundle_payload(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = render_bundle_payload(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


def render_bundle_payload(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return dumps_json(payload)
    if output_format == "md":
        return _render_markdown(payload)
    if output_format == "table":
        return _render_table(payload)
    return _render_text_payload(payload)


def _render_markdown(payload: dict[str, Any]) -> str:
    if payload.get("schema_version") == "dpone.schema_migration_bundle_diff.v1":
        return render_bundle_diff_markdown(payload)
    if payload.get("schema_version") == "dpone.schema_migration_bundle_gate.v1":
        return render_gate_markdown(payload)
    return str(payload.get("markdown") or _render_generic_markdown(payload))


def _render_table(payload: dict[str, Any]) -> str:
    if payload.get("schema_version") == "dpone.schema_migration_bundle_diff.v1":
        return render_bundle_diff_table(payload)
    if payload.get("schema_version") == "dpone.schema_migration_bundle_gate.v1":
        return render_gate_table(payload)
    return _render_text_payload(payload)


def _render_text_payload(payload: dict[str, Any]) -> str:
    if payload.get("schema_version") == "dpone.schema_migration_bundle_diff.v1":
        return render_bundle_diff_text(payload)
    if payload.get("schema_version") == "dpone.schema_migration_bundle_gate.v1":
        return render_gate_text(payload)
    return _render_generic_text(payload)


def _render_generic_text(payload: dict[str, Any]) -> str:
    command = str(payload.get("command") or payload.get("schema_version", "schema_migration_bundle"))
    lines = [command, f"- status: {payload.get('status')}"]
    for key in ("bundle_id", "pack_id"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_generic_markdown(payload: dict[str, Any]) -> str:
    return "# Schema Migration Bundle\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


__all__ = ["emit_bundle_payload", "render_bundle_payload"]
