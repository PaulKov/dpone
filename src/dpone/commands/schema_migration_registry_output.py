"""Output rendering for schema migration registry CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text


def emit_registry_payload(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = render_registry_payload(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


def render_registry_payload(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return dumps_json(payload)
    if output_format == "md":
        return str(payload.get("markdown") or _render_markdown(payload))
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "schema_migration_registry"))]
    for key in ("status", "record_id", "pack_id", "bundle_id", "gate_id", "trust_verification_id"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_markdown(payload: dict[str, Any]) -> str:
    return (
        "# Schema Migration Evidence Registry\n\n```json\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
        + "\n```\n"
    )


def _render_table(payload: dict[str, Any]) -> str:
    records = payload.get("records")
    if not isinstance(records, list) and isinstance(payload.get("record"), dict):
        records = [payload["record"]]
    lines = [
        "schema migration evidence registry",
        "recorded_at | environment | stage | status | target | pack_id | bundle_id",
        "--- | --- | --- | --- | --- | --- | ---",
    ]
    for record in records or []:
        if not isinstance(record, dict):
            continue
        target = record.get("target", {})
        target_text = f"{target.get('sink_type')}.{target.get('table')}" if isinstance(target, dict) else ""
        lines.append(
            f"{record.get('recorded_at')} | {record.get('environment')} | {record.get('stage')} | "
            f"{record.get('status')} | {target_text} | {record.get('pack_id')} | {record.get('bundle_id')}"
        )
    return "\n".join(lines) + "\n"


__all__ = ["emit_registry_payload", "render_registry_payload"]
