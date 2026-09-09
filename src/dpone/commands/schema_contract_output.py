"""Output rendering for schema contract CLI commands."""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any


def emit_schema_contract_payload(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = render_schema_contract_payload(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    import_module("dpone.commands.output_text").write_text(rendered)


def render_schema_contract_payload(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "pytest":
        return str(payload.get("content", ""))
    if output_format == "json":
        return import_module("dpone.commands.output_json").dumps_json(payload)
    if output_format == "md":
        return _render_markdown(payload)
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "dpone.schema_contract"))]
    for key in ("status", "contract_id", "version", "required_bump"):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_markdown(payload: dict[str, Any]) -> str:
    if payload.get("markdown"):
        return str(payload["markdown"])
    title = "Schema Contract Compatibility" if "compatibility_plan_id" in payload else "Schema Contract"
    return f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("versions") or payload.get("consumers") or [payload]
    lines = ["schema contract registry"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                lines.append(
                    " | ".join(
                        str(item.get(key, ""))
                        for key in ("contract_id", "version", "status", "owner", "id")
                        if item.get(key, "") != ""
                    )
                )
    return "\n".join(lines) + "\n"


__all__ = ["emit_schema_contract_payload", "render_schema_contract_payload"]
