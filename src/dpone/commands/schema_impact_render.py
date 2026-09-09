from __future__ import annotations

import json
from typing import Any

from dpone.commands.output_json import dumps_json


def render_schema_impact(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return dumps_json(payload)
    if output_format == "md":
        return "# Schema Impact Plan\n\n```json\n" + json.dumps(payload, indent=2, ensure_ascii=False) + "\n```\n"
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [f"dpone schema impact {payload.get('command', 'plan')}", f"- status: {payload.get('status', 'planned')}"]
    for key in ("pack_id", "impact_plan_id"):
        if payload.get(key):
            lines.append(f"- {key}: {payload[key]}")
    summary = payload.get("summary", {})
    if isinstance(summary, dict):
        lines.append(f"- max_severity: {summary.get('max_severity', 'none')}")
        lines.append(f"- impacted_consumers: {summary.get('impacted_consumers', 0)}")
    for key in ("required_approvals", "blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_table(payload: dict[str, Any]) -> str:
    consumers = payload.get("impacted_consumers", [])
    lines = ["schema impact consumers", "id | type | owner", "--- | --- | ---"]
    if isinstance(consumers, list):
        for item in consumers:
            if isinstance(item, dict):
                lines.append(f"{item.get('id')} | {item.get('node_type')} | {item.get('owner', '')}")
    return "\n".join(lines) + "\n"


__all__ = ["render_schema_impact"]
