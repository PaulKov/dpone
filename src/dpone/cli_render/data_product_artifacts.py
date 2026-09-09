"""Shared render/emit helpers for data product artifact commands."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.output_json import dumps_json
from dpone.output_text import write_text


def emit_data_product_artifact(
    payload: Mapping[str, Any],
    output_format: str,
    output_path: str | None,
    *,
    default_schema: str,
    text_keys: Sequence[str],
    markdown_title: str,
    table_title: str,
) -> int:
    """Render, optionally persist and print a data product command artifact."""
    rendered = render_data_product_artifact(
        payload,
        output_format,
        default_schema=default_schema,
        text_keys=text_keys,
        markdown_title=markdown_title,
        table_title=table_title,
    )
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)
    return 2 if payload.get("status") == "blocked" else 0


def render_data_product_artifact(
    payload: Mapping[str, Any],
    output_format: str,
    *,
    default_schema: str,
    text_keys: Sequence[str],
    markdown_title: str,
    table_title: str,
) -> str:
    """Render the common json/md/table/text formats used by data product CLI commands."""
    if output_format == "json":
        return dumps_json(dict(payload))
    if output_format == "md":
        return str(payload.get("markdown") or _render_md(payload, markdown_title))
    if output_format == "table":
        return _render_table(payload, table_title)
    return _render_text(payload, default_schema, text_keys)


def _render_text(payload: Mapping[str, Any], default_schema: str, keys: Sequence[str]) -> str:
    lines = [str(payload.get("schema_version", default_schema))]
    lines.extend(f"- {key}: {payload.get(key)}" for key in keys if payload.get(key) is not None)
    return "\n".join(lines) + "\n"


def _render_md(payload: Mapping[str, Any], title: str) -> str:
    return f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n"


def _render_table(payload: Mapping[str, Any], title: str) -> str:
    lines = [title, "schema | status | blockers", "--- | --- | ---"]
    lines.append(
        f"{payload.get('schema_version', '')} | {payload.get('status', '')} | "
        f"{', '.join(str(value) for value in payload.get('blockers', []))}"
    )
    return "\n".join(lines) + "\n"


__all__ = ["emit_data_product_artifact", "render_data_product_artifact"]
