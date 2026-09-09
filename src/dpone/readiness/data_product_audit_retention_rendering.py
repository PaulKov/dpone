"""Renderers for data product audit archive contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def render_markdown(payload: Mapping[str, Any], *, title: str = "Data Product Audit Retention") -> str:
    lines = [
        f"# {title}",
        "",
        f"- status: {payload.get('status')}",
        f"- product_id: {payload.get('product_id') or _product_id(payload)}",
    ]
    for key in (
        "audit_archive_plan_id",
        "audit_archive_run_id",
        "audit_archive_verification_id",
        "audit_retention_plan_id",
        "legal_hold_id",
        "merkle_root",
        "decision",
    ):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    lines.extend(["", "## Blockers", ""])
    blockers = list(payload.get("blockers", []))
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    warnings = list(payload.get("warnings", []))
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    return "\n".join(lines) + "\n"


def render_text(payload: Mapping[str, Any]) -> str:
    keys = (
        "schema_version",
        "status",
        "product_id",
        "audit_archive_plan_id",
        "audit_archive_run_id",
        "audit_archive_verification_id",
        "audit_retention_plan_id",
        "legal_hold_id",
    )
    lines = [f"- {key}: {payload.get(key)}" for key in keys if payload.get(key) is not None]
    return "\n".join(lines) + "\n"


def render_table(payload: Mapping[str, Any]) -> str:
    rows = payload.get("artifact_refs") or payload.get("artifacts") or payload.get("retention_items") or [payload]
    lines = ["kind | status | id", "--- | --- | ---"]
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        kind = row.get("kind") or row.get("schema_version") or "archive"
        status = row.get("status") or payload.get("status") or ""
        identity = row.get("artifact_id") or row.get("sha256") or row.get("archive_uri") or ""
        lines.append(f"{kind} | {status} | {identity}")
    return "\n".join(lines) + "\n"


def render_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _product_id(payload: Mapping[str, Any]) -> str | None:
    product = payload.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None


__all__ = ["render_json", "render_markdown", "render_table", "render_text"]
