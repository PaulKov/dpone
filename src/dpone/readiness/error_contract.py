"""Structured error payload helpers for dpone self-service readiness surfaces."""

from __future__ import annotations

from typing import Any


def error_docs_url(code: str) -> str:
    return f"docs/errors/{code}.md"


def dpone_error(
    code: str,
    message: str,
    *,
    stage: str,
    severity: str = "error",
    path: str | None = None,
    entity: dict[str, str] | None = None,
    fixes: list[dict[str, str]] | None = None,
    docs_url: str | None = None,
    trace_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": stage,
        "severity": severity,
        "message": message,
        "fixes": fixes or [],
    }
    if path:
        payload["path"] = path
    if entity:
        payload["entity"] = entity
    if docs_url:
        payload["docs_url"] = docs_url
    if trace_id:
        payload["trace_id"] = trace_id
    if extra:
        payload.update(extra)
    return payload


def manual_fix(fix_id: str, *, command: str | None = None) -> dict[str, str]:
    payload = {"id": fix_id, "safety": "manual"}
    if command:
        payload["command"] = command
    return payload


def safe_fix(fix_id: str, *, command: str | None = None) -> dict[str, str]:
    payload = {"id": fix_id, "safety": "safe"}
    if command:
        payload["command"] = command
    return payload


__all__ = ["dpone_error", "error_docs_url", "manual_fix", "safe_fix"]
