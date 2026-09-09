"""Topology-free text rendering for Airflow Connection Secret GC."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence


def connection_gc_plan_text(payload: Mapping[str, object]) -> str:
    status = str(payload.get("status") or "failed").upper()
    inventory = payload.get("inventory")
    counts = inventory if isinstance(inventory, Mapping) else {}
    items = _mapping_items(payload.get("items"))
    lines = [
        f"dpone airflow connection Secret GC: {status}",
        f"- namespace: {payload.get('namespace') or 'unknown'}",
        f"- active references: {counts.get('active_references', 0)}",
        f"- delete candidates: {len(_text_items(payload.get('delete_candidates')))}",
        f"- quarantined: {counts.get('quarantined', 0)}",
    ]
    for index, item in enumerate(items, start=1):
        lines.append(f"- item {index}: {item.get('action')} ({item.get('reason')}, age={item.get('age_seconds')})")
    if payload.get("status") == "needs_cleanup":
        namespace = shlex.quote(str(payload.get("namespace") or ""))
        age = _integer(payload.get("minimum_age_seconds"), default=86_400)
        page_size = _integer(payload.get("page_size"), default=500)
        lines.append(
            "- action: dpone airflow connection-secret-gc-apply"
            f" --namespace {namespace} --minimum-age-seconds {age} --page-size {page_size}"
            ' --actor "${DPONE_SECRET_GC_ACTOR:?set DPONE_SECRET_GC_ACTOR}"'
            ' --allowed-actor "${DPONE_SECRET_GC_ACTOR:?set DPONE_SECRET_GC_ACTOR}" --confirm-delete'
        )
    elif payload.get("status") == "needs_attention":
        lines.append("- action: inspect quarantined digest references before enabling apply")
    else:
        lines.append("- action: no connection Secret deletion needed")
    lines.append("- details: rerun with --format json for protected digest references")
    return "\n".join(lines) + "\n"


def connection_gc_apply_text(payload: Mapping[str, object]) -> str:
    status = str(payload.get("status") or "failed").upper()
    lines = [
        f"dpone airflow connection Secret GC apply: {status}",
        f"- namespace: {payload.get('namespace') or 'unknown'}",
        f"- deleted: {len(_text_items(payload.get('deleted_secret_refs')))}",
        f"- skipped: {len(_text_items(payload.get('skipped_secret_refs')))}",
        f"- failed: {len(_text_items(payload.get('failed_secret_refs')))}",
    ]
    for index, item in enumerate(_mapping_items(payload.get("items")), start=1):
        lines.append(f"- item {index}: {item.get('action')} ({item.get('reason')})")
    lines.append("- action: rerun connection-secret-gc-plan to verify remaining inventory")
    lines.append("- details: rerun with --format json for protected digest references")
    return "\n".join(lines) + "\n"


def _mapping_items(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _text_items(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value)


def _integer(value: object, *, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


# Compatibility aliases preserve the pre-v0.72.3 internal import surface.
connection_secret_gc_plan_text = connection_gc_plan_text
connection_secret_gc_apply_text = connection_gc_apply_text


__all__ = [
    "connection_gc_apply_text",
    "connection_gc_plan_text",
    "connection_secret_gc_apply_text",
    "connection_secret_gc_plan_text",
]
