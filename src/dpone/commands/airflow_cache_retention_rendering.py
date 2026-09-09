"""Human-readable rendering for Airflow deployment cache retention."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence

from dpone.commands.airflow_cache_path_rendering import cache_root_flag


def self_service_cache_retention_plan_text(payload: Mapping[str, object], *, cache_root: str) -> str:
    """Render plan-first cache retention diagnostics without local paths."""

    environment = str(payload.get("environment") or "unknown")
    delete_candidates = _text_sequence(payload.get("delete_candidates"))
    items = _mapping_sequence(payload.get("items"))
    quarantined_items = _quarantined_items(items)
    status = "NEEDS_ATTENTION" if quarantined_items else "NEEDS_CLEANUP" if delete_candidates else "OK"
    lines = [
        f"dpone airflow cache retention: {status}",
        f"- environment: {environment}",
    ]
    current_deployment_id = _optional_text(payload.get("current_deployment_id"))
    protected_deployment_ids = _text_sequence(payload.get("protected_deployment_ids"))
    plan_sha256 = _optional_text(payload.get("plan_sha256"))
    if current_deployment_id:
        lines.append(f"- current deployment: {current_deployment_id}")
    lines.append(f"- delete candidates: {len(delete_candidates)}")
    if quarantined_items:
        lines.append(f"- quarantined entries: {len(quarantined_items)}")
    lines.extend(_retention_plan_item_lines(items))
    lines.append(
        _retention_plan_action_line(
            delete_candidates,
            environment,
            cache_root=cache_root,
            protected_deployment_ids=protected_deployment_ids,
            plan_sha256=plan_sha256,
            has_quarantine=bool(quarantined_items),
        )
    )
    lines.append("- details: rerun with --format json for the full cache-retention plan")
    return "\n".join(lines) + "\n"


def self_service_cache_retention_apply_text(payload: Mapping[str, object]) -> str:
    """Render a concise cache retention apply report without local paths."""

    items = _mapping_sequence(payload.get("items"))
    quarantined_items = _quarantined_items(items)
    status = "FAILED" if not bool(payload.get("passed")) else "NEEDS_ATTENTION" if quarantined_items else "OK"
    lines = [
        f"dpone airflow cache retention apply: {status}",
        f"- environment: {payload.get('environment') or 'unknown'}",
    ]
    current_deployment_id = _optional_text(payload.get("current_deployment_id"))
    if current_deployment_id:
        lines.append(f"- current deployment: {current_deployment_id}")
    deleted_items = _items_with_action(items, "deleted")
    skipped_items = _items_with_action(items, "skipped")
    lines.append(f"- deleted deployments: {len(deleted_items)}")
    lines.extend(_retention_apply_item_lines(deleted_items, action="deleted"))
    lines.append(f"- skipped deployments: {len(skipped_items)}")
    if quarantined_items:
        lines.append(f"- quarantined entries: {len(quarantined_items)}")
    lines.extend(_retention_apply_item_lines(skipped_items, action="skipped"))
    if status == "OK":
        lines.append("- action: run dpone airflow cache-retention-plan to verify cache health")
    elif status == "NEEDS_ATTENTION":
        lines.append("- action: inspect quarantine diagnostics before the next cache retention apply")
    else:
        lines.append("- action: rerun with --format json and inspect structured errors")
    lines.append("- details: rerun with --format json for deleted/skipped deployment paths")
    return "\n".join(lines) + "\n"


def _optional_text(value: object) -> str:
    return str(value or "").strip()


def _text_sequence(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value if str(item).strip())


def _retention_plan_item_lines(value: object) -> list[str]:
    lines: list[str] = []
    for item in _mapping_sequence(value):
        deployment_id = _item_identity(item)
        action = _optional_text(item.get("action")) or "inspect"
        reason = _item_reason(item)
        if action == "delete":
            lines.append(f"- delete candidate: {deployment_id} ({reason})")
        elif action == "protect":
            lines.append(f"- protected: {deployment_id} ({reason})")
        elif action == "quarantine":
            lines.append(f"- quarantine: {deployment_id} ({reason})")
    return lines


def _retention_apply_item_lines(value: object, *, action: str) -> list[str]:
    lines: list[str] = []
    for item in _mapping_sequence(value):
        if item.get("action") != action:
            continue
        deployment_id = _item_identity(item)
        reason = _item_reason(item)
        lines.append(f"- {action}: {deployment_id} ({reason})")
    return lines


def _mapping_sequence(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _items_with_action(
    items: tuple[Mapping[str, object], ...],
    action: str,
) -> tuple[Mapping[str, object], ...]:
    return tuple(item for item in items if item.get("action") == action)


def _quarantined_items(items: tuple[Mapping[str, object], ...]) -> tuple[Mapping[str, object], ...]:
    return tuple(
        item
        for item in items
        if item.get("action") == "quarantine"
        or item.get("reason") in {"incomplete", "invalid"}
        or not _optional_text(item.get("deployment_id"))
    )


def _item_identity(item: Mapping[str, object]) -> str:
    return _optional_text(item.get("deployment_id")) or "unidentified cache entry"


def _item_reason(item: Mapping[str, object]) -> str:
    reason = _optional_text(item.get("reason")) or "unknown"
    error_code = _optional_text(item.get("error_code"))
    return f"{reason}; {error_code}" if error_code else reason


def _retention_plan_action_line(
    deployment_ids: tuple[str, ...],
    environment: str,
    *,
    cache_root: str,
    protected_deployment_ids: tuple[str, ...],
    plan_sha256: str,
    has_quarantine: bool,
) -> str:
    if has_quarantine:
        return "- action: inspect quarantine diagnostics before running cache-retention-apply"
    if not deployment_ids:
        return "- action: no cache retention delete needed"
    if not plan_sha256:
        return "- action: rerun cache-retention-plan with --format json; reviewed plan digest is missing"
    protection_flags = "".join(
        f" --protect-deployment-id {shlex.quote(deployment_id)}" for deployment_id in protected_deployment_ids
    )
    return (
        "- action: dpone airflow cache-retention-apply"
        f"{cache_root_flag(cache_root)}"
        f" --environment {shlex.quote(environment)}"
        f"{protection_flags}"
        f" --expected-plan-sha256 {shlex.quote(plan_sha256)}"
        ' --review-id "${DPONE_CACHE_RETENTION_REVIEW_ID:?set one approved UUIDv4 attempt id}"'
        ' --loader-ack-file "${DPONE_LOADER_ACK_FILE:?set DPONE_LOADER_ACK_FILE}"'
        ' --promoted-by "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}"'
        ' --allowed-promoter "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}"'
        " --confirm-delete"
    )


__all__ = ["self_service_cache_retention_apply_text", "self_service_cache_retention_plan_text"]
