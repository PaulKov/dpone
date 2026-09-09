"""Human-readable rendering for Airflow deployment cache recovery."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence

from dpone.commands.airflow_cache_path_rendering import cache_root_flag


def self_service_cache_recovery_plan_text(payload: Mapping[str, object], *, cache_root: str) -> str:
    """Render local cache recovery diagnostics with an explicit platform action."""

    status = str(payload.get("status") or "unknown").upper()
    environment = str(payload.get("environment") or "unknown")
    lines = [
        f"dpone airflow cache recovery: {status}",
        f"- environment: {environment}",
    ]
    current_deployment_id = _optional_text(payload.get("current_deployment_id"))
    if current_deployment_id:
        lines.append(f"- pointer deployment: {current_deployment_id}")
    current_path_deployment_id = _optional_text(payload.get("current_path_deployment_id"))
    lines.append(f"- active deployment: {current_path_deployment_id or 'absent'}")
    preferred_repair_id = _optional_text(payload.get("preferred_repair_deployment_id"))
    if preferred_repair_id:
        lines.append(f"- preferred repair: {preferred_repair_id}")
    issue_lines = _cache_recovery_issue_lines(payload.get("issues"))
    lines.extend(issue_lines or ["- issues: none"])
    candidate_lines = _cache_recovery_candidate_lines(payload.get("repair_candidates"))
    lines.extend(candidate_lines or ["- repair candidates: none"])
    lines.append(
        _cache_recovery_action_line(
            status,
            environment,
            preferred_repair_id,
            current_path_deployment_id=current_path_deployment_id,
            cache_root=cache_root,
        )
    )
    lines.append("- details: rerun with --format json for the full cache-recovery plan")
    return "\n".join(lines) + "\n"


def self_service_cache_recovery_apply_text(payload: Mapping[str, object]) -> str:
    """Render a concise recovery apply report without exposing local cache paths."""

    status = "OK" if bool(payload.get("passed")) else "FAILED"
    lines = [
        f"dpone airflow cache recovery apply: {status}",
        f"- environment: {payload.get('environment') or 'unknown'}",
    ]
    recovered_deployment_id = _optional_text(payload.get("recovered_deployment_id"))
    if recovered_deployment_id:
        lines.append(f"- recovered deployment: {recovered_deployment_id}")
    release_id = _optional_text(payload.get("release_id"))
    if release_id:
        lines.append(f"- release: {release_id}")
    if status == "OK":
        lines.append("- current pointer: repaired")
        lines.append("- action: run dpone airflow cache-recovery-plan to verify cache health")
    else:
        error = _first_error(payload.get("errors"))
        code = _optional_text(error.get("code")) or "DPONE_CACHE_RECOVERY_FAILED"
        message = _optional_text(error.get("message")) or "cache recovery failed"
        lines.append(f"- issue: {code}: {message}")
        recovery_required = bool(payload.get("recovery_required")) or (
            code == "DPONE_CACHE_PROMOTION_WRITE_FAILED" and "recovery_required" not in payload
        )
        if recovery_required:
            lines.append("- current pointer: recovery required")
            lines.append("- action: run dpone airflow cache-recovery-plan before retrying recovery apply")
        else:
            lines.append("- current pointer: not changed")
            lines.append("- action: rerun with --format json and inspect structured errors")
    lines.append("- details: rerun with --format json for current/pointer paths")
    return "\n".join(lines) + "\n"


def _optional_text(value: object) -> str:
    return str(value or "").strip()


def _first_error(value: object) -> Mapping[str, object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return {}
    return next((item for item in value if isinstance(item, Mapping)), {})


def _cache_recovery_issue_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("code") or "DPONE_CACHE_RECOVERY_ISSUE")
        message = str(item.get("message") or "").strip()
        lines.append(f"- issue: {code}: {message}" if message else f"- issue: {code}")
    return lines


def _cache_recovery_candidate_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        deployment_id = str(item.get("deployment_id") or "").strip()
        if not deployment_id:
            continue
        reason = str(item.get("reason") or "complete")
        lines.append(f"- repair candidate: {deployment_id} ({reason})")
    return lines


def _cache_recovery_action_line(
    status: str,
    environment: str,
    deployment_id: str,
    *,
    current_path_deployment_id: str,
    cache_root: str,
) -> str:
    if status == "OK":
        return "- action: no repair needed"
    if not deployment_id:
        return "- action: fix deployment cache projections, then rerun cache-recovery-plan"
    current_guard = (
        f" --expected-current-deployment-id {shlex.quote(current_path_deployment_id)}"
        if current_path_deployment_id
        else " --expect-current-absent"
    )
    return (
        "- action: dpone airflow cache-recovery-apply"
        f"{cache_root_flag(cache_root)}"
        f" --environment {shlex.quote(environment)}"
        f" --deployment-id {shlex.quote(deployment_id)}"
        ' --promoted-by "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}"'
        ' --allowed-promoter "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}"'
        f"{current_guard}"
        " --confirm-repair"
    )


__all__ = ["self_service_cache_recovery_apply_text", "self_service_cache_recovery_plan_text"]
