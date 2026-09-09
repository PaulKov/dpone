"""Human-readable rendering for plan-first Airflow reruns."""

from __future__ import annotations

from collections.abc import Mapping


def airflow_rerun_plan_text(payload: Mapping[str, object]) -> str:
    status = str(payload.get("status") or "blocked").upper()
    selection = _mapping(payload.get("selection"))
    resolved = _mapping(payload.get("resolved"))
    request = _mapping(payload.get("airflow_request"))
    lines = [
        f"dpone airflow rerun plan: {status}",
        f"- bundle: {selection.get('bundle', '')}",
        f"- artifacts: {selection.get('artifacts', '')}",
        f"- release: {resolved.get('release_id', '')}",
        f"- deployment: {resolved.get('deployment_id', '')}",
        f"- execution: {request.get('execution_mode', '')}",
    ]
    for blocker in _issues(payload.get("blockers")):
        lines.append(f"- blocker: {blocker.get('code', '')}: {blocker.get('message', '')}")
    for warning in _issues(payload.get("warnings")):
        lines.append(f"- warning: {warning.get('code', '')}: {warning.get('message', '')}")
    lines.append("- details: rerun with --format json for full pinned identities and retention refs")
    return "\n".join(lines) + "\n"


def _issues(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["airflow_rerun_plan_text"]
