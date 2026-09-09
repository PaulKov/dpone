"""Render the bounded beginner Airflow preview summary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from dpone.commands.docs_url_rendering import public_docs_url


def self_service_preview_text(payload: Mapping[str, object], *, pipeline_ref: str) -> str:
    """Render quiet success preview text; keep richer lines on failure."""

    status = "OK" if payload.get("passed") is True else "FAILED"
    if status == "OK" and payload.get("selection_scope") is True:
        selected = payload.get("selected_workload_ids")
        selected_ids = (
            [str(item) for item in selected]
            if isinstance(selected, Sequence) and not isinstance(selected, str | bytes)
            else []
        )
        lines = [
            f"dpone airflow preview: {status}",
            f"- scope: project selection ({len(selected_ids)} workload(s))",
        ]
        if selected_ids:
            lines.append(f"- workloads: {', '.join(selected_ids)}")
        lines.extend(_preview_projection_lines(payload))
        return "\n".join(lines) + "\n"
    pipeline_id = str(payload.get("pipeline_id") or _pipeline_id_from_target(pipeline_ref) or pipeline_ref)
    if status == "OK":
        lines = [
            f"dpone airflow preview: {status}",
            f"- pipeline: {pipeline_id}",
        ]
        lines.extend(_preview_projection_lines(payload))
        lines.append(f"- next: dpone test {pipeline_id}")
        return "\n".join(lines) + "\n"

    lines = [
        f"dpone airflow preview: {status}",
        f"- pipeline: {pipeline_id}",
    ]
    deployment = payload.get("deployment")
    if isinstance(deployment, Mapping):
        lines.extend(
            (
                f"- deployment type: {deployment.get('deployment_type') or 'unknown'}",
                f"- runnable: {_yes_no(deployment.get('runnable'))}",
            )
        )
    index_path = payload.get("airflow_index_path")
    if index_path:
        lines.append(f"- airflow index: {index_path}")
    lines.extend(_preview_node_lines(payload.get("dag_spec_nodes")))
    task_plan = payload.get("visible_task_plan")
    if isinstance(task_plan, Mapping):
        lines.append(
            f"- visible tasks: {task_plan.get('estimated_total') or 0} "
            f"(warn {task_plan.get('warn_threshold') or 0}, max {task_plan.get('max_tasks') or 0})"
        )
    error_lines = _error_lines(payload.get("errors"))
    lines.extend(error_lines)
    if status == "FAILED" and not any(line.startswith("  docs:") or line.startswith("  fix ") for line in error_lines):
        lines.append("- action: correct the target or initialize it, then rerun dpone airflow preview")
    return "\n".join(lines) + "\n"


def _preview_node_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        node_id = str(item.get("node_id") or "unknown")
        visibility = str(item.get("visibility") or "task")
        estimate = item.get("estimated_visible_tasks") or 0
        selector = str(item.get("selector") or "-")
        lines.append(f"- node {node_id}: {visibility}, {estimate} visible task(s), selector={selector}")
    return lines


def _preview_projection_lines(payload: Mapping[str, object]) -> list[str]:
    lines: list[str] = []
    deployment = payload.get("deployment")
    if isinstance(deployment, Mapping):
        deployment_type = str(deployment.get("deployment_type") or "unknown")
        runnable = deployment.get("runnable")
        qualifier = "not runnable" if runnable is False else "runnable" if runnable is True else "runnable unknown"
        lines.append(f"- deployment: {deployment_type} ({qualifier})")
    index_path = payload.get("airflow_index_path")
    if isinstance(index_path, str) and index_path:
        lines.append(f"- airflow index: {index_path}")
    return lines


def _pipeline_id_from_target(target: str) -> str:
    normalized = target.strip().removesuffix("/pipeline.yaml").rstrip("/")
    if normalized in {"", ".", "./"}:
        return ""
    return normalized.rsplit("/", 1)[-1] if normalized else ""


def _yes_no(value: object) -> str:
    return "yes" if value is True else "no" if value is False else str(value)


def _error_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("code") or "DPONE_ERROR")
        message = str(item.get("message") or "").strip()
        lines.append(f"- error: {code}: {message}" if message else f"- error: {code}")
        docs_url = item.get("docs_url")
        if isinstance(docs_url, str) and docs_url:
            lines.append(f"  docs: {public_docs_url(docs_url)}")
        fixes = item.get("fixes")
        if not isinstance(fixes, Sequence) or isinstance(fixes, (str, bytes)):
            continue
        for fix in fixes:
            if not isinstance(fix, Mapping):
                continue
            command = fix.get("command")
            if isinstance(command, str) and command:
                lines.append(f"  fix {fix.get('safety') or 'manual'}: {command}")
                continue
            fix_id = fix.get("id")
            if isinstance(fix_id, str) and fix_id:
                lines.append(f"  fix {fix.get('safety') or 'manual'}: {fix_id}")
    return lines


__all__ = ["self_service_preview_text"]
