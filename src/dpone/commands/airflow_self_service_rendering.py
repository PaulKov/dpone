from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence

from dpone.commands.airflow_preview_rendering import public_docs_url, self_service_preview_text


def live_preflight_lines(payload: dict[str, object]) -> list[str]:
    report = payload.get("live_preflight_report")
    if not isinstance(report, dict) or report.get("schema") != "dpone.live-preflight.v1":
        return []

    lines = [f"Live preflight: {payload.get('live_preflight') or report.get('runner') or 'unknown'}"]
    connections = _string_list(report.get("resolved_connection_refs")) or _string_list(report.get("connection_refs"))
    probes = _probe_names(report.get("probes"))
    if connections:
        lines.append(f"- connections: {', '.join(connections)}")
    if probes:
        lines.append(f"- planned probes: {', '.join(probes)}")
    lines.append("- network/secrets/source queries: planned, not executed")
    return lines


def _probe_names(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    names = [
        str(probe.get("probe"))
        for probe in value
        if isinstance(probe, dict) and isinstance(probe.get("probe"), str) and probe.get("probe")
    ]
    return sorted(set(names), key=names.index)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    return sorted({item for item in value if isinstance(item, str) and item})


def self_service_init_text(title: str, payload: Mapping[str, object]) -> str:
    """Render beginner init output without dumping diffs or rollback internals."""

    changes = _changes(payload.get("changes"))
    error_lines = _structured_error_lines(payload.get("errors"))
    status = "passed" if payload.get("passed") is True else "failed"
    lines = [
        title,
        f"- status: {status}",
        f"- changes: {_change_summary(changes)}",
    ]
    if title == "dpone init pipeline" and isinstance(payload.get("airflow_enabled"), bool):
        airflow_state = "enabled" if payload["airflow_enabled"] is True else "disabled"
        lines.append(f"- airflow: {airflow_state} ({payload.get('airflow_source') or 'unknown'})")
    if status != "passed":
        conflicts = [change for change in changes if change["action"] == "conflict"]
        if conflicts:
            for change in conflicts:
                lines.append(f"- conflict: {change['path']}")
                lines.append(f"- reason: {change['message'] or 'existing file differs from generated content'}")
            lines.append("- preserved: existing files were left unchanged")
            lines.append(f"- inspect: {_init_json_command(title, payload)}")
            if title == "dpone init project" and any(change["path"] == "dags/dpone.py" for change in conflicts):
                lines.append("- next: merge the guarded loader, then rerun dpone init project --airflow")
        file_paths = [
            change["path"] for change in changes if change.get("path") and change["action"] not in {"conflict", "no_op"}
        ]
        if file_paths and not conflicts:
            lines.append("- files:")
            lines.extend(f"  - {path}" for path in file_paths)
        if error_lines:
            lines.append("- errors:")
            lines.extend(f"  {line.removeprefix('- ')}" for line in error_lines)
    next_command = _next_command(title, payload)
    if next_command:
        lines.append(f"- next: {next_command}")
    if status != "passed" and _has_hidden_contract_details(payload):
        lines.append("- details: rerun with --format json for the full change plan and rollback journal")
    return "\n".join(lines) + "\n"


def self_service_check_text(payload: Mapping[str, object], *, target: str) -> str:
    """Render the default check result as a short beginner success summary."""

    status = "OK" if payload.get("passed") is True else "FAILED"
    lines = [f"dpone check: {status}"]
    deprecated_aliases = payload.get("deprecated_aliases")
    if isinstance(deprecated_aliases, list) and deprecated_aliases:
        lines.append(f"- deprecated input: {', '.join(str(item) for item in deprecated_aliases)}")
    pipeline_id = _pipeline_id_from_target(target)
    if pipeline_id:
        lines.append(f"- pipeline: {pipeline_id}")
    next_command = payload.get("next_command")
    if status == "OK" and isinstance(next_command, str) and next_command:
        lines.append(f"- next: {next_command}")
    elif status == "OK" and pipeline_id:
        lines.append(f"- next: dpone airflow preview {pipeline_id}")
    elif status != "OK":
        lines.extend(_structured_error_lines(payload.get("errors")))
        lines.append("- action: correct the target or initialize it, then rerun dpone check")
    return "\n".join(lines) + "\n"


def self_service_connections_text(payload: Mapping[str, object], *, target: str) -> str:
    status = "OK" if payload.get("passed") is True else "FAILED"
    lines = [
        f"dpone check connections: {status}",
        f"- target: {target}",
        f"- handshake: {payload.get('handshake') or 'unknown'}",
        f"- network: {_yes_no(payload.get('network'))}",
        "- secrets: no",
        f"- source queries: {_yes_no(payload.get('source_queries'))}",
    ]
    refs = _string_list(payload.get("connection_refs"))
    resolved_refs = _string_list(payload.get("resolved_connection_refs"))
    if refs:
        lines.append(f"- connection refs: {', '.join(refs)}")
    if resolved_refs:
        lines.append(f"- resolved refs: {', '.join(resolved_refs)}")
    bridge = payload.get("airflow_connection_bridge")
    if isinstance(bridge, Mapping):
        lines.extend(_airflow_bridge_lines(bridge))
    pipeline_id = _pipeline_id_from_target(target)
    if status == "OK" and pipeline_id:
        lines.append(f"- next: dpone airflow preview {pipeline_id}")
    elif status != "OK":
        lines.extend(_structured_error_lines(payload.get("errors")))
        lines.append("- details: rerun with --format json for the full connection-check report")
    return "\n".join(lines) + "\n"


def self_service_live_check_text(payload: Mapping[str, object], *, target: str) -> str:
    """Render live preflight checks as a bounded, topology-free readiness summary."""

    status = "OK" if payload.get("passed") is True else "FAILED"
    report = _mapping(payload.get("live_preflight_report"))
    runner = str(payload.get("live_preflight") or report.get("runner") or "unknown")
    probes = _probe_names(report.get("probes"))
    lines = [
        f"dpone check live: {status}",
        f"- target: {target}",
        f"- runner: {runner}",
        f"- network: {_live_io_state(payload.get('network'), payload.get('planned_network'))}",
        f"- secrets: {_credential_access_state(runner=runner, probes=probes)}",
        f"- source queries: {_source_query_state(payload.get('source_queries'), payload.get('planned_source_queries'))}",
    ]
    connections = _string_list(report.get("resolved_connection_refs")) or _string_list(
        payload.get("resolved_connection_refs")
    )
    if not connections:
        connections = _string_list(report.get("connection_refs")) or _string_list(payload.get("connection_refs"))
    if connections:
        lines.append(f"- connections: {', '.join(connections)}")
    if probes:
        lines.append(f"- planned probes: {', '.join(probes)}")
    if status == "OK":
        lines.append("- outcome: bounded live preflight configured")
    else:
        lines.extend(_structured_error_lines(payload.get("errors")))
        lines.append("- details: rerun with --format json for the full live-preflight report")
    return "\n".join(lines) + "\n"


def _credential_access_state(*, runner: str, probes: list[str]) -> str:
    if runner in {"configured", "runner_configured"}:
        return "executed" if probes else "not executed"
    return "planned"


def self_service_explain_text(payload: Mapping[str, object], *, pipeline_ref: str) -> str:
    """Render diagnostic Airflow explain output without exposing platform topology."""

    payload_ref = payload.get("pipeline_ref")
    pipeline_id = _pipeline_id_from_target(str(payload_ref)) if isinstance(payload_ref, str) else ""
    if not pipeline_id:
        pipeline_id = _pipeline_id_from_target(pipeline_ref) or pipeline_ref
    artifact_state = _mapping(payload.get("artifact_state"))
    diagnostics = _mapping(payload.get("operator_diagnostics"))
    runtime_delivery = _mapping(diagnostics.get("runtime_artifact_delivery"))
    diagnostic_status = str(diagnostics.get("status") or "")
    status = _explain_status(payload, diagnostic_status=diagnostic_status)
    lines = [
        f"dpone airflow explain: {status}",
        f"- pipeline: {pipeline_id}",
        f"- manifest: {artifact_state.get('manifest') or 'unknown'}",
        f"- dag spec: {artifact_state.get('dag_spec') or 'unknown'}",
        f"- airflow pack: {artifact_state.get('airflow_pack') or 'unknown'}",
        f"- operator diagnostics: {diagnostics.get('status') or 'unknown'}",
        f"- operator pinning: {diagnostics.get('operator_pinning') or 'unknown'}",
    ]
    delivery_mode = runtime_delivery.get("mode")
    if delivery_mode:
        lines.append(f"- runtime delivery: {delivery_mode}")
    if _parse_side_effects_none(diagnostics.get("parse_side_effects")):
        lines.append("- parse side effects: none")
    next_action = _first_next_action(diagnostics.get("next_actions"))
    if status == "FAILED":
        lines.extend(_structured_error_lines(payload.get("errors")))
        lines.append(
            f"- action: {next_action}"
            if next_action
            else "- action: correct or initialize the pipeline source, then run dpone check"
        )
    elif next_action:
        lines.append(f"- action: {next_action}")
    elif _explain_needs_preview(artifact_state, diagnostic_status=diagnostic_status):
        lines.append(f"- next: dpone airflow preview {pipeline_id}")
    elif status == "OK":
        lines.append(f"- next: dpone test {pipeline_id}")
    lines.append("- details: rerun with --format json for fingerprints and full operator checks")
    return "\n".join(lines) + "\n"


def _changes(value: object) -> list[dict[str, str]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    changes: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        action = item.get("action")
        path = item.get("path")
        changes.append(
            {
                "action": str(action) if action else "unknown",
                "path": str(path) if path else "",
                "message": str(item.get("message") or ""),
            }
        )
    return changes


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _change_summary(changes: list[dict[str, str]]) -> str:
    if not changes:
        return "0"
    counts = Counter(change["action"] for change in changes)
    return ", ".join(f"{count} {action}" for action, count in sorted(counts.items()))


def _next_command(title: str, payload: Mapping[str, object]) -> str:
    if payload.get("passed") is not True:
        return ""
    if title == "dpone init project":
        if payload.get("layout_mode") == "domain_first":
            return "dpone init domain sales --owner-team <team> --owner-contact <contact> --approver-team <github-team>"
        return "dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental"
    if title == "dpone init domain":
        domain = payload.get("domain") or "<domain>"
        return (
            f"dpone init pipeline orders_daily --domain {domain} "
            "--route mssql:clickhouse:incremental_merge "
            "--from <source-ref>:<schema>.<table> "
            "--to <sink-ref>:<schema>.<table> --key <column>"
        )
    if title == "dpone init pipeline":
        pipeline_path = str(payload.get("pipeline_path") or "").strip()
        if pipeline_path.endswith("/pipeline.yaml"):
            return f"dpone check {_pipeline_id_from_target(pipeline_path)}"
        if pipeline_path:
            return f"dpone check {pipeline_path}"
    if title == "dpone workload init":
        next_commands = payload.get("next_commands")
        if isinstance(next_commands, list) and next_commands:
            first = next_commands[0]
            if isinstance(first, str) and first:
                return first if payload.get("mode") == "apply" else f"{first}  # after --apply"
    return ""


def _init_json_command(title: str, payload: Mapping[str, object]) -> str:
    rerun = payload.get("rerun_command")
    if isinstance(rerun, str) and rerun:
        return f"{rerun} --format json"
    return {
        "dpone init project": "dpone init project --airflow --format json",
        "dpone init pipeline": "dpone init pipeline <pipeline_id> --format json",
        "dpone init domain": (
            "dpone init domain <domain> --owner-team <team> "
            "--owner-contact <contact> --approver-team <github-team> --format json"
        ),
    }.get(title, "rerun the init command with --format json")


def _has_hidden_contract_details(payload: Mapping[str, object]) -> bool:
    changes = payload.get("changes")
    if isinstance(changes, Sequence) and not isinstance(changes, (str, bytes)):
        for change in changes:
            if isinstance(change, Mapping) and "diff" in change:
                return True
    return "rollback_journal" in payload


def _yes_no(value: object) -> str:
    return "yes" if value is True else "no" if value is False else str(value)


def _live_io_state(executed: object, planned: object) -> str:
    if executed is True:
        return "executed"
    if planned is True:
        return "planned"
    if executed is False:
        return "no"
    return str(executed or planned or "unknown")


def _source_query_state(executed: object, planned: object) -> str:
    if executed is True:
        return "executed"
    if isinstance(planned, str) and planned:
        return f"planned {planned.replace('_', ' ')}"
    if planned is True:
        return "planned"
    if executed is False:
        return "no"
    return str(executed or planned or "unknown")


def _pipeline_id_from_target(target: str) -> str:
    normalized = target.strip().removesuffix("/pipeline.yaml").rstrip("/")
    if normalized in {"", ".", "./"}:
        return ""
    return normalized.rsplit("/", 1)[-1]


def _parse_side_effects_none(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    return bool(value) and all(item is False for item in value.values())


def _first_next_action(value: object) -> str:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ""
    for item in value:
        if isinstance(item, str) and item:
            return item
        if not isinstance(item, Mapping):
            continue
        action = item.get("action")
        if isinstance(action, str) and action:
            return action
    return ""


def _airflow_bridge_lines(bridge: Mapping[str, object]) -> list[str]:
    if bridge.get("required") is not True:
        return ["- bridge: not required"]
    execution_mode = bridge.get("execution_mode") or "unknown"
    lines = [f"- bridge: required ({execution_mode})"]
    connection_refs = _bridge_connection_refs(bridge.get("connections"))
    if connection_refs:
        lines.append(f"- bridge connections: {', '.join(connection_refs)}")
    next_action = _first_next_action(bridge.get("next_actions"))
    if next_action:
        lines.append(f"- action: {next_action}")
    return lines


def _bridge_connection_refs(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    refs: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        connection_ref = item.get("connection_ref")
        if isinstance(connection_ref, str) and connection_ref:
            refs.append(connection_ref)
    return sorted(set(refs))


def _structured_error_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("code") or "DPONE_ERROR")
        message = str(item.get("message") or "").strip()
        lines.append(f"- error: {code}: {message}" if message else f"- error: {code}")
        lines.extend(_safe_error_detail_lines(item))
        docs_url = item.get("docs_url")
        if isinstance(docs_url, str) and docs_url:
            lines.append(f"  docs: {public_docs_url(docs_url)}")
        lines.extend(_fix_lines(item.get("fixes")))
    return lines


def _safe_error_detail_lines(error: Mapping[str, object]) -> list[str]:
    lines: list[str] = []
    resolver = error.get("resolver")
    if isinstance(resolver, str) and resolver:
        lines.append(f"  resolver: {resolver}")
    supported_resolvers = _string_list(error.get("supported_resolvers"))
    if supported_resolvers:
        lines.append(f"  supported resolvers: {', '.join(supported_resolvers)}")
    return lines


def _fix_lines(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    lines: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        safety = str(item.get("safety") or "manual")
        command = item.get("command")
        if isinstance(command, str) and command:
            lines.append(f"  fix {safety}: {command}")
            continue
        fix_id = item.get("id")
        if isinstance(fix_id, str) and fix_id:
            lines.append(f"  fix {safety}: {fix_id}")
    return lines


def _explain_status(payload: Mapping[str, object], *, diagnostic_status: str) -> str:
    if payload.get("passed") is not True:
        return "FAILED"
    artifact_state = payload.get("artifact_state")
    has_stale_artifact = isinstance(artifact_state, Mapping) and any(
        artifact_state.get(name) == "stale" for name in ("dag_spec", "airflow_pack")
    )
    if diagnostic_status in {"invalid", "operator_issues"} or has_stale_artifact:
        return "NEEDS_ATTENTION"
    return "OK"


def _explain_needs_preview(artifact_state: Mapping[str, object], *, diagnostic_status: str) -> bool:
    return artifact_state.get("dag_spec") != "materialized" or diagnostic_status == "planned"


__all__ = [
    "live_preflight_lines",
    "self_service_check_text",
    "self_service_connections_text",
    "self_service_explain_text",
    "self_service_init_text",
    "self_service_live_check_text",
    "self_service_preview_text",
]
