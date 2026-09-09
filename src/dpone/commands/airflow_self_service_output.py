"""Single output adapter for Airflow self-service command results."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_self_service_models import SelfServiceResult


import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.commands.airflow_artifact_delivery_rendering import (
    airflow_artifact_publish_text,
    airflow_cache_materialize_text,
)
from dpone.commands.airflow_authoring_fix_rendering import self_service_authoring_fix_text
from dpone.commands.airflow_build_rendering import self_service_airflow_build_text
from dpone.commands.airflow_cache_rendering import self_service_cache_text
from dpone.commands.airflow_connection_secret_gc_rendering import (
    connection_gc_apply_text,
    connection_gc_plan_text,
)
from dpone.commands.airflow_deployment_attestation_rendering import (
    airflow_attestation_prepare_text,
    airflow_attestation_publish_text,
    airflow_trust_policy_render_text,
)
from dpone.commands.airflow_self_service_rendering import (
    live_preflight_lines,
    public_docs_url,
    self_service_check_text,
    self_service_connections_text,
    self_service_explain_text,
    self_service_live_check_text,
    self_service_preview_text,
)
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.security_redaction import redact_public_value

_INTERNAL_CHECK_CODE = "DPONE_INTERNAL_CHECK_FAILED"
_INTERNAL_CHECK_MESSAGE = "Check failed unexpectedly. Use the trace id with platform support."
_TRACE_ID_RE = re.compile(r"[0-9a-f]{32}")


def emit_authoring_migration_payload(payload: Mapping[str, object], fmt: str) -> None:
    """Emit a public authoring migration plan/receipt without topology details."""

    public_payload = _public_mapping(payload)
    if fmt == "json":
        write_json(public_payload)
        return
    write_text(_authoring_migration_text(public_payload))


def _authoring_migration_text(payload: Mapping[str, object]) -> str:
    raw_source = payload.get("source")
    raw_target = payload.get("target")
    source = raw_source if isinstance(raw_source, dict) else {}
    target = raw_target if isinstance(raw_target, dict) else {}
    status = str(payload.get("status") or "blocked").upper().replace("_", " ")
    lines = [
        f"dpone migrate authoring: {status}",
        f"- mode: {source.get('mode', 'unknown')} -> {target.get('mode', 'unknown')}",
        f"- semantic fingerprint: {target.get('semantic_fingerprint', 'unknown')}",
        f"- plan id: {payload.get('plan_id', 'unknown')}",
    ]
    changes = payload.get("changes")
    if isinstance(changes, list) and changes:
        lines.append("- changes:")
        for change in changes:
            if isinstance(change, dict):
                lines.append(f"  {change.get('action', 'change')}: {change.get('path', 'unknown')}")
    retained = payload.get("retained_files")
    if isinstance(retained, list) and retained:
        lines.append("- retained files:")
        lines.extend(f"  {path}" for path in retained if isinstance(path, str))
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        lines.extend(f"- warning: {warning}" for warning in warnings if isinstance(warning, str))
    errors = payload.get("errors")
    if isinstance(errors, list):
        for error in errors:
            if isinstance(error, dict):
                lines.append(f"- error {error.get('code', 'unknown')}: {error.get('message', '')}")
    if isinstance(changes, list):
        diffs = [
            str(change["unified_diff"]).rstrip()
            for change in changes
            if isinstance(change, dict) and isinstance(change.get("unified_diff"), str) and change["unified_diff"]
        ]
        if diffs:
            lines.extend(("", *diffs))
    return "\n".join(lines) + "\n"


def emit_self_service_result(
    result: SelfServiceResult,
    fmt: str,
    *,
    command: str = "",
    target: str = "",
) -> None:
    """Render one service result through the command-specific stable contract."""

    payload = _public_mapping(result.to_dict())
    public_target = (
        _public_cache_root(target)
        if command in {"airflow_cache_recovery_plan", "airflow_cache_retention_plan"}
        else _public_target(target)
    )
    if fmt == "json":
        write_json(payload)
        return
    internal_failure_text = _internal_check_failure_text(payload) if command == "check" else None
    if internal_failure_text is not None:
        write_text(internal_failure_text)
        return
    if command == "airflow_connection_secret_gc_plan" and payload.get("schema"):
        write_text(connection_gc_plan_text(payload))
        return
    if command == "airflow_connection_secret_gc_apply" and payload.get("schema"):
        write_text(connection_gc_apply_text(payload))
        return
    if command == "airflow_artifact_publish" and payload.get("schema"):
        write_text(airflow_artifact_publish_text(payload))
        return
    if command == "airflow_artifact_attestation_prepare" and payload.get("schema"):
        write_text(airflow_attestation_prepare_text(payload))
        return
    if command == "airflow_artifact_attestation_publish" and payload.get("schema"):
        write_text(airflow_attestation_publish_text(payload))
        return
    if command == "airflow_deployment_trust_policy_render" and payload.get("schema"):
        write_text(airflow_trust_policy_render_text(payload))
        return
    if command == "airflow_cache_materialize" and payload.get("schema"):
        write_text(airflow_cache_materialize_text(payload))
        return
    if command == "check" and payload.get("mode") == "connections":
        lines = self_service_connections_text(payload, target=public_target).rstrip("\n").splitlines()
        lines.extend(_connection_registry_migration_plan_lines(payload))
        write_text(_with_trace_id("\n".join(lines) + "\n", payload))
        return
    if command == "check" and payload.get("mode") == "live":
        write_text(_with_trace_id(self_service_live_check_text(payload, target=public_target), payload))
        return
    if command == "check":
        write_text(_with_trace_id(self_service_check_text(payload, target=public_target), payload))
        return
    if command == "airflow_preview":
        write_text(self_service_preview_text(payload, pipeline_ref=public_target))
        return
    if command == "airflow_explain":
        write_text(self_service_explain_text(payload, pipeline_ref=public_target))
        return
    build_text = self_service_airflow_build_text(payload) if command == "airflow_build" else None
    if build_text is not None:
        write_text(build_text)
        return
    cache_text = self_service_cache_text(payload, command=command, target=public_target)
    if cache_text is not None:
        write_text(cache_text)
        return
    fix_text = self_service_authoring_fix_text(payload, target=public_target) if command == "fix" else None
    if fix_text is not None:
        write_text(fix_text)
        return
    status = "OK" if payload.get("passed") is True else "FAILED"
    lines = [f"dpone self-service: {status}"]
    for change in payload.get("changes", ()):
        lines.append(f"- {change['action']}: {change['path']}")
    for error in payload.get("errors", ()):
        suffix = f" ({error['path']})" if error.get("path") else ""
        lines.append(f"- {error['code']}: {error['message']}{suffix}")
        docs_url = error.get("docs_url")
        if isinstance(docs_url, str) and docs_url:
            lines.append(f"  docs: {public_docs_url(docs_url)}")
        lines.extend(_fix_lines(error))
    lines.extend(live_preflight_lines(payload))
    lines.extend(_connection_registry_migration_plan_lines(payload))
    if "airflow_index_path" in payload:
        lines.append(f"Airflow index: {payload['airflow_index_path']}")
    write_text("\n".join(lines) + "\n")


def emit_internal_check_failure(fmt: str, *, trace_id: str) -> None:
    """Emit a known-safe check error when the recursive output adapter fails."""

    safe_trace_id = trace_id if _TRACE_ID_RE.fullmatch(trace_id) else "unavailable"
    payload = {
        "passed": False,
        "changes": [],
        "errors": [
            {
                "schema": "dpone.error.v1",
                "code": _INTERNAL_CHECK_CODE,
                "stage": "check",
                "severity": "error",
                "message": _INTERNAL_CHECK_MESSAGE,
                "fixes": [],
                "trace_id": safe_trace_id,
            }
        ],
        "exit_code": 5,
    }
    if fmt == "json":
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        return
    sys.stdout.write(_internal_check_failure_text(payload) or "dpone check: FAILED\n")


def _public_target(target: str) -> str:
    if not target:
        return ""
    path = Path(target)
    if not path.is_absolute():
        return str(redact_public_value(path.as_posix()))
    try:
        public_target = path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        public_target = path.name or "pipeline.yaml"
    return str(redact_public_value(public_target))


def _public_cache_root(target: str) -> str:
    if not target:
        return ""
    path = Path(target)
    if not path.is_absolute():
        return str(redact_public_value(path.as_posix()))
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return "$DPONE_CACHE_ROOT"


def _public_mapping(payload: Mapping[str, object]) -> dict[str, Any]:
    public_payload = redact_public_value(dict(payload))
    if not isinstance(public_payload, dict):
        raise TypeError("public redactor must return a mapping")
    return public_payload


def _internal_check_failure_text(payload: Mapping[str, object]) -> str | None:
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return None
    for error in errors:
        if not isinstance(error, Mapping) or error.get("code") != _INTERNAL_CHECK_CODE:
            continue
        message = str(error.get("message") or _INTERNAL_CHECK_MESSAGE)
        trace_id = str(error.get("trace_id") or "unavailable")
        return f"dpone check: FAILED\n- error: {_INTERNAL_CHECK_CODE}: {message}\n- trace id: {trace_id}\n"
    return None


def _with_trace_id(text: str, payload: Mapping[str, object]) -> str:
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return text
    for error in errors:
        if not isinstance(error, Mapping):
            continue
        trace_id = error.get("trace_id")
        if isinstance(trace_id, str) and trace_id:
            return f"{text.rstrip()}\n- trace id: {trace_id}\n"
    return text


def _fix_lines(error: object) -> list[str]:
    if not isinstance(error, dict):
        return []
    fixes = error.get("fixes")
    if not isinstance(fixes, list):
        return []
    lines: list[str] = []
    for fix in fixes:
        if not isinstance(fix, dict):
            continue
        command = fix.get("command")
        safety = str(fix.get("safety") or "manual")
        if isinstance(command, str) and command:
            lines.append(f"  fix {safety}: {command}")
            continue
        fix_id = fix.get("id")
        if isinstance(fix_id, str) and fix_id:
            lines.append(f"  fix {safety}: {fix_id}")
    return lines


def _connection_registry_migration_plan_lines(payload: dict[str, object]) -> list[str]:
    plan = payload.get("connection_registry_migration_plan")
    if not isinstance(plan, dict) or plan.get("kind") != "dpone.connection-registry-migration-plan.v1":
        return []
    registry_path = plan.get("registry_path")
    lines = (
        [f"Registry migration plan: {registry_path}"]
        if isinstance(registry_path, str)
        else ["Registry migration plan:"]
    )
    changes = plan.get("changes")
    if not isinstance(changes, list):
        return lines
    for change in changes:
        if not isinstance(change, dict):
            continue
        action = str(change.get("action") or "change")
        connection_ref = str(change.get("connection_ref") or change.get("path") or "<unknown>")
        lines.append(f"- {action}: {connection_ref}")
        diff = change.get("unified_diff")
        if isinstance(diff, str) and diff.strip():
            lines.extend(f"  {line}" if line else "" for line in diff.rstrip("\n").splitlines())
    return lines


__all__ = [
    "emit_authoring_migration_payload",
    "emit_internal_check_failure",
    "emit_self_service_result",
]
