from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.airflow_runtime_models import GitOpsAirflowRuntimeEvidence, GitOpsAirflowRuntimeStep


import json
import math
from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_correlation import AirflowCorrelationError
from dpone.security_redaction import public_path_label, redact_absolute_paths, redact_text

INLINE_SCHEMA_VERSION = "dpone.airflow.inline_runtime_evidence.v1"
INLINE_SECTION_KEYS = (
    "acceptance",
    "cleanup",
    "data_quality",
    "lineage",
    "load_steps",
    "metrics",
    "quality",
    "quality_gates",
    "quality_report",
    "resource_usage",
    "route_capabilities",
    "runtime_decisions",
    "step_timeline",
    "throughput",
)
INLINE_SECTION_CONTAINER_KEYS = ("result", "details", "runtime_evidence", "reconciliation_metrics")
SECRET_KEY_FRAGMENTS = (
    "access_key",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "session_token",
    "token",
)


def build_inline_runtime_evidence(
    evidence: GitOpsAirflowRuntimeEvidence,
    *,
    inline_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sections = _runtime_output_sections(evidence.steps, inline_payload=inline_payload)
    step_timeline = [_step_timeline_item(step) for step in evidence.steps]
    if not step_timeline:
        step_timeline = _load_step_timeline(sections.get("load_steps"))
    payload: dict[str, Any] = {
        "schema_version": INLINE_SCHEMA_VERSION,
        "status": evidence.status,
        "metrics": {
            "duration_seconds": evidence.duration_seconds,
            "step_count": len(evidence.steps),
        },
        "step_timeline": step_timeline,
    }
    dpone_run = _dpone_run_identity(evidence.steps, inline_payload=inline_payload)
    if dpone_run:
        payload["dpone_run"] = dpone_run
    payload.update({key: value for key, value in sections.items() if key != "step_timeline"})
    return _bounded(payload)


def _dpone_run_identity(
    steps: tuple[GitOpsAirflowRuntimeStep, ...],
    *,
    inline_payload: Mapping[str, Any] | None,
) -> dict[str, str]:
    candidates: list[tuple[str, str]] = []
    if inline_payload is not None:
        _append_dpone_run_candidate(candidates, inline_payload)
    for step in steps:
        for output in _json_objects(step.stdout):
            _append_dpone_run_candidate(candidates, output)
    unique = set(candidates)
    if len(unique) > 1:
        raise AirflowCorrelationError(
            "DPONE_AIRFLOW_CORRELATION_MISMATCH",
            "runtime output contains conflicting dpone run identities",
        )
    if not unique:
        return {}
    run_id, process = unique.pop()
    return {key: value for key, value in (("run_id", run_id), ("process", process)) if value}


def _append_dpone_run_candidate(candidates: list[tuple[str, str]], output: Mapping[str, Any]) -> None:
    run_id = _safe_identity_text(output.get("run_id"))
    process = _safe_identity_text(output.get("process"), maximum=253)
    if run_id or process:
        candidates.append((run_id, process))


def _safe_identity_text(value: object, *, maximum: int = 1024) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if len(text) > maximum or any(char in text for char in ("\x00", "\n", "\r")):
        raise AirflowCorrelationError(
            "DPONE_AIRFLOW_CORRELATION_INVALID",
            "runtime output identity is outside the public text bounds",
        )
    return text


def _step_timeline_item(step: GitOpsAirflowRuntimeStep) -> dict[str, Any]:
    return {
        "name": step.name,
        "kind": step.kind,
        "status": step.status,
        "required": step.required,
        "exit_code": step.exit_code,
        "duration_seconds": step.duration_seconds,
        "manifest": public_path_label(step.manifest, fallback="manifest") if step.manifest is not None else None,
    }


def _runtime_output_sections(
    steps: tuple[GitOpsAirflowRuntimeStep, ...],
    *,
    inline_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    sections: dict[str, list[dict[str, Any]]] = {}
    if inline_payload is not None:
        _append_output_sections(sections, step_name="runtime", output=inline_payload)
    for step in steps:
        for output in _json_objects(step.stdout):
            _append_output_sections(sections, step_name=step.name, output=output)
    return {key: values[0]["value"] if len(values) == 1 else values for key, values in sections.items()}


def _append_output_sections(
    sections: dict[str, list[dict[str, Any]]],
    *,
    step_name: str,
    output: Mapping[str, Any],
) -> None:
    for source in _section_sources(output):
        _append_sections(sections, step_name=step_name, output=source)


def _section_sources(output: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    sources: list[Mapping[str, Any]] = []
    queue: list[Mapping[str, Any]] = [output]
    seen: set[int] = set()
    while queue:
        current = queue.pop(0)
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        sources.append(current)
        for key in INLINE_SECTION_CONTAINER_KEYS:
            child = current.get(key)
            if isinstance(child, Mapping):
                queue.append(child)
    return tuple(sources)


def _append_sections(sections: dict[str, list[dict[str, Any]]], *, step_name: str, output: Mapping[str, Any]) -> None:
    for key in INLINE_SECTION_KEYS:
        value = output.get(key)
        if isinstance(value, (Mapping, list)):
            sections.setdefault(key, []).append({"step": step_name, "value": _redact(value)})
    run_throughput = output.get("run_throughput")
    if isinstance(run_throughput, Mapping) and "throughput" not in output:
        sections.setdefault("throughput", []).append({"step": step_name, "value": _redact(run_throughput)})


def _load_step_timeline(load_steps: object) -> list[dict[str, Any]]:
    if not isinstance(load_steps, list):
        return []
    timeline: list[dict[str, Any]] = []
    for item in load_steps:
        if not isinstance(item, Mapping):
            continue
        details = _step_details(item.get("details_json"))
        raw_throughput = details.get("throughput")
        throughput = dict(raw_throughput) if isinstance(raw_throughput, Mapping) else {}
        entry = {
            "name": str(item.get("step_id") or item.get("name") or ""),
            "kind": str(item.get("kind") or item.get("phase") or ""),
            "phase": str(item.get("phase") or ""),
            "status": str(item.get("status") or ""),
            "duration_seconds": _number(item.get("duration_seconds")),
            "rows": _integer(item.get("rows") or item.get("row_count")),
        }
        rows_per_second = _number(throughput.get("rows_per_second"))
        if rows_per_second is not None:
            entry["rows_per_second"] = rows_per_second
        timeline.append({key: value for key, value in entry.items() if value not in {"", None}})
    return timeline


def _step_details(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _number(value: object) -> float | None:
    if isinstance(value, int | float):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return None
        return number if math.isfinite(number) else None
    return None


def _integer(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _json_objects(text: str) -> tuple[Mapping[str, Any], ...]:
    decoder = json.JSONDecoder()
    objects: list[Mapping[str, Any]] = []
    cursor = 0
    while True:
        start = text.find("{", cursor)
        if start < 0:
            break
        try:
            value, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            cursor = start + 1
            continue
        if isinstance(value, Mapping):
            objects.append(value)
        cursor = start + end
    return tuple(objects)


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(fragment in key_text.lower() for fragment in SECRET_KEY_FRAGMENTS):
                result[key_text] = "***REDACTED***"
            else:
                result[key_text] = _redact(item)
        return result
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return redact_absolute_paths(redact_text(value))
    return value


def _bounded(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        return {"truncated": True, "reason": "max_depth"}
    if isinstance(value, Mapping):
        return {
            str(key): _bounded(item, depth=depth + 1) for index, (key, item) in enumerate(value.items()) if index < 100
        }
    if isinstance(value, list):
        bounded = [_bounded(item, depth=depth + 1) for item in value[:100]]
        if len(value) > 100:
            bounded.append({"truncated": True, "remaining_items": len(value) - 100})
        return bounded
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str) and len(value) > 4096:
        return {"truncated": True, "chars": len(value), "preview": value[:4096]}
    return value


__all__ = ["INLINE_SCHEMA_VERSION", "build_inline_runtime_evidence"]
