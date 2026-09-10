"""Build Airflow KPO ``return.json`` from captured runtime evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_run_identity import AIRFLOW_DEPLOYMENT_IDENTITY_ENV
from dpone.contracts.dbt_publishing import canonical_dbt_execution_evidence_bytes
from dpone.contracts.stable_error_codes import (
    is_stable_error_code,
    stable_error_code_from_text,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.gitops.airflow_runtime_models import read_airflow_runtime_evidence
from dpone.gitops.airflow_runtime_profile_models import GitOpsAirflowXComSummary
from dpone.gitops.airflow_xcom_outcome import (
    AIRFLOW_RUN_IDENTITY_ENV,
    GitOpsAirflowXComOutcomeBuilder,
    parse_optional_airflow_deployment_identity,
    parse_optional_airflow_run_identity,
)
from dpone.gitops.models import GitOpsIssue
from dpone.security_redaction import public_path_label, redact_absolute_paths


def write_airflow_xcom_from_evidence(
    *,
    evidence_path: Path | str,
    xcom_output: Path | str,
    runtime_evidence_path: str | None = None,
    stderr_path: str | None = None,
    status: str | None = None,
) -> Path:
    """Write ``gitops.airflow_xcom_summary`` for the KPO xcom sidecar.

    Always produces a JSON object so Airflow outcome evaluation never sees an
    empty ``/airflow/xcom/return.json`` after a verified runtime child exits.
    """

    evidence = Path(evidence_path)
    xcom_path = Path(xcom_output)
    runtime_label = public_path_label(
        runtime_evidence_path or str(evidence_path),
        fallback="runtime-evidence.json",
    )
    status_hint = _status_hint(status)
    digest: str | None
    try:
        raw_text = evidence.read_text(encoding="utf-8")
        raw_payload, dbt_evidence_ref = _runtime_evidence_payload(
            raw_text,
            status_hint=status_hint,
        )
        parsed = read_airflow_runtime_evidence(raw_payload)
        digest = "sha256:" + hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        summary = GitOpsAirflowXComOutcomeBuilder().build(
            evidence=parsed,
            runtime_evidence_path=runtime_label,
            runtime_evidence_sha256=digest,
            inline_payload=raw_payload,
            run_identity=parse_optional_airflow_run_identity(os.environ.get(AIRFLOW_RUN_IDENTITY_ENV)),
            deployment_identity=parse_optional_airflow_deployment_identity(
                os.environ.get(AIRFLOW_DEPLOYMENT_IDENTITY_ENV)
            ),
            dbt_execution_evidence_ref=dbt_evidence_ref,
        )
    except Exception as exc:  # noqa: BLE001 - KPO still needs a machine-readable XCom blocker.
        digest = _evidence_digest(evidence)
        stable_code = getattr(exc, "code", None)
        identity_error = isinstance(stable_code, str) and stable_code.startswith("DPONE_AIRFLOW_")
        issue_code = stable_code if isinstance(stable_code, str) else "runtime_evidence_xcom_build_unavailable"
        issue = GitOpsIssue(
            code=issue_code,
            message=_fallback_message(exc, stderr_path=stderr_path),
            path=public_path_label(evidence, fallback="runtime-evidence.json"),
            source="dpone gitops airflow xcom-from-evidence",
        )
        summary = _fallback_summary(
            runtime_evidence_path=runtime_label,
            runtime_evidence_sha256=digest,
            status="failed" if identity_error else (status_hint or "failed"),
            issue=issue,
        )
    xcom_path.parent.mkdir(parents=True, exist_ok=True)
    xcom_path.write_text(summary.to_json(), encoding="utf-8")
    return xcom_path


def _runtime_evidence_payload(
    raw_text: str,
    *,
    status_hint: str,
) -> tuple[dict[str, Any], dict[str, object] | None]:
    try:
        # Duplicate keys stay fail-closed; non-finite metrics are allowed so the
        # inline evidence sanitizer can rewrite NaN/Infinity to null.
        payload = strict_json_object(raw_text, allow_nonfinite=True)
    except StrictJsonError:
        payloads = _json_objects(raw_text)
        if not payloads:
            raise
        payload = payloads[-1]
    if payload.get("kind") == "gitops.airflow_runtime_evidence":
        if _is_failure_signal(status_hint):
            payload = {
                **payload,
                "status": "failed",
                "blockers": _blockers_from_runtime_output(payload, status="failed"),
            }
        return payload, None
    return (
        _runtime_output_payload(payload, status_hint=status_hint),
        _dbt_execution_evidence_ref(payload),
    )


def _dbt_execution_evidence_ref(
    payload: dict[str, Any],
) -> dict[str, object] | None:
    if payload.get("schema") != "dpone.dbt-execution-evidence.v1":
        return None
    workflow_id = payload.get("workflow_id")
    if not isinstance(workflow_id, str) or not workflow_id:
        return None
    try:
        exact = canonical_dbt_execution_evidence_bytes(payload)
    except (TypeError, ValueError):
        return None
    return {
        "schema": "dpone.dbt-execution-evidence-ref.v1",
        "workflow_id": workflow_id,
        "sha256": "sha256:" + hashlib.sha256(exact).hexdigest(),
        "bytes": len(exact),
        "storage_scope": "dbt_spool",
    }


def _runtime_output_payload(payload: dict[str, Any], *, status_hint: str) -> dict[str, Any]:
    result = dict(payload)
    result["kind"] = "gitops.airflow_runtime_evidence"
    result.setdefault("schema_version", "1")
    result.setdefault("producer", "dpone gitops airflow xcom-from-evidence")
    result.setdefault("run_spec_path", "")
    result.setdefault("bundle_path", "")
    result.setdefault("image", "")
    result.setdefault("image_digest", None)
    nested_result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    nested_status = nested_result.get("status") if isinstance(nested_result, dict) else None
    result["status"] = _resolved_status(
        payload.get("status"),
        nested_status,
        status_hint=status_hint,
        nested_result=nested_result if isinstance(nested_result, dict) else {},
    )
    result.setdefault("started_at", "")
    result.setdefault("finished_at", "")
    result.setdefault("duration_seconds", _duration_seconds(payload))
    result.setdefault("steps", [])
    result["warnings"] = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    result["blockers"] = _blockers_from_runtime_output(payload, status=result["status"])
    return result


def _blockers_from_runtime_output(payload: dict[str, Any], *, status: str) -> list[dict[str, str]]:
    """Project dpone-run ``errors``/``error_code`` into Airflow XCom blockers.

    Compact and init_fetch pack-exec both write ``dpone run --format json``
    evidence. That payload uses ``result.errors`` (strings) rather than
    ``blockers`` (GitOpsIssue objects). Dropping them left AF3 with
    ``status=failed`` and an empty blocker list — opaque INLINE_OUTCOME_FAILED.
    """

    blockers: list[dict[str, str]] = []
    seen: set[str] = set()

    def _add(*, code: str, message: str) -> None:
        clean_message = redact_absolute_paths(_redact(message.strip()))
        if not clean_message:
            return
        clean_code = (code or "runtime_execution_failed").strip() or "runtime_execution_failed"
        key = f"{clean_code}\0{clean_message}"
        if key in seen:
            return
        seen.add(key)
        blockers.append(
            {
                "code": clean_code,
                "message": clean_message,
                "path": "runtime-evidence.json",
                "source": "dpone gitops airflow xcom-from-evidence",
            }
        )

    raw_blockers = payload.get("blockers")
    if isinstance(raw_blockers, list):
        for item in raw_blockers:
            if isinstance(item, dict):
                _add(
                    code=str(item.get("code") or "runtime_execution_failed"),
                    message=str(item.get("message") or item.get("code") or ""),
                )
            elif isinstance(item, str):
                _add(code=_error_code_from_text(item) or "runtime_execution_failed", message=item)

    nested = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    for source in (payload, nested):
        if not isinstance(source, dict):
            continue
        error_code = source.get("error_code")
        errors = source.get("errors")
        if isinstance(errors, list):
            for item in errors:
                if isinstance(item, str):
                    _add(
                        code=(
                            str(error_code)
                            if is_stable_error_code(error_code)
                            else (_error_code_from_text(item) or "runtime_execution_failed")
                        ),
                        message=item,
                    )
                elif isinstance(item, dict):
                    message = str(item.get("message") or item.get("error") or item.get("code") or "")
                    code = str(
                        item.get("code")
                        or item.get("error_code")
                        or (error_code if isinstance(error_code, str) else "")
                        or _error_code_from_text(message)
                        or "runtime_execution_failed"
                    )
                    _add(code=code, message=message or code)
        elif is_stable_error_code(error_code):
            _add(code=error_code, message=error_code)

    if status == "failed" and not blockers:
        _add(code="runtime_execution_failed", message="dpone run failed without structured errors")
    return blockers


def _error_code_from_text(text: str) -> str | None:
    return stable_error_code_from_text(text)


def _fallback_summary(
    *,
    runtime_evidence_path: str,
    runtime_evidence_sha256: str | None,
    status: str,
    issue: GitOpsIssue,
) -> GitOpsAirflowXComSummary:
    inline = {
        "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
        "status": status,
        "metrics": {"duration_seconds": 0.0, "step_count": 0},
        "step_timeline": [],
        "warnings": [issue.to_jsonable()],
    }
    return GitOpsAirflowXComSummary(
        runtime_profile_path="",
        run_spec_path="",
        runtime_evidence_path=runtime_evidence_path,
        runtime_evidence_sha256=runtime_evidence_sha256,
        runtime_evidence=inline,
        status=status,
        artifact_paths={"runtime_evidence": runtime_evidence_path},
        warnings=(issue,) if status != "failed" else (),
        blockers=(issue,) if status == "failed" else (),
        producer="dpone gitops airflow xcom-from-evidence",
        step_counts={"total": 0, "passed": 0, "failed": 0},
    )


def _status_hint(value: str | None) -> str:
    # Unset CLI ``--status`` must not inject a synthetic failure over evidence.
    if value is None or not str(value).strip():
        return ""
    return _normal_status(value, fallback="failed")


def _resolved_status(
    *candidates: object,
    status_hint: str,
    nested_result: dict[str, Any],
) -> str:
    """Prefer any failure/COMMIT_UNKNOWN signal over a last-wins passed value."""

    values = (*candidates, status_hint, nested_result.get("error_code"))
    if any(_is_failure_signal(value) for value in values):
        return "failed"
    for value in values:
        normalized = _normal_status(value, fallback="")
        if normalized == "passed":
            return "passed"
    return _normal_status(status_hint, fallback="failed")


def _is_failure_signal(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"failed", "error", "blocked", "commit_unknown"}:
        return True
    return text == "COMMIT_UNKNOWN" or text.startswith("COMMIT_UNKNOWN")


def _normal_status(value: object, *, fallback: str) -> str:
    status = str(value or "").strip()
    lowered = status.lower()
    if lowered in {"passed", "success", "ok", "succeeded"}:
        return "passed"
    if lowered in {"failed", "error", "blocked", "commit_unknown"} or status == "COMMIT_UNKNOWN":
        return "failed"
    return fallback


def _duration_seconds(payload: dict[str, Any]) -> float:
    value = payload.get("duration_seconds")
    if value is None and isinstance(payload.get("metrics"), dict):
        value = payload["metrics"].get("duration_seconds")
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _evidence_digest(path: Path) -> str | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _fallback_message(exc: Exception, *, stderr_path: str | None) -> str:
    message = f"Runtime evidence could not be converted to Airflow XCom summary: {exc.__class__.__name__}"
    tail = _stderr_tail(stderr_path)
    if tail:
        return f"{message}; stderr_tail={tail}"
    return message


def _stderr_tail(stderr_path: str | None) -> str:
    if not stderr_path:
        return ""
    try:
        text = Path(stderr_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return redact_absolute_paths(_redact(" ".join(text.split())[-500:]))


_SECRET_KEY = r"password|passwd|pwd|token|secret|authorization|access[_-]?key|secret[_-]?key"
_QUOTED_SECRET_RE = re.compile(rf"(?i)([\"']?(?:{_SECRET_KEY})[\"']?\s*:\s*[\"'])(.*?)([\"'])")
_BARE_SECRET_RE = re.compile(rf"(?i)((?:{_SECRET_KEY})\s*[:=]\s*)\S+")


def _redact(value: str) -> str:
    redacted = _QUOTED_SECRET_RE.sub(lambda match: f"{match.group(1)}<redacted>{match.group(3)}", value)
    return _BARE_SECRET_RE.sub(lambda match: f"{match.group(1)}<redacted>", redacted)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_objects(text: str) -> tuple[dict[str, Any], ...]:
    decoder = json.JSONDecoder()
    objects: list[dict[str, Any]] = []
    cursor = 0
    while True:
        start = text.find("{", cursor)
        if start < 0:
            break
        try:
            _, end = decoder.raw_decode(text[start:])
            objects.append(strict_json_object(text[start : start + end], allow_nonfinite=True))
        except (json.JSONDecodeError, StrictJsonError, ValueError):
            cursor = start + 1
            continue
        cursor = start + end
    return tuple(objects)


__all__ = ["write_airflow_xcom_from_evidence"]
