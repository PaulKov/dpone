"""Pure validation helpers for execution-time dev evidence export."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone_airflow_pack.dag_spec_contract import redact_parse_error_message
from dpone_airflow_pack.deployment_identity import (
    deployment_identity_error,
    deployment_identity_from_context,
)
from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    DevEvidenceStoreError,
    logical_evidence_filename,
)
from dpone_airflow_pack.strict_json import loads_strict_json_object

ATTEMPT_EVIDENCE_SCHEMA = "dpone.dbt-airflow-attempt-evidence.v2"
DEV_EVIDENCE_AUTHORITY_CONF_KEY = "dpone_evidence_authority"
WORKFLOW_EVIDENCE_SCHEMA = "dpone.dbt-workflow-evidence-outcome.v2"
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_SENSITIVE_KEY = re.compile(
    r"(?i)^(?:password|passwd|pwd|token|secret|authorization|api[_-]?key|"
    r"access[_-]?key|private[_-]?key)$"
)


class DevEvidenceExportError(RuntimeError):
    """One requested evidence set could not be produced safely."""

    code = "DPONE_DBT_DEV_EVIDENCE_EXPORT_FAILED"


def attempt_evidence(
    summary: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    workload_id: str,
    attempt: Mapping[str, Any],
    evidence_set_id: str,
) -> dict[str, Any]:
    """Validate one runtime summary and bind it to its provider attempt."""

    expected = {key: value for key, value in context.items() if not str(key).startswith("_")}
    packs = context.get("_workload_pack_sha256")
    pack_sha256 = packs.get(workload_id) if isinstance(packs, Mapping) else None
    expected["workload_pack"] = {
        "id": workload_id,
        "sha256": digest(pack_sha256, "workload pack"),
    }
    observed = summary.get("run_identity")
    if not isinstance(observed, Mapping) or dict(observed) != expected:
        raise DevEvidenceExportError("runtime XCom identity differs from the provider-pinned workload")
    try:
        expected_deployment_identity = deployment_identity_from_context(context)
    except ValueError:
        raise DevEvidenceExportError("provider deployment identity is invalid") from None
    if expected_deployment_identity is None:
        raise DevEvidenceExportError("exact provider evidence requires a cache activation identity")
    observed_deployment_identity = summary.get("deployment_identity")
    if (
        (expected_deployment_identity is None) != (observed_deployment_identity is None)
        or expected_deployment_identity is not None
        and (
            not isinstance(observed_deployment_identity, Mapping)
            or dict(observed_deployment_identity) != expected_deployment_identity
        )
    ):
        raise DevEvidenceExportError("runtime XCom deployment identity differs from the provider activation")
    if (
        summary.get("kind") != "gitops.airflow_xcom_summary"
        or summary.get("status") != "passed"
        or summary.get("blockers") != []
        or not _DIGEST.fullmatch(str(summary.get("runtime_evidence_sha256") or ""))
        or not isinstance(summary.get("runtime_evidence"), Mapping)
    ):
        raise DevEvidenceExportError("runtime XCom does not prove a passed workload")
    reject_sensitive_fields(summary)
    canonical_summary = canonical_bytes(summary)
    payload = {
        "schema": ATTEMPT_EVIDENCE_SCHEMA,
        "status": "passed",
        "evidence_set_id": evidence_set_id,
        "run_identity": expected,
        "attempt": dict(attempt),
        "xcom_summary_sha256": sha256(canonical_summary),
        "xcom_summary": dict(summary),
    }
    payload["deployment_identity"] = expected_deployment_identity
    return payload


def airflow_attempt(dag_run: Any, task_id: str) -> dict[str, Any]:
    """Return one unambiguous successful Airflow task attempt."""

    if not hasattr(dag_run, "get_task_instances"):
        raise DevEvidenceExportError("Airflow DagRun cannot provide task instances")
    matches = [item for item in dag_run.get_task_instances() if str(getattr(item, "task_id", "") or "") == task_id]
    if len(matches) != 1:
        raise DevEvidenceExportError("runtime task attempt identity is ambiguous or missing")
    task = matches[0]
    try_number = getattr(task, "try_number", None)
    map_index = getattr(task, "map_index", -1)
    if (
        getattr(task, "state", None) != "success"
        or isinstance(try_number, bool)
        or not isinstance(try_number, int)
        or try_number < 1
        or isinstance(map_index, bool)
        or not isinstance(map_index, int)
    ):
        raise DevEvidenceExportError("runtime task did not finish as one valid successful attempt")
    return {
        "dag_id": (str(getattr(dag_run, "dag_id", "") or "") or _dag_id_from_task(task)),
        "task_id": task_id,
        "run_id": str(getattr(dag_run, "run_id", "") or ""),
        "try_number": try_number,
        "map_index": map_index,
    }


def xcom_summary(ti: Any, runtime_task_id: str) -> dict[str, Any]:
    """Read one strict, bounded-by-Airflow runtime XCom summary."""

    if ti is None or not hasattr(ti, "xcom_pull"):
        raise DevEvidenceExportError("Airflow task context cannot read runtime XCom")
    raw = ti.xcom_pull(task_ids=runtime_task_id)
    try:
        encoded = (
            raw
            if isinstance(raw, str)
            else json.dumps(
                raw,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return loads_strict_json_object(encoded)
    except (TypeError, ValueError, RecursionError):
        raise DevEvidenceExportError("runtime task XCom is not a strict JSON object") from None


def read_dbt_evidence(
    store: DevEvidenceConfinedStore,
    *,
    base: tuple[str, ...],
    raw_ref: Mapping[str, Any],
    workflow_id: str,
    attempt: Mapping[str, Any],
) -> bytes:
    """Read and validate the dbt evidence emitted by the exact task attempt."""

    if set(raw_ref) != {
        "schema",
        "workflow_id",
        "sha256",
        "bytes",
        "storage_scope",
    }:
        raise DevEvidenceExportError("dbt execution evidence descriptor is invalid")
    if (
        raw_ref.get("schema") != "dpone.dbt-execution-evidence-ref.v1"
        or raw_ref.get("workflow_id") != workflow_id
        or raw_ref.get("storage_scope") != "dbt_spool"
    ):
        raise DevEvidenceExportError("dbt execution evidence descriptor identity is invalid")
    try:
        raw = store.read(
            directory_parts=("dbt-spool", *base, "dbt"),
            filename=logical_evidence_filename(workflow_id),
            expected_sha256=str(raw_ref.get("sha256") or ""),
            expected_bytes=raw_ref.get("bytes"),  # type: ignore[arg-type]
        )
        payload = loads_strict_json_object(raw.decode("utf-8"))
    except (
        DevEvidenceStoreError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        RecursionError,
    ):
        raise DevEvidenceExportError("dbt execution evidence is not a strict JSON object") from None
    if (
        payload.get("schema") != "dpone.dbt-execution-evidence.v1"
        or payload.get("status") != "passed"
        or payload.get("workflow_id") != workflow_id
        or payload.get("airflow") != dict(attempt)
    ):
        raise DevEvidenceExportError("dbt execution evidence differs from its Airflow attempt")
    reject_sensitive_fields(payload)
    return raw


def workflow_outcome(
    value: Mapping[str, Any],
    *,
    workflow_id: str,
    release_id: str,
    deployment_id: str,
    run_id: str,
    evidence_set_id: str,
    expected_deployment_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and upgrade one terminal workflow outcome."""

    payload = dict(value)
    if (
        payload.get("schema") != "dpone.dbt-workflow-outcome.v2"
        or payload.get("status") != "passed"
        or payload.get("workflow_id") != workflow_id
        or payload.get("release_id") != release_id
        or payload.get("deployment_id") != deployment_id
        or payload.get("dag_run_id") != run_id
    ):
        raise DevEvidenceExportError("workflow outcome identity is invalid")
    deployment_identity = payload.get("deployment_identity")
    if (
        deployment_identity_error(deployment_identity)
        or not isinstance(deployment_identity, Mapping)
        or dict(deployment_identity) != dict(expected_deployment_identity)
    ):
        raise DevEvidenceExportError("workflow outcome deployment identity is invalid")
    reject_sensitive_fields(payload)
    return {
        **payload,
        "schema": WORKFLOW_EVIDENCE_SCHEMA,
        "evidence_set_id": evidence_set_id,
    }


def run_identity_context(
    value: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Require the provider-pinned release, deployment and pack inventory."""

    if not isinstance(value, Mapping):
        raise DevEvidenceExportError("provider run identity context is missing")
    payload = dict(value)
    packs = payload.get("_workload_pack_sha256")
    if not isinstance(packs, Mapping) or not packs:
        raise DevEvidenceExportError("provider workload identity inventory is missing")
    return payload


def workload_tasks(
    value: Sequence[Mapping[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Return a stable, unique workload-to-runtime-task inventory."""

    result: list[tuple[str, str]] = []
    for item in value:
        workload_id = str(item.get("workload_id") or "")
        runtime_task_id = str(item.get("runtime_task_id") or "")
        if not workload_id or not runtime_task_id:
            raise DevEvidenceExportError("workflow workload task inventory is invalid")
        result.append((workload_id, runtime_task_id))
    if not result or len(set(result)) != len(result) or len({item[0] for item in result}) != len(result):
        raise DevEvidenceExportError("workflow workload task inventory is empty or duplicated")
    return tuple(sorted(result))


def evidence_authority(dag_run: Any) -> Mapping[str, Any] | None:
    """Read the optional campaign authority from the current DagRun."""

    conf = getattr(dag_run, "conf", None)
    value = conf.get(DEV_EVIDENCE_AUTHORITY_CONF_KEY) if isinstance(conf, Mapping) else None
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise DevEvidenceExportError("dev evidence authority is invalid")
    return value


def validated_authority(
    value: Mapping[str, Any],
    *,
    workflow_id: str,
    release_id: str,
    deployment_id: str,
    dag_run: Any,
) -> str:
    """Bind the supplied campaign authority to the current DagRun."""

    expected_fields = {
        "schema",
        "request_id",
        "evidence_set_id",
        "release_id",
        "deployment_id",
        "workflow_id",
        "dag_id",
        "dag_run_id",
    }
    evidence_set_id = digest(value.get("evidence_set_id"), "evidence set")
    if (
        set(value) != expected_fields
        or value.get("schema") != "dpone.dbt-dev-evidence-authority.v1"
        or value.get("request_id") != evidence_set_id
        or value.get("release_id") != release_id
        or value.get("deployment_id") != deployment_id
        or value.get("workflow_id") != workflow_id
        or value.get("dag_id") != str(getattr(dag_run, "dag_id", "") or "")
        or value.get("dag_run_id") != str(getattr(dag_run, "run_id", "") or "")
    ):
        raise DevEvidenceExportError("dev evidence authority differs from the current DAG run")
    return evidence_set_id


def digest(value: object, field: str) -> str:
    """Return one validated content identity."""

    text = value if isinstance(value, str) else ""
    if _DIGEST.fullmatch(text) is None:
        raise DevEvidenceExportError(f"{field} identity is invalid")
    return text


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    """Return deterministic JSON bytes for evidence hashing and storage."""

    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def sha256(value: bytes) -> str:
    """Return a dpone-formatted SHA-256 digest."""

    return "sha256:" + hashlib.sha256(value).hexdigest()


def inventory_item(
    *,
    category: str,
    logical_id: str,
    payload: bytes,
) -> dict[str, object]:
    """Build one content-bound evidence inventory item."""

    return {
        "category": category,
        "logical_id": logical_id,
        "sha256": sha256(payload),
        "bytes": len(payload),
    }


def reject_sensitive_fields(value: object) -> None:
    """Reject secret-shaped keys and values before evidence persistence."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            if _SENSITIVE_KEY.fullmatch(str(key)):
                raise DevEvidenceExportError("evidence contains a sensitive field")
            reject_sensitive_fields(child)
    elif isinstance(value, list):
        for child in value:
            reject_sensitive_fields(child)
    elif isinstance(value, str) and redact_parse_error_message(value) != value:
        raise DevEvidenceExportError("evidence contains a sensitive value")


def _dag_id_from_task(task: Any) -> str:
    dag_id = str(getattr(task, "dag_id", "") or "")
    if not dag_id:
        raise DevEvidenceExportError("runtime task DAG identity is missing")
    return dag_id


__all__ = [
    "ATTEMPT_EVIDENCE_SCHEMA",
    "DEV_EVIDENCE_AUTHORITY_CONF_KEY",
    "DevEvidenceExportError",
    "WORKFLOW_EVIDENCE_SCHEMA",
    "airflow_attempt",
    "attempt_evidence",
    "canonical_bytes",
    "digest",
    "evidence_authority",
    "inventory_item",
    "read_dbt_evidence",
    "run_identity_context",
    "validated_authority",
    "workflow_outcome",
    "workload_tasks",
    "xcom_summary",
]
