"""Closed validators for Airflow attempt and workflow evidence envelopes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from dpone_airflow_pack.outcome_gate import GitOpsAirflowOutcomeGateEvaluator

from dpone.services.dbt_deployment_identity_evidence import (
    attempt_deployment_identity,
    workflow_deployment_identity_is_valid,
)

_Attempt = TypeVar("_Attempt")


class AirflowEvidenceContractError(ValueError):
    """An attempt or terminal workflow evidence envelope is invalid."""


_WORKFLOW_OUTCOME_FIELDS = frozenset(
    {
        "schema",
        "status",
        "code",
        "workflow_id",
        "release_id",
        "deployment_id",
        "dag_run_id",
        "tasks",
    }
)
_WORKFLOW_TASK_FIELDS = frozenset({"task_id", "state", "mapped_instances"})
_LEGACY_WORKFLOW_TASK_FIELDS = frozenset({"task_id", "state"})
_ATTEMPT_EVIDENCE_FIELDS = frozenset(
    {
        "schema",
        "status",
        "evidence_set_id",
        "run_identity",
        "attempt",
        "xcom_summary_sha256",
        "xcom_summary",
    }
)
_ATTEMPT_EVIDENCE_EXACT_FIELDS = _ATTEMPT_EVIDENCE_FIELDS | frozenset({"deployment_identity"})
_WORKFLOW_EVIDENCE_FIELDS = _WORKFLOW_OUTCOME_FIELDS | frozenset({"evidence_set_id", "artifacts"})
_WORKFLOW_EVIDENCE_EXACT_FIELDS = _WORKFLOW_EVIDENCE_FIELDS | frozenset({"deployment_identity"})
_WORKFLOW_ARTIFACT_FIELDS = frozenset({"category", "logical_id", "sha256", "bytes"})


def validate_dbt_airflow_attempt_evidence(
    payload: Mapping[str, Any],
    *,
    parse_attempt: Callable[[object], _Attempt],
) -> tuple[_Attempt, Mapping[str, Any], str]:
    """Validate one immutable provider-produced attempt envelope."""

    try:
        schema = payload.get("schema")
        expected_fields = (
            _ATTEMPT_EVIDENCE_EXACT_FIELDS
            if schema == "dpone.dbt-airflow-attempt-evidence.v2"
            else _ATTEMPT_EVIDENCE_FIELDS
        )
        if (
            set(payload) != expected_fields
            or schema
            not in {
                "dpone.dbt-airflow-attempt-evidence.v1",
                "dpone.dbt-airflow-attempt-evidence.v2",
            }
            or payload.get("status") != "passed"
            or not _sha256(payload.get("evidence_set_id"))
            or not isinstance(payload.get("run_identity"), Mapping)
            or not isinstance(payload.get("xcom_summary"), Mapping)
        ):
            raise ValueError("attempt envelope is invalid")
        attempt = parse_attempt(payload["attempt"])
        summary = payload["xcom_summary"]
        expected_identity = (
            attempt_deployment_identity(payload, summary) if schema == "dpone.dbt-airflow-attempt-evidence.v2" else None
        )
        if schema == "dpone.dbt-airflow-attempt-evidence.v1" and "deployment_identity" in summary:
            raise ValueError("legacy attempt evidence cannot bind deployment identity")
        if payload.get("xcom_summary_sha256") != _digest(_canonical_bytes(summary)):
            raise ValueError("XCom digest is invalid")
        report = GitOpsAirflowOutcomeGateEvaluator().evaluate(
            xcom_summary_path="xcom://promotion-evidence",
            xcom_summary=summary,
            required_status="passed",
            expected_run_identity=payload["run_identity"],
            expected_deployment_identity=expected_identity,
        )
        if not report.passed:
            raise ValueError("XCom outcome is not passed")
        return attempt, payload["run_identity"], str(payload["evidence_set_id"])
    except (KeyError, TypeError, ValueError):
        raise AirflowEvidenceContractError("airflow_evidence_contract_invalid") from None


def validate_workflow_outcome_contract(payload: Mapping[str, Any]) -> str | None:
    """Validate one immutable terminal workflow outcome envelope."""

    tasks = payload.get("tasks")
    schema_value = payload.get("schema")
    if not isinstance(schema_value, str):
        raise AirflowEvidenceContractError("workflow_outcome_evidence_invalid")
    schema = schema_value
    expected_fields = {
        "dpone.dbt-workflow-outcome.v1": _WORKFLOW_OUTCOME_FIELDS,
        "dpone.dbt-workflow-outcome.v2": _WORKFLOW_OUTCOME_FIELDS | frozenset({"deployment_identity"}),
        "dpone.dbt-workflow-evidence-outcome.v1": _WORKFLOW_EVIDENCE_FIELDS,
        "dpone.dbt-workflow-evidence-outcome.v2": _WORKFLOW_EVIDENCE_EXACT_FIELDS,
    }.get(schema)
    if (
        expected_fields is None
        or set(payload) != expected_fields
        or not isinstance(tasks, list)
        or not tasks
        or any(not _valid_task(item) for item in tasks)
    ):
        raise AirflowEvidenceContractError("workflow_outcome_evidence_invalid")
    if not workflow_deployment_identity_is_valid(payload):
        raise AirflowEvidenceContractError("workflow_outcome_evidence_invalid")
    if schema in {"dpone.dbt-workflow-outcome.v1", "dpone.dbt-workflow-outcome.v2"}:
        return None
    artifacts = payload.get("artifacts")
    if (
        not _sha256(payload.get("evidence_set_id"))
        or not isinstance(artifacts, list)
        or not artifacts
        or any(not _valid_artifact(item) for item in artifacts)
    ):
        raise AirflowEvidenceContractError("workflow_outcome_evidence_invalid")
    identities = [(str(item["category"]), str(item["logical_id"])) for item in artifacts if isinstance(item, Mapping)]
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise AirflowEvidenceContractError("workflow_outcome_evidence_invalid")
    return str(payload["evidence_set_id"])


def _valid_task(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    mapped_instances = value.get("mapped_instances")
    return (
        set(value) in {_WORKFLOW_TASK_FIELDS, _LEGACY_WORKFLOW_TASK_FIELDS}
        and isinstance(value.get("task_id"), str)
        and isinstance(value.get("state"), str)
        and (
            "mapped_instances" not in value
            or (not isinstance(mapped_instances, bool) and isinstance(mapped_instances, int) and mapped_instances >= 1)
        )
    )


def _valid_artifact(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    byte_count = value.get("bytes")
    return (
        set(value) == _WORKFLOW_ARTIFACT_FIELDS
        and value.get("category") in {"airflow", "dbt"}
        and isinstance(value.get("logical_id"), str)
        and bool(value["logical_id"])
        and _sha256(value.get("sha256"))
        and not isinstance(byte_count, bool)
        and isinstance(byte_count, int)
        and byte_count >= 1
    )


def _sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


__all__ = [
    "AirflowEvidenceContractError",
    "validate_dbt_airflow_attempt_evidence",
    "validate_workflow_outcome_contract",
]
