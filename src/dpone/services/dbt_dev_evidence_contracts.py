"""Strict, dependency-light validators for promoted dbt dev evidence."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.dbt_publishing import (
    DbtCredentialVersion,
    DbtExecutionEvidence,
    DbtNodeOutcome,
    DbtPublishingError,
    DbtSqlServerRuntimePolicy,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_airflow_schema_adapter import (
    airflow_evidence_schema_is_valid,
)
from dpone.security_redaction import is_sensitive_field_name, redact_text
from dpone.services.dbt_dev_airflow_evidence_contracts import (
    AirflowEvidenceContractError,
)
from dpone.services.dbt_dev_airflow_evidence_contracts import (
    validate_dbt_airflow_attempt_evidence as _validate_dbt_airflow_attempt_evidence,
)
from dpone.services.dbt_dev_airflow_evidence_contracts import (
    validate_workflow_outcome_contract as _validate_workflow_outcome_contract,
)

_DBT_EVIDENCE_FIELDS = frozenset(
    {
        "schema",
        "status",
        "code",
        "workflow_id",
        "release_id",
        "deployment_id",
        "workload_pack_sha256",
        "project_bundle_sha256",
        "manifest_sha256",
        "selection_sha256",
        "toolchain_sha256",
        "invocation_context_sha256",
        "logical_target_sha256",
        "target_binding_sha256",
        "adapter_runtime",
        "adapter_policy_sha256",
        "graph_policy_sha256",
        "preflight_status",
        "build_started",
        "dbt_exit_code",
        "dbt_warning_policy",
        "dbt_warning_count",
        "dbt_schema_version",
        "dbt_version",
        "invocation_id",
        "started_at",
        "finished_at",
        "airflow",
        "credential_versions",
        "nodes",
    }
)
_DBT_EVIDENCE_OPTIONAL_FIELDS = frozenset({"recovery"})
_CREDENTIAL_FIELDS = frozenset({"connection_ref", "resolver", "resolved_version"})
_NODE_FIELDS = frozenset({"unique_id", "status", "execution_time"})
_AIRFLOW_ARTIFACT_FIELDS = frozenset(
    {
        "name",
        "path",
        "expected_kind",
        "actual_kind",
        "required",
        "exists",
        "sha256",
        "bytes",
        "passed",
        "reason",
    }
)
_REQUIRED_AIRFLOW_ARTIFACTS = {
    "bundle": "gitops.bundle",
    "run_spec": "gitops.airflow_run_spec",
    "runtime_profile": "gitops.airflow_runtime_profile",
    "pod_contract": "gitops.airflow_pod_contract",
    "runtime_evidence": "gitops.airflow_runtime_evidence",
    "xcom_summary": "gitops.airflow_xcom_summary",
}
_SAFE_CREDENTIAL_METADATA_FIELDS = frozenset(
    {
        "connection_ref",
        "credential_runtime_ref",
        "credential_versions",
        "resolved_version",
        "resolver",
    }
)


class DbtDevEvidenceContractError(ValueError):
    """A promoted evidence object is malformed or contains secret material."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def read_evidence_object(root: Path, relative: str, *, max_bytes: int) -> Mapping[str, Any]:
    """Read one duplicate-free, secret-free JSON object below an evidence root."""

    try:
        raw = read_confined_file(root.absolute(), relative, max_bytes=max_bytes)
        payload = json.loads(
            raw,
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except (OSError, ValueError, json.JSONDecodeError):
        raise DbtDevEvidenceContractError("evidence_file_invalid") from None
    if not isinstance(payload, Mapping):
        raise DbtDevEvidenceContractError("evidence_file_invalid")
    _assert_no_secret_fields(payload)
    return payload


def validate_airflow_evidence_contract(payload: Mapping[str, Any]) -> AirflowAttemptCorrelation:
    """Validate the public bundle schema and its non-downgradable artifact set."""

    if not airflow_evidence_schema_is_valid(payload):
        raise DbtDevEvidenceContractError("airflow_evidence_contract_invalid")
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise DbtDevEvidenceContractError("airflow_evidence_artifact_failed")
    observed: dict[str, Mapping[str, Any]] = {}
    for item in artifacts:
        if not isinstance(item, Mapping) or set(item) != _AIRFLOW_ARTIFACT_FIELDS:
            raise DbtDevEvidenceContractError("airflow_evidence_artifact_failed")
        name = item.get("name")
        if not isinstance(name, str) or not name or name in observed:
            raise DbtDevEvidenceContractError("airflow_evidence_artifact_failed")
        observed[name] = item
    if not _required_artifacts_pass(observed):
        raise DbtDevEvidenceContractError("airflow_evidence_artifact_failed")
    try:
        return AirflowAttemptCorrelation.from_mapping(payload.get("attempt"))
    except ValueError:
        raise DbtDevEvidenceContractError("airflow_evidence_identity_invalid") from None


def validate_dbt_execution_evidence_contract(
    payload: Mapping[str, Any],
) -> DbtExecutionEvidence:
    """Parse one exact ``dpone.dbt-execution-evidence.v1`` object."""

    try:
        fields = set(payload)
        if not _DBT_EVIDENCE_FIELDS.issubset(fields) or not fields.issubset(
            _DBT_EVIDENCE_FIELDS | _DBT_EVIDENCE_OPTIONAL_FIELDS
        ):
            raise ValueError("unexpected dbt evidence fields")
        credentials = payload.get("credential_versions")
        nodes = payload.get("nodes")
        if not isinstance(credentials, list) or not isinstance(nodes, list):
            raise ValueError("dbt evidence arrays are invalid")
        if "recovery" in payload and not isinstance(payload["recovery"], Mapping):
            raise ValueError("dbt evidence recovery is invalid")
        return DbtExecutionEvidence(
            status=payload["status"],
            code=payload["code"],
            workflow_id=payload["workflow_id"],
            release_id=payload["release_id"],
            deployment_id=payload["deployment_id"],
            workload_pack_sha256=payload["workload_pack_sha256"],
            project_bundle_sha256=payload["project_bundle_sha256"],
            manifest_sha256=payload["manifest_sha256"],
            selection_sha256=payload["selection_sha256"],
            toolchain_sha256=payload["toolchain_sha256"],
            invocation_context_sha256=payload["invocation_context_sha256"],
            logical_target_sha256=payload["logical_target_sha256"],
            target_binding_sha256=payload["target_binding_sha256"],
            adapter_runtime=DbtSqlServerRuntimePolicy.from_mapping(payload["adapter_runtime"]),
            adapter_policy_sha256=payload["adapter_policy_sha256"],
            graph_policy_sha256=payload["graph_policy_sha256"],
            preflight_status=payload["preflight_status"],
            build_started=payload["build_started"],
            dbt_exit_code=payload["dbt_exit_code"],
            dbt_warning_policy=payload["dbt_warning_policy"],
            dbt_warning_count=payload["dbt_warning_count"],
            dbt_schema_version=_optional_text(payload["dbt_schema_version"]),
            dbt_version=_optional_text(payload["dbt_version"]),
            invocation_id=_optional_text(payload["invocation_id"]),
            started_at=payload["started_at"],
            finished_at=payload["finished_at"],
            airflow=AirflowAttemptCorrelation.from_mapping(payload["airflow"]).to_dict(),
            credential_versions=tuple(_credential(item) for item in credentials),
            nodes=tuple(_node(item) for item in nodes),
            recovery=payload.get("recovery"),
            schema=payload["schema"],
        )
    except (DbtPublishingError, KeyError, TypeError, ValueError):
        raise DbtDevEvidenceContractError("dbt_execution_evidence_invalid") from None


def validate_dbt_airflow_attempt_evidence(
    payload: Mapping[str, Any],
) -> tuple[AirflowAttemptCorrelation, Mapping[str, Any], str]:
    """Validate one provider attempt while preserving the public error contract."""

    try:
        return _validate_dbt_airflow_attempt_evidence(
            payload,
            parse_attempt=AirflowAttemptCorrelation.from_mapping,
        )
    except AirflowEvidenceContractError as exc:
        raise DbtDevEvidenceContractError(str(exc)) from None


def validate_workflow_outcome_contract(payload: Mapping[str, Any]) -> str | None:
    """Validate one workflow outcome while preserving the public error contract."""

    try:
        return _validate_workflow_outcome_contract(payload)
    except AirflowEvidenceContractError as exc:
        raise DbtDevEvidenceContractError(str(exc)) from None


def _required_artifacts_pass(observed: Mapping[str, Mapping[str, Any]]) -> bool:
    for name, expected_kind in _REQUIRED_AIRFLOW_ARTIFACTS.items():
        item = observed.get(name)
        if (
            item is None
            or item.get("required") is not True
            or item.get("exists") is not True
            or item.get("passed") is not True
            or item.get("expected_kind") != expected_kind
            or item.get("actual_kind") != expected_kind
            or not _raw_sha256(item.get("sha256"))
            or isinstance(item.get("bytes"), bool)
            or not isinstance(item.get("bytes"), int)
            or item["bytes"] < 0
        ):
            return False
    return True


def _credential(value: object) -> DbtCredentialVersion:
    if not isinstance(value, Mapping) or set(value) != _CREDENTIAL_FIELDS:
        raise ValueError("credential version is invalid")
    return DbtCredentialVersion(
        connection_ref=value["connection_ref"],
        resolver=value["resolver"],
        resolved_version=_optional_text(value["resolved_version"]),
    )


def _node(value: object) -> DbtNodeOutcome:
    if not isinstance(value, Mapping) or set(value) != _NODE_FIELDS:
        raise ValueError("node outcome is invalid")
    return DbtNodeOutcome(
        unique_id=value["unique_id"],
        status=value["status"],
        execution_time=value["execution_time"],
    )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("optional text is invalid")
    return value


def _raw_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _unique_pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _assert_no_secret_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            field = str(key)
            if field not in _SAFE_CREDENTIAL_METADATA_FIELDS and is_sensitive_field_name(field):
                raise DbtDevEvidenceContractError("evidence_secret_field_forbidden")
            _assert_no_secret_fields(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_secret_fields(item)
    elif isinstance(value, str) and redact_text(value) != value:
        raise DbtDevEvidenceContractError("evidence_secret_value_forbidden")


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


__all__ = [
    "DbtDevEvidenceContractError",
    "read_evidence_object",
    "validate_airflow_evidence_contract",
    "validate_dbt_airflow_attempt_evidence",
    "validate_dbt_execution_evidence_contract",
    "validate_workflow_outcome_contract",
]
