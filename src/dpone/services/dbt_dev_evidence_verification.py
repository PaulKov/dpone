"""Fail-closed dev execution evidence gate for dbt release promotion."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_correlation import (
    parse_airflow_correlation,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.airflow_run_identity import (
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
    AirflowRunIdentityError,
)
from dpone.contracts.dbt_release_expectations import airflow_evidence_target_binding
from dpone.services.dbt_dev_evidence_contracts import (
    DbtDevEvidenceContractError,
    validate_airflow_evidence_contract,
    validate_dbt_airflow_attempt_evidence,
)
from dpone.services.dbt_dev_evidence_inventory import read_evidence_category
from dpone.services.dbt_dev_evidence_release import (
    DbtDevEvidenceReleaseError,
    DbtExpectedReleaseLoader,
    ExpectedDbtWorkflow,
    load_expected_dbt_release,
)
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
)
from dpone.services.dbt_dev_evidence_verification_report import (
    DBT_DEV_EVIDENCE_UNVERIFIED,
    DBT_DEV_EVIDENCE_VERIFIED,
    DbtDevEvidenceVerificationReport,
)
from dpone.services.dbt_dev_evidence_verification_report import (
    unverified_report as _unverified,
)
from dpone.services.dbt_dev_evidence_workflow_verification import (
    EvidenceViolation,
    VerifiedAirflowWorkload,
    validate_campaign_request,
    verify_exact_activation_identity,
    verify_workflow_outcomes,
    workflows_by_workload,
)
from dpone.services.dbt_dev_evidence_workflow_verification import (
    verify_dbt_workflow_evidence as _verified_dbt_evidence,
)


class DbtDevEvidenceVerificationService:
    """Verify exact-depth evidence directories against one immutable release."""

    def __init__(self, *, release_loader: DbtExpectedReleaseLoader | None = None) -> None:
        self._release_loader = release_loader

    def verify(
        self,
        *,
        compiled_root: str | Path,
        evidence_root: str | Path,
        expected_release_id: str,
        expected_deployment_id: str,
        expected_evidence_set_id: str | None = None,
        expected_campaign_request: DbtDevEvidenceRequest | None = None,
        expected_activation_id: str | None = None,
        require_exact_activation: bool = False,
    ) -> DbtDevEvidenceVerificationReport:
        if not all(is_canonical_sha256_digest(value) for value in (expected_release_id, expected_deployment_id)) or (
            expected_evidence_set_id is not None and not is_canonical_sha256_digest(expected_evidence_set_id)
        ):
            return _unverified(
                expected_release_id,
                expected_deployment_id,
                "identity_invalid",
                evidence_set_id=expected_evidence_set_id,
            )
        if expected_activation_id is not None:
            try:
                AirflowDeploymentIdentity.from_mapping(
                    {
                        "schema": "dpone.airflow-deployment-identity.v1",
                        "release_id": expected_release_id,
                        "deployment_id": expected_deployment_id,
                        "activation_id": expected_activation_id,
                    }
                )
            except ValueError:
                return _unverified(
                    expected_release_id,
                    expected_deployment_id,
                    "identity_invalid",
                    evidence_set_id=expected_evidence_set_id,
                )
        try:
            expected = (self._release_loader or load_expected_dbt_release)(
                Path(compiled_root),
                expected_release_id,
            )
            required = expected.required_workloads
            airflow_payloads = read_evidence_category(Path(evidence_root), "airflow")
            dbt_payloads = read_evidence_category(Path(evidence_root), "dbt")
            outcome_payloads = read_evidence_category(Path(evidence_root), "outcomes")
            verified_airflow = _verified_airflow_evidence(
                airflow_payloads,
                required=required,
                expected=expected.dbt_workflows,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                require_exact_activation=require_exact_activation,
            )
            if set(verified_airflow) != set(required):
                return _unverified(
                    expected_release_id,
                    expected_deployment_id,
                    "airflow_workload_coverage_incomplete",
                    required=tuple(required),
                    verified=tuple(sorted(verified_airflow)),
                )
            evidence_sets = {item.evidence_set_id for item in verified_airflow.values()}
            if len(evidence_sets) != 1:
                raise EvidenceViolation("airflow_evidence_set_mismatch")
            evidence_set_id = next(iter(evidence_sets))
            exact_activation = verify_exact_activation_identity(
                verified_airflow,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                expected_activation_id=expected_activation_id,
                required=require_exact_activation,
            )
            if expected_evidence_set_id is not None and evidence_set_id != expected_evidence_set_id:
                raise EvidenceViolation("airflow_evidence_set_mismatch")
            if expected_campaign_request is not None:
                validate_campaign_request(
                    expected_campaign_request,
                    expected=expected.dbt_workflows,
                    airflow_workloads=verified_airflow,
                    release_id=expected_release_id,
                    deployment_id=expected_deployment_id,
                    evidence_set_id=evidence_set_id,
                )
            verified_dbt = _verified_dbt_evidence(
                dbt_payloads,
                expected=expected.dbt_workflows,
                airflow_workloads=verified_airflow,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
            )
            expected_dbt = tuple(sorted(expected.dbt_workflows))
            if set(verified_dbt) != set(expected_dbt):
                return _unverified(
                    expected_release_id,
                    expected_deployment_id,
                    "dbt_workflow_coverage_incomplete",
                    required=tuple(required),
                    verified=tuple(sorted(verified_airflow)),
                )
            verified_outcomes = verify_workflow_outcomes(
                outcome_payloads,
                expected=expected.dbt_workflows,
                airflow_workloads=verified_airflow,
                dbt_workflows=verified_dbt,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                require_exact_activation=require_exact_activation,
            )
        except (
            DbtDevEvidenceContractError,
            DbtDevEvidenceReleaseError,
            OSError,
            ValueError,
            EvidenceViolation,
        ) as exc:
            reason = (
                exc.reason
                if isinstance(exc, DbtDevEvidenceContractError | EvidenceViolation)
                else "release_evidence_contract_invalid"
            )
            return _unverified(
                expected_release_id,
                expected_deployment_id,
                reason,
            )
        if set(verified_outcomes) != set(expected_dbt):
            return _unverified(
                expected_release_id,
                expected_deployment_id,
                "workflow_outcome_coverage_incomplete",
                required=tuple(required),
                verified=tuple(sorted(verified_airflow)),
            )
        return DbtDevEvidenceVerificationReport(
            code=DBT_DEV_EVIDENCE_VERIFIED,
            release_id=expected_release_id,
            deployment_id=expected_deployment_id,
            evidence_set_id=evidence_set_id,
            required_workloads=tuple(required),
            verified_workloads=tuple(sorted(verified_airflow)),
            verified_dbt_workflows=tuple(f"dbt__{item}" for item in sorted(verified_dbt)),
            exact_activation=exact_activation,
        )


def _verified_airflow_evidence(
    payloads: tuple[Mapping[str, Any], ...],
    *,
    required: Mapping[str, str],
    expected: Mapping[str, ExpectedDbtWorkflow],
    release_id: str,
    deployment_id: str,
    require_exact_activation: bool,
) -> dict[str, VerifiedAirflowWorkload]:
    verified: dict[str, VerifiedAirflowWorkload] = {}
    workflows_by_workload_map = workflows_by_workload(expected)
    if set(workflows_by_workload_map) != set(required):
        raise EvidenceViolation("release_evidence_contract_invalid")
    for payload in payloads:
        try:
            evidence_schema = payload.get("schema")
            if require_exact_activation and evidence_schema != "dpone.dbt-airflow-attempt-evidence.v2":
                raise EvidenceViolation("exact_activation_evidence_required")
            if payload.get("schema") in {
                "dpone.dbt-airflow-attempt-evidence.v1",
                "dpone.dbt-airflow-attempt-evidence.v2",
            }:
                attempt, raw_identity, evidence_set_id = validate_dbt_airflow_attempt_evidence(payload)
                identity = AirflowRunIdentity.from_mapping(raw_identity)
                deployment_identity = payload.get("deployment_identity")
                if payload.get("schema") == "dpone.dbt-airflow-attempt-evidence.v2" and not isinstance(
                    deployment_identity, Mapping
                ):
                    raise EvidenceViolation("airflow_deployment_identity_mismatch")
                correlation = None
            else:
                if (
                    payload.get("kind") != "gitops.airflow_evidence_bundle"
                    or payload.get("runner_policy") != "release"
                    or _nonempty_list(payload.get("blockers"))
                ):
                    raise EvidenceViolation("airflow_evidence_blocked")
                attempt = validate_airflow_evidence_contract(payload)
                identity = AirflowRunIdentity.from_mapping(payload.get("run_identity"))
                correlation = parse_airflow_correlation(payload.get("correlation"))
                evidence_set_id = None
                deployment_identity = None
        except (AirflowRunIdentityError, DbtDevEvidenceContractError, ValueError) as exc:
            if isinstance(exc, DbtDevEvidenceContractError):
                raise
            raise EvidenceViolation("airflow_evidence_identity_invalid") from None
        workload_id = identity.workload_pack.id
        workflow = workflows_by_workload_map.get(workload_id)
        target_binding = airflow_evidence_target_binding(
            workflow=workflow,
            identity=identity,
            attempt=attempt,
            correlation=correlation,
            release_id=release_id,
            deployment_id=deployment_id,
            expected_pack_sha256=required.get(workload_id),
            duplicate_workload=workload_id in verified,
        )
        if target_binding is None:
            raise EvidenceViolation("airflow_evidence_identity_mismatch")
        verified[workload_id] = VerifiedAirflowWorkload(
            pack_sha256=identity.workload_pack.sha256,
            task_id=attempt.task_id,
            attempt=attempt,
            target_binding_sha256=target_binding,
            evidence_set_id=evidence_set_id,
            evidence_sha256=_canonical_digest(payload),
            evidence_bytes=len(_canonical_bytes(payload)),
            deployment_identity=(dict(deployment_identity) if isinstance(deployment_identity, Mapping) else None),
            exact_activation=evidence_schema == "dpone.dbt-airflow-attempt-evidence.v2",
        )
    return verified


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


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _canonical_digest(value: Mapping[str, Any]) -> str:
    return _sha256(_canonical_bytes(value))


def _nonempty_list(value: object) -> bool:
    return isinstance(value, list) and bool(value)


__all__ = [
    "DBT_DEV_EVIDENCE_UNVERIFIED",
    "DBT_DEV_EVIDENCE_VERIFIED",
    "DbtDevEvidenceVerificationReport",
    "DbtDevEvidenceVerificationService",
]
