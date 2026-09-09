"""Terminal workflow and campaign closure verification for dev evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.dbt_execution_evidence import canonical_dbt_execution_evidence_bytes
from dpone.services.dbt_dev_evidence_contracts import (
    validate_dbt_execution_evidence_contract,
    validate_workflow_outcome_contract,
)

if TYPE_CHECKING:
    from dpone.services.dbt_dev_evidence_release import ExpectedDbtWorkflow
    from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest


class EvidenceViolation(RuntimeError):
    """One semantic evidence invariant was violated."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class VerifiedAirflowWorkload:
    """Trusted Airflow attempt and content identity for one workload."""

    pack_sha256: str
    task_id: str
    attempt: AirflowAttemptCorrelation
    target_binding_sha256: str
    evidence_set_id: str | None
    evidence_sha256: str
    evidence_bytes: int
    deployment_identity: Mapping[str, Any] | None
    exact_activation: bool


@dataclass(frozen=True, slots=True)
class VerifiedDbtWorkflow:
    """Trusted dbt evidence content identity for one workflow."""

    pack_sha256: str
    evidence_sha256: str
    evidence_bytes: int


def verify_dbt_workflow_evidence(
    payloads: tuple[Mapping[str, Any], ...],
    *,
    expected: Mapping[str, ExpectedDbtWorkflow],
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
    release_id: str,
    deployment_id: str,
) -> dict[str, VerifiedDbtWorkflow]:
    verified: dict[str, VerifiedDbtWorkflow] = {}
    for payload in payloads:
        evidence = validate_dbt_execution_evidence_contract(payload)
        workflow = evidence.workflow_id
        contract = expected.get(workflow)
        airflow_workload = airflow_workloads.get(contract.workload_id) if contract is not None else None
        evidence_attempt = AirflowAttemptCorrelation.from_mapping(evidence.airflow)
        observed_nodes = {item.unique_id for item in evidence.nodes}
        if (
            contract is None
            or airflow_workload is None
            or evidence.status != "passed"
            or evidence.code != "DPONE_DBT_EXECUTION_PASSED"
            or evidence.dbt_exit_code != 0
            or evidence.release_id != release_id
            or evidence.deployment_id != deployment_id
            or evidence.workload_pack_sha256 != contract.workload_pack_sha256
            or evidence.project_bundle_sha256 != contract.project_bundle_sha256
            or evidence.manifest_sha256 != contract.manifest_sha256
            or evidence.selection_sha256 != contract.selection_sha256
            or evidence.toolchain_sha256 != contract.toolchain_sha256
            or evidence.invocation_context_sha256 != contract.invocation_context_sha256
            or evidence.logical_target_sha256 != contract.logical_target_sha256
            or evidence.adapter_runtime != contract.adapter_runtime
            or evidence.adapter_policy_sha256 != contract.adapter_policy_sha256
            or evidence.graph_policy_sha256 != contract.graph_policy_sha256
            or evidence.target_binding_sha256 != airflow_workload.target_binding_sha256
            or evidence.dbt_warning_policy != contract.dbt_warning_policy
            or evidence.dbt_version != contract.dbt_version
            or evidence.dbt_schema_version != contract.dbt_schema_version
            or evidence.invocation_id is None
            or evidence_attempt != airflow_workload.attempt
            or observed_nodes != set(contract.expected_run_result_unique_ids)
            or workflow in verified
        ):
            raise EvidenceViolation("dbt_execution_evidence_invalid")
        evidence_bytes = canonical_dbt_execution_evidence_bytes(dict(payload))
        verified[workflow] = VerifiedDbtWorkflow(
            pack_sha256=evidence.workload_pack_sha256,
            evidence_sha256="sha256:" + hashlib.sha256(evidence_bytes).hexdigest(),
            evidence_bytes=len(evidence_bytes),
        )
    return verified


def workflows_by_workload(
    expected: Mapping[str, ExpectedDbtWorkflow],
) -> dict[str, ExpectedDbtWorkflow]:
    """Map every release workload to exactly one expected workflow."""

    result: dict[str, ExpectedDbtWorkflow] = {}
    for workflow in expected.values():
        for workload_id in workflow.expected_workload_ids:
            if workload_id in result:
                raise EvidenceViolation("release_evidence_contract_invalid")
            result[workload_id] = workflow
    return result


def verify_exact_activation_identity(
    workloads: Mapping[str, VerifiedAirflowWorkload],
    *,
    release_id: str,
    deployment_id: str,
    expected_activation_id: str | None,
    required: bool,
) -> bool:
    """Verify that one release was executed from one exact activation."""

    exact_flags = {item.exact_activation for item in workloads.values()}
    if len(exact_flags) > 1:
        raise EvidenceViolation("airflow_deployment_identity_mismatch")
    if exact_flags != {True}:
        if required:
            raise EvidenceViolation("exact_activation_evidence_required")
        return False
    identities = {
        tuple(sorted(item.deployment_identity.items()))
        for item in workloads.values()
        if isinstance(item.deployment_identity, Mapping)
    }
    if len(identities) != 1:
        raise EvidenceViolation("airflow_deployment_identity_mismatch")
    identity = dict(next(iter(identities)))
    if (
        identity.get("release_id") != release_id
        or identity.get("deployment_id") != deployment_id
        or (expected_activation_id is not None and identity.get("activation_id") != expected_activation_id)
    ):
        raise EvidenceViolation("airflow_deployment_identity_mismatch")
    if required and expected_activation_id is None:
        raise EvidenceViolation("expected_activation_identity_required")
    return True


def verify_workflow_outcomes(
    payloads: tuple[Mapping[str, Any], ...],
    *,
    expected: Mapping[str, ExpectedDbtWorkflow],
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
    dbt_workflows: Mapping[str, VerifiedDbtWorkflow],
    release_id: str,
    deployment_id: str,
    require_exact_activation: bool = False,
) -> set[str]:
    """Verify one terminal passed outcome for every release workflow."""

    verified: set[str] = set()
    for payload in payloads:
        if require_exact_activation and payload.get("schema") != "dpone.dbt-workflow-evidence-outcome.v2":
            raise EvidenceViolation("exact_activation_evidence_required")
        evidence_set_id = validate_workflow_outcome_contract(payload)
        workflow_id = payload.get("workflow_id")
        tasks = payload.get("tasks")
        workflow = expected.get(workflow_id) if isinstance(workflow_id, str) else None
        expected_task_ids = set(workflow.expected_terminal_task_ids) if workflow is not None else set()
        observed_task_ids = _observed_task_ids(tasks)
        expected_run_id = workflow_run_id(workflow, airflow_workloads) if workflow is not None else None
        expected_evidence_set = workflow_evidence_set(workflow, airflow_workloads) if workflow is not None else None
        expected_deployment_identity = (
            workflow_deployment_identity(workflow, airflow_workloads)
            if workflow is not None and payload.get("schema") == "dpone.dbt-workflow-evidence-outcome.v2"
            else None
        )
        if (
            not isinstance(workflow_id, str)
            or workflow is None
            or workflow_id in verified
            or payload.get("status") != "passed"
            or payload.get("code") != "DPONE_DBT_WORKFLOW_PASSED"
            or payload.get("release_id") != release_id
            or payload.get("deployment_id") != deployment_id
            or payload.get("dag_run_id") != expected_run_id
            or not isinstance(tasks, list)
            or not tasks
            or observed_task_ids != expected_task_ids
            or len(observed_task_ids) != len(tasks)
            or any(
                not isinstance(item, Mapping)
                or not isinstance(item.get("task_id"), str)
                or item.get("state") != "success"
                for item in tasks
            )
            or evidence_set_id != expected_evidence_set
            or payload.get("deployment_identity") != expected_deployment_identity
            or not _workflow_inventory_matches(
                payload,
                workflow=workflow,
                airflow_workloads=airflow_workloads,
                dbt_workflows=dbt_workflows,
            )
        ):
            raise EvidenceViolation("workflow_outcome_evidence_invalid")
        verified.add(workflow_id)
    return verified


def validate_campaign_request(
    request: DbtDevEvidenceRequest,
    *,
    expected: Mapping[str, ExpectedDbtWorkflow],
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
    release_id: str,
    deployment_id: str,
    evidence_set_id: str | None,
) -> None:
    """Bind a persisted campaign request to the verified Airflow attempts."""

    request_workflows = {item.workflow_id: item for item in request.workflows}
    if (
        request.release_id != release_id
        or request.deployment_id != deployment_id
        or request.evidence_set_id != evidence_set_id
        or set(request_workflows) != set(expected)
    ):
        raise EvidenceViolation("campaign_request_identity_mismatch")
    for workflow_id, workflow in expected.items():
        requested = request_workflows[workflow_id]
        if requested.dag_id != workflow.dag_id or workflow_run_id(workflow, airflow_workloads) != requested.dag_run_id:
            raise EvidenceViolation("campaign_request_identity_mismatch")


def workflow_run_id(
    workflow: ExpectedDbtWorkflow,
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
) -> str:
    """Return the single DagRun identity shared by all workflow workloads."""

    if any(workload_id not in airflow_workloads for workload_id in workflow.expected_workload_ids):
        raise EvidenceViolation("airflow_evidence_run_mismatch")
    run_ids = {airflow_workloads[workload_id].attempt.run_id for workload_id in workflow.expected_workload_ids}
    if len(run_ids) != 1:
        raise EvidenceViolation("airflow_evidence_run_mismatch")
    return next(iter(run_ids))


def workflow_evidence_set(
    workflow: ExpectedDbtWorkflow,
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
) -> str | None:
    """Return the single evidence-set identity shared by a workflow."""

    values = {
        airflow_workloads[workload_id].evidence_set_id
        for workload_id in workflow.expected_workload_ids
        if workload_id in airflow_workloads
    }
    if len(values) != 1:
        raise EvidenceViolation("airflow_evidence_set_mismatch")
    return next(iter(values))


def workflow_deployment_identity(
    workflow: ExpectedDbtWorkflow,
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
) -> dict[str, Any]:
    """Return the single exact activation identity shared by a workflow."""

    identities = [
        airflow_workloads[workload_id].deployment_identity
        for workload_id in workflow.expected_workload_ids
        if workload_id in airflow_workloads
    ]
    if any(not isinstance(identity, Mapping) for identity in identities):
        raise EvidenceViolation("airflow_deployment_identity_mismatch")
    values = {tuple(sorted(identity.items())) for identity in identities if isinstance(identity, Mapping)}
    if len(values) != 1:
        raise EvidenceViolation("airflow_deployment_identity_mismatch")
    return dict(next(iter(values)))


def _observed_task_ids(tasks: object) -> set[str]:
    return (
        {
            str(item.get("task_id"))
            for item in tasks
            if isinstance(item, Mapping) and isinstance(item.get("task_id"), str)
        }
        if isinstance(tasks, list)
        else set()
    )


def _workflow_inventory_matches(
    payload: Mapping[str, Any],
    *,
    workflow: ExpectedDbtWorkflow | None,
    airflow_workloads: Mapping[str, VerifiedAirflowWorkload],
    dbt_workflows: Mapping[str, VerifiedDbtWorkflow],
) -> bool:
    artifacts = payload.get("artifacts")
    if artifacts is None:
        return all(
            airflow_workloads[workload_id].evidence_set_id is None
            for workload_id in (workflow.expected_workload_ids if workflow is not None else ())
        )
    if workflow is None or not isinstance(artifacts, list):
        return False
    dbt_evidence = dbt_workflows.get(workflow.workflow_id)
    if dbt_evidence is None:
        return False
    expected = [
        {
            "category": "airflow",
            "logical_id": workload_id,
            "sha256": airflow_workloads[workload_id].evidence_sha256,
            "bytes": airflow_workloads[workload_id].evidence_bytes,
        }
        for workload_id in workflow.expected_workload_ids
    ]
    expected.append(
        {
            "category": "dbt",
            "logical_id": workflow.workflow_id,
            "sha256": dbt_evidence.evidence_sha256,
            "bytes": dbt_evidence.evidence_bytes,
        }
    )
    return artifacts == sorted(
        expected,
        key=lambda item: (
            str(item["category"]),
            str(item["logical_id"]),
        ),
    )


__all__ = [
    "EvidenceViolation",
    "VerifiedAirflowWorkload",
    "VerifiedDbtWorkflow",
    "validate_campaign_request",
    "verify_dbt_workflow_evidence",
    "verify_exact_activation_identity",
    "verify_workflow_outcomes",
    "workflows_by_workload",
]
