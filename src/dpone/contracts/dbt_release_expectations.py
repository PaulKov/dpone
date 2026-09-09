"""Pure release-bound expectations; acquisition and SDK validation stay outside.

Legacy v1 metadata rules deliberately do not become the stricter workspace
source index. The service preserves its bounded read and diagnostic ordering.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation, AirflowCorrelation
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_execution_pack import DbtExecutionPack, dbt_target_identity_sha256
from dpone.contracts.dbt_release import dbt_release_producer_violation
from dpone.contracts.dbt_release_workload_binding import (
    DbtDevEvidenceReleaseError,
    ExpectedWorkflowDag,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_digest as _digest,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_mapping as _mapping,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_object as _object,
)
from dpone.contracts.dbt_release_workload_binding import (
    release_text as _required_text,
)
from dpone.contracts.dbt_release_workload_binding import (
    require_dbt_workflow_dag as _required_workflow_dag,
)
from dpone.contracts.dbt_runtime import dbt_target_binding_identity_sha256
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_sqlserver_policy import DbtSqlServerRuntimePolicy


@dataclass(frozen=True, slots=True)
class ExpectedDbtWorkflow:
    workflow_id: str
    workload_id: str
    workload_pack_sha256: str
    project_bundle_sha256: str
    manifest_sha256: str
    selection_sha256: str
    toolchain_sha256: str
    invocation_context_sha256: str
    logical_target_sha256: str
    adapter_runtime: DbtSqlServerRuntimePolicy
    adapter_policy_sha256: str
    graph_policy_sha256: str
    dbt_warning_policy: str
    dbt_version: str
    dbt_schema_version: str
    expected_run_result_unique_ids: tuple[str, ...]
    expected_workload_ids: tuple[str, ...]
    expected_terminal_task_ids: tuple[str, ...]
    dag_id: str = ""


@dataclass(frozen=True, slots=True)
class ExpectedDbtRelease:
    release_id: str
    required_workloads: Mapping[str, str]
    dbt_workflows: Mapping[str, ExpectedDbtWorkflow]


@dataclass(frozen=True, slots=True)
class DbtLegacyEvidencePlan:
    """Validated legacy metadata, not a signature or complete-source certificate.

    Ordered methods let the acquisition service retain v1 failure priority:
    metadata, DAG bytes, required sources, then each selection and execution.
    No method reads paths, verifies SDK fingerprints or mutates supplied inputs.
    """

    release_id: str
    workload_packs: Mapping[str, Mapping[str, object]]
    dag_specs: Mapping[str, Mapping[str, object]]
    runtime_payloads: Mapping[str, Mapping[str, object]]

    @classmethod
    def from_release(cls, release: Mapping[str, object], expected_release_id: str) -> DbtLegacyEvidencePlan:
        if (
            release.get("schema") != "dpone.release-set.v2"
            or release.get("release_id") != expected_release_id
            or compute_release_id(dict(release)) != expected_release_id
            or dbt_release_producer_violation(release) is not None
        ):
            raise DbtDevEvidenceReleaseError("release identity is invalid")
        artifacts = _mapping(release.get("artifacts"), "release artifacts")
        return cls(
            expected_release_id,
            _artifact_map(artifacts.get("workload_packs"), "workload packs"),
            _artifact_map(artifacts.get("dag_specs"), "DAG specs"),
            _artifact_map(artifacts.get("runtime_payloads"), "runtime payloads"),
        )

    def source_artifacts(self) -> tuple[Mapping[str, object], Mapping[str, object]]:
        """Check these only after the service has verified all DAG bytes."""

        project = _required_artifact(self.runtime_payloads, "dbt_project", "dbt_project_bundle")
        manifest = _required_artifact(self.runtime_payloads, "dbt_manifest", "dbt_manifest")
        return project, manifest

    def selection_descriptor(self, workflow_id: str) -> Mapping[str, object]:
        return _required_artifact(self.runtime_payloads, f"dbt_selection_{workflow_id}", "dbt_selection_lock")

    @staticmethod
    def selection_from_bytes(
        descriptor: Mapping[str, object], payload: bytes, manifest: Mapping[str, object]
    ) -> DbtSelectionLock:
        if len(payload) != descriptor.get("bytes") or sha256_bytes(payload) != descriptor.get("sha256"):
            raise DbtDevEvidenceReleaseError("selection descriptor differs from selection lock bytes")
        lock = DbtSelectionLock.from_mapping(_object(payload, "selection lock"))
        if lock.manifest_sha256 != manifest["sha256"]:
            raise DbtDevEvidenceReleaseError("selection lock and manifest identity differ")
        return lock

    def workflow_expectation(
        self,
        execution: DbtExecutionPack,
        *,
        workflow_id: str,
        workload_id: str,
        lock: DbtSelectionLock,
        project: Mapping[str, object],
        manifest: Mapping[str, object],
        workflow_dags: Mapping[str, ExpectedWorkflowDag],
    ) -> ExpectedDbtWorkflow:
        """Bind execution to selection before demanding matching DAG membership."""

        if execution.workflow_id != workflow_id or execution.selection_lock != lock:
            raise DbtDevEvidenceReleaseError("dbt execution pack differs from release selection")
        workflow_dag = _required_workflow_dag(workflow_dags, workflow_id, workload_id)
        return expected_dbt_workflow(
            execution,
            workload_pack_sha256=_digest(self.workload_packs[workload_id].get("sha256"), "workload pack sha256"),
            project_bundle_sha256=_digest(project.get("sha256"), "project bundle sha256"),
            manifest_sha256=_digest(manifest.get("sha256"), "manifest sha256"),
            workflow_dag=workflow_dag,
        )

    def finish(self, workflows: Mapping[str, ExpectedDbtWorkflow]) -> ExpectedDbtRelease:
        if not workflows:
            raise DbtDevEvidenceReleaseError("release contains no dbt workflow packs")
        return ExpectedDbtRelease(
            release_id=self.release_id,
            required_workloads={
                item_id: _digest(item.get("sha256"), "workload pack sha256")
                for item_id, item in sorted(self.workload_packs.items())
            },
            dbt_workflows=workflows,
        )


def expected_dbt_workflow(
    execution: DbtExecutionPack,
    *,
    workload_pack_sha256: str,
    project_bundle_sha256: str,
    manifest_sha256: str,
    workflow_dag: ExpectedWorkflowDag,
) -> ExpectedDbtWorkflow:
    return ExpectedDbtWorkflow(
        workflow_id=execution.workflow_id,
        workload_id=f"dbt__{execution.workflow_id}",
        workload_pack_sha256=workload_pack_sha256,
        project_bundle_sha256=project_bundle_sha256,
        manifest_sha256=manifest_sha256,
        selection_sha256=execution.selection_lock.selection_sha256,
        toolchain_sha256=execution.selection_lock.toolchain_sha256,
        invocation_context_sha256=(execution.invocation_context.invocation_context_sha256),
        logical_target_sha256=dbt_target_identity_sha256(execution.profile),
        adapter_runtime=execution.adapter_runtime,
        adapter_policy_sha256=execution.adapter_policy.adapter_policy_sha256,
        graph_policy_sha256=execution.selection_lock.graph_policy_sha256,
        dbt_warning_policy=execution.dbt_warning_policy,
        dbt_version=execution.dbt_core_version,
        dbt_schema_version=(f"https://schemas.getdbt.com/dbt/run-results/{execution.run_results_schema_version}.json"),
        expected_run_result_unique_ids=execution.selection_lock.expected_run_result_unique_ids,
        expected_workload_ids=workflow_dag.workload_ids,
        expected_terminal_task_ids=workflow_dag.terminal_task_ids,
        dag_id=workflow_dag.dag_id,
    )


def airflow_evidence_target_binding(
    *,
    workflow: ExpectedDbtWorkflow | None,
    identity: AirflowRunIdentity,
    attempt: AirflowAttemptCorrelation,
    correlation: AirflowCorrelation | None,
    release_id: str,
    deployment_id: str,
    expected_pack_sha256: str | None,
    duplicate_workload: bool,
) -> str | None:
    """Match an already validated envelope to the release's expected workload.

    ``None`` preserves the coordinator's single identity-mismatch diagnosis.
    This neither decodes an envelope nor replaces SDK/schema validation. Content
    evidence must still hash the original complete envelope, not these inputs.
    """

    if (
        identity.release_id != release_id
        or identity.deployment_id != deployment_id
        or expected_pack_sha256 != identity.workload_pack.sha256
        or workflow is None
        or attempt.dag_id != workflow.dag_id
        or identity.dag_spec is None
        or identity.dag_spec.id != workflow.dag_id
        or duplicate_workload
        or (
            correlation is not None
            and (
                not correlation.complete
                or correlation.artifacts.workload_id != identity.workload_pack.id
                or correlation.artifacts.release_id != release_id
                or correlation.artifacts.deployment_id != deployment_id
                or attempt != correlation.airflow
            )
        )
    ):
        return None
    return dbt_target_binding_identity_sha256(
        logical_target_sha256=workflow.logical_target_sha256, run_identity=identity
    )


def _artifact_map(value: object, field: str) -> dict[str, Mapping[str, object]]:
    if not isinstance(value, list) or not value:
        raise DbtDevEvidenceReleaseError(f"{field} must be a non-empty array")
    result: dict[str, Mapping[str, object]] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise DbtDevEvidenceReleaseError(f"{field} contains an invalid descriptor")
        item_id = _required_text(item.get("id"), f"{field} id")
        if item_id in result:
            raise DbtDevEvidenceReleaseError(f"{field} contains a duplicate id")
        result[item_id] = item
    return result


def _required_artifact(
    artifacts: Mapping[str, Mapping[str, object]],
    item_id: str,
    kind: str,
) -> Mapping[str, object]:
    item = artifacts.get(item_id)
    if item is None or item.get("kind") != kind:
        raise DbtDevEvidenceReleaseError(f"required {kind} artifact is missing")
    _digest(item.get("sha256"), f"{kind} sha256")
    _required_text(item.get("path"), f"{kind} path")
    return item


__all__ = [
    "DbtLegacyEvidencePlan",
    "ExpectedDbtRelease",
    "ExpectedDbtWorkflow",
    "expected_dbt_workflow",
    "airflow_evidence_target_binding",
]
