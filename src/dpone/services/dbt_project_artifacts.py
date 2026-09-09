"""Project one validated dbt project before singleton or workspace publication."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import yaml

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_contract_validation import artifact_json_bytes as artifact_json_bytes
from dpone.contracts.dbt_project_artifacts import (
    DbtProjectArtifacts as DbtProjectArtifacts,
)
from dpone.contracts.dbt_project_artifacts import (
    DbtProjectRuntimePayloads,
    DbtReleaseInputs,
    project_runtime_payloads,
)
from dpone.contracts.dbt_project_artifacts import (
    _report_identity as _report_identity,
)
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.readiness.dbt_airflow_artifact_projection import (
    dag_spec,
    dbt_pool,
    dbt_task_executor,
    is_semantic_refresh_workflow,
    optional_mapping,
    route_certifications,
    semantic_refresh_pre_release_bundles,
    semantic_refresh_topology_template,
    workload_definition,
    xcom_sidecar_image,
)
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder, strict_transfer_pack
from dpone.readiness.dbt_airflow_pack_adapter import AirflowPackBuilderPort, DbtAirflowPackBuilder
from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
    SemanticRefreshTemplateProofAuthority,
    semantic_refresh_template_pack,
)
from dpone.services.dbt_release_builder import build_release_inputs

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtCompileReport
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPreReleaseProofBundle
    from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
    from dpone.ports.dbt_publishing import DbtProjectPolicyValidator
    from dpone.ports.dbt_selection import DbtSelectionResolver

_LOGICAL_OUTPUT_ROOT = ".dpone/gitops/airflow"


class DbtProjectArtifactProjector:
    """Capture once, derive payload identities, then build packs without mutation.

    This service never publishes an output directory or creates a release ID.
    Both release assemblers consume the same projection and source snapshot.
    """

    def __init__(
        self,
        *,
        selection_resolver: DbtSelectionResolver,
        bundle_operations: DbtProjectBundleOperations,
        project_policy: DbtProjectPolicyValidator,
        pack_builder: AirflowPackBuilderPort | None = None,
        dbt_pack_builder: DbtAirflowExecutionPackBuilder | None = None,
    ) -> None:
        self._selection = selection_resolver
        self._bundles = bundle_operations
        self._policy = project_policy
        self._packs = pack_builder if pack_builder is not None else DbtAirflowPackBuilder()
        self._dbt_packs = dbt_pack_builder if dbt_pack_builder is not None else DbtAirflowExecutionPackBuilder()

    def project(
        self,
        report: DbtCompileReport,
        *,
        project_root: Path,
        wire_contract: str = DBT_RUNTIME_WIRE_V1,
        manifest_payload: bytes | None = None,
        profiles_dir: Path | None = None,
        semantic_refresh_pre_release_bundles: Mapping[str, SemanticRefreshPreReleaseProofBundle] | None = None,
    ) -> DbtProjectArtifacts:
        if not report.passed or not report.models or not report.workflows:
            raise ValueError("project projection requires a nonempty successful compile")
        if wire_contract not in {DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2}:
            raise ValueError("unsupported dbt project wire contract")
        if wire_contract == DBT_RUNTIME_WIRE_V2 and any(
            model.profile.semantic_refresh is not None for model in report.models
        ):
            raise DbtPublishingError(
                "DPONE_DBT_COMPILE_FAILED",
                "Workspace releases do not admit non-executable semantic-refresh templates",
                path="workspace",
                remediation="Keep semantic-refresh templates on the existing singleton path; do not remove their checks or emit a partial workspace release.",
            )
        report_identity = _report_identity(report)
        receipts = artifact_json_bytes(route_certifications(report))
        inputs = build_release_inputs(
            report,
            project_root=project_root,
            selection_resolver=self._selection,
            bundle_builder=self._bundles,
            bundle_extractor=self._bundles,
            bundle_verifier=self._bundles,
            project_policy=self._policy,
            profiles_dir=profiles_dir,
            manifest_payload=manifest_payload,
            wire_contract=wire_contract,
        )
        payloads = project_runtime_payloads(inputs, wire_contract=wire_contract)
        with TemporaryDirectory(prefix="dpone-dbt-project-") as directory:
            result = self._project_at(
                report,
                inputs,
                payloads,
                Path(directory),
                semantic_refresh_pre_release_bundles,
                report_identity,
                receipts,
            )
        # Injected compilers/builders must not mutate previously checked inputs.
        result.route_certifications(report)
        return result

    def _project_at(
        self,
        report: DbtCompileReport,
        inputs: DbtReleaseInputs,
        payloads: DbtProjectRuntimePayloads,
        build_root: Path,
        semantic_proofs: Mapping[str, SemanticRefreshPreReleaseProofBundle] | None,
        report_identity: str,
        receipts: bytes,
    ) -> DbtProjectArtifacts:
        files: dict[str, bytes] = {}
        artifacts: dict[str, str] = {}
        packs: dict[str, bytes] = {}
        dags: dict[str, bytes] = {}
        pack_payload_ids: dict[str, tuple[str, ...]] = {}
        proofs = semantic_refresh_pre_release_bundles(report, semantic_proofs)
        for model in report.models:
            if model.profile.semantic_refresh is not None:
                continue
            path = f"_dbt/manifests/{model.workload_id}.yaml"
            payload = yaml.safe_dump(model.manifest, allow_unicode=True, sort_keys=False).encode("utf-8")
            _insert(files, path, payload)
            source = build_root / path
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(payload)
            artifacts[f"manifest:{model.workload_id}"] = path
            pack_path = f"{model.workload_id}/airflow-pack.json"
            pack = self._packs.build(
                workload=workload_definition(model, path),
                output_path=f"{_LOGICAL_OUTPUT_ROOT}/{pack_path}",
                repo_root=build_root,
                mode="build",
                runner_policy="release",
                include_live_gates=True,
                outlet_binding="logical",
            )
            if not pack.passed:
                raise ValueError("generated Airflow pack is blocked")
            content = artifact_json_bytes(
                strict_transfer_pack(pack.to_jsonable(), xcom_sidecar_image=xcom_sidecar_image(model))
            )
            _insert(files, pack_path, content)
            _insert(packs, model.workload_id, content)
            artifacts[f"pack:{model.workload_id}"] = pack_path
        for workflow in report.workflows:
            ids = payloads.workflow_ids[workflow.workflow]
            if is_semantic_refresh_workflow(workflow):
                pack_id = f"semantic__{workflow.workflow}"
                pack_payload = semantic_refresh_template_pack(
                    workflow_id=workflow.workflow,
                    execution_pack=inputs.execution_packs[workflow.workflow],
                    runtime_payload_ids=ids,
                    topology=semantic_refresh_topology_template(workflow),
                    proof_authority=SemanticRefreshTemplateProofAuthority.from_pre_release(proofs[workflow.workflow]),
                )
            else:
                pack_id = f"dbt__{workflow.workflow}"
                pack_payload = self._dbt_packs.build(
                    workflow_id=workflow.workflow,
                    execution_pack=inputs.execution_packs[workflow.workflow],
                    runtime_payload_ids=ids,
                    xcom_sidecar_image=xcom_sidecar_image(workflow.models[0]),
                    pool=dbt_pool(workflow),
                    pod_spec=optional_mapping(workflow.models[0].profile.runtime.get("pod_spec")),
                    executor=dbt_task_executor(workflow.models[0]),
                )
                dag_path = f"_dags/{workflow.dag_id}.dag-spec.json"
                dag_content = artifact_json_bytes(dag_spec(workflow))
                _insert(files, dag_path, dag_content)
                _insert(dags, workflow.dag_id, dag_content)
                artifacts[f"dag:{workflow.dag_id}"] = dag_path
            pack_path = f"{pack_id}/airflow-pack.json"
            content = artifact_json_bytes(pack_payload)
            _insert(files, pack_path, content)
            _insert(packs, pack_id, content)
            pack_payload_ids[pack_id] = ids
            artifacts[f"pack:{pack_id}"] = pack_path
        for path, content in payloads.files.items():
            _insert(files, path, content)
        for path in sorted(payloads.files):
            artifacts[f"runtime:{path.removeprefix('runtime/dbt/')}"] = path
        for dag_id, content in dags.items():
            _insert(files, f"dags/{dag_id}.dag-spec.json", content)
        for pack_id, content in packs.items():
            _insert(files, f"packs/{pack_id}.airflow-pack.json", content)
        return DbtProjectArtifacts(
            inputs, files, artifacts, packs, dags, pack_payload_ids, payloads, report_identity, receipts
        )


def _insert(files: dict[str, bytes], key: str, payload: bytes) -> None:
    if key in files:
        raise ValueError("generated project artifact identity collides")
    files[key] = payload
