from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.airflow_compact_pack_bootstrap import RuntimePayloadBuilder, RuntimeWorkloadPayload
    from dpone.gitops.airflow_compact_process_plans import GitOpsAirflowProcessPlan
    from dpone.gitops.airflow_pack_models import GitOpsAirflowPackStep
    from dpone.gitops.airflow_runner_contract import GitOpsAirflowRunnerContract
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
    from dpone.gitops.workload_dependencies import WorkloadFileDependency
    from dpone.manifest.runtime_materialization import RuntimeManifestMaterialization


import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)
from dpone_airflow_pack.provider_execution_contract import (
    WORKLOAD_ID_METADATA_KEY,
    kubernetes_label_value,
    kubernetes_pod_name,
)

from dpone.gitops.airflow_asset_uri import ResolvedMssqlAssetRegistry
from dpone.gitops.airflow_compact_errors import AirflowCompactPackBuildError, project_compact_connections
from dpone.gitops.airflow_compact_pack_bootstrap import (
    InlineWorkloadConfigurationError,
    InlineWorkloadDependencyError,
    inline_workload_bootstrap,
    runtime_workload_bootstrap,
    runtime_workload_payload_builder,
)
from dpone.gitops.airflow_compact_pack_helpers import (
    compact_pack_artifact_index,
    compact_pack_execution_policy,
    compact_pack_image_pull_secrets,
    compact_pack_outcome_gate,
    compact_pack_pod_annotations,
    compact_pack_runtime_image_pull_policy,
    compact_pack_workload_dependencies,
    compact_pack_xcom,
    dict_mapping,
)
from dpone.gitops.airflow_compact_pack_outlets import OutletBinding, execution_policy_with_inferred_outlets
from dpone.gitops.airflow_compact_process_plans import (
    compact_pack_kpo_field,
    process_plans_jsonable,
    resolve_compact_process_projection,
)
from dpone.gitops.airflow_compact_runtime import compact_kpo_kwargs, compact_provider_execution
from dpone.gitops.airflow_runner_contract import resolve_airflow_runner_contract
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue
from dpone.gitops.workload_dependencies import WorkloadDependencyError
from dpone.manifest.airflow_resources import workload_airflow_resources
from dpone.manifest.runtime_materialization import RuntimeManifestMaterializationError, materialize_runtime_manifest

COMPACT_PACK_SOURCE = "dpone gitops airflow pack"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowCompactPackReport:
    workload: GitOpsWorkloadDefinition
    output_path: str
    artifact_dir: str
    bundle_path: str
    kpo_kwargs: dict[str, Any]
    provider_execution: dict[str, Any]
    artifact_index: dict[str, str]
    runtime_manifest: RuntimeManifestMaterialization
    runtime_payload: RuntimeWorkloadPayload | None
    steps: tuple[GitOpsAirflowPackStep, ...]
    process_plans: tuple[GitOpsAirflowProcessPlan, ...] = ()
    airflow_execution: dict[str, Any] = field(default_factory=dict)
    workload_dependencies: tuple[WorkloadFileDependency, ...] = ()
    runtime_command: str = ""
    pod_spec: dict[str, Any] | None = None
    connection_projection: dict[str, Any] | None = None
    xcom: dict[str, Any] | None = None
    outcome_gate: dict[str, Any] | None = None
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    mode: str = "plan"
    runner_policy: str = "advisory"
    include_live_gates: bool = False
    kind: str = "gitops.airflow_pack"
    schema_version: str = "3"
    producer: str = COMPACT_PACK_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        effective_config = dict(self.workload.effective_config)
        runtime_bootstrap = runtime_workload_bootstrap(
            runtime_manifest_path=self.runtime_manifest.path,
            workload_id=self.workload.workload_id,
            process_selectors=tuple(plan.selector for plan in self.process_plans),
        )
        runtime_payload = self.runtime_payload.to_jsonable() if self.runtime_payload is not None else None
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
            "producer": self.producer,
            "meta": {"kind": self.kind, "path": self.output_path},
            "airflow": {"execution": self.airflow_execution or compact_pack_execution_policy(effective_config)},
            "artifact_dir": self.artifact_dir,
            "output_path": self.output_path,
            "bundle_path": self.bundle_path,
            "image": self.kpo_kwargs.get("image"),
            "image_digest": effective_config.get("image_digest"),
            "mode": self.mode,
            "runner_policy": self.runner_policy,
            "include_live_gates": self.include_live_gates,
            "artifacts": [],
            "steps": [step.to_jsonable() for step in self.steps],
            "runtime_selection": (
                {
                    "mode": "process_plan",
                    "required_for_selected_nodes": True,
                }
                if self.process_plans
                else {}
            ),
            "process_plans": process_plans_jsonable(self.process_plans),
            **({"mapping_plan": self.process_plans[0].mapping_plan} if len(self.process_plans) == 1 else {}),
            "workload_dependencies": [dependency.to_jsonable() for dependency in self.workload_dependencies],
            "next_actions": [],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
            "workload": self.workload.to_jsonable(),
            "effective_config": effective_config,
            "runtime_command": self.runtime_command,
            "runtime_manifest": self.runtime_manifest.to_jsonable(),
            "runtime_bootstrap": runtime_bootstrap,
            **({"runtime_payload": runtime_payload} if runtime_payload is not None else {}),
            "pod_spec": dict(self.pod_spec or {}),
            "connection_projection": dict(self.connection_projection or {}),
            "xcom": dict(self.xcom or {}),
            "outcome_gate": dict(self.outcome_gate or {}),
            "provider_execution": dict(self.provider_execution),
            compact_pack_kpo_field(self.process_plans): dict(self.kpo_kwargs),
            "artifact_index": dict(sorted(self.artifact_index.items())),
        }
        payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


class AirflowCompactPackBuilder:
    """Build a scheduler-static Airflow pack from an effective workload."""

    def build(
        self,
        *,
        workload: GitOpsWorkloadDefinition,
        output_path: str,
        repo_root: str | Path | None = None,
        mode: str = "plan",
        runner_policy: str | None = None,
        include_live_gates: bool = False,
        env: str = "dev",
        mssql_registry: ResolvedMssqlAssetRegistry | None = None,
        outlet_binding: OutletBinding = "physical",
    ) -> GitOpsAirflowCompactPackReport:
        artifact_dir = output_path.rsplit("/", 1)[0] if "/" in output_path else "."
        effective = workload.effective_config
        resolved_policy = runner_policy or str(effective.get("runner_policy") or "advisory")
        repo_path = Path(repo_root) if repo_root is not None else None
        try:
            dependencies = compact_pack_workload_dependencies(workload=workload, repo_root=repo_path)
            runtime_manifest = materialize_runtime_manifest(
                workload_id=workload.workload_id,
                manifest=workload.manifest,
                repo_root=repo_path,
            )
        except (RuntimeManifestMaterializationError, WorkloadDependencyError) as exc:
            raise AirflowCompactPackBuildError(str(exc)) from exc
        runner_contract = resolve_airflow_runner_contract(
            dict_mapping(effective.get("airflow")),
            repo_root=repo_path,
            workload_id=workload.workload_id,
        )
        bundle_path = f".dpone/gitops/bundle/{workload.workload_id}/bundle.json"
        blockers = list(runner_contract.blockers)
        warnings = list(runner_contract.warnings)
        process_projection = resolve_compact_process_projection(
            workload=workload,
            runtime_manifest_path=runtime_manifest.path,
            runtime_manifest_kind=runtime_manifest.kind,
            repo_root=repo_path,
            output_path=output_path,
        )
        blockers.extend(process_projection.blockers)
        warnings.extend(process_projection.warnings)
        connection_projection = project_compact_connections(process_projection, runner_contract.connection_projection)
        runtime_command = process_projection.runtime_command
        try:
            pod_spec, runtime_payload = _pod_spec(
                workload=workload,
                repo_root=repo_path,
                dependencies=dependencies,
                runtime_command=runtime_command,
                runner_contract=runner_contract,
                runtime_manifest=runtime_manifest,
            )
        except InlineWorkloadConfigurationError as exc:
            raise AirflowCompactPackBuildError(str(exc)) from exc
        except InlineWorkloadDependencyError as exc:
            pod_spec = {}
            runtime_payload = None
            blockers.append(
                GitOpsWorkloadCatalogIssue(
                    code=exc.code,
                    message="A workload dependency changed while the inline runtime archive was being built.",
                    path=exc.path,
                    source=COMPACT_PACK_SOURCE,
                )
            )
        if runtime_manifest.kind == "unmaterialized_manifest":
            blockers.append(
                GitOpsWorkloadCatalogIssue(
                    code="runtime_manifest_repo_root_required",
                    message="A runnable Airflow pack requires repo_root to materialize its runtime manifest.",
                    path=workload.manifest,
                    source=COMPACT_PACK_SOURCE,
                )
            )
        airflow_execution, outlet_warnings, outlet_blockers = execution_policy_with_inferred_outlets(
            workload=workload,
            execution=compact_pack_execution_policy(workload.effective_config),
            repo_root=repo_path,
            env=env,
            mssql_registry=mssql_registry,
            outlet_binding=outlet_binding,
        )
        warnings.extend(outlet_warnings)
        blockers.extend(outlet_blockers)
        kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
        return GitOpsAirflowCompactPackReport(
            workload=workload,
            output_path=output_path,
            artifact_dir=artifact_dir,
            bundle_path=bundle_path,
            kpo_kwargs=kpo_kwargs,
            provider_execution=compact_provider_execution(
                kpo_kwargs=kpo_kwargs,
                pod_spec=pod_spec,
                retry_authority=process_projection.retry_authority,
            ),
            artifact_index=compact_pack_artifact_index(
                workload_id=workload.workload_id,
                output_path=output_path,
            ),
            runtime_manifest=runtime_manifest,
            runtime_payload=runtime_payload,
            steps=process_projection.steps,
            process_plans=process_projection.plans,
            airflow_execution=airflow_execution,
            workload_dependencies=dependencies,
            runtime_command=runtime_command,
            pod_spec=pod_spec,
            connection_projection=connection_projection,
            xcom=compact_pack_xcom(workload),
            outcome_gate=compact_pack_outcome_gate(workload),
            mode=mode,
            runner_policy=resolved_policy,
            include_live_gates=include_live_gates,
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


def _pod_spec(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path | None,
    dependencies: tuple[WorkloadFileDependency, ...],
    runtime_command: str,
    runner_contract: GitOpsAirflowRunnerContract,
    runtime_manifest: RuntimeManifestMaterialization,
) -> tuple[dict[str, Any], RuntimeWorkloadPayload | None]:
    contract = runner_contract
    config = workload.effective_config
    resources = workload_airflow_resources(config)
    airflow = dict_mapping(config.get("airflow"))
    image = str(config.get("image") or "<IMAGE>")
    namespace = str(config.get("namespace") or "default")
    service_account = str(airflow.get("service_account_name") or "default")
    image_pull_policy = compact_pack_runtime_image_pull_policy(image)
    node_selector = contract.placement.node_selector
    tolerations = contract.placement.tolerations
    runner_embed_paths = contract.embed_paths
    payload_builder: RuntimePayloadBuilder | None = None
    pod: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": kubernetes_pod_name(workload.workload_id),
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/component": "dpone-runner",
                WORKLOAD_ID_METADATA_KEY: kubernetes_label_value(workload.workload_id),
            },
            "annotations": {
                **compact_pack_pod_annotations(dependencies),
                WORKLOAD_ID_METADATA_KEY: workload.workload_id,
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": service_account,
            "containers": [
                {
                    "name": "base",
                    "image": image,
                    "imagePullPolicy": image_pull_policy,
                    "command": ["/bin/sh", "-ec"],
                    "args": [runtime_command],
                    "volumeMounts": [{"name": "dpone-worktree", "mountPath": "/workspace", "readOnly": False}],
                    "workingDir": "/workspace/repo",
                }
            ],
            "volumes": [{"name": "dpone-worktree", "emptyDir": {}}],
        },
    }
    if resources is not None:
        pod["spec"]["containers"][0]["resources"] = resources
    pull_secrets = compact_pack_image_pull_secrets(airflow)
    if pull_secrets:
        pod["spec"]["imagePullSecrets"] = [{"name": secret} for secret in pull_secrets]
    if node_selector:
        pod["spec"]["nodeSelector"] = dict(node_selector)
    if tolerations:
        pod["spec"]["tolerations"] = [
            {"key": item.key, "operator": "Equal", "value": item.value, "effect": item.effect} for item in tolerations
        ]
    if repo_root is not None and not contract.blockers:
        payload_builder = runtime_workload_payload_builder(
            workload=workload,
            repo_root=repo_root,
            dependencies=dependencies,
            runner_embed_paths=runner_embed_paths,
            generated_files=(
                {runtime_manifest.path: runtime_manifest.content} if runtime_manifest.content is not None else None
            ),
        )
        pod["spec"]["initContainers"] = [
            inline_workload_bootstrap(
                workload=workload,
                repo_root=repo_root,
                image=image,
                dependencies=dependencies,
                runner_embed_paths=runner_embed_paths,
                payload_builder=payload_builder,
            )
        ]
    runtime_payload = payload_builder.build() if payload_builder is not None else None
    return pod, runtime_payload


__all__ = ["AirflowCompactPackBuildError", "AirflowCompactPackBuilder", "GitOpsAirflowCompactPackReport"]
