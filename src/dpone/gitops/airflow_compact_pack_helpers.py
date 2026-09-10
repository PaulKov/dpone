"""Pure serialization helpers for compact Airflow packs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.gitops.airflow_asset_partition import normalize_asset_items
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.gitops.workload_dependencies import WorkloadDependencyResolver, WorkloadFileDependency


def compact_pack_artifact_index(*, workload_id: str, output_path: str) -> dict[str, str]:
    return {
        "airflow_pack": output_path,
        "runtime_evidence": f".dpone/runs/{workload_id}/runtime-evidence.json",
        "xcom_summary": f".dpone/runs/{workload_id}/xcom-summary.json",
    }


def dict_mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def compact_pack_xcom(workload: GitOpsWorkloadDefinition) -> dict[str, Any]:
    airflow = dict_mapping(workload.effective_config.get("airflow"))
    sidecar = str(airflow.get("xcom_sidecar_image") or "").strip()
    return {"sidecar_image": sidecar} if sidecar else {}


def compact_pack_outcome_gate(workload: GitOpsWorkloadDefinition) -> dict[str, Any]:
    return {"task_id": f"{workload.workload_id}__dpone_outcome_gate", "required_status": "passed"}


def compact_pack_workload_dependencies(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path | None,
) -> tuple[WorkloadFileDependency, ...]:
    if repo_root is None:
        return ()
    return WorkloadDependencyResolver().resolve(repo_root=repo_root, manifest=workload.manifest)


def compact_pack_pod_annotations(dependencies: tuple[WorkloadFileDependency, ...]) -> dict[str, str]:
    if not dependencies:
        return {}
    return {
        "dpone.dev/workload-dependencies": ",".join(dependency.path for dependency in dependencies),
    }


def compact_pack_image_pull_secrets(airflow: dict[str, Any]) -> tuple[str, ...]:
    raw = airflow.get("image_pull_secrets", airflow.get("imagePullSecrets"))
    if raw is None:
        raw = airflow.get("image_pull_secret")
    if isinstance(raw, str):
        return (raw,) if raw.strip() else ()
    if isinstance(raw, list):
        secrets: list[str] = []
        for item in raw:
            if isinstance(item, dict) and item.get("name"):
                secrets.append(str(item["name"]))
            elif item:
                secrets.append(str(item))
        return tuple(secrets)
    return ()


def compact_pack_runtime_image_pull_policy(image: str) -> str:
    if "@sha256:" in image:
        return "IfNotPresent"
    image_name = image.rsplit("/", maxsplit=1)[-1]
    if ":" not in image_name:
        return "Always"
    tag = image_name.rsplit(":", maxsplit=1)[-1].lower()
    if tag in {"latest", "master", "main"}:
        return "Always"
    return "IfNotPresent"


def compact_pack_execution_policy(effective_config: dict[str, Any]) -> dict[str, Any]:
    airflow = dict_mapping(effective_config.get("airflow"))
    execution = dict_mapping(airflow.get("execution"))
    normalized = dict(execution)
    for asset_field in ("inlets", "outlets"):
        if asset_field in normalized:
            normalized[asset_field] = normalize_asset_items(normalized[asset_field])
    return normalized


__all__ = [
    "compact_pack_execution_policy",
    "compact_pack_artifact_index",
    "compact_pack_image_pull_secrets",
    "compact_pack_outcome_gate",
    "compact_pack_pod_annotations",
    "compact_pack_runtime_image_pull_policy",
    "compact_pack_workload_dependencies",
    "compact_pack_xcom",
    "dict_mapping",
]
