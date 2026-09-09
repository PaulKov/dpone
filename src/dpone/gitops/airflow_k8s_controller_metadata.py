from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SUPPORTED_AIRFLOW_K8S_GITOPS_CONTROLLERS = ("plain", "argocd", "flux")

_ARGO_SYNC_WAVES = {
    "ServiceAccount": "-20",
    "Secret": "-20",
    "ExternalSecret": "-20",
    "Role": "-10",
    "RoleBinding": "-5",
    "NetworkPolicy": "0",
}


def normalize_airflow_k8s_gitops_controller(value: object) -> str:
    controller = str(value or "plain").strip().lower()
    return controller or "plain"


def apply_airflow_k8s_gitops_metadata(body: Mapping[str, Any], *, controller: str) -> dict[str, Any]:
    if controller == "plain":
        return dict(body)
    rendered = dict(body)
    metadata = dict(_mapping(rendered.get("metadata")))
    labels = dict(_mapping(metadata.get("labels")))
    labels.update(
        {
            "app.kubernetes.io/managed-by": "dpone",
            "app.kubernetes.io/part-of": "dpone-airflow-runtime",
            "dpone.io/gitops-controller": controller,
        }
    )
    metadata["labels"] = labels
    if controller == "argocd":
        annotations = dict(_mapping(metadata.get("annotations")))
        annotations.setdefault("argocd.argoproj.io/sync-wave", _ARGO_SYNC_WAVES.get(str(body.get("kind") or ""), "0"))
        metadata["annotations"] = annotations
    rendered["metadata"] = metadata
    return rendered


def airflow_k8s_gitops_controller_hints(controller: str) -> tuple[str, ...]:
    if controller == "argocd":
        return (
            "Commit airflow-k8s-manifests.yaml under the Argo CD Application path.",
            "Generated objects include Argo CD sync-wave annotations for deterministic infra ordering.",
            "Keep Secret stringData empty and source secret values from ExternalSecret, SealedSecret, or the cluster secret workflow.",
        )
    if controller == "flux":
        return (
            "Commit airflow-k8s-manifests.yaml under the Flux Kustomization path.",
            "Generated objects include dpone ownership labels; configure prune/wait in the Flux Kustomization CR.",
            "Keep Secret stringData empty and source secret values from ExternalSecret, SOPS, SealedSecret, or the cluster secret workflow.",
        )
    return (
        "Commit or publish airflow-k8s-manifests.yaml beside the Airflow runtime artifacts.",
        "Use the generated manifest pack as ordinary Kubernetes YAML or controller input.",
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "SUPPORTED_AIRFLOW_K8S_GITOPS_CONTROLLERS",
    "airflow_k8s_gitops_controller_hints",
    "apply_airflow_k8s_gitops_metadata",
    "normalize_airflow_k8s_gitops_controller",
]
