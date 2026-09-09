from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_K8S_MANIFESTS_SOURCE = "dpone gitops airflow k8s-manifests"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowK8sManifestObject:
    api_version: str
    kind: str
    name: str
    namespace: str
    source: str
    required: bool
    body: Mapping[str, Any]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "api_version": self.api_version,
            "kind": self.kind,
            "name": self.name,
            "namespace": self.namespace,
            "source": self.source,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowK8sManifestsReport:
    artifact_dir: str
    manifest_path: str
    runtime_profile_path: str
    pod_contract_path: str
    connection_bridge_plan_path: str | None
    gitops_controller: str
    namespace: str
    service_account: str
    objects: tuple[GitOpsAirflowK8sManifestObject, ...]
    controller_hints: tuple[str, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_k8s_manifests"
    schema_version: str = "1"
    producer: str = AIRFLOW_K8S_MANIFESTS_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    @property
    def documents(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(obj.body for obj in self.objects)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "manifest_path": self.manifest_path,
            "runtime_profile_path": self.runtime_profile_path,
            "pod_contract_path": self.pod_contract_path,
            "connection_bridge_plan_path": self.connection_bridge_plan_path,
            "gitops_controller": self.gitops_controller,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "objects": [obj.to_jsonable() for obj in self.objects],
            "controller_hints": list(self.controller_hints),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


def airflow_k8s_manifests_issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_K8S_MANIFESTS_SOURCE)


__all__ = [
    "AIRFLOW_K8S_MANIFESTS_SOURCE",
    "GitOpsAirflowK8sManifestObject",
    "GitOpsAirflowK8sManifestsReport",
    "airflow_k8s_manifests_issue",
]
