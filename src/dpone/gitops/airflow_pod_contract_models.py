from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_connection_bridge_models import GitOpsAirflowConnectionBridge
from dpone.gitops.airflow_git_sync_models import GitOpsAirflowGitSyncContract
from dpone.gitops.models import GitOpsIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowXComContract:
    enabled: bool
    return_path: str
    summary_path: str
    mode: str = "final_outcome"
    outcome_mode: str = "strict_fail"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "return_path": self.return_path,
            "summary_path": self.summary_path,
            "mode": self.mode,
            "outcome_mode": self.outcome_mode,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodContract:
    bundle_path: str
    run_spec_path: str
    runtime_profile_path: str
    pod_spec_path: str
    kpo_kwargs_path: str
    image: str
    namespace: str
    service_account: str
    xcom: GitOpsAirflowXComContract
    pod_spec: dict[str, Any]
    kpo_kwargs: dict[str, Any]
    git_sync: GitOpsAirflowGitSyncContract | None = None
    connection_bridge: GitOpsAirflowConnectionBridge | None = None
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_pod_contract"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow pod-contract"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "bundle_path": self.bundle_path,
            "run_spec_path": self.run_spec_path,
            "runtime_profile_path": self.runtime_profile_path,
            "pod_spec_path": self.pod_spec_path,
            "kpo_kwargs_path": self.kpo_kwargs_path,
            "image": self.image,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "xcom": self.xcom.to_jsonable(),
            "pod_spec": dict(self.pod_spec),
            "kpo_kwargs": dict(self.kpo_kwargs),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }
        if self.git_sync is not None:
            payload["git_sync"] = self.git_sync.to_jsonable()
        if self.connection_bridge is not None:
            payload["connection_bridge"] = self.connection_bridge.to_jsonable()
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "GitOpsAirflowPodContract",
    "GitOpsAirflowXComContract",
]
