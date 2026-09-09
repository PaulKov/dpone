from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_correlation import AirflowCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity, AirflowRunIdentityError
from dpone.gitops.models import GitOpsIssue

AIRFLOW_EVIDENCE_BUNDLE_SOURCE = "dpone gitops airflow evidence-bundle"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowEvidenceArtifact:
    name: str
    path: str
    expected_kind: str
    actual_kind: str | None
    required: bool
    exists: bool
    sha256: str | None
    bytes: int | None
    passed: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "expected_kind": self.expected_kind,
            "actual_kind": self.actual_kind,
            "required": self.required,
            "exists": self.exists,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "passed": self.passed,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowAttemptCorrelation:
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    map_index: int

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "try_number": self.try_number,
            "map_index": self.map_index,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodCorrelation:
    pod_name: str | None
    pod_uid: str | None
    namespace: str | None
    service_account: str | None
    image: str | None
    image_digest: str | None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "pod_name": self.pod_name,
            "pod_uid": self.pod_uid,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "image": self.image,
            "image_digest": self.image_digest,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowEvidenceBundleReport:
    attempt: GitOpsAirflowAttemptCorrelation
    pod: GitOpsAirflowPodCorrelation
    runner_policy: str
    artifacts: tuple[GitOpsAirflowEvidenceArtifact, ...]
    run_identity: AirflowRunIdentity | None = None
    correlation: AirflowCorrelation | None = None
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_evidence_bundle"
    schema_version: str = "1"
    producer: str = AIRFLOW_EVIDENCE_BUNDLE_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers and all(artifact.passed for artifact in self.artifacts if artifact.required)

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "runner_policy": self.runner_policy,
            "attempt": self.attempt.to_jsonable(),
            "pod": self.pod.to_jsonable(),
            "artifacts": [artifact.to_jsonable() for artifact in self.artifacts],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }
        if self.run_identity is not None:
            payload["run_identity"] = self.run_identity.to_dict()
        if self.correlation is not None:
            payload["correlation"] = self.correlation.to_dict()
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "AIRFLOW_EVIDENCE_BUNDLE_SOURCE",
    "AirflowRunIdentity",
    "AirflowRunIdentityError",
    "AirflowCorrelation",
    "GitOpsAirflowAttemptCorrelation",
    "GitOpsAirflowEvidenceArtifact",
    "GitOpsAirflowEvidenceBundleReport",
    "GitOpsAirflowPodCorrelation",
    "GitOpsIssue",
]
