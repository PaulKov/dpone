from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_connection_bridge_models import GitOpsAirflowConnectionBridge
from dpone.gitops.airflow_git_sync_models import GitOpsAirflowGitSyncContract
from dpone.gitops.models import GitOpsIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRuntimeResources:
    requests: dict[str, str]
    limits: dict[str, str]

    def to_jsonable(self) -> dict[str, dict[str, str]]:
        return {
            "requests": dict(self.requests),
            "limits": dict(self.limits),
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifactSink:
    kind: str
    path: str | None

    def to_jsonable(self) -> dict[str, str | None]:
        return {
            "kind": self.kind,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRuntimeProfile:
    bundle_path: str
    run_spec_path: str
    runtime_evidence_path: str
    xcom_summary_path: str
    dag_factory_path: str
    outcome_gate_path: str
    image: str
    namespace: str
    service_account: str
    resources: GitOpsAirflowRuntimeResources
    artifact_sink: GitOpsAirflowArtifactSink
    runner_policy: str
    bundle_digest: str | None = None
    image_digest: str | None = None
    env: tuple[dict[str, str], ...] = ()
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None
    git_sync: GitOpsAirflowGitSyncContract | None = None
    connection_bridge: GitOpsAirflowConnectionBridge | None = None
    outcome_mode: str = "strict_fail"
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_runtime_profile"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow runtime-profile"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def with_issues(
        self,
        *,
        warnings: tuple[GitOpsIssue, ...] | None = None,
        blockers: tuple[GitOpsIssue, ...] | None = None,
    ) -> GitOpsAirflowRuntimeProfile:
        return GitOpsAirflowRuntimeProfile(
            bundle_path=self.bundle_path,
            run_spec_path=self.run_spec_path,
            runtime_evidence_path=self.runtime_evidence_path,
            xcom_summary_path=self.xcom_summary_path,
            dag_factory_path=self.dag_factory_path,
            outcome_gate_path=self.outcome_gate_path,
            image=self.image,
            namespace=self.namespace,
            service_account=self.service_account,
            resources=self.resources,
            artifact_sink=self.artifact_sink,
            runner_policy=self.runner_policy,
            bundle_digest=self.bundle_digest,
            image_digest=self.image_digest,
            env=self.env,
            labels=self.labels,
            annotations=self.annotations,
            git_sync=self.git_sync,
            connection_bridge=self.connection_bridge,
            outcome_mode=self.outcome_mode,
            warnings=warnings if warnings is not None else self.warnings,
            blockers=blockers if blockers is not None else self.blockers,
            kind=self.kind,
            schema_version=self.schema_version,
            producer=self.producer,
        )

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "bundle_path": self.bundle_path,
            "bundle_digest": self.bundle_digest,
            "run_spec_path": self.run_spec_path,
            "runtime_evidence_path": self.runtime_evidence_path,
            "xcom_summary_path": self.xcom_summary_path,
            "dag_factory_path": self.dag_factory_path,
            "outcome_gate_path": self.outcome_gate_path,
            "image": self.image,
            "image_digest": self.image_digest,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "resources": self.resources.to_jsonable(),
            "artifact_sink": self.artifact_sink.to_jsonable(),
            "env": [dict(item) for item in self.env],
            "labels": dict(self.labels or {}),
            "annotations": dict(self.annotations or {}),
            "runner_policy": self.runner_policy,
            "outcome_mode": self.outcome_mode,
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


@dataclass(frozen=True, slots=True)
class GitOpsAirflowXComSummary:
    runtime_profile_path: str
    run_spec_path: str
    runtime_evidence_path: str
    status: str = "planned"
    failed_step: str | None = None
    runtime_evidence_sha256: str | None = None
    runtime_evidence: dict[str, Any] | None = None
    step_counts: dict[str, int] | None = None
    artifact_paths: dict[str, str] | None = None
    warnings: tuple[GitOpsIssue, ...] = ()
    interval: dict[str, Any] | None = None
    """Logical data interval of the DAG run (see dpone.contracts.run_interval)."""
    backfill: dict[str, Any] | None = None
    """Bounded chunked-backfill progress summary (no per-chunk list)."""
    recovery: dict[str, Any] | None = None
    """Bounded operator recovery contract for a non-retryable runtime outcome."""
    run_identity: dict[str, Any] | None = None
    """Verified non-secret dpone artifact identity for this workload attempt."""
    deployment_identity: dict[str, str] | None = None
    """Exact cache activation occurrence, separate from immutable artifact identity."""
    dbt_execution_evidence_ref: dict[str, Any] | None = None
    """Bounded descriptor of exact dbt evidence stored outside XCom."""
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_xcom_summary"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow runtime-profile"

    @property
    def passed(self) -> bool:
        return self.status != "failed" and not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "status": self.status,
            "runtime_profile_path": self.runtime_profile_path,
            "run_spec_path": self.run_spec_path,
            "runtime_evidence_path": self.runtime_evidence_path,
            "failed_step": self.failed_step,
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }
        if self.runtime_evidence_sha256 is not None:
            payload["runtime_evidence_sha256"] = self.runtime_evidence_sha256
        if self.runtime_evidence is not None:
            payload["runtime_evidence"] = dict(self.runtime_evidence)
        if self.step_counts is not None:
            payload["step_counts"] = dict(self.step_counts)
        if self.artifact_paths is not None:
            payload["artifact_paths"] = dict(self.artifact_paths)
        if self.interval is not None:
            payload["interval"] = dict(self.interval)
        if self.backfill is not None:
            payload["backfill"] = dict(self.backfill)
        if self.recovery is not None:
            payload["recovery"] = dict(self.recovery)
        if self.run_identity is not None:
            payload["run_identity"] = dict(self.run_identity)
        if self.deployment_identity is not None:
            payload["deployment_identity"] = dict(self.deployment_identity)
        if self.dbt_execution_evidence_ref is not None:
            payload["dbt_execution_evidence_ref"] = dict(self.dbt_execution_evidence_ref)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "GitOpsAirflowArtifactSink",
    "GitOpsAirflowRuntimeProfile",
    "GitOpsAirflowRuntimeResources",
    "GitOpsAirflowXComSummary",
]
