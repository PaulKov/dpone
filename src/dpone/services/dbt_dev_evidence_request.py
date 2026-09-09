"""Canonical campaign request for one release-bound dbt dev evidence set."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    is_canonical_sha256_digest,
)
from dpone.contracts.dbt_dev_evidence_campaign import (
    MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY,
    MAX_DBT_DEV_EVIDENCE_SOURCE_FILES,
    MAX_DBT_DEV_EVIDENCE_WORKFLOWS,
)
from dpone.services.dbt_dev_evidence_release import (
    DbtDevEvidenceReleaseError,
    DbtExpectedReleaseLoader,
    load_expected_dbt_release,
)

DBT_DEV_EVIDENCE_REQUEST_SCHEMA = "dpone.dbt-dev-evidence-request.v1"
DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID = "DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID"
_FULL_COMMIT = re.compile(r"[0-9a-f]{40}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_SAFE_TEXT = re.compile(r"[^\x00-\x1f\x7f]{1,2048}")
_REQUEST_FIELDS = frozenset(
    {
        "schema",
        "evidence_set_id",
        "release_id",
        "deployment_id",
        "producer_repository",
        "producer_workflow",
        "source_commit",
        "orchestration_run_id",
        "orchestration_run_attempt",
        "workflows",
    }
)
_WORKFLOW_FIELDS = frozenset({"workflow_id", "dag_id", "dag_run_id"})


class DbtDevEvidenceRequestError(ValueError):
    """A campaign request cannot authorize one unambiguous evidence set."""

    code = DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceWorkflowRequest:
    """One expected workflow and its deterministic Airflow run identity."""

    workflow_id: str
    dag_id: str
    dag_run_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "workflow_id": self.workflow_id,
            "dag_id": self.dag_id,
            "dag_run_id": self.dag_run_id,
        }


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceRequest:
    """Closed request from which the promotion evidence-set identity is derived."""

    evidence_set_id: str
    release_id: str
    deployment_id: str
    producer_repository: str
    producer_workflow: str
    source_commit: str
    orchestration_run_id: str
    orchestration_run_attempt: int
    workflows: tuple[DbtDevEvidenceWorkflowRequest, ...]
    schema: str = DBT_DEV_EVIDENCE_REQUEST_SCHEMA

    @classmethod
    def build(
        cls,
        *,
        compiled_root: str | Path,
        release_id: str,
        deployment_id: str,
        producer_repository: str,
        producer_workflow: str,
        source_commit: str,
        orchestration_run_id: str,
        orchestration_run_attempt: int,
        release_loader: DbtExpectedReleaseLoader | None = None,
    ) -> DbtDevEvidenceRequest:
        _validate_identity(release_id, "release")
        _validate_identity(deployment_id, "deployment")
        _validate_producer(
            producer_repository=producer_repository,
            producer_workflow=producer_workflow,
            source_commit=source_commit,
            orchestration_run_id=orchestration_run_id,
            orchestration_run_attempt=orchestration_run_attempt,
        )
        try:
            expected = (release_loader or load_expected_dbt_release)(
                Path(compiled_root),
                release_id,
            )
        except (DbtDevEvidenceReleaseError, OSError) as exc:
            raise DbtDevEvidenceRequestError("compiled release cannot define the evidence campaign") from exc
        workflow_identities = tuple(
            sorted(
                (
                    workflow.workflow_id,
                    workflow.dag_id,
                )
                for workflow in expected.dbt_workflows.values()
            )
        )
        required_workload_count = len(expected.required_workloads)
        if (
            not workflow_identities
            or len(workflow_identities) > MAX_DBT_DEV_EVIDENCE_WORKFLOWS
            or required_workload_count > MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY
            or required_workload_count + 2 * len(workflow_identities) > MAX_DBT_DEV_EVIDENCE_SOURCE_FILES
            or len({item[0] for item in workflow_identities}) != len(workflow_identities)
            or len({item[1] for item in workflow_identities}) != len(workflow_identities)
        ):
            raise DbtDevEvidenceRequestError("compiled release has an ambiguous workflow inventory")
        unsigned = _identity_payload(
            release_id=release_id,
            deployment_id=deployment_id,
            producer_repository=producer_repository,
            producer_workflow=producer_workflow,
            source_commit=source_commit,
            orchestration_run_id=orchestration_run_id,
            orchestration_run_attempt=orchestration_run_attempt,
            workflows=workflow_identities,
        )
        evidence_set_id = canonical_fingerprint(unsigned)
        return cls(
            evidence_set_id=evidence_set_id,
            release_id=release_id,
            deployment_id=deployment_id,
            producer_repository=producer_repository,
            producer_workflow=producer_workflow,
            source_commit=source_commit,
            orchestration_run_id=orchestration_run_id,
            orchestration_run_attempt=orchestration_run_attempt,
            workflows=tuple(
                DbtDevEvidenceWorkflowRequest(
                    workflow_id=workflow_id,
                    dag_id=dag_id,
                    dag_run_id=_dag_run_id(evidence_set_id, workflow_id),
                )
                for workflow_id, dag_id in workflow_identities
            ),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DbtDevEvidenceRequest:
        try:
            if set(value) != _REQUEST_FIELDS or value.get("schema") != DBT_DEV_EVIDENCE_REQUEST_SCHEMA:
                raise ValueError("request fields are invalid")
            raw_workflows = value.get("workflows")
            if (
                not isinstance(raw_workflows, list)
                or not raw_workflows
                or len(raw_workflows) > MAX_DBT_DEV_EVIDENCE_WORKFLOWS
            ):
                raise ValueError("workflow inventory is invalid")
            workflows = tuple(_workflow_from_mapping(item) for item in raw_workflows)
            request = cls(
                evidence_set_id=str(value["evidence_set_id"]),
                release_id=str(value["release_id"]),
                deployment_id=str(value["deployment_id"]),
                producer_repository=str(value["producer_repository"]),
                producer_workflow=str(value["producer_workflow"]),
                source_commit=str(value["source_commit"]),
                orchestration_run_id=str(value["orchestration_run_id"]),
                orchestration_run_attempt=_attempt(value["orchestration_run_attempt"]),
                workflows=workflows,
            )
            request._validate()
            return request
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, DbtDevEvidenceRequestError):
                raise
            raise DbtDevEvidenceRequestError("dev evidence request is invalid") from None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "evidence_set_id": self.evidence_set_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "producer_repository": self.producer_repository,
            "producer_workflow": self.producer_workflow,
            "source_commit": self.source_commit,
            "orchestration_run_id": self.orchestration_run_id,
            "orchestration_run_attempt": self.orchestration_run_attempt,
            "workflows": [item.to_dict() for item in self.workflows],
        }

    def _validate(self) -> None:
        _validate_identity(self.release_id, "release")
        _validate_identity(self.deployment_id, "deployment")
        _validate_identity(self.evidence_set_id, "evidence set")
        _validate_producer(
            producer_repository=self.producer_repository,
            producer_workflow=self.producer_workflow,
            source_commit=self.source_commit,
            orchestration_run_id=self.orchestration_run_id,
            orchestration_run_attempt=self.orchestration_run_attempt,
        )
        identities = tuple((item.workflow_id, item.dag_id) for item in self.workflows)
        if (
            not 1 <= len(identities) <= MAX_DBT_DEV_EVIDENCE_WORKFLOWS
            or identities != tuple(sorted(identities))
            or len(set(identities)) != len(identities)
            or len({item[0] for item in identities}) != len(identities)
            or len({item[1] for item in identities}) != len(identities)
            or any(item.dag_run_id != _dag_run_id(self.evidence_set_id, item.workflow_id) for item in self.workflows)
        ):
            raise DbtDevEvidenceRequestError("dev evidence workflow inventory is invalid")
        expected_id = canonical_fingerprint(
            _identity_payload(
                release_id=self.release_id,
                deployment_id=self.deployment_id,
                producer_repository=self.producer_repository,
                producer_workflow=self.producer_workflow,
                source_commit=self.source_commit,
                orchestration_run_id=self.orchestration_run_id,
                orchestration_run_attempt=self.orchestration_run_attempt,
                workflows=identities,
            )
        )
        if self.evidence_set_id != expected_id:
            raise DbtDevEvidenceRequestError("dev evidence request fingerprint is invalid")


def _identity_payload(
    *,
    release_id: str,
    deployment_id: str,
    producer_repository: str,
    producer_workflow: str,
    source_commit: str,
    orchestration_run_id: str,
    orchestration_run_attempt: int,
    workflows: tuple[tuple[str, str], ...],
) -> dict[str, object]:
    return {
        "schema": DBT_DEV_EVIDENCE_REQUEST_SCHEMA,
        "release_id": release_id,
        "deployment_id": deployment_id,
        "producer_repository": producer_repository,
        "producer_workflow": producer_workflow,
        "source_commit": source_commit,
        "orchestration_run_id": orchestration_run_id,
        "orchestration_run_attempt": orchestration_run_attempt,
        "workflows": [{"workflow_id": workflow_id, "dag_id": dag_id} for workflow_id, dag_id in workflows],
    }


def _workflow_from_mapping(value: object) -> DbtDevEvidenceWorkflowRequest:
    if not isinstance(value, Mapping) or set(value) != _WORKFLOW_FIELDS:
        raise ValueError("workflow request is invalid")
    workflow_id = _safe_text(value.get("workflow_id"), "workflow id")
    dag_id = _safe_text(value.get("dag_id"), "DAG id")
    dag_run_id = _safe_text(value.get("dag_run_id"), "DAG run id")
    return DbtDevEvidenceWorkflowRequest(
        workflow_id=workflow_id,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
    )


def _dag_run_id(evidence_set_id: str, workflow_id: str) -> str:
    workflow_suffix = hashlib.sha256(workflow_id.encode("utf-8")).hexdigest()[:12]
    return f"dpone_evidence__{evidence_set_id.removeprefix('sha256:')[:20]}__{workflow_suffix}"


def _validate_identity(value: object, field: str) -> None:
    if not isinstance(value, str) or not is_canonical_sha256_digest(value):
        raise DbtDevEvidenceRequestError(f"{field} identity is invalid")


def _validate_producer(
    *,
    producer_repository: object,
    producer_workflow: object,
    source_commit: object,
    orchestration_run_id: object,
    orchestration_run_attempt: object,
) -> None:
    if not isinstance(producer_repository, str) or _REPOSITORY.fullmatch(producer_repository) is None:
        raise DbtDevEvidenceRequestError("producer repository is invalid")
    _safe_text(producer_workflow, "producer workflow")
    if not isinstance(source_commit, str) or _FULL_COMMIT.fullmatch(source_commit) is None:
        raise DbtDevEvidenceRequestError("source commit is invalid")
    _safe_text(orchestration_run_id, "orchestration run id")
    _attempt(orchestration_run_attempt)


def _safe_text(value: object, field: str) -> str:
    if not isinstance(value, str) or value.strip() != value or _SAFE_TEXT.fullmatch(value) is None:
        raise DbtDevEvidenceRequestError(f"{field} is invalid")
    return value


def _attempt(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1_000:
        raise DbtDevEvidenceRequestError("orchestration run attempt is invalid")
    return value


__all__ = [
    "DBT_DEV_EVIDENCE_REQUEST_SCHEMA",
    "DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID",
    "DbtDevEvidenceRequest",
    "DbtDevEvidenceRequestError",
    "DbtDevEvidenceWorkflowRequest",
]
