"""Durable local activation contracts for Airflow desired state."""

from __future__ import annotations

import json
from dataclasses import dataclass

from dpone.contracts.airflow_desired_state import (
    GIT_SHA_PATTERN,
    AirflowDesiredDeployment,
    DesiredStateRevision,
    canonical_uuid,
    dag_ids,
    digest,
    environment,
    mapping,
    project,
    reject_constant,
    reject_unknown,
    text,
    unique_object,
)
from dpone.contracts.airflow_desired_state_reconcile_evidence import (
    DesiredStateReconcileEvidence,
)

AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA = "dpone.airflow-desired-state-checkpoint.v1"
AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA = "dpone.airflow-desired-state-recovery.v1"
MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES = 96 * 1024
_RECOVERY_FIELDS = frozenset({"schema", "observed_revision", "desired_state"})
_CHECKPOINT_FIELDS = frozenset(
    {
        "schema",
        "environment",
        "observed_revision",
        "desired_state_sha256",
        "registry_scope_id",
        "source_project",
        "source_ref",
        "release_id",
        "deployment_id",
        "occurrence_id",
        "source_git_sha",
        "airflow_index_sha256",
        "runtime_image_digest",
        "expected_dag_ids",
        "activation_id",
    }
)


@dataclass(frozen=True, slots=True)
class DesiredStateRecoveryRecord:
    """Desired bytes and opaque revision durably staged before cache activation."""

    desired: AirflowDesiredDeployment
    observed_revision: DesiredStateRevision
    schema: str = AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA:
            raise ValueError("desired-state recovery schema is invalid")
        _bounded_control(self.to_json_bytes())

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> DesiredStateRecoveryRecord:
        _bounded_control(raw)
        try:
            payload = json.loads(
                raw.decode("utf-8", errors="strict"),
                object_pairs_hook=unique_object,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("desired-state recovery record must be valid UTF-8 JSON") from exc
        value = mapping(payload, field="desired-state recovery record")
        reject_unknown(value, _RECOVERY_FIELDS, field="desired-state recovery record")
        record = cls(
            schema=text(value.get("schema"), field="schema", maximum=128),
            observed_revision=DesiredStateRevision(
                text(
                    value.get("observed_revision"),
                    field="observed_revision",
                    maximum=1024,
                )
            ),
            desired=AirflowDesiredDeployment.from_mapping(value.get("desired_state")),
        )
        if record.to_json_bytes() != raw:
            raise ValueError("desired-state recovery record must use canonical JSON bytes")
        return record

    def to_json_bytes(self) -> bytes:
        body = json.dumps(
            {
                "schema": self.schema,
                "observed_revision": self.observed_revision.value,
                "desired_state": self.desired.to_dict(),
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        _bounded_control(body)
        return body


@dataclass(frozen=True, slots=True)
class DesiredStateCheckpoint:
    """Last desired-state occurrence proven active in the local cache."""

    environment: str
    observed_revision: DesiredStateRevision
    desired_state_sha256: str
    registry_scope_id: str
    source_project: str
    source_ref: str
    release_id: str
    deployment_id: str
    occurrence_id: str
    source_git_sha: str
    airflow_index_sha256: str
    runtime_image_digest: str
    expected_dag_ids: tuple[str, ...]
    activation_id: str
    schema: str = AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA:
            raise ValueError("desired-state checkpoint schema is invalid")
        environment(self.environment)
        project(self.source_project)
        text(self.source_ref, field="source_ref", maximum=256)
        for field in (
            "desired_state_sha256",
            "registry_scope_id",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "runtime_image_digest",
        ):
            digest(getattr(self, field), field=field)
        dag_ids(self.expected_dag_ids)
        canonical_uuid(self.occurrence_id, field="occurrence_id")
        canonical_uuid(self.activation_id, field="activation_id")
        if GIT_SHA_PATTERN.fullmatch(self.source_git_sha) is None:
            raise ValueError("desired-state checkpoint source_git_sha is invalid")

    @classmethod
    def from_desired(
        cls,
        desired: AirflowDesiredDeployment,
        *,
        observed_revision: DesiredStateRevision,
        activation_id: str,
    ) -> DesiredStateCheckpoint:
        return cls(
            environment=desired.environment,
            observed_revision=observed_revision,
            desired_state_sha256=desired.sha256,
            registry_scope_id=desired.promotion.registry_scope_id,
            source_project=desired.source.project,
            source_ref=desired.source.ref,
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            occurrence_id=desired.source.occurrence_id,
            source_git_sha=desired.source.git_sha,
            airflow_index_sha256=desired.promotion.airflow_index_sha256,
            runtime_image_digest=desired.promotion.runtime_image_digest,
            expected_dag_ids=desired.promotion.expected_dag_ids,
            activation_id=activation_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "environment": self.environment,
            "observed_revision": self.observed_revision.value,
            "desired_state_sha256": self.desired_state_sha256,
            "registry_scope_id": self.registry_scope_id,
            "source_project": self.source_project,
            "source_ref": self.source_ref,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "occurrence_id": self.occurrence_id,
            "source_git_sha": self.source_git_sha,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
            "expected_dag_ids": list(self.expected_dag_ids),
            "activation_id": self.activation_id,
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> DesiredStateCheckpoint:
        try:
            payload = json.loads(
                raw.decode("utf-8", errors="strict"),
                object_pairs_hook=unique_object,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("desired-state checkpoint must be valid UTF-8 JSON") from exc
        value = mapping(payload, field="desired-state checkpoint")
        reject_unknown(value, _CHECKPOINT_FIELDS, field="desired-state checkpoint")
        checkpoint = cls(
            schema=text(value.get("schema"), field="schema", maximum=128),
            environment=text(value.get("environment"), field="environment", maximum=64),
            observed_revision=DesiredStateRevision(
                text(
                    value.get("observed_revision"),
                    field="observed_revision",
                    maximum=1024,
                )
            ),
            desired_state_sha256=digest(
                value.get("desired_state_sha256"),
                field="desired_state_sha256",
            ),
            registry_scope_id=digest(
                value.get("registry_scope_id"),
                field="registry_scope_id",
            ),
            source_project=text(
                value.get("source_project"),
                field="source_project",
                maximum=512,
            ),
            source_ref=text(value.get("source_ref"), field="source_ref", maximum=256),
            release_id=digest(value.get("release_id"), field="release_id"),
            deployment_id=digest(value.get("deployment_id"), field="deployment_id"),
            occurrence_id=text(
                value.get("occurrence_id"),
                field="occurrence_id",
                maximum=36,
            ),
            source_git_sha=text(
                value.get("source_git_sha"),
                field="source_git_sha",
                maximum=64,
            ),
            airflow_index_sha256=digest(
                value.get("airflow_index_sha256"),
                field="airflow_index_sha256",
            ),
            runtime_image_digest=digest(
                value.get("runtime_image_digest"),
                field="runtime_image_digest",
            ),
            expected_dag_ids=dag_ids(value.get("expected_dag_ids")),
            activation_id=text(
                value.get("activation_id"),
                field="activation_id",
                maximum=36,
            ),
        )
        if checkpoint.to_json_bytes() != raw:
            raise ValueError("desired-state checkpoint must use canonical JSON bytes")
        return checkpoint


def activated_evidence(
    desired: AirflowDesiredDeployment,
    *,
    revision: DesiredStateRevision,
    activation_id: str,
    previous_deployment_id: str | None,
    predecessor: str,
) -> DesiredStateReconcileEvidence:
    return DesiredStateReconcileEvidence(
        status="activated",
        environment=desired.environment,
        observed_revision=revision.value,
        desired_state_sha256=desired.sha256,
        registry_scope_id=desired.promotion.registry_scope_id,
        source_project=desired.source.project,
        source_ref=desired.source.ref,
        release_id=desired.promotion.release_id,
        deployment_id=desired.promotion.deployment_id,
        occurrence_id=desired.source.occurrence_id,
        source_git_sha=desired.source.git_sha,
        airflow_index_sha256=desired.promotion.airflow_index_sha256,
        runtime_image_digest=desired.promotion.runtime_image_digest,
        expected_dag_ids=desired.promotion.expected_dag_ids,
        activation_id=activation_id,
        previous_deployment_id=previous_deployment_id,
        predecessor_status=predecessor,
        materialized=True,
        activated=True,
    )


def _bounded_control(body: bytes) -> None:
    if not body or len(body) > MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES:
        raise ValueError("desired-state control record exceeds its bounded size")


__all__ = [
    "AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA",
    "AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA",
    "MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES",
    "DesiredStateCheckpoint",
    "DesiredStateRecoveryRecord",
    "activated_evidence",
]
