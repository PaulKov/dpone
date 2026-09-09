"""Stable evidence contract for one Airflow desired-state reconcile cycle."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_desired_state_validation import (
    GIT_SHA_PATTERN,
    canonical_uuid,
    dag_ids,
    digest,
    environment,
    mapping,
    optional_digest,
    project,
    reject_constant,
    reject_unknown,
    text,
    unique_object,
)

AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA = "dpone.airflow-desired-state-reconcile.v1"
_RECONCILE_FIELDS = frozenset(
    {
        "schema",
        "passed",
        "status",
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
        "previous_deployment_id",
        "predecessor_status",
        "materialized",
        "activated",
    }
)


@dataclass(frozen=True, slots=True)
class DesiredStateReconcileEvidence:
    """Credential-free result of one complete watcher cycle."""

    status: str
    environment: str
    observed_revision: str
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
    previous_deployment_id: str | None
    predecessor_status: str
    materialized: bool
    activated: bool
    schema: str = AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA:
            raise ValueError("desired-state reconcile schema is invalid")
        if self.status not in {"activated", "recovered", "unchanged"}:
            raise ValueError("desired-state reconcile status is invalid")
        if self.activated != (self.status == "activated"):
            raise ValueError("desired-state reconcile activation flags conflict")
        if self.materialized != self.activated:
            raise ValueError("desired-state reconcile materialization flags conflict")
        if self.predecessor_status not in {
            "bootstrap",
            "continuous",
            "recovered",
            "skipped",
            "unchanged",
        }:
            raise ValueError("desired-state reconcile predecessor status is invalid")
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
        canonical_uuid(self.occurrence_id, field="occurrence_id")
        canonical_uuid(self.activation_id, field="activation_id")
        if GIT_SHA_PATTERN.fullmatch(self.source_git_sha) is None:
            raise ValueError("desired-state reconcile source_git_sha is invalid")
        dag_ids(self.expected_dag_ids)
        optional_digest(
            self.previous_deployment_id,
            field="previous_deployment_id",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "passed": True,
            "status": self.status,
            "environment": self.environment,
            "observed_revision": self.observed_revision,
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
            "previous_deployment_id": self.previous_deployment_id,
            "predecessor_status": self.predecessor_status,
            "materialized": self.materialized,
            "activated": self.activated,
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> DesiredStateReconcileEvidence:
        try:
            payload = json.loads(
                raw.decode("utf-8", errors="strict"),
                object_pairs_hook=unique_object,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("desired-state reconcile evidence must be valid UTF-8 JSON") from exc
        return cls.from_mapping(mapping(payload, field="desired-state reconcile evidence"))

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, object],
    ) -> DesiredStateReconcileEvidence:
        reject_unknown(value, _RECONCILE_FIELDS, field="desired-state reconcile evidence")
        if set(value) != _RECONCILE_FIELDS or value.get("passed") is not True:
            raise ValueError("desired-state reconcile evidence is incomplete")
        return cls(
            schema=text(value.get("schema"), field="schema", maximum=128),
            status=text(value.get("status"), field="status", maximum=32),
            environment=environment(value.get("environment")),
            observed_revision=text(
                value.get("observed_revision"),
                field="observed_revision",
                maximum=1024,
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
            occurrence_id=text(value.get("occurrence_id"), field="occurrence_id", maximum=36),
            source_git_sha=text(value.get("source_git_sha"), field="source_git_sha", maximum=64),
            airflow_index_sha256=digest(
                value.get("airflow_index_sha256"),
                field="airflow_index_sha256",
            ),
            runtime_image_digest=digest(
                value.get("runtime_image_digest"),
                field="runtime_image_digest",
            ),
            expected_dag_ids=dag_ids(value.get("expected_dag_ids")),
            activation_id=text(value.get("activation_id"), field="activation_id", maximum=36),
            previous_deployment_id=optional_digest(
                value.get("previous_deployment_id"),
                field="previous_deployment_id",
            ),
            predecessor_status=text(
                value.get("predecessor_status"),
                field="predecessor_status",
                maximum=32,
            ),
            materialized=_boolean(value.get("materialized"), field="materialized"),
            activated=_boolean(value.get("activated"), field="activated"),
        )


def _boolean(value: object, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"desired-state reconcile {field} must be boolean")
    return value


__all__ = [
    "AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA",
    "DesiredStateReconcileEvidence",
]
