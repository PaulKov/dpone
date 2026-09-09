"""Durable retry identity for one Airflow desired-state publication."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.contracts.airflow_desired_state_validation import (
    GIT_SHA_PATTERN,
    bounded,
    bounded_revision,
    canonical_uuid,
    ci_id,
    dag_ids,
    digest,
    environment,
    invalid,
    project,
    reject_constant,
    text,
    utc_timestamp,
)

AIRFLOW_DESIRED_STATE_PUBLISH_INTENT_SCHEMA = "dpone.airflow-desired-state-publish-intent.v1"
AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA = "dpone.airflow-desired-state-publish-preparation.v1"
_INTENT_FIELDS = frozenset({"schema", "candidate_sha256", "occurrence_id", "promoted_at"})
_CANDIDATE_FIELDS = frozenset(
    {
        "environment",
        "authority_sha256",
        "project",
        "source_ref",
        "pipeline_id",
        "job_id",
        "git_sha",
        "registry_scope_id",
        "release_id",
        "deployment_id",
        "airflow_index_sha256",
        "runtime_image_digest",
        "runtime_image_dbt_digest",
        "expected_dag_ids",
        "publication_evidence_sha256",
        "expected_revision",
    }
)
_CANDIDATE_OPTIONAL_FIELDS = frozenset({"authority_sha256", "runtime_image_dbt_digest"})
_CANDIDATE_REQUIRED_FIELDS = _CANDIDATE_FIELDS - _CANDIDATE_OPTIONAL_FIELDS
_PREPARATION_FIELDS = frozenset({"schema", "candidate", "intent"})


@dataclass(frozen=True, slots=True)
class DesiredStatePublishCandidate:
    """Immutable promotion input before one durable occurrence is assigned."""

    environment: str
    project: str
    source_ref: str
    pipeline_id: str
    job_id: str
    git_sha: str
    registry_scope_id: str
    release_id: str
    deployment_id: str
    airflow_index_sha256: str
    runtime_image_digest: str
    expected_dag_ids: tuple[str, ...]
    publication_evidence_sha256: str
    expected_revision: DesiredStateRevision | None
    authority_sha256: str | None = None
    runtime_image_dbt_digest: str | None = None

    def __post_init__(self) -> None:
        environment(self.environment)
        if self.authority_sha256 is not None:
            digest(self.authority_sha256, field="authority_sha256")
        project(self.project)
        text(self.source_ref, field="source_ref", maximum=256)
        ci_id(self.pipeline_id, field="pipeline_id")
        ci_id(self.job_id, field="job_id")
        if GIT_SHA_PATTERN.fullmatch(self.git_sha) is None:
            raise invalid("git_sha must be 40 or 64 lowercase hexadecimal characters")
        for field in (
            "registry_scope_id",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "runtime_image_digest",
            "publication_evidence_sha256",
        ):
            digest(getattr(self, field), field=field)
        if self.runtime_image_dbt_digest is not None:
            digest(self.runtime_image_dbt_digest, field="runtime_image_dbt_digest")
        dag_ids(self.expected_dag_ids)
        if self.expected_revision is not None:
            bounded_revision(self.expected_revision.value)

    @property
    def sha256(self) -> str:
        """Return the canonical retry identity excluding generated occurrence fields."""

        payload = {
            "environment": self.environment,
            "project": self.project,
            "source_ref": self.source_ref,
            "pipeline_id": self.pipeline_id,
            "job_id": self.job_id,
            "git_sha": self.git_sha,
            "registry_scope_id": self.registry_scope_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
            "expected_dag_ids": list(self.expected_dag_ids),
            "publication_evidence_sha256": self.publication_evidence_sha256,
            "expected_revision": (None if self.expected_revision is None else self.expected_revision.value),
        }
        if self.authority_sha256 is not None:
            payload["authority_sha256"] = self.authority_sha256
        if self.runtime_image_dbt_digest is not None:
            payload["runtime_image_dbt_digest"] = self.runtime_image_dbt_digest
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        return "sha256:" + hashlib.sha256(body).hexdigest()

    @classmethod
    def from_mapping(cls, payload: object) -> DesiredStatePublishCandidate:
        if not isinstance(payload, dict):
            raise ValueError("desired-state publish candidate fields are invalid")
        keys = set(payload)
        if not _CANDIDATE_REQUIRED_FIELDS.issubset(keys) or keys - _CANDIDATE_FIELDS:
            raise ValueError("desired-state publish candidate fields are invalid")
        expected_revision = payload.get("expected_revision")
        expected_dag_ids = payload.get("expected_dag_ids")
        if not isinstance(expected_dag_ids, list) or any(not isinstance(item, str) for item in expected_dag_ids):
            raise ValueError("desired-state publish candidate DAG identities are invalid")
        if expected_revision is not None and not isinstance(expected_revision, str):
            raise ValueError("desired-state publish candidate revision is invalid")
        return cls(
            environment=_candidate_text(payload, "environment"),
            project=_candidate_text(payload, "project"),
            source_ref=_candidate_text(payload, "source_ref"),
            pipeline_id=_candidate_text(payload, "pipeline_id"),
            job_id=_candidate_text(payload, "job_id"),
            git_sha=_candidate_text(payload, "git_sha"),
            registry_scope_id=_candidate_text(payload, "registry_scope_id"),
            release_id=_candidate_text(payload, "release_id"),
            deployment_id=_candidate_text(payload, "deployment_id"),
            airflow_index_sha256=_candidate_text(payload, "airflow_index_sha256"),
            runtime_image_digest=_candidate_text(payload, "runtime_image_digest"),
            expected_dag_ids=tuple(expected_dag_ids),
            publication_evidence_sha256=_candidate_text(
                payload,
                "publication_evidence_sha256",
            ),
            expected_revision=(None if expected_revision is None else DesiredStateRevision(expected_revision)),
            authority_sha256=(_candidate_text(payload, "authority_sha256") if "authority_sha256" in payload else None),
            runtime_image_dbt_digest=(
                _candidate_text(payload, "runtime_image_dbt_digest") if "runtime_image_dbt_digest" in payload else None
            ),
        )

    def to_dict(self) -> dict[str, object]:
        if self.authority_sha256 is None:
            raise ValueError("legacy publish candidate cannot become a preparation")
        payload: dict[str, object] = {
            "environment": self.environment,
            "authority_sha256": self.authority_sha256,
            "project": self.project,
            "source_ref": self.source_ref,
            "pipeline_id": self.pipeline_id,
            "job_id": self.job_id,
            "git_sha": self.git_sha,
            "registry_scope_id": self.registry_scope_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
            "expected_dag_ids": list(self.expected_dag_ids),
            "publication_evidence_sha256": self.publication_evidence_sha256,
            "expected_revision": (None if self.expected_revision is None else self.expected_revision.value),
        }
        if self.runtime_image_dbt_digest is not None:
            payload["runtime_image_dbt_digest"] = self.runtime_image_dbt_digest
        return payload

    def promotion_mapping(self) -> dict[str, object]:
        """Project only the fields owned by ``DesiredStatePromotion``."""

        payload: dict[str, object] = {
            "registry_scope_id": self.registry_scope_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
            "expected_dag_ids": list(self.expected_dag_ids),
            "publication_evidence_sha256": self.publication_evidence_sha256,
        }
        if self.runtime_image_dbt_digest is not None:
            payload["runtime_image_dbt_digest"] = self.runtime_image_dbt_digest
        return payload


@dataclass(frozen=True, slots=True)
class DesiredStatePublishIntent:
    """Persisted occurrence reused after crashes or uncertain remote writes."""

    candidate_sha256: str
    occurrence_id: str
    promoted_at: str
    schema: str = AIRFLOW_DESIRED_STATE_PUBLISH_INTENT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_PUBLISH_INTENT_SCHEMA:
            raise ValueError("desired-state publish intent schema is invalid")
        digest(self.candidate_sha256, field="candidate_sha256")
        canonical_uuid(self.occurrence_id, field="occurrence_id")
        utc_timestamp(self.promoted_at, field="promoted_at")

    @classmethod
    def from_json_bytes(cls, body: bytes) -> DesiredStatePublishIntent:
        try:
            payload = json.loads(
                body.decode("utf-8", errors="strict"),
                object_pairs_hook=_unique_object,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("desired-state publish intent is invalid JSON") from exc
        if not isinstance(payload, dict) or set(payload) != _INTENT_FIELDS:
            raise ValueError("desired-state publish intent fields are invalid")
        intent = cls(
            schema=str(payload.get("schema") or ""),
            candidate_sha256=str(payload.get("candidate_sha256") or ""),
            occurrence_id=str(payload.get("occurrence_id") or ""),
            promoted_at=str(payload.get("promoted_at") or ""),
        )
        if body != intent.to_json_bytes():
            raise ValueError("desired-state publish intent must use canonical JSON")
        return intent

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "schema": self.schema,
                "candidate_sha256": self.candidate_sha256,
                "occurrence_id": self.occurrence_id,
                "promoted_at": self.promoted_at,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()


@dataclass(frozen=True, slots=True)
class DesiredStatePublishRequest:
    """Candidate plus its durable cross-process occurrence identity."""

    candidate: DesiredStatePublishCandidate
    intent: DesiredStatePublishIntent
    publisher_job_id: str | None = None

    def __post_init__(self) -> None:
        if self.intent.candidate_sha256 != self.candidate.sha256:
            raise ValueError("publish intent belongs to another candidate")
        if self.publisher_job_id is not None:
            ci_id(self.publisher_job_id, field="publisher_job_id")


@dataclass(frozen=True, slots=True)
class DesiredStatePublishPreparation:
    """Canonical cross-job input consumed by the mutating publication job."""

    candidate: DesiredStatePublishCandidate
    intent: DesiredStatePublishIntent
    schema: str = AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA:
            raise ValueError("desired-state publish preparation schema is invalid")
        if self.intent.candidate_sha256 != self.candidate.sha256:
            raise ValueError("publish intent belongs to another candidate")

    @classmethod
    def from_json_bytes(cls, body: bytes) -> DesiredStatePublishPreparation:
        bounded(body)
        try:
            payload = json.loads(
                body.decode("utf-8", errors="strict"),
                object_pairs_hook=_unique_object,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("desired-state publish preparation is invalid JSON") from exc
        if not isinstance(payload, dict) or set(payload) != _PREPARATION_FIELDS:
            raise ValueError("desired-state publish preparation fields are invalid")
        intent_payload = payload.get("intent")
        if not isinstance(intent_payload, dict):
            raise ValueError("desired-state publish preparation intent is invalid")
        preparation = cls(
            schema=str(payload.get("schema") or ""),
            candidate=DesiredStatePublishCandidate.from_mapping(payload.get("candidate")),
            intent=DesiredStatePublishIntent.from_json_bytes(
                json.dumps(
                    intent_payload,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
            ),
        )
        if body != preparation.to_json_bytes():
            raise ValueError("desired-state publish preparation must use canonical JSON")
        return preparation

    def to_json_bytes(self) -> bytes:
        body = json.dumps(
            {
                "schema": self.schema,
                "candidate": self.candidate.to_dict(),
                "intent": json.loads(self.intent.to_json_bytes()),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        bounded(body)
        return body


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("desired-state publish intent contains duplicate keys")
        result[key] = value
    return result


def _candidate_text(payload: dict[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise ValueError(f"desired-state publish candidate {field} is invalid")
    return value


__all__ = [
    "AIRFLOW_DESIRED_STATE_PUBLISH_INTENT_SCHEMA",
    "AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA",
    "DesiredStatePublishCandidate",
    "DesiredStatePublishIntent",
    "DesiredStatePublishPreparation",
    "DesiredStatePublishRequest",
]
