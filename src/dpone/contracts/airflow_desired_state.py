"""Dependency-free contracts for Airflow desired-state selection.

The mutable desired-state object selects immutable release/deployment content.
This module owns only bounded parsing, canonical serialization, identities, and
publish evidence; object-store behavior belongs to ports and adapters.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dpone.contracts.airflow_desired_state_promotion import DesiredStatePromotion
from dpone.contracts.airflow_desired_state_validation import (
    GIT_SHA_PATTERN,
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    MAX_DESIRED_STATE_REVISION_BYTES,
    PREVIOUS_FIELDS,
    PROMOTION_FIELDS,
    ROOT_FIELDS,
    SOURCE_FIELDS,
    AirflowDesiredStateError,
    bounded,
    bounded_revision,
    canonical_uuid,
    ci_id,
    dag_ids,
    digest,
    environment,
    invalid,
    mapping,
    optional_digest,
    project,
    reject_constant,
    reject_secret_like_fields,
    reject_unknown,
    text,
    unique_object,
    utc_timestamp,
)

AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA = "dpone.airflow-desired-deployment.v1"
AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA = "dpone.airflow-desired-state-publish.v1"
AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA = "dpone.airflow-desired-state-publish.v2"
AIRFLOW_DESIRED_STATE_FETCH_SCHEMA = "dpone.airflow-desired-state-fetch.v1"


@dataclass(frozen=True, slots=True)
class DesiredStateRevision:
    """Opaque object-store revision used only for exact equality and CAS."""

    value: str

    def __post_init__(self) -> None:
        bounded_revision(self.value)

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class DesiredStateSource:
    """Protected CI source identity for one promotion occurrence."""

    project: str
    ref: str
    pipeline_id: str
    job_id: str
    occurrence_id: str
    git_sha: str

    def __post_init__(self) -> None:
        project(self.project)
        text(self.ref, field="source.ref", maximum=256)
        ci_id(self.pipeline_id, field="source.pipeline_id")
        ci_id(self.job_id, field="source.job_id")
        canonical_uuid(self.occurrence_id, field="source.occurrence_id")
        if GIT_SHA_PATTERN.fullmatch(self.git_sha) is None:
            raise invalid("source.git_sha must be 40 or 64 lowercase hexadecimal characters")

    def to_dict(self) -> dict[str, str]:
        return {
            "project": self.project,
            "ref": self.ref,
            "pipeline_id": self.pipeline_id,
            "job_id": self.job_id,
            "occurrence_id": self.occurrence_id,
            "git_sha": self.git_sha,
        }


@dataclass(frozen=True, slots=True)
class DesiredStatePrevious:
    """Exact predecessor observed before this desired-state occurrence."""

    revision: DesiredStateRevision | None
    deployment_id: str | None

    def __post_init__(self) -> None:
        if (self.revision is None) != (self.deployment_id is None):
            raise invalid("previous revision and deployment_id must both be null or both be present")
        if self.deployment_id is not None:
            digest(self.deployment_id, field="previous.deployment_id")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "revision": None if self.revision is None else self.revision.value,
            "deployment_id": self.deployment_id,
        }


@dataclass(frozen=True, slots=True)
class AirflowDesiredDeployment:
    """Strict, immutable, canonical desired-state envelope."""

    environment: str
    source: DesiredStateSource
    promotion: DesiredStatePromotion
    previous: DesiredStatePrevious
    promoted_at: str
    schema: str = AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA:
            raise invalid(f"schema must be {AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA}")
        environment(self.environment)
        utc_timestamp(self.promoted_at, field="promoted_at")
        bounded(self.to_json_bytes())

    @classmethod
    def from_mapping(cls, value: object) -> AirflowDesiredDeployment:
        payload = mapping(value, field="desired state")
        reject_secret_like_fields(payload)
        reject_unknown(payload, ROOT_FIELDS, field="desired state")
        source = mapping(payload.get("source"), field="source")
        promotion = mapping(payload.get("promotion"), field="promotion")
        previous = mapping(payload.get("previous"), field="previous")
        reject_unknown(source, SOURCE_FIELDS, field="source")
        reject_unknown(promotion, PROMOTION_FIELDS, field="promotion")
        reject_unknown(previous, PREVIOUS_FIELDS, field="previous")
        expected_dag_ids = promotion.get("expected_dag_ids")
        if not isinstance(expected_dag_ids, list):
            raise invalid("promotion.expected_dag_ids must be an array")
        revision_value = previous.get("revision")
        if revision_value is not None and not isinstance(revision_value, str):
            raise invalid("previous.revision must be a string or null")
        return cls(
            schema=text(payload.get("schema"), field="schema", maximum=128),
            environment=text(payload.get("environment"), field="environment", maximum=64),
            source=DesiredStateSource(
                project=text(source.get("project"), field="source.project", maximum=512),
                ref=text(source.get("ref"), field="source.ref", maximum=256),
                pipeline_id=text(source.get("pipeline_id"), field="source.pipeline_id", maximum=32),
                job_id=text(source.get("job_id"), field="source.job_id", maximum=32),
                occurrence_id=text(source.get("occurrence_id"), field="source.occurrence_id", maximum=36),
                git_sha=text(source.get("git_sha"), field="source.git_sha", maximum=64),
            ),
            promotion=DesiredStatePromotion(
                registry_scope_id=digest(
                    promotion.get("registry_scope_id"),
                    field="promotion.registry_scope_id",
                ),
                release_id=digest(promotion.get("release_id"), field="promotion.release_id"),
                deployment_id=digest(promotion.get("deployment_id"), field="promotion.deployment_id"),
                airflow_index_sha256=digest(
                    promotion.get("airflow_index_sha256"),
                    field="promotion.airflow_index_sha256",
                ),
                runtime_image_digest=digest(
                    promotion.get("runtime_image_digest"),
                    field="promotion.runtime_image_digest",
                ),
                runtime_image_dbt_digest=optional_digest(
                    promotion.get("runtime_image_dbt_digest"),
                    field="promotion.runtime_image_dbt_digest",
                ),
                expected_dag_ids=dag_ids(expected_dag_ids),
                publication_evidence_sha256=digest(
                    promotion.get("publication_evidence_sha256"),
                    field="promotion.publication_evidence_sha256",
                ),
            ),
            previous=DesiredStatePrevious(
                revision=None if revision_value is None else DesiredStateRevision(revision_value),
                deployment_id=optional_digest(previous.get("deployment_id"), field="previous.deployment_id"),
            ),
            promoted_at=text(payload.get("promoted_at"), field="promoted_at", maximum=40),
        )

    @classmethod
    def from_json(cls, value: bytes | str) -> AirflowDesiredDeployment:
        raw = value.encode("utf-8") if isinstance(value, str) else value
        if not isinstance(raw, bytes):
            raise invalid("desired state must be UTF-8 JSON bytes or text")
        bounded(raw)
        try:
            text = raw.decode("utf-8", errors="strict")
            payload = json.loads(text, object_pairs_hook=unique_object, parse_constant=reject_constant)
        except UnicodeDecodeError as exc:
            raise invalid("desired state must be valid UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise invalid("desired state must be valid JSON") from exc
        model = cls.from_mapping(payload)
        if raw != model.to_json_bytes():
            raise invalid("desired state must use canonical JSON bytes")
        return model

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "environment": self.environment,
            "source": self.source.to_dict(),
            "promotion": self.promotion.to_dict(),
            "previous": self.previous.to_dict(),
            "promoted_at": self.promoted_at,
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @property
    def sha256(self) -> str:
        return desired_state_sha256(self.to_json_bytes())


@dataclass(frozen=True, slots=True)
class DesiredStatePublishEvidence:
    """Credential-free evidence emitted only after remote reconciliation."""

    outcome: str
    environment: str
    occurrence_id: str
    release_id: str
    deployment_id: str
    desired_state_sha256: str
    previous_revision: DesiredStateRevision | None
    committed_revision: DesiredStateRevision
    published_at: str
    state_may_have_changed: bool = False
    schema: str = AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA
    preparation_job_id: str | None = None
    publisher_job_id: str | None = None

    def __post_init__(self) -> None:
        if self.schema not in {
            AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA,
            AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA,
        }:
            raise invalid("publish evidence schema is invalid")
        if self.outcome not in {"created", "replaced", "idempotent"}:
            raise invalid("publish evidence outcome is invalid")
        environment(self.environment)
        canonical_uuid(self.occurrence_id, field="occurrence_id")
        provenance = (self.preparation_job_id, self.publisher_job_id)
        if self.schema == AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA:
            if provenance != (None, None):
                raise invalid("publish evidence v1 cannot contain job provenance")
        elif any(value is None for value in provenance):
            raise invalid("publish evidence v2 requires job provenance")
        else:
            assert self.preparation_job_id is not None
            assert self.publisher_job_id is not None
            ci_id(self.preparation_job_id, field="preparation_job_id")
            ci_id(self.publisher_job_id, field="publisher_job_id")
        for field in ("release_id", "deployment_id", "desired_state_sha256"):
            digest(getattr(self, field), field=field)
        utc_timestamp(self.published_at, field="published_at")
        if self.state_may_have_changed:
            raise invalid("successful publish evidence cannot report uncertain state")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": self.schema,
            "passed": True,
            "status": "published",
            "outcome": self.outcome,
            "environment": self.environment,
            "occurrence_id": self.occurrence_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "desired_state_sha256": self.desired_state_sha256,
            "previous_revision": None if self.previous_revision is None else self.previous_revision.value,
            "committed_revision": self.committed_revision.value,
            "published_at": self.published_at,
            "state_may_have_changed": self.state_may_have_changed,
        }
        if self.schema == AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA:
            payload["preparation_job_id"] = self.preparation_job_id
            payload["publisher_job_id"] = self.publisher_job_id
        return payload

    def to_v1_dict(self) -> dict[str, object]:
        """Project canonical evidence to the legacy same-job wire contract."""

        payload = self.to_dict()
        payload["schema"] = AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA
        payload.pop("preparation_job_id", None)
        payload.pop("publisher_job_id", None)
        return payload


@dataclass(frozen=True, slots=True)
class DesiredStateFetchEvidence:
    """Credential-free result of one bounded desired-state fetch."""

    status: str
    environment: str
    observed_revision: DesiredStateRevision
    desired_state_sha256: str | None
    deployment_id: str | None
    schema: str = AIRFLOW_DESIRED_STATE_FETCH_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AIRFLOW_DESIRED_STATE_FETCH_SCHEMA:
            raise invalid("desired-state fetch evidence schema is invalid")
        if self.status not in {"fetched", "unchanged"}:
            raise invalid("desired-state fetch evidence status is invalid")
        has_digest = self.desired_state_sha256 is not None
        has_deployment = self.deployment_id is not None
        if self.status == "fetched" and not (has_digest and has_deployment):
            raise invalid("desired-state fetch evidence content identities conflict")
        if self.status == "unchanged" and (has_digest or has_deployment):
            raise invalid("desired-state fetch evidence content identities conflict")

    @classmethod
    def fetched(
        cls,
        desired: AirflowDesiredDeployment,
        revision: DesiredStateRevision,
    ) -> DesiredStateFetchEvidence:
        return cls(
            status="fetched",
            environment=desired.environment,
            observed_revision=revision,
            desired_state_sha256=desired.sha256,
            deployment_id=desired.promotion.deployment_id,
        )

    @classmethod
    def unchanged(
        cls,
        *,
        environment: str,
        revision: DesiredStateRevision,
    ) -> DesiredStateFetchEvidence:
        return cls(
            status="unchanged",
            environment=environment,
            observed_revision=revision,
            desired_state_sha256=None,
            deployment_id=None,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "passed": True,
            "status": self.status,
            "environment": self.environment,
            "observed_revision": self.observed_revision.value,
            "desired_state_sha256": self.desired_state_sha256,
            "deployment_id": self.deployment_id,
        }


def desired_state_sha256(body: bytes) -> str:
    """Return the canonical SHA-256 identity for exact desired-state bytes."""

    return "sha256:" + hashlib.sha256(body).hexdigest()


__all__ = [
    "AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA",
    "AIRFLOW_DESIRED_STATE_FETCH_SCHEMA",
    "AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA",
    "AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA",
    "MAX_AIRFLOW_DESIRED_STATE_BYTES",
    "MAX_DESIRED_STATE_REVISION_BYTES",
    "AirflowDesiredDeployment",
    "AirflowDesiredStateError",
    "DesiredStatePrevious",
    "DesiredStateFetchEvidence",
    "DesiredStatePromotion",
    "DesiredStatePublishEvidence",
    "DesiredStateRevision",
    "DesiredStateSource",
    "desired_state_sha256",
]
