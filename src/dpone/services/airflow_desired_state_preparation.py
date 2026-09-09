"""Application policy for retry-safe Airflow desired-state preparation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dpone.contracts.airflow_desired_state_publish import (
    DesiredStatePublishCandidate,
    DesiredStatePublishIntent,
    DesiredStatePublishPreparation,
    DesiredStatePublishRequest,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state import DesiredStateRevision
    from dpone.ports.airflow_desired_state_publish import (
        DesiredStatePromotionInput,
        DesiredStatePublicationAuthority,
    )


class AirflowDesiredStatePreparationService:
    """Build and validate one immutable publication occurrence."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime],
        occurrence_id_factory: Callable[[], object],
    ) -> None:
        self._clock = clock
        self._occurrence_id_factory = occurrence_id_factory

    def candidate(
        self,
        *,
        authority: DesiredStatePublicationAuthority,
        promotion: DesiredStatePromotionInput,
        pipeline_id: str,
        preparation_job_id: str,
        expected_revision: DesiredStateRevision | None,
    ) -> DesiredStatePublishCandidate:
        """Freeze all trusted inputs that may affect canonical desired bytes."""

        return self._candidate(
            authority=authority,
            promotion=promotion,
            pipeline_id=pipeline_id,
            job_id=preparation_job_id,
            expected_revision=expected_revision,
            authority_sha256=authority.publish_authority_sha256,
        )

    def legacy_candidate(
        self,
        *,
        authority: DesiredStatePublicationAuthority,
        promotion: DesiredStatePromotionInput,
        pipeline_id: str,
        job_id: str,
        expected_revision: DesiredStateRevision | None,
    ) -> DesiredStatePublishCandidate:
        """Reproduce the v0.73.24 same-job candidate and intent identity."""

        return self._candidate(
            authority=authority,
            promotion=promotion,
            pipeline_id=pipeline_id,
            job_id=job_id,
            expected_revision=expected_revision,
            authority_sha256=None,
        )

    @staticmethod
    def _candidate(
        *,
        authority: DesiredStatePublicationAuthority,
        promotion: DesiredStatePromotionInput,
        pipeline_id: str,
        job_id: str,
        expected_revision: DesiredStateRevision | None,
        authority_sha256: str | None,
    ) -> DesiredStatePublishCandidate:
        if promotion.environment != authority.environment or promotion.registry_scope_id != authority.registry_scope_id:
            raise ValueError("promotion evidence is outside the trusted authority")
        return DesiredStatePublishCandidate(
            environment=authority.environment,
            project=authority.source_project,
            source_ref=authority.source_ref,
            pipeline_id=pipeline_id,
            job_id=job_id,
            git_sha=promotion.source_git_sha,
            registry_scope_id=promotion.registry_scope_id,
            release_id=promotion.release_id,
            deployment_id=promotion.deployment_id,
            airflow_index_sha256=promotion.airflow_index_sha256,
            runtime_image_digest=promotion.runtime_image_digest,
            runtime_image_dbt_digest=getattr(promotion, "runtime_image_dbt_digest", None),
            expected_dag_ids=promotion.expected_dag_ids,
            publication_evidence_sha256=promotion.evidence_sha256,
            expected_revision=expected_revision,
            authority_sha256=authority_sha256,
        )

    def preparation(
        self,
        candidate: DesiredStatePublishCandidate,
    ) -> DesiredStatePublishPreparation:
        """Assign one durable occurrence to an already frozen candidate."""

        intent = DesiredStatePublishIntent(
            candidate_sha256=candidate.sha256,
            occurrence_id=str(self._occurrence_id_factory()),
            promoted_at=_format_utc(self._clock()),
        )
        return DesiredStatePublishPreparation(candidate=candidate, intent=intent)

    @staticmethod
    def request(
        *,
        candidate: DesiredStatePublishCandidate,
        intent: DesiredStatePublishIntent,
        publisher_job_id: str,
    ) -> DesiredStatePublishRequest:
        """Attach one actual publishing job to stable occurrence inputs."""

        return DesiredStatePublishRequest(
            candidate=candidate,
            intent=intent,
            publisher_job_id=publisher_job_id,
        )

    def validate(
        self,
        *,
        preparation: DesiredStatePublishPreparation,
        authority: DesiredStatePublicationAuthority,
        promotion: DesiredStatePromotionInput,
        pipeline_id: str,
        publisher_job_id: str,
    ) -> DesiredStatePublishRequest:
        """Revalidate a preparation and attach the current mutating job."""

        expected = self.candidate(
            authority=authority,
            promotion=promotion,
            pipeline_id=pipeline_id,
            preparation_job_id=preparation.candidate.job_id,
            expected_revision=preparation.candidate.expected_revision,
        )
        if preparation.candidate != expected:
            raise ValueError("publish preparation is outside the current trusted candidate")
        return self.request(
            candidate=preparation.candidate,
            intent=preparation.intent,
            publisher_job_id=publisher_job_id,
        )


def _format_utc(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return an offset-aware datetime")
    normalized = value.astimezone(timezone.utc)  # noqa: UP017
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


__all__ = ["AirflowDesiredStatePreparationService"]
