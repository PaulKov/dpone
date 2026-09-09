"""Application service for exact Airflow desired-state CAS publication."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.contracts.airflow_desired_state import (
    AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA,
    AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA,
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
    AirflowDesiredStateError,
    DesiredStatePrevious,
    DesiredStatePublishEvidence,
    DesiredStateRevision,
)
from dpone.ports.airflow_desired_state import (
    AirflowDesiredStateReader,
    AirflowDesiredStateWriter,
    DesiredStateConditionalWriteConflict,
    DesiredStatePortError,
    DesiredStateReadStatus,
    DesiredStateReadUnavailable,
    DesiredStateSourceAuthorizer,
    DesiredStateSourceUnauthorized,
    DesiredStateSourceUnavailable,
    DesiredStateWriteContention,
    DesiredStateWriteResult,
    DesiredStateWriteUncertain,
)
from dpone.ports.airflow_desired_state import (
    ObservedDesiredState as _ObservedDesiredState,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state_publish import (
        DesiredStatePublishRequest,
    )


def _evidence(
    *,
    desired: AirflowDesiredDeployment,
    outcome: str,
    committed_revision: DesiredStateRevision,
    publisher_job_id: str | None,
) -> DesiredStatePublishEvidence:
    evidence = DesiredStatePublishEvidence(
        outcome=outcome,
        environment=desired.environment,
        occurrence_id=desired.source.occurrence_id,
        release_id=desired.promotion.release_id,
        deployment_id=desired.promotion.deployment_id,
        desired_state_sha256=desired.sha256,
        previous_revision=desired.previous.revision,
        committed_revision=committed_revision,
        published_at=desired.promoted_at,
    )
    if publisher_job_id is None:
        return evidence
    return replace(
        evidence,
        preparation_job_id=desired.source.job_id,
        publisher_job_id=publisher_job_id,
        schema=AIRFLOW_DESIRED_STATE_PUBLISH_V2_SCHEMA,
    )


class DesiredStatePublishError(RuntimeError):
    """A publish request failed without exposing backend or credential data."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        state_may_have_changed: bool = False,
    ) -> None:
        self.code = code
        self.state_may_have_changed = state_may_have_changed
        super().__init__(f"{code}: {message}")


class AirflowDesiredStatePublisher:
    """Publish one canonical envelope without any unconditional write path."""

    def __init__(
        self,
        *,
        reader: AirflowDesiredStateReader,
        writer: AirflowDesiredStateWriter,
        source_authorizer: DesiredStateSourceAuthorizer,
        max_write_attempts: int = 4,
    ) -> None:
        if not 1 <= max_write_attempts <= 4:
            raise ValueError("max_write_attempts must be between one and four")
        self._reader = reader
        self._writer = writer
        self._source_authorizer = source_authorizer
        self._max_write_attempts = max_write_attempts

    def publish(self, request: DesiredStatePublishRequest) -> DesiredStatePublishEvidence:
        """Create, replace, or reconcile one exact desired-state occurrence."""

        candidate = request.candidate
        desired_without_predecessor = self._build_desired(request)
        current = self._read_current()
        if current is not None and current.desired.environment != candidate.environment:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "authoritative desired state belongs to another environment",
            )
        if (
            current is not None
            and current.desired.source.occurrence_id == desired_without_predecessor.source.occurrence_id
        ):
            return self._replay_existing(
                request=request,
                candidate=desired_without_predecessor,
                current=current,
            )
        self._require_expected_predecessor(current, candidate.expected_revision)
        desired = replace(
            desired_without_predecessor,
            previous=DesiredStatePrevious(
                revision=None if current is None else current.revision,
                deployment_id=None if current is None else current.desired.promotion.deployment_id,
            ),
        )
        return self._write_and_reconcile(
            desired=desired,
            predecessor=current,
            publisher_job_id=request.publisher_job_id,
        )

    def _build_desired(self, request: DesiredStatePublishRequest) -> AirflowDesiredDeployment:
        candidate = request.candidate
        intent = request.intent
        try:
            return AirflowDesiredDeployment.from_mapping(
                {
                    "schema": AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA,
                    "environment": candidate.environment,
                    "source": {
                        "project": candidate.project,
                        "ref": candidate.source_ref,
                        "pipeline_id": candidate.pipeline_id,
                        "job_id": candidate.job_id,
                        "occurrence_id": intent.occurrence_id,
                        "git_sha": candidate.git_sha,
                    },
                    "promotion": candidate.promotion_mapping(),
                    "previous": {"revision": None, "deployment_id": None},
                    "promoted_at": intent.promoted_at,
                }
            )
        except (AirflowDesiredStateError, TypeError, ValueError) as exc:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_REQUEST_INVALID",
                "desired-state publish input is invalid",
            ) from exc

    def _read_current(self) -> _ObservedDesiredState | None:
        try:
            result = self._reader.read(
                max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES,
                if_changed_from=None,
            )
        except DesiredStateReadUnavailable as exc:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE",
                "authoritative desired state could not be read",
            ) from exc
        except DesiredStatePortError as exc:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE",
                "desired-state reader failed",
            ) from exc
        if result.status is DesiredStateReadStatus.ABSENT:
            return None
        if result.status is DesiredStateReadStatus.UNCHANGED:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "unconditional authoritative read returned unchanged",
            )
        assert result.body is not None and result.revision is not None
        try:
            desired = AirflowDesiredDeployment.from_json(result.body)
        except AirflowDesiredStateError as exc:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "authoritative desired state is invalid",
            ) from exc
        return _ObservedDesiredState(result.body, result.revision, desired)

    @staticmethod
    def _require_expected_predecessor(
        current: _ObservedDesiredState | None,
        expected_revision: DesiredStateRevision | None,
    ) -> None:
        observed_revision = None if current is None else current.revision
        if observed_revision != expected_revision:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT",
                "expected revision does not match authoritative desired state",
            )

    def _replay_existing(
        self,
        *,
        request: DesiredStatePublishRequest,
        candidate: AirflowDesiredDeployment,
        current: _ObservedDesiredState,
    ) -> DesiredStatePublishEvidence:
        if current.desired.previous.revision != request.candidate.expected_revision:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT",
                "replayed occurrence does not match its predecessor revision",
            )
        replay = replace(candidate, previous=current.desired.previous)
        if replay.to_json_bytes() != current.body:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "one occurrence identifier resolves to different canonical bytes",
            )
        return _evidence(
            desired=replay,
            outcome="idempotent",
            committed_revision=current.revision,
            publisher_job_id=request.publisher_job_id,
        )

    def _write_and_reconcile(
        self,
        *,
        desired: AirflowDesiredDeployment,
        predecessor: _ObservedDesiredState | None,
        publisher_job_id: str | None,
    ) -> DesiredStatePublishEvidence:
        body = desired.to_json_bytes()
        for attempt in range(1, self._max_write_attempts + 1):
            try:
                self._authorize_source(desired)
                result = self._conditional_write(predecessor=predecessor, body=body)
            except DesiredStateSourceUnauthorized as exc:
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_UNAUTHORIZED",
                    "source SHA is not the trusted protected-ref head",
                ) from exc
            except DesiredStateSourceUnavailable as exc:
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_UNAVAILABLE",
                    "protected source head could not be established",
                ) from exc
            except DesiredStateConditionalWriteConflict as exc:
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT",
                    "conditional desired-state write lost",
                ) from exc
            except DesiredStateWriteContention as exc:
                committed = self._reconcile_after_write(
                    desired=desired,
                    predecessor=predecessor,
                    cause=exc,
                )
                if committed is not None:
                    return _evidence(
                        desired=desired,
                        outcome="idempotent",
                        committed_revision=committed,
                        publisher_job_id=publisher_job_id,
                    )
                if attempt == self._max_write_attempts:
                    raise DesiredStatePublishError(
                        "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT",
                        "conditional write contention exhausted its bounded attempts",
                    ) from exc
                continue
            except DesiredStateWriteUncertain as exc:
                committed = self._reconcile_after_write(
                    desired=desired,
                    predecessor=predecessor,
                    cause=exc,
                )
                if committed is not None:
                    return _evidence(
                        desired=desired,
                        outcome="idempotent",
                        committed_revision=committed,
                        publisher_job_id=publisher_job_id,
                    )
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN",
                    "ambiguous write could not be reconciled safely",
                    state_may_have_changed=True,
                ) from exc
            except DesiredStatePortError as exc:
                committed = self._reconcile_after_write(
                    desired=desired,
                    predecessor=predecessor,
                    cause=exc,
                )
                if committed is not None:
                    return _evidence(
                        desired=desired,
                        outcome="idempotent",
                        committed_revision=committed,
                        publisher_job_id=publisher_job_id,
                    )
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN",
                    "conditional desired-state writer returned an unknown outcome",
                    state_may_have_changed=True,
                ) from exc
            except Exception as exc:  # noqa: BLE001 - unknown port outcome is ambiguous.
                committed = self._reconcile_after_write(
                    desired=desired,
                    predecessor=predecessor,
                    cause=DesiredStateWriteUncertain("unknown writer failure"),
                )
                if committed is not None:
                    return _evidence(
                        desired=desired,
                        outcome="idempotent",
                        committed_revision=committed,
                        publisher_job_id=publisher_job_id,
                    )
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN",
                    "conditional desired-state writer returned an unknown outcome",
                    state_may_have_changed=True,
                ) from exc
            outcome = "created" if predecessor is None else "replaced"
            return _evidence(
                desired=desired,
                outcome=outcome,
                committed_revision=result.revision,
                publisher_job_id=publisher_job_id,
            )
        raise AssertionError("bounded write loop must return or raise")

    def _authorize_source(self, desired: AirflowDesiredDeployment) -> None:
        self._source_authorizer.authorize(
            project=desired.source.project,
            git_sha=desired.source.git_sha,
        )

    def _conditional_write(
        self,
        *,
        predecessor: _ObservedDesiredState | None,
        body: bytes,
    ) -> DesiredStateWriteResult:
        if predecessor is None:
            return self._writer.create_if_absent(body)
        return self._writer.replace_if_revision(predecessor.revision, body)

    def _reconcile_after_write(
        self,
        *,
        desired: AirflowDesiredDeployment,
        predecessor: _ObservedDesiredState | None,
        cause: DesiredStatePortError,
    ) -> DesiredStateRevision | None:
        try:
            observed = self._read_current()
        except DesiredStatePublishError as exc:
            if exc.code == "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE":
                raise DesiredStatePublishError(
                    "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN",
                    "remote state could not be reconciled after a conditional write",
                    state_may_have_changed=True,
                ) from cause
            raise
        body = desired.to_json_bytes()
        if observed is not None and observed.body == body:
            return observed.revision
        if observed is not None and observed.desired.environment != desired.environment:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "authoritative desired state belongs to another environment",
            )
        if observed is not None and observed.desired.source.occurrence_id == desired.source.occurrence_id:
            raise DesiredStatePublishError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "one occurrence identifier resolves to different canonical bytes",
            )
        if _same_predecessor(observed, predecessor):
            return None
        raise DesiredStatePublishError(
            "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT",
            "authoritative predecessor changed during reconciliation",
        )


def _same_predecessor(
    observed: _ObservedDesiredState | None,
    predecessor: _ObservedDesiredState | None,
) -> bool:
    if observed is None or predecessor is None:
        return observed is predecessor
    return observed.revision == predecessor.revision and observed.body == predecessor.body


__all__ = [
    "AirflowDesiredStatePublisher",
    "DesiredStatePublishError",
]
