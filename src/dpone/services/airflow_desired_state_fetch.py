"""Application service for bounded Airflow desired-state fetch."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.airflow_desired_state import (
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
    AirflowDesiredStateError,
    DesiredStateFetchEvidence,
    DesiredStateRevision,
)
from dpone.ports.airflow_desired_state import (
    AirflowDesiredStateReader,
    AirflowDesiredStateSnapshotWriter,
    DesiredStatePortError,
    DesiredStateReadResult,
    DesiredStateReadStatus,
)


class DesiredStateFetchError(RuntimeError):
    """A fetch failed without exposing object-storage details."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class DesiredStateFetchRequest:
    environment: str
    previous_revision: DesiredStateRevision | None = None


class AirflowDesiredStateFetcher:
    """Fetch, validate, and atomically commit one desired-state snapshot."""

    def __init__(
        self,
        *,
        reader: AirflowDesiredStateReader,
        snapshot_writer: AirflowDesiredStateSnapshotWriter,
    ) -> None:
        self._reader = reader
        self._snapshot_writer = snapshot_writer

    def fetch(self, request: DesiredStateFetchRequest) -> DesiredStateFetchEvidence:
        result = self._read(request.previous_revision)
        if result.status is DesiredStateReadStatus.UNCHANGED:
            result = self._read(None)
        if result.status is DesiredStateReadStatus.ABSENT:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_NOT_FOUND",
                "desired state is not present",
            )
        if result.status is DesiredStateReadStatus.UNCHANGED:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "unconditional desired-state read returned unchanged",
            )
        assert result.body is not None and result.revision is not None
        try:
            desired = AirflowDesiredDeployment.from_json(result.body)
        except AirflowDesiredStateError as exc:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
                "desired state is invalid",
            ) from exc
        if desired.environment != request.environment:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_ENVIRONMENT_MISMATCH",
                "desired state belongs to another environment",
            )
        try:
            self._snapshot_writer.commit(result.body)
        except OSError as exc:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_LOCAL_COMMIT_FAILED",
                "verified desired state could not be committed locally",
            ) from exc
        return DesiredStateFetchEvidence.fetched(desired, result.revision)

    def _read(
        self,
        revision: DesiredStateRevision | None,
    ) -> DesiredStateReadResult:
        try:
            return self._reader.read(
                max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES,
                if_changed_from=revision,
            )
        except DesiredStatePortError as exc:
            raise DesiredStateFetchError(
                "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE",
                "desired state could not be fetched",
            ) from exc


__all__ = [
    "AirflowDesiredStateFetcher",
    "DesiredStateFetchError",
    "DesiredStateFetchRequest",
]
