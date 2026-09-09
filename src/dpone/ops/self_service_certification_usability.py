"""Privacy-safe reader and metric derivation for first-time-user studies."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any

from dpone.ops.self_service_certification_files import SelfServiceEvidenceFileError, read_json
from dpone.ops.self_service_certification_models import (
    SelfServiceCertificationError,
    UsabilityEvidence,
    UsabilityThresholds,
)

_MAX_STUDY_BYTES = 2 * 1024 * 1024
_MAX_SESSIONS = 100
_MAX_SESSION_SECONDS = 24 * 60 * 60
_STUDY_FIELDS = frozenset(
    {"schema", "protocol", "target_commit", "facilitator_ref", "started_at", "completed_at", "sessions"}
)
_SESSION_FIELDS = frozenset(
    {
        "session_id",
        "participant_ref",
        "first_time_dpone_user",
        "consent_recorded",
        "started_at",
        "dag_preview_at",
        "safe_sample_at",
        "finished_at",
        "outcome",
        "commands_used",
        "assistance_events",
        "authored_airflow_python",
        "transcript_sha256",
        "blocker_codes",
    }
)


class SelfServiceUsabilityReader:
    """Read one bounded study and expose aggregate metrics only."""

    def __init__(self, *, thresholds: UsabilityThresholds | None = None) -> None:
        self._thresholds = thresholds or UsabilityThresholds()

    def read(self, path: Path, *, expected_commit: str) -> UsabilityEvidence:
        try:
            payload, raw = read_json(
                path,
                max_bytes=_MAX_STUDY_BYTES,
                label="self-service usability study",
            )
        except SelfServiceEvidenceFileError as exc:
            if exc.code == "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE":
                raise SelfServiceCertificationError(
                    "DPONE_SELF_SERVICE_STUDY_INPUT_UNSAFE",
                    "Usability study must be an available regular local file without symlink traversal.",
                ) from exc
            return self._failed(None, "self_service.usability_input_invalid")
        source_digest = "sha256:" + sha256(raw).hexdigest()
        try:
            sessions = _parse_study(payload, expected_commit=expected_commit)
        except _StudyViolation as exc:
            return self._failed(source_digest, exc.code)
        return self._evaluate(sessions, source_digest=source_digest)

    def missing(self) -> UsabilityEvidence:
        return UsabilityEvidence(
            status="UNVERIFIED",
            supplied=False,
            source_sha256=None,
            valid_sessions=0,
            successful_sessions=0,
            complete_success_rate=None,
            first_dag_target_rate=None,
            safe_sample_target_rate=None,
            no_airflow_python_rate=None,
            p50_first_dag_seconds=None,
            p50_safe_sample_seconds=None,
            thresholds=self._thresholds,
            blockers=("self_service.usability_study_missing",),
        )

    def _evaluate(self, sessions: tuple[_Session, ...], *, source_digest: str) -> UsabilityEvidence:
        count = len(sessions)
        successful = sum(session.complete_success(self._thresholds) for session in sessions)
        dag_times = tuple(session.first_dag_seconds for session in sessions if session.first_dag_seconds is not None)
        sample_times = tuple(
            session.safe_sample_seconds for session in sessions if session.safe_sample_seconds is not None
        )
        success_rate = _rate(successful, count)
        dag_rate = _rate(
            sum(value <= self._thresholds.maximum_first_dag_seconds for value in dag_times),
            count,
        )
        sample_rate = _rate(
            sum(value <= self._thresholds.maximum_safe_sample_seconds for value in sample_times),
            count,
        )
        no_python_rate = _rate(sum(not session.authored_airflow_python for session in sessions), count)
        enough = count >= self._thresholds.minimum_participants
        passed = (
            enough
            and success_rate >= self._thresholds.minimum_success_rate
            and dag_rate >= self._thresholds.minimum_success_rate
            and sample_rate >= self._thresholds.minimum_success_rate
            and no_python_rate == 1.0
        )
        status = "PASS" if passed else ("FAIL" if enough else "UNVERIFIED")
        blockers = _study_blockers(
            status=status,
            enough=enough,
            success_rate=success_rate,
            dag_rate=dag_rate,
            sample_rate=sample_rate,
            no_python_rate=no_python_rate,
            thresholds=self._thresholds,
        )
        return UsabilityEvidence(
            status=status,
            supplied=True,
            source_sha256=source_digest,
            valid_sessions=count,
            successful_sessions=successful,
            complete_success_rate=success_rate,
            first_dag_target_rate=dag_rate,
            safe_sample_target_rate=sample_rate,
            no_airflow_python_rate=no_python_rate,
            p50_first_dag_seconds=_median(dag_times),
            p50_safe_sample_seconds=_median(sample_times),
            thresholds=self._thresholds,
            blockers=blockers,
        )

    def _failed(self, source_digest: str | None, blocker: str) -> UsabilityEvidence:
        return UsabilityEvidence(
            status="FAIL",
            supplied=True,
            source_sha256=source_digest,
            valid_sessions=0,
            successful_sessions=0,
            complete_success_rate=None,
            first_dag_target_rate=None,
            safe_sample_target_rate=None,
            no_airflow_python_rate=None,
            p50_first_dag_seconds=None,
            p50_safe_sample_seconds=None,
            thresholds=self._thresholds,
            blockers=(blocker,),
        )


class _StudyViolation(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _Session:
    __slots__ = (
        "authored_airflow_python",
        "assistance_events",
        "blocker_codes",
        "commands_used",
        "first_dag_seconds",
        "outcome",
        "safe_sample_seconds",
    )

    def __init__(
        self,
        *,
        outcome: str,
        commands_used: int,
        assistance_events: int,
        authored_airflow_python: bool,
        blocker_codes: tuple[str, ...],
        first_dag_seconds: float | None,
        safe_sample_seconds: float | None,
    ) -> None:
        self.outcome = outcome
        self.commands_used = commands_used
        self.assistance_events = assistance_events
        self.authored_airflow_python = authored_airflow_python
        self.blocker_codes = blocker_codes
        self.first_dag_seconds = first_dag_seconds
        self.safe_sample_seconds = safe_sample_seconds

    def complete_success(self, thresholds: UsabilityThresholds) -> bool:
        return bool(
            self.outcome == "passed"
            and self.first_dag_seconds is not None
            and self.first_dag_seconds <= thresholds.maximum_first_dag_seconds
            and self.safe_sample_seconds is not None
            and self.safe_sample_seconds <= thresholds.maximum_safe_sample_seconds
            and self.commands_used == thresholds.maximum_commands
            and self.assistance_events == 0
            and not self.authored_airflow_python
            and not self.blocker_codes
        )


def _parse_study(payload: Mapping[str, Any], *, expected_commit: str) -> tuple[_Session, ...]:
    _exact_fields(payload, _STUDY_FIELDS, code="self_service.usability_schema_invalid")
    if (
        payload.get("schema") != "dpone.self-service-usability-study.v1"
        or payload.get("protocol") != "airflow_first_dag_and_safe_sample_v1"
        or payload.get("target_commit") != expected_commit
    ):
        raise _StudyViolation("self_service.usability_identity_invalid")
    _bounded_text(payload.get("facilitator_ref"), maximum=128)
    study_start = _time(payload.get("started_at"))
    study_end = _time(payload.get("completed_at"))
    if study_end < study_start:
        raise _StudyViolation("self_service.usability_timestamps_invalid")
    raw_sessions = payload.get("sessions")
    if not isinstance(raw_sessions, list) or len(raw_sessions) > _MAX_SESSIONS:
        raise _StudyViolation("self_service.usability_sessions_invalid")
    session_ids: set[str] = set()
    participant_refs: set[str] = set()
    sessions = []
    for raw in raw_sessions:
        session, session_id, participant_ref = _parse_session(raw, study_start=study_start, study_end=study_end)
        if session_id in session_ids or participant_ref in participant_refs:
            raise _StudyViolation("self_service.usability_identity_duplicate")
        session_ids.add(session_id)
        participant_refs.add(participant_ref)
        sessions.append(session)
    return tuple(sessions)


def _parse_session(raw: object, *, study_start: datetime, study_end: datetime) -> tuple[_Session, str, str]:
    if not isinstance(raw, Mapping):
        raise _StudyViolation("self_service.usability_session_invalid")
    _exact_fields(raw, _SESSION_FIELDS, code="self_service.usability_session_invalid")
    session_id = _bounded_text(raw.get("session_id"), maximum=128)
    participant_ref = _digest(raw.get("participant_ref"))
    _digest(raw.get("transcript_sha256"))
    if raw.get("first_time_dpone_user") is not True or raw.get("consent_recorded") is not True:
        raise _StudyViolation("self_service.usability_eligibility_invalid")
    started = _time(raw.get("started_at"))
    finished = _time(raw.get("finished_at"))
    preview = _optional_time(raw.get("dag_preview_at"))
    sample = _optional_time(raw.get("safe_sample_at"))
    timeline = tuple(value for value in (started, preview, sample, finished) if value is not None)
    if (
        started < study_start
        or finished > study_end
        or finished < started
        or (finished - started).total_seconds() > _MAX_SESSION_SECONDS
        or timeline != tuple(sorted(timeline))
    ):
        raise _StudyViolation("self_service.usability_timestamps_invalid")
    outcome = raw.get("outcome")
    if outcome not in {"passed", "failed", "abandoned"}:
        raise _StudyViolation("self_service.usability_session_invalid")
    commands = _bounded_integer(raw.get("commands_used"))
    assistance = _bounded_integer(raw.get("assistance_events"))
    authored_python = raw.get("authored_airflow_python")
    blocker_codes = _blocker_codes(raw.get("blocker_codes"))
    if not isinstance(authored_python, bool) or blocker_codes is None:
        raise _StudyViolation("self_service.usability_session_invalid")
    return (
        _Session(
            outcome=outcome,
            commands_used=commands,
            assistance_events=assistance,
            authored_airflow_python=authored_python,
            blocker_codes=blocker_codes,
            first_dag_seconds=_seconds(started, preview),
            safe_sample_seconds=_seconds(started, sample),
        ),
        session_id,
        participant_ref,
    )


def _study_blockers(
    *,
    status: str,
    enough: bool,
    success_rate: float,
    dag_rate: float,
    sample_rate: float,
    no_python_rate: float,
    thresholds: UsabilityThresholds,
) -> tuple[str, ...]:
    if status == "PASS":
        return ()
    blockers = []
    if not enough:
        blockers.append("self_service.usability_participants_insufficient")
    if enough and success_rate < thresholds.minimum_success_rate:
        blockers.append("self_service.usability_complete_success_below_target")
    if enough and dag_rate < thresholds.minimum_success_rate:
        blockers.append("self_service.usability_first_dag_below_target")
    if enough and sample_rate < thresholds.minimum_success_rate:
        blockers.append("self_service.usability_safe_sample_below_target")
    if enough and no_python_rate < 1.0:
        blockers.append("self_service.usability_airflow_python_authored")
    return tuple(blockers)


def _exact_fields(payload: Mapping[str, Any], allowed: frozenset[str], *, code: str) -> None:
    if set(payload) != allowed:
        raise _StudyViolation(code)


def _time(value: object) -> datetime:
    if not isinstance(value, str):
        raise _StudyViolation("self_service.usability_timestamps_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _StudyViolation("self_service.usability_timestamps_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _StudyViolation("self_service.usability_timestamps_invalid")
    return parsed


def _optional_time(value: object) -> datetime | None:
    return None if value is None else _time(value)


def _bounded_text(value: object, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or any(char.isspace() for char in value):
        raise _StudyViolation("self_service.usability_session_invalid")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:"):
        raise _StudyViolation("self_service.usability_privacy_identity_invalid")
    if any(char not in "0123456789abcdef" for char in value[7:]):
        raise _StudyViolation("self_service.usability_privacy_identity_invalid")
    return value


def _bounded_integer(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 100:
        raise _StudyViolation("self_service.usability_session_invalid")
    return value


def _blocker_codes(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, list) or len(value) > 50:
        return None
    if not all(isinstance(item, str) and 0 < len(item) <= 128 and item.replace("_", "").isalnum() for item in value):
        return None
    return tuple(value) if len(set(value)) == len(value) else None


def _seconds(start: datetime, end: datetime | None) -> float | None:
    return None if end is None else (end - start).total_seconds()


def _rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else round(numerator / denominator, 4)


def _median(values: tuple[float, ...]) -> float | None:
    return None if not values else float(median(values))


__all__ = ["SelfServiceUsabilityReader"]
