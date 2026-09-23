"""Guarded SQL Server effects for exact failed-attempt retirement."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256

from dpone.adapters.mssql_sqlclient_failed_retirement_operator import DropOutcome
from dpone.adapters.mssql_sqlclient_native_retirement_session import (
    LOCK_RESOURCE,
    OpenManagementSession,
    SqlClientRetirementSession,
    TdsLockObservation,
)
from dpone.adapters.mssql_sqlclient_native_retirement_sql import (
    DROP_EXACT_STAGE_SQL,
    OBSERVE_EXACT_STAGE_SQL,
    drop_exact_parameters,
    observe_stage_parameters,
    stage_catalog_status,
)
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import SqlClientFailedRetirementRequest
from dpone.ports.mssql_sqlclient_writer_settlement import SqlClientObserverAdmission

_ERROR = "mssql_native.sqlclient_failed_retirement_unknown"


def _digest(*parts: object) -> str:
    return sha256(":".join(str(part) for part in parts).encode()).hexdigest()


class ProductionSqlClientFailedRetirementEffects:
    """Execute exact stage observation and DROP under the canonical DDL lock."""

    def __init__(
        self,
        *,
        open_management_session: OpenManagementSession,
        admission: SqlClientObserverAdmission,
        expected_server: object,
        expected_database: object,
        assert_current: Callable[[SqlClientFailedRetirementRequest], None],
        observe_capacity_release: Callable[[SqlClientFailedRetirementRequest], bool],
        monotonic: Callable[[], float] = time.monotonic,
        operation_timeout: float = 30.0,
    ) -> None:
        if (
            type(admission) is not SqlClientObserverAdmission
            or admission.server != expected_server
            or admission.database != expected_database
            or not callable(open_management_session)
            or not callable(assert_current)
            or not callable(observe_capacity_release)
            or not callable(monotonic)
            or type(operation_timeout) not in (int, float)
            or operation_timeout <= 0
        ):
            raise ValueError(_ERROR)
        admission.__post_init__()
        self._open, self._admission = open_management_session, admission
        self._assert_current, self._capacity = assert_current, observe_capacity_release
        self._clock, self._timeout = monotonic, float(operation_timeout)

    @contextmanager
    def _session(self, request: SqlClientFailedRetirementRequest) -> Iterator[tuple[SqlClientRetirementSession, float]]:
        stage, database = request.subject.stage, self._admission.database
        if stage is None or (stage.database_id, str(stage.database_guid), stage.database_name) != (
            database.database_id,
            database.database_guid,
            database.database_name,
        ):
            raise ValueError(_ERROR)
        deadline, context = self._clock() + self._timeout, self._open()
        session, primary = context.__enter__(), None
        try:
            if session.admission != self._admission:
                raise ValueError(_ERROR)
            lock = session.acquire_exclusive(LOCK_RESOURCE, deadline=deadline)
            if type(lock) is not TdsLockObservation:
                raise ValueError(_ERROR)
            lock.__post_init__()
            self._assert_current(request)
            yield session, deadline
            self._assert_current(request)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                session.contain(deadline=deadline)
            except BaseException as containment:
                if primary is None:
                    raise
                primary.add_note(f"retirement session containment failed: {type(containment).__name__}")
            finally:
                context.__exit__(
                    type(primary) if primary else None, primary, primary.__traceback__ if primary else None
                )

    def _status(self, request: SqlClientFailedRetirementRequest) -> str:
        stage = request.subject.stage
        if stage is None:
            raise ValueError(_ERROR)
        with self._session(request) as (session, deadline):
            rows = session.query(OBSERVE_EXACT_STAGE_SQL, observe_stage_parameters(stage), deadline=deadline)
        return stage_catalog_status(rows, stage)

    def observe_exact_incarnation(self, request: SqlClientFailedRetirementRequest) -> str:
        if self._status(request) != "exact":
            raise RuntimeError(_ERROR)
        return self._incarnation(request)

    def drop_exact(self, request: SqlClientFailedRetirementRequest, intent_sha256: str) -> DropOutcome:
        stage = request.subject.stage
        if stage is None or type(intent_sha256) is not str:
            raise ValueError(_ERROR)
        try:
            with self._session(request) as (session, deadline):
                try:
                    rows = session.query(OBSERVE_EXACT_STAGE_SQL, observe_stage_parameters(stage), deadline=deadline)
                    status = stage_catalog_status(rows, stage)
                    if status == "absent":
                        return "succeeded"
                    if status != "exact":
                        raise RuntimeError(_ERROR)
                    session.execute(DROP_EXACT_STAGE_SQL, drop_exact_parameters(stage), deadline=deadline)
                    self._assert_current(request)
                    session.commit(deadline=deadline)
                except BaseException as primary:
                    try:
                        session.rollback(deadline=deadline)
                    except BaseException as rollback:
                        primary.add_note(f"retirement rollback failed: {type(rollback).__name__}")
                    raise
        except (ValueError, RuntimeError):
            raise
        except Exception:
            return "unknown"
        return "succeeded"

    def reconcile_drop(self, request: SqlClientFailedRetirementRequest, intent_sha256: str) -> DropOutcome:
        if type(intent_sha256) is not str:
            raise ValueError(_ERROR)
        status = self._status(request)
        if status == "absent":
            return "succeeded"
        if status == "exact":
            return "no_effect"
        raise RuntimeError(_ERROR)

    def observe_absence(self, request: SqlClientFailedRetirementRequest, drop_sha256: str) -> str:
        if type(drop_sha256) is not str or self._status(request) != "absent":
            raise RuntimeError(_ERROR)
        return _digest(self._incarnation(request), drop_sha256, "absent")

    def observe_capacity_release(self, request: SqlClientFailedRetirementRequest, directory_sha256: str) -> str:
        self._assert_current(request)
        if type(directory_sha256) is not str or self._capacity(request) is not True:
            raise RuntimeError(_ERROR)
        return _digest(request.request_sha256, directory_sha256, "released")

    @staticmethod
    def _incarnation(request: SqlClientFailedRetirementRequest) -> str:
        subject = request.subject
        return _digest(subject.subject_sha256, subject.object_identity, subject.expected_object_nonce)


__all__ = ("ProductionSqlClientFailedRetirementEffects",)
