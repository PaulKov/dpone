from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from runpy import run_path

import pytest

from dpone.adapters.mssql_sqlclient_native_retirement_operator import SqlClientNativeRetirementOperator
from dpone.adapters.mssql_sqlclient_native_retirement_sql import (
    DROP_EXACT_STAGE_SQL,
    drop_exact_parameters,
    observe_stage_parameters,
    stage_catalog_status,
    stage_is_exact,
)
from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkDropIntent,
    NativeChunkDropOutcome,
    NativeChunkRetirementReservation,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_coordinator_authority import LOCK_RESOURCE, TdsLockObservation
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits

_FIXTURES = run_path(str(Path(__file__).with_name("test_mssql_native_chunk_retirement.py")))


def _admission(request):
    stage = request.projection.stage
    server = SqlClientServerAuthority("server", "machine", "instance", "physical")
    database = SqlClientDatabaseAuthority(stage.database_id, stage.database_name, str(stage.database_guid), "aa")
    login = SqlClientLoginAuthority(5, "manager", "bb", "manager", "bb", 1, False)
    transport = SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE")
    return SqlClientObserverAdmission(server, database, login, transport)


def _row(stage):
    return (
        stage.object_id,
        stage.schema_id,
        stage.table_name,
        stage.create_date,
        stage.owner_binding,
        str(stage.object_nonce),
        stage.database_id,
        str(stage.database_guid),
        stage.database_name,
    )


class _Session:
    def __init__(self, admission, rows, *, fail=False, fail_commit=False, fail_rollback=False, fail_contain=False):
        self.admission = admission
        self.rows = rows
        self.fail = fail
        self.fail_commit = fail_commit
        self.fail_rollback = fail_rollback
        self.fail_contain = fail_contain
        self.calls = []

    def acquire_exclusive(self, resource, *, deadline):
        self.calls.append(("lock", resource, deadline))
        return TdsLockObservation(0, resource=resource)

    def query(self, sql, parameters, *, deadline):
        self.calls.append(("query", sql, parameters))
        return self.rows

    def execute(self, sql, parameters, *, deadline):
        self.calls.append(("execute", sql, parameters))
        if self.fail:
            raise RuntimeError("private driver diagnostic")

    def commit(self, *, deadline):
        self.calls.append(("commit", deadline))
        if self.fail_commit:
            raise RuntimeError("lost commit ack")

    def rollback(self, *, deadline):
        self.calls.append(("rollback", deadline))
        if self.fail_rollback:
            raise RuntimeError("rollback unknown")

    def contain(self, *, deadline):
        self.calls.append(("contain", deadline))
        if self.fail_contain:
            raise RuntimeError("containment unknown")


class _Unused:
    def read(self, *args):
        return None


def _operator(request, sessions, progress=None, *, exit_error=False):
    admission = _admission(request)

    @contextmanager
    def open_session():
        try:
            yield sessions.pop(0)
        finally:
            if exit_error:
                raise RuntimeError("context exit failed")

    operator = SqlClientNativeRetirementOperator(
        open_management_session=open_session,
        admission=admission,
        expected_server=admission.server,
        expected_database=admission.database,
        attempts=_Unused(),
        directories=_Unused(),
        directory_limits=TdsDirectoryLimits(4, 1, 4096, 512),
        progress=progress or _Unused(),
    )
    operator._assert_current = lambda _request: None
    return operator


def test_guarded_drop_uses_parameters_and_commits_exact_identity():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    session = _Session(admission, ())
    operator = _operator(request, [session])
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = operator.drop_exact(request, reservation, intent)

    assert proof.outcome is NativeChunkDropOutcome.SUCCEEDED
    assert session.calls[-2][0:3] == (
        "execute",
        DROP_EXACT_STAGE_SQL,
        drop_exact_parameters(request.projection.stage),
    )
    assert session.calls[-1][0] == "commit"
    assert session.calls[0][0:2] == ("lock", LOCK_RESOURCE)
    assert request.projection.stage.table_name not in DROP_EXACT_STAGE_SQL


def test_driver_uncertainty_rolls_back_and_returns_unknown_without_diagnostic():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    session = _Session(admission, (), fail=True)
    operator = _operator(request, [session])
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = operator.drop_exact(request, reservation, intent)

    assert proof.outcome is NativeChunkDropOutcome.UNKNOWN
    assert [call[0] for call in session.calls[-2:]] == ["rollback", "contain"]
    assert "private" not in repr(proof)


@pytest.mark.parametrize(
    "rows,outcome", [((), NativeChunkDropOutcome.SUCCEEDED), ("exact", NativeChunkDropOutcome.NO_EFFECT)]
)
def test_reconciliation_distinguishes_absence_from_exact_no_effect(rows, outcome):
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    result_rows = (_row(request.projection.stage),) if rows == "exact" else ()
    operator = _operator(request, [_Session(admission, result_rows)])
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = operator.reconcile_drop(request, reservation, intent, None)

    assert proof.outcome is outcome


def test_changed_incarnation_fails_closed():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    changed = list(_row(request.projection.stage))
    changed[5] = "00000000-0000-0000-0000-000000000099"
    operator = _operator(request, [_Session(admission, (tuple(changed),))])

    with pytest.raises(ValueError, match="retirement_effect_invalid"):
        operator.observe_exact_incarnation(request)


def test_catalog_parser_rejects_extra_or_partial_rows():
    request = _FIXTURES["_request"]()
    stage = request.projection.stage
    assert stage_is_exact((_row(stage),), stage)
    assert not stage_is_exact((_row(stage), _row(stage)), stage)
    assert not stage_is_exact(((_row(stage)[:-1]),), stage)


def test_catalog_query_binds_old_object_and_qualified_namespace():
    request = _FIXTURES["_request"]()
    assert observe_stage_parameters(request.projection.stage) == (
        request.projection.stage.object_id,
        request.projection.stage.schema_id,
        request.projection.stage.table_name,
    )


def test_foreign_namespace_replacement_fails_closed():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    replacement = list(_row(request.projection.stage))
    replacement[0] += 1
    replacement[5] = "00000000-0000-0000-0000-000000000099"
    session = _Session(admission, (tuple(replacement),))
    operator = _operator(request, [session])
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    with pytest.raises(ValueError, match="retirement_effect_invalid"):
        operator.reconcile_drop(request, reservation, intent, None)

    assert stage_catalog_status((tuple(replacement),), request.projection.stage) == "foreign"
    assert session.calls[-1][0] == "contain"


@pytest.mark.parametrize("fail_rollback,fail_contain", [(False, False), (True, False), (True, True)])
def test_lost_commit_ack_is_unknown_and_always_contains_session(fail_rollback, fail_contain):
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    session = _Session(
        admission,
        (),
        fail_commit=True,
        fail_rollback=fail_rollback,
        fail_contain=fail_contain,
    )
    operator = _operator(request, [session])
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = operator.drop_exact(request, reservation, intent)

    assert proof.outcome is NativeChunkDropOutcome.UNKNOWN
    assert [call[0] for call in session.calls].count("contain") == 1


def test_takeover_between_execute_and_commit_returns_unknown_and_contains():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    session = _Session(admission, ())
    operator = _operator(request, [session])
    checks = iter((None, RuntimeError("lease lost")))

    def assert_current(_request):
        outcome = next(checks)
        if outcome is not None:
            raise outcome

    operator._assert_current = assert_current
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = operator.drop_exact(request, reservation, intent)

    assert proof.outcome is NativeChunkDropOutcome.UNKNOWN
    assert not any(call[0] == "commit" for call in session.calls)
    assert session.calls[-1][0] == "contain"


def test_restart_after_unknown_opens_a_new_canonical_authority():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    failed = _Session(admission, (), fail=True)
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)
    assert (
        _operator(request, [failed]).drop_exact(request, reservation, intent).outcome is NativeChunkDropOutcome.UNKNOWN
    )

    restarted = _Session(admission, ())
    proof = _operator(request, [restarted]).reconcile_drop(request, reservation, intent, None)

    assert proof.outcome is NativeChunkDropOutcome.SUCCEEDED
    assert restarted.calls[0][0:2] == ("lock", LOCK_RESOURCE)


def test_unknown_is_preserved_after_context_exit_failure():
    request = _FIXTURES["_request"]()
    admission = _admission(request)
    session = _Session(admission, (), fail=True)
    reservation = NativeChunkRetirementReservation.deterministic(request)
    intent = NativeChunkDropIntent(reservation.operation_sha256, 0, reservation.operation_sha256)

    proof = _operator(request, [session], exit_error=True).drop_exact(request, reservation, intent)

    assert proof.outcome is NativeChunkDropOutcome.UNKNOWN
    assert session.calls[-1][0] == "contain"
