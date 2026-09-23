"""Trusted entry rejects unauthenticated inputs without invoking providers."""

from pathlib import Path

import pytest

from dpone.app.mssql_sqlclient_observe_departure_composition import settle_prepared_observe
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation


def test_invalid_composition_types_do_not_call_clock_or_supplier(tmp_path):
    calls = []
    with pytest.raises(Exception):
        settle_prepared_observe(
            object(),
            admitted_factory=object(),
            pool=object(),
            evidence_root=Path(tmp_path),
            departure_launcher=object(),
            observer_admission=object(),
            management_credentials=lambda: calls.append("credentials"),
            deadline=10.0,
            helper_startup_timeout=10.0,
            termination_timeout=2.0,
            clock=lambda: calls.append("clock"),
        )
    assert calls == []


@pytest.fixture
def prepared(composed_preparation, tmp_path):
    from tests.test_mssql_sqlclient_preparation_composition import (
        test_actual_original_join_acknowledges_file_before_prepared,
    )

    test_actual_original_join_acknowledges_file_before_prepared(composed_preparation, tmp_path)
    return composed_preparation


@pytest.mark.parametrize("clock_failure", [True, False])
def test_first_callback_failure_retains_owner_and_consumes_original(prepared, clock_failure):
    from time import monotonic

    from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
    from dpone.app.mssql_sqlclient_observe_departure_supervision import SqlClientObserveDepartureUnknown

    h = prepared
    callbacks = []
    deadline = h.attempt._prepared_origin.deadline
    original = h.attempt._prepared_origin
    old_cleanup = h.handle._cleanup_deadline
    launcher = object.__new__(PythonSqlClientDepartureLauncher)

    def clock():
        callbacks.append("clock")
        assert h.attempt._busy
        assert h.attempt._observe_settlement is not None
        if clock_failure:
            raise RuntimeError("clock unavailable")
        # A provider may catch nested entry, but that cannot revive the owner.
        with pytest.raises(Exception):
            invoke()
        return monotonic()

    def invoke():
        return settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=launcher,
            observer_admission=h.cursor.management,
            management_credentials=lambda: callbacks.append("supplier"),
            deadline=deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
            clock=clock,
        )

    with pytest.raises(SqlClientObserveDepartureUnknown) as error:
        invoke()
    state = error.value.retained
    assert state.owner is h.attempt._observe_settlement
    assert state.owner.origin is original
    assert h.attempt._poisoned
    assert state.helper.child is None
    assert "supplier" not in callbacks
    assert h.handle._cleanup_deadline == old_cleanup


def scripted_helper(h, monkeypatch, *, fault=None):
    """Script only process/SQL I/O; run the actual observer and private codecs."""
    import sys
    from dataclasses import astuple, replace
    from datetime import datetime
    from time import monotonic_ns
    from types import SimpleNamespace
    from uuid import UUID

    from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import SqlClientCreateExclusionObserverV2
    from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
    from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL, PRINCIPALS_SQL, VISIBILITY_SQL
    from dpone.app.mssql_sqlclient_departure_request import decode_observe_departure_credentials
    from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
    from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
        encode_observe_departure_result,
        make_observe_departure_result,
    )
    from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
    from dpone.contracts.mssql_tds_worker import TdsChildExit
    from tests.mssql_sqlclient_departure_v2_fixtures import sample
    from tests.test_mssql_sqlclient_create_exclusion_v2 import V2Cursor

    origin = h.attempt._prepared_origin
    management = origin.management_admission
    principal = origin.management_incarnation.authority.principal_resolution.principal
    original = sample(reused=False)
    own = replace(
        original.observer,
        connection_id=UUID(int=777),
        connect_time=datetime.max,
        login_time=datetime.max,
        session_id=origin.authority.session.session_id + 1,
        authority=replace(
            origin.management_incarnation.authority,
            login=replace(
                management.login,
                name="verifier",
                original_name="verifier",
                sid="cc",
                original_sid="cc",
                principal_id=302,
                is_sysadmin=False,
            ),
            principal_resolution=replace(
                origin.management_incarnation.authority.principal_resolution,
                kind="mapped_user",
                principal_id=6,
                name="verifier_user",
                sid="cc",
            ),
        ),
        visibility=replace(original.observer.visibility, database_id=management.database.database_id),
    )
    digest = observer_incarnation_digest(own)
    data = replace(
        original,
        original=origin.authority.session,
        database=origin.authority.database,
        admission=management,
        principal=principal,
        observer=own,
        samples=tuple(replace(value, before_sha256=digest, after_sha256=digest) for value in original.samples),
    )
    cursor = V2Cursor(data)
    db, login = management.database, management.login
    cursor.results[VISIBILITY_SQL] = [(16, 3, 1, 1, db.database_id)]
    cursor.results[ADMISSION_SQL] = [
        (
            *astuple(management.server),
            db.database_id,
            db.database_name,
            UUID(db.database_guid),
            bytes.fromhex(db.owner_sid),
            login.principal_id,
            login.name,
            bytes.fromhex(login.sid),
            int(login.is_sysadmin),
            "SQL_LOGIN",
        )
    ]
    cursor.results[PRINCIPALS_SQL] = [
        (principal.principal_id, principal.name, bytes.fromhex(principal.sid), "SQL_USER", "INSTANCE")
    ]
    admission = SqlClientObserverAdmission(
        own.authority.server, own.authority.database, own.authority.login, own.authority.transport
    )
    launcher = PythonSqlClientDepartureLauncher(
        python_executable=Path(sys.executable),
        package_root=Path(__file__).parents[1] / "src",
        implementation_sha256="e" * 64,
        admission=h.admission,
        max_address_space_bytes=2**30,
    )
    helper_startup = replace(h.helper_startup, package_root=str(launcher.package_root))
    events = []

    class Child:
        identity = helper_startup.process
        declared_startup = helper_startup
        startup_receipt = None
        received_result = None

        def startup(self, *, deadline):
            events.append("startup")
            self.startup_receipt = helper_startup
            return helper_startup

        def send_request(self, body, *, deadline):
            events.append("send")
            decoded = decode_observe_departure_credentials(
                body,
                startup=helper_startup,
                admission_sha256=launcher.admission_sha256,
                startup_deadline=self.deadlines["startup_deadline"],
                operation_deadline=deadline,
                max_address_space_bytes=launcher.max_address_space_bytes,
                observer_admission=admission,
            )
            self.request = decoded.request
            assert decoded.connection_material.username == admission.login.name

        def receive_result(self, *, deadline):
            observer = SqlClientCreateExclusionObserverV2(
                cursor,
                admission=management,
                observer_admission=admission,
                operation_deadline_ns=int(deadline * 10**9),
                monotonic_ns=monotonic_ns,
            )
            value = observer.observe_departure_v2(original=data.original, database=data.database, principal=principal)
            result = make_observe_departure_result(self.request, value, observer_admission=admission)
            self.received_result = encode_observe_departure_result(
                result, request=self.request, observer_admission=admission
            )
            events.append("natural EOF")
            if fault == "result":
                raise RuntimeError("post EOF uncertainty")
            return self.received_result

        def wait(self, *, deadline):
            events.append("wait")
            return TdsChildExit(self.identity, 0, True)

        def terminate(self, *, deadline):
            events.append("contain")
            return TdsChildExit(self.identity, -9, True)

        def close(self):
            events.append("close")

    child = Child()

    def spawn(self, **deadlines):
        events.append("spawn")
        child.deadlines = deadlines
        return child

    monkeypatch.setattr(PythonSqlClientDepartureLauncher, "spawn", spawn)
    return SimpleNamespace(
        launcher=launcher,
        admission=admission,
        child=child,
        cursor=cursor,
        events=events,
        material=replace(h.material, username=admission.login.name),
    )


@pytest.mark.parametrize("fault", [None, "result", "final_close"])
def test_actual_prepared_entry_settles_only_after_six_acks_and_cleanup(prepared, monkeypatch, fault):
    from time import monotonic
    from uuid import uuid4

    from dpone.adapters.mssql_sqlclient_observe_containment_evidence_actor import (
        SqlClientObserveContainmentEvidenceActor,
    )
    from dpone.app.mssql_sqlclient_observe_departure_supervision import SqlClientObserveDepartureUnknown
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand

    h = prepared
    fixture = scripted_helper(h, monkeypatch, fault=fault)
    original = h.attempt._prepared_origin
    expected = original.expected
    if fault == "final_close":
        actual_close = SqlClientObserveContainmentEvidenceActor.close

        def close(self, *, deadline):
            actual_close(self, deadline=deadline)
            raise RuntimeError("close acknowledgement lost")

        monkeypatch.setattr(SqlClientObserveContainmentEvidenceActor, "close", close)

    def run():
        return settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=fixture.launcher,
            observer_admission=fixture.admission,
            management_credentials=lambda: fixture.material,
            deadline=original.deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
        )

    if fault:
        with pytest.raises(SqlClientObserveDepartureUnknown) as error:
            run()
        state = error.value.retained
        assert type(state.raw_result) is bytes
        assert "natural EOF" in fixture.events
        assert state.raw_result is fixture.child.received_result
        assert h.attempt._poisoned and not state.owner.complete
        assert (state.owner.remote_ack is not None) == (fault == "final_close")
        with pytest.raises(Exception):
            h.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.GRANT, "a" * 64, deadline=monotonic() + 1)
    else:
        outcome = run()
        assert outcome.prepared == expected
        assert len(outcome.helper_receipts) == 6
        assert fixture.cursor.capture_count == 13
        assert fixture.events == ["spawn", "startup", "send", "natural EOF", "wait", "close"]
        assert h.attempt._observe_settlement.complete
        association = h.attempt._permission_grant(
            uuid4(), "a" * 64, original.identity.implementation_sha256, deadline=original.deadline
        )
        reservation = association.reserve().reservation
        assert association.identity.command is TdsCoordinatorCommand.GRANT
        assert reservation.state.slots[-1].operation_id == association.identity.operation_id
    assert h.attempt._lifecycle.snapshot == expected


@pytest.mark.parametrize(
    "field",
    [
        "_child",
        "_helper_evidence",
        "_child_closed",
        "_helper_evidence_closed",
        "_process",
        "_startup",
        "_local_exit",
        "_raw_result",
        "_unresolved_launch",
    ],
)
def test_last_sequence_actor_callback_cannot_replace_actual_custody(prepared, monkeypatch, field):
    from contextlib import contextmanager
    from time import monotonic
    from uuid import uuid4

    from dpone.app.mssql_sqlclient_observe_departure_supervision import (
        SqlClientObserveDepartureUnknown,
        _ObserveDepartureRetention,
    )
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
    from dpone.services.mssql_tds_observe_settlement import ObserveSettlement

    h = prepared
    fixture = scripted_helper(h, monkeypatch)
    captured = []
    original_init = _ObserveDepartureRetention.__init__

    def initialize(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        captured.append(self)

    monkeypatch.setattr(_ObserveDepartureRetention, "__init__", initialize)
    actual = h.pool.assert_deadline
    final_exit, mutated = [False], []
    original_sequence = ObserveSettlement.sequence

    @contextmanager
    def sequence(self):
        with original_sequence(self) as owner:
            yield owner
            final_exit[0] = True

    monkeypatch.setattr(ObserveSettlement, "sequence", sequence)

    def assert_deadline(*, deadline):
        actual(deadline=deadline)
        if final_exit[0] and not mutated:
            mutated.append(field)
            from dataclasses import replace

            helper = captured[0].helper
            value = getattr(helper, field)
            if field in ("_process", "_startup", "_local_exit"):
                changed = replace(value)  # Equal DTO values cannot replace the actual producer.
            elif field == "_raw_result":
                changed = bytes(bytearray(value))
                assert changed == value and changed is not value
            else:
                changed = False if field.endswith("closed") else object()
            setattr(helper, field, changed)

    monkeypatch.setattr(h.pool, "assert_deadline", assert_deadline)
    with pytest.raises(SqlClientObserveDepartureUnknown) as error:
        settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=fixture.launcher,
            observer_admission=fixture.admission,
            management_credentials=lambda: fixture.material,
            deadline=h.attempt._prepared_origin.deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
        )
    assert mutated == [field]
    owner = error.value.retained.owner
    assert owner.remote_ack is not None and not owner.complete and owner.pending
    assert h.attempt._poisoned
    with pytest.raises(Exception):
        h.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.GRANT, "a" * 64, deadline=monotonic() + 1)


def test_different_verifier_database_or_server_rejects_before_allocation(prepared, monkeypatch):
    from dataclasses import replace

    from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
    from dpone.app.mssql_sqlclient_observe_departure_supervision import SqlClientObserveDepartureUnknown

    h = prepared
    calls = []
    admitted = h.cursor.management
    admitted = replace(admitted, server=replace(admitted.server, server_name="different"))
    monkeypatch.setattr(h.pool, "open", lambda *args, **kwargs: calls.append("allocation"))
    with pytest.raises(SqlClientObserveDepartureUnknown) as error:
        settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=object.__new__(PythonSqlClientDepartureLauncher),
            observer_admission=admitted,
            management_credentials=lambda: calls.append("supplier"),
            deadline=h.attempt._prepared_origin.deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
        )
    assert calls == []
    assert error.value.retained.owner.local_ack is None
    assert h.attempt._poisoned
