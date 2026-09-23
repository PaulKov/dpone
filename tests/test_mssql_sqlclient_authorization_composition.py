"""Sequencing contracts for the exact SQLClient authorization root."""

import hashlib
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path
from threading import Thread
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    decode_connection_admission,
    encode_connection_admission,
)
from dpone.app import mssql_sqlclient_authorization_composition as module
from dpone.app.mssql_sqlclient_permission_reservation_composition import (
    SqlClientPermissionReservationInputs,
)
from dpone.contracts import mssql_sqlclient_restricted_writer_settlement_codec as settlement_codec
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import RestrictedWriterSettlementOperations
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.services.mssql_tds_restricted_writer_settlement import RestrictedWriterVerified
from dpone.services.mssql_tds_writer_admission import admit_sqlclient_writer
from dpone.services.mssql_tds_writer_authority import SqlClientWriterAdmitted
from tests.mssql_sqlclient_authorization_chain_fakes import (
    DepartureLauncher,
    PermissionLauncher,
    VerifyLauncher,
    deployment,
)
from tests.test_mssql_sqlclient_observe_departure_composition import prepared as prepared
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_permission_grant_association import _settled


def _inputs(prepared, tmp_path):
    origin = prepared.attempt._prepared_origin
    pin = TdsBinaryPin(Path(sys.executable), "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    digest = hashlib.sha256(admission).hexdigest()
    launcher = SimpleNamespace(
        admission=admission,
        admission_sha256=digest,
        implementation_sha256=origin.identity.implementation_sha256,
        package_root=tmp_path,
        max_address_space_bytes=1024,
        launch=lambda *args, **kwargs: None,
        spawn=lambda **kwargs: None,
    )
    permission = SqlClientPermissionReservationInputs(
        pool=prepared.pool,
        store_factory=lambda: object(),
        limits=TdsDirectoryLimits(10, 1, 1_000_000, 100),
        lease=WindowLease("target", "owner", 1),
        launcher=launcher,
        admission=admission,
        evidence_root=tmp_path,
        supervisor_token="grant-supervisor",
        operation_id=uuid4(),
        profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
        admission_sha256=digest,
        startup_deadline=origin.deadline - 1.0,
        operation_deadline=origin.deadline,
        termination_timeout=1.0,
        session_nonce=b"n" * 32,
        material_supplier=lambda: TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret"),
    )
    return module.SqlClientAuthorizationInputs(
        permission=permission,
        grant_release_evidence_root=tmp_path,
        grant_settlement_evidence_root=tmp_path,
        grant_departure_launcher=launcher,
        grant_management_credentials=lambda: TdsConnectionMaterial("localhost", 1433, "db", "sa", "secret"),
        containment_deadline=origin.deadline,
        grant_helper_startup_timeout=1.0,
        grant_cleanup_deadline=origin.deadline,
        verify_operation_id=uuid4(),
        verify_launcher=launcher,
        verify_profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
        verify_admission_sha256=digest,
        verify_startup_deadline=origin.deadline - 1.0,
        verify_termination_timeout=1.0,
        verify_credentials=lambda: (
            TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret"),
            b"v" * 32,
        ),
        verify_evidence_root=tmp_path,
        verify_supervisor_token="verify-supervisor",
        restricted_departure_launcher=launcher,
        restricted_management_credentials=lambda: TdsConnectionMaterial("localhost", 1433, "db", "sa", "secret"),
        restricted_evidence_root=tmp_path,
        restricted_helper_startup_timeout=1.0,
        restricted_cleanup_deadline=origin.deadline,
        settlement_operations=RestrictedWriterSettlementOperations(lambda value: b"x", lambda *a: None, lambda x: b"x"),
    )


def test_sequences_existing_phases_and_creates_verify_coordinator_after_reservation(prepared, tmp_path, monkeypatch):
    inputs = _inputs(prepared, tmp_path)
    events = []
    association = SimpleNamespace(_verify_grant_request_ref=object(), _verify_effective={})
    request = replace(
        launch_request().request,
        operation_id=inputs.verify_operation_id,
        implementation_sha256=inputs.verify_launcher.implementation_sha256,
    )
    terminal = tuple.__new__(RestrictedWriterVerified, ())
    monkeypatch.setattr(
        module,
        "reserve_and_hold_mssql_sqlclient_permission",
        lambda attempt, permission: (events.append("reserve-hold") or association, object()),
    )
    monkeypatch.setattr(
        module,
        "release_mssql_sqlclient_permission_locally",
        lambda *a, **k: events.append("release") or object(),
    )
    monkeypatch.setattr(
        module,
        "settle_mssql_sqlclient_permission_grant",
        lambda *a, **k: events.append("grant-settle") or object(),
    )
    monkeypatch.setattr(
        module,
        "bind_mssql_sqlclient_restricted_writer_verify",
        lambda *a, **k: events.append("bind") or association,
    )
    monkeypatch.setattr(module, "build_origin_request", lambda *a, **k: events.append("build") or request)
    coordinator = object()
    monkeypatch.setattr(
        module,
        "create_tds_coordinator",
        lambda *a, **k: events.append("coordinator") or coordinator,
    )

    def run(*args, coordinator_factory, **kwargs):
        events.append("verify-reserve")
        association._verify_phase = "ready"
        association._verify_identity = object()
        assert coordinator_factory() is coordinator
        with pytest.raises(ValueError, match="authorization_composition_invalid"):
            coordinator_factory()
        events.append("verify")
        return object()

    monkeypatch.setattr(module, "run_mssql_sqlclient_restricted_writer_verify", run)
    monkeypatch.setattr(
        module,
        "settle_mssql_sqlclient_restricted_writer_managed",
        lambda *a, **k: events.append("restricted-settle") or terminal,
    )
    assert module.authorize_mssql_sqlclient_writer(prepared.attempt, inputs) is terminal
    assert events == [
        "reserve-hold",
        "release",
        "grant-settle",
        "bind",
        "build",
        "verify-reserve",
        "coordinator",
        "verify",
        "restricted-settle",
    ]


def test_phase_exception_propagates_without_retry_or_later_cleanup(prepared, tmp_path, monkeypatch):
    inputs = _inputs(prepared, tmp_path)
    failure = RuntimeError("retained custody")
    calls = []
    monkeypatch.setattr(
        module,
        "reserve_and_hold_mssql_sqlclient_permission",
        lambda *a: (calls.append("reserve-hold") or object(), object()),
    )
    monkeypatch.setattr(
        module,
        "release_mssql_sqlclient_permission_locally",
        lambda *a, **k: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(
        module,
        "settle_mssql_sqlclient_permission_grant",
        lambda *a, **k: calls.append("unexpected"),
    )
    with pytest.raises(RuntimeError) as caught:
        module.authorize_mssql_sqlclient_writer(prepared.attempt, inputs)
    assert caught.value is failure and calls == ["reserve-hold"]


def test_invalid_runtime_rejects_before_any_phase(prepared, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        module,
        "reserve_and_hold_mssql_sqlclient_permission",
        lambda *a: calls.append("effect"),
    )
    with pytest.raises(ValueError):
        replace(_inputs(prepared, tmp_path), verify_evidence_root=Path("relative"))
    assert calls == []


def test_malformed_verify_implementation_rejects_before_any_phase_or_credential_callback(
    prepared, tmp_path, monkeypatch
):
    inputs = _inputs(prepared, tmp_path)
    effects = []
    monkeypatch.setattr(
        module,
        "reserve_and_hold_mssql_sqlclient_permission",
        lambda *args: effects.append("phase"),
    )
    launcher = SimpleNamespace(**vars(inputs.verify_launcher))
    launcher.implementation_sha256 = "invalid"
    with pytest.raises(ValueError, match="authorization_composition_invalid"):
        replace(
            inputs,
            verify_launcher=launcher,
            verify_credentials=lambda: effects.append("credential"),
        )
    assert effects == []


def test_exact_original_chain_settles_and_admits_writer(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin = attempt._prepared_origin
    end = origin.deadline
    start = min(end - 1.0, monotonic() + 60.0)
    admission = prepared.admission
    profile = decode_connection_admission(admission)[1]
    digest = hashlib.sha256(admission).hexdigest()
    grant_deploy = deployment(admission, digest, origin.identity.implementation_sha256, tmp_path)
    verify_deploy = deployment(admission, digest, origin.expected.state.identity.implementation_sha256, tmp_path)
    grant_launcher = PermissionLauncher(
        grant_deploy,
        origin.handle.request.management_admission,
        lambda: attempt._permission_grant_owner._held_ready_owner.binding.startup.process,
    )
    grant_departure = DepartureLauncher(grant_deploy, restricted=False)
    verify_launcher = VerifyLauncher(verify_deploy, origin.handle.request.writer_admission)
    restricted_departure = DepartureLauncher(verify_deploy, restricted=True)
    stage = origin.handle.request.selected_stage
    management = origin.handle.request.management_admission.login.name
    writer = origin.handle.request.writer_admission.login.name
    calls = Counter()

    def material(name):
        calls[name] += 1
        username = writer if name == "verify" else management
        return TdsConnectionMaterial("localhost", 1433, stage.database_name, username, "secret")

    roots = {}
    for name in ("grant", "release", "grant-settle", "verify", "restricted"):
        roots[name] = tmp_path / name
        roots[name].mkdir()
    permission = SqlClientPermissionReservationInputs(
        pool=prepared.pool,
        store_factory=prepared.factory,
        limits=attempt.directory.state.limits,
        lease=prepared.lease,
        launcher=grant_launcher,
        admission=admission,
        evidence_root=roots["grant"],
        supervisor_token=prepared.owner.supervisor_id,
        operation_id=uuid4(),
        profile=profile,
        admission_sha256=digest,
        startup_deadline=start,
        operation_deadline=end,
        termination_timeout=30.0,
        session_nonce=b"g" * 32,
        material_supplier=lambda: material("grant"),
    )
    inputs = module.SqlClientAuthorizationInputs(
        permission=permission,
        grant_release_evidence_root=roots["release"],
        grant_settlement_evidence_root=roots["grant-settle"],
        grant_departure_launcher=grant_departure,
        grant_management_credentials=lambda: material("grant-management"),
        containment_deadline=end,
        grant_helper_startup_timeout=30.0,
        grant_cleanup_deadline=end,
        verify_operation_id=uuid4(),
        verify_launcher=verify_launcher,
        verify_profile=profile,
        verify_admission_sha256=digest,
        verify_startup_deadline=start,
        verify_termination_timeout=30.0,
        verify_credentials=lambda: (material("verify"), b"v" * 32),
        verify_evidence_root=roots["verify"],
        verify_supervisor_token=prepared.owner.supervisor_id,
        restricted_departure_launcher=restricted_departure,
        restricted_management_credentials=lambda: material("restricted-management"),
        restricted_evidence_root=roots["restricted"],
        restricted_helper_startup_timeout=30.0,
        restricted_cleanup_deadline=end,
        settlement_operations=RestrictedWriterSettlementOperations(
            settlement_codec.encode_request,
            settlement_codec.validate_result,
            settlement_codec.remote_settlement_payload,
        ),
    )
    terminal = module.authorize_mssql_sqlclient_writer(attempt, inputs)
    assert type(terminal) is RestrictedWriterVerified
    association = attempt._permission_grant_owner
    assert association is terminal._owner.retained._owner._association
    slots = attempt.directory.state.slots
    assert [slot.command.value for slot in slots[-2:]] == ["grant", "verify"]
    assert all(slot.settled for slot in slots[-2:])
    assert calls == Counter({"grant": 1, "grant-management": 1, "verify": 1, "restricted-management": 1})
    now = monotonic()
    admitted = admit_sqlclient_writer(
        terminal,
        now=now,
        startup_deadline=min(end, now + 1.0),
        operation_deadline=min(end, now + 2.0),
        termination_timeout_seconds=origin.policy.terminate_timeout_seconds,
        max_worker_address_space_bytes=origin.policy.max_worker_address_space_bytes,
    )
    assert isinstance(admitted, SqlClientWriterAdmitted)
    with pytest.raises(Exception):
        module.authorize_mssql_sqlclient_writer(attempt, inputs)


def test_foreign_thread_rejects_before_credential_callback(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    calls = []
    inputs = _inputs(prepared, tmp_path)
    inputs = replace(
        inputs,
        permission=replace(inputs.permission, material_supplier=lambda: calls.append("supplier")),
    )
    failures = []

    def invoke():
        try:
            module.authorize_mssql_sqlclient_writer(attempt, inputs)
        except Exception as error:
            failures.append(error)

    thread = Thread(target=invoke)
    thread.start()
    thread.join()
    assert calls == [] and len(failures) == 1


def test_changed_producer_fact_rejects_before_credential_callback(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    calls = []
    inputs = _inputs(prepared, tmp_path)
    inputs = replace(
        inputs,
        permission=replace(inputs.permission, material_supplier=lambda: calls.append("supplier")),
    )
    writer = attempt._prepared_origin.opening.inventory.writer
    object.__setattr__(writer, "name", writer.name + "_changed")
    with pytest.raises(Exception):
        module.authorize_mssql_sqlclient_writer(attempt, inputs)
    assert calls == [] and attempt._permission_grant_owner is None
