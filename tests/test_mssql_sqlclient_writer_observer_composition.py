"""Preclaim composition releases observer credentials only after request ACK."""

from dataclasses import replace

import pytest

from dpone.adapters.mssql_sqlclient_writer_observer_process import PythonSqlClientWriterObserverLauncher
from dpone.app.mssql_sqlclient_writer_observer_composition import open_sqlclient_writer_observer
from dpone.contracts.mssql_sqlclient_credential_admission import SqlClientCredentialProfile
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_writer_observer_wire import WriterObserverRequest, encode_ready, encode_request_ack
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.ports.mssql_sqlclient_writer_observer import SqlClientWriterObserverCustody
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.mssql_sqlclient_departure_v2_fixtures import sample

NONCE = b"n" * 32


class _Process:
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 123, 4)
    channel = object()
    operation_deadline = 30.0

    def startup(self):
        return TdsCoordinatorStartup(self.identity, "b" * 64, "/tmp/root", NONCE)

    def assert_current(self):
        return None


def test_materializes_ready_observer_without_target_command(monkeypatch):
    incarnation = sample().observer
    observer = SqlClientObserverAdmission(
        incarnation.authority.server,
        incarnation.authority.database,
        incarnation.authority.login,
        incarnation.authority.transport,
    )
    target = replace(
        observer,
        login=replace(
            observer.login, principal_id=9, name="writer", sid="09", original_name="writer", original_sid="09"
        ),
    )
    request = WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="c" * 64,
        observer_admission=observer,
        target_admission=target,
        operation_deadline_ns=30_000_000_000,
    )
    profile = SqlClientCredentialProfile(
        writer_admission=observer,
        host="localhost",
        port=1433,
        database=observer.database.database_name,
        username=observer.login.name,
        tls_profile="disposable_test",
        allow_disposable_test=True,
    )
    holder = preload_sqlclient_credentials(
        profile,
        SqlClientCredentials("localhost", 1433, profile.database, profile.username, "secret-canary", "disposable_test"),
    )
    launcher = PythonSqlClientWriterObserverLauncher.__new__(PythonSqlClientWriterObserverLauncher)
    process = _Process()
    events = []
    monkeypatch.setattr(
        PythonSqlClientWriterObserverLauncher, "spawn", lambda self, **kw: (events.append("spawn"), process)[1]
    )
    frames = [encode_request_ack(request, NONCE), encode_ready(request, NONCE, incarnation)]
    monkeypatch.setattr("dpone.app.mssql_sqlclient_writer_observer_composition.monotonic", lambda: 1.0)
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_observer_process.write_frame",
        lambda channel, payload, **kw: events.append("write"),
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_observer_process.read_frame", lambda *a, **kw: frames.pop(0)
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_observer_process.require_quiet", lambda channel: events.append("quiet")
    )

    custody = open_sqlclient_writer_observer(
        request,
        profile=profile,
        credential_custody=holder,
        launcher=launcher,
        startup_timeout=2.0,
        termination_timeout=2.0,
    )

    assert isinstance(custody, SqlClientWriterObserverCustody)
    assert events == ["spawn", "write", "write", "quiet"]
    assert frames == []


def test_unresolved_launch_is_contained_and_closed_before_failure(monkeypatch):
    incarnation = sample().observer
    observer = SqlClientObserverAdmission(
        incarnation.authority.server,
        incarnation.authority.database,
        incarnation.authority.login,
        incarnation.authority.transport,
    )
    target = replace(
        observer,
        login=replace(
            observer.login,
            principal_id=9,
            name="writer",
            sid="09",
            original_name="writer",
            original_sid="09",
        ),
    )
    request = WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="c" * 64,
        observer_admission=observer,
        target_admission=target,
        operation_deadline_ns=30_000_000_000,
    )
    profile = SqlClientCredentialProfile(
        writer_admission=observer,
        host="localhost",
        port=1433,
        database=observer.database.database_name,
        username=observer.login.name,
        tls_profile="disposable_test",
        allow_disposable_test=True,
    )
    holder = preload_sqlclient_credentials(
        profile,
        SqlClientCredentials("localhost", 1433, profile.database, profile.username, "secret-canary", "disposable_test"),
    )
    events = []

    class Unresolved:
        def contain(self, *, deadline):
            events.append(("contain", deadline))

        def close(self):
            events.append(("close",))

    launcher = PythonSqlClientWriterObserverLauncher.__new__(PythonSqlClientWriterObserverLauncher)
    monkeypatch.setattr(
        PythonSqlClientWriterObserverLauncher,
        "spawn",
        lambda self, **kw: (_ for _ in ()).throw(TdsLaunchUnknown(Unresolved())),
    )
    monkeypatch.setattr("dpone.app.mssql_sqlclient_writer_observer_composition.monotonic", lambda: 1.0)

    with pytest.raises(ValueError, match="writer_observer_composition_unknown"):
        open_sqlclient_writer_observer(
            request,
            profile=profile,
            credential_custody=holder,
            launcher=launcher,
            startup_timeout=2.0,
            termination_timeout=2.0,
        )

    assert events == [("contain", 32.0), ("close",)]


def test_cleanup_deadline_overflow_is_rejected_before_launch(monkeypatch):
    incarnation = sample().observer
    observer = SqlClientObserverAdmission(
        incarnation.authority.server,
        incarnation.authority.database,
        incarnation.authority.login,
        incarnation.authority.transport,
    )
    target = replace(
        observer,
        login=replace(
            observer.login, principal_id=9, name="writer", sid="09", original_name="writer", original_sid="09"
        ),
    )
    request = WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="c" * 64,
        observer_admission=observer,
        target_admission=target,
        operation_deadline_ns=2**63 - 1 - 1024,
    )
    profile = SqlClientCredentialProfile(
        writer_admission=observer,
        host="localhost",
        port=1433,
        database=observer.database.database_name,
        username=observer.login.name,
        tls_profile="disposable_test",
        allow_disposable_test=True,
    )
    holder = preload_sqlclient_credentials(
        profile,
        SqlClientCredentials("localhost", 1433, profile.database, profile.username, "secret-canary", "disposable_test"),
    )
    launcher = PythonSqlClientWriterObserverLauncher.__new__(PythonSqlClientWriterObserverLauncher)
    monkeypatch.setattr(
        PythonSqlClientWriterObserverLauncher,
        "spawn",
        lambda self, **kw: pytest.fail("overflow allocated observer process"),
    )
    monkeypatch.setattr("dpone.app.mssql_sqlclient_writer_observer_composition.monotonic", lambda: 1.0)

    with pytest.raises(ValueError, match="writer_observer_composition_unknown"):
        open_sqlclient_writer_observer(
            request,
            profile=profile,
            credential_custody=holder,
            launcher=launcher,
            startup_timeout=2.0,
            termination_timeout=1.0,
        )

    assert holder.release_once(profile).password == "secret-canary"


@pytest.mark.parametrize("relation", ["same_login", "same_sid", "server", "database"])
def test_invalid_admission_relation_rejects_before_spawn_and_preserves_credentials(monkeypatch, relation):
    incarnation = sample().observer
    observer = SqlClientObserverAdmission(
        incarnation.authority.server,
        incarnation.authority.database,
        incarnation.authority.login,
        incarnation.authority.transport,
    )
    target = replace(
        observer,
        login=replace(
            observer.login, principal_id=9, name="writer", sid="09", original_name="writer", original_sid="09"
        ),
    )
    request = WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="c" * 64,
        observer_admission=observer,
        target_admission=target,
        operation_deadline_ns=30_000_000_000,
    )
    if relation == "same_login":
        target = replace(target, login=observer.login)
    elif relation == "same_sid":
        target = replace(
            target,
            login=replace(target.login, sid=observer.login.sid, original_sid=observer.login.original_sid),
        )
    elif relation == "server":
        target = replace(target, server=replace(target.server, server_name="other"))
    else:
        target = replace(target, database=replace(target.database, database_id=target.database.database_id + 1))
    object.__setattr__(request, "target_admission", target)
    profile = SqlClientCredentialProfile(
        writer_admission=observer,
        host="localhost",
        port=1433,
        database=observer.database.database_name,
        username=observer.login.name,
        tls_profile="disposable_test",
        allow_disposable_test=True,
    )
    holder = preload_sqlclient_credentials(
        profile,
        SqlClientCredentials("localhost", 1433, profile.database, profile.username, "secret-canary", "disposable_test"),
    )
    launcher = PythonSqlClientWriterObserverLauncher.__new__(PythonSqlClientWriterObserverLauncher)
    monkeypatch.setattr(
        PythonSqlClientWriterObserverLauncher,
        "spawn",
        lambda self, **kw: pytest.fail("invalid relation allocated observer process"),
    )

    with pytest.raises(ValueError, match="writer_observer_wire_invalid"):
        open_sqlclient_writer_observer(
            request,
            profile=profile,
            credential_custody=holder,
            launcher=launcher,
            startup_timeout=2.0,
            termination_timeout=2.0,
        )

    assert holder.release_once(profile).password == "secret-canary"
