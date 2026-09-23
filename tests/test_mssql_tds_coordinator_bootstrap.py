"""Real five-pipe child flow with scripted SQL/build admission, not Linux certification."""

import os
from concurrent.futures import ThreadPoolExecutor
from time import monotonic
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    TdsSqlConnection,
    encode_connection_admission,
)
from dpone.app.mssql_tds_coordinator_bootstrap import run_coordinator
from dpone.app.mssql_tds_coordinator_request import decode_create_response, encode_credentials, encode_grant
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    TdsCoordinatorResultKind,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest, decode_authority
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup
from dpone.contracts.mssql_tds_frames import encode_message
from tests.test_mssql_tds_coordinator_create import CreateCursor
from tests.test_mssql_tds_coordinator_request import values


@pytest.fixture
def harness(tmp_path, monkeypatch):
    from dpone.adapters import (
        mssql_tds_coordinator_connection as connection,
    )
    from dpone.adapters import (
        mssql_tds_installation as installation,
    )
    from dpone.adapters import (
        mssql_tds_process as process,
    )
    from dpone.adapters import (
        mssql_tds_worker_guard as guard,
    )

    credentials, _, _, _, _ = values()
    events = []
    cursor = CreateCursor()
    fail = {"admission": False, "source": False, "close": False}
    monkeypatch.setattr(guard, "install_worker_guard", lambda **kwargs: events.append("guard"))
    monkeypatch.setattr(
        installation,
        "worker_installation_digest",
        lambda root: "f" * 64 if fail["source"] else credentials.identity.implementation_sha256,
    )
    monkeypatch.setattr(process.LinuxTdsProcess, "identify", lambda pid: credentials.process)

    class Factory:
        def __init__(self, build, profile):
            events.append("admission")
            assert profile is credentials.driver_profile
            if fail["admission"]:
                raise RuntimeError("unadmitted")

        def connect(self, material, *, deadline):
            events.append("connect")
            assert material == credentials.connection_material

            def close():
                events.append("close")
                if fail["close"]:
                    raise RuntimeError("close unknown")

            return TdsSqlConnection(SimpleNamespace(close=close), cursor)

    monkeypatch.setattr(connection, "TdsCoordinatorConnection", Factory)
    pin = TdsBinaryPin(tmp_path / "not-loaded-by-scripted-admission", "a" * 64)
    admission = encode_connection_admission(TdsCoordinatorBuild(pin, pin, pin, pin), credentials.driver_profile)
    pairs = [os.pipe() for _ in range(5)]
    for pair in pairs:
        for fd in pair:
            os.set_blocking(fd, False)
    child = dict(
        startup_fd=pairs[0][1],
        credentials_fd=pairs[1][0],
        authority_fd=pairs[2][1],
        grant_fd=pairs[3][0],
        result_fd=pairs[4][1],
    )
    parent = dict(
        startup=pairs[0][0], credentials=pairs[1][1], authority=pairs[2][0], grant=pairs[3][1], result=pairs[4][0]
    )
    deadline = monotonic() + 2
    args = dict(
        expected_parent_pid=os.getpid(),
        address_space=128 * 1024**2,
        startup_deadline=deadline,
        operation_deadline=deadline,
        launch_nonce=credentials.launch_nonce,
        implementation_sha256=credentials.identity.implementation_sha256,
        admission=admission,
        **child,
    )
    yield SimpleNamespace(
        args=args, parent=parent, credentials=credentials, events=events, cursor=cursor, fail=fail, deadline=deadline
    )
    for pair in pairs:
        for fd in pair:
            try:
                os.close(fd)
            except OSError:
                pass


def write(fd, body, deadline, limit=1048576):
    write_worker_control(fd, encode_message(body, max_payload=limit), deadline=deadline, max_bytes=limit + 4)
    os.close(fd)


def authenticate(h):
    startup = decode_startup(read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384))
    assert startup.process == h.credentials.process
    assert h.events == ["guard", "admission"]
    write(h.parent["credentials"], encode_credentials(h.credentials), h.deadline)
    auth = decode_authority(read_worker_message(h.parent["authority"], deadline=h.deadline, max_payload=16384))
    assert not h.cursor.created
    grant = TdsCoordinatorGrant(
        coordinator_identity_digest(h.credentials.identity),
        h.credentials.execution_owner,
        h.credentials.process,
        auth.session,
        authority_digest(auth),
        UUID(int=123),
    )
    return auth, grant


@pytest.mark.parametrize("close_failure", [False, True])
def test_result_precedes_cleanup_and_survives_nonzero_exit(harness, close_failure):
    h = harness
    h.fail["close"] = close_failure
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_coordinator, **h.args)
        auth, grant = authenticate(h)
        write(h.parent["grant"], encode_grant(h.credentials.identity, grant), h.deadline, 16384)
        body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=262144)
        response = decode_create_response(
            body, request=h.credentials.request, identity=h.credentials.identity, grant=grant, authority=auth
        )
        assert response.result.outcome is TdsCoordinatorResultKind.SUCCEEDED and response.evidence is not None
        assert future.result(timeout=2) == int(close_failure)
        assert os.read(h.parent["result"], 1) == b""
    assert h.cursor.committed and h.events == ["guard", "admission", "connect", "close"]


@pytest.mark.parametrize("invalid", ["partial", "duplicate", "wrong_binding"])
def test_invalid_grant_has_no_create_or_fabricated_result(harness, invalid):
    h = harness
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_coordinator, **h.args)
        _, grant = authenticate(h)
        body = encode_grant(h.credentials.identity, grant)
        frame = encode_message(body, max_payload=16384)
        if invalid == "partial":
            frame = frame[:-1]
        elif invalid == "duplicate":
            frame = frame + frame
        else:
            from dataclasses import replace

            frame = encode_message(
                encode_grant(h.credentials.identity, replace(grant, authority_sha256="f" * 64)), max_payload=16384
            )
        write_worker_control(h.parent["grant"], frame, deadline=h.deadline, max_bytes=32776)
        os.close(h.parent["grant"])
        assert future.result(timeout=2) == 1
        assert os.read(h.parent["result"], 1) == b""
    assert not h.cursor.created


@pytest.mark.parametrize("fault", ["source", "admission"])
def test_failed_admission_never_acknowledges_startup_or_reads_credentials(harness, fault):
    h = harness
    h.fail[fault] = True
    assert run_coordinator(**h.args) == 1
    assert os.read(h.parent["startup"], 1) == b"" and "connect" not in h.events
    with pytest.raises(BrokenPipeError):
        os.write(h.parent["credentials"], b"not consumed")


def test_invalid_credentials_do_not_open_sql(harness):
    h = harness
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_coordinator, **h.args)
        read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384)
        write(h.parent["credentials"], b"{}", h.deadline)
        assert future.result(timeout=2) == 1
    assert "connect" not in h.events and os.read(h.parent["authority"], 1) == b""


def test_valid_grant_failure_emits_only_static_bound_failure(harness):
    h = harness
    h.cursor.fail = "CREATE TABLE"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_coordinator, **h.args)
        auth, grant = authenticate(h)
        write(h.parent["grant"], encode_grant(h.credentials.identity, grant), h.deadline, 16384)
        body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=262144)
        response = decode_create_response(
            body, request=h.credentials.request, identity=h.credentials.identity, grant=grant, authority=auth
        )
        assert response.evidence is None and response.failure is not None
        assert b"secret" not in body and future.result(timeout=2) == 1


def test_cli_has_only_fixed_roles_and_admission_inputs():
    import inspect

    from dpone.app.mssql_tds_coordinator_bootstrap import main

    source = inspect.getsource(main)
    assert '"admission"' in source and '"launch-nonce"' in source
    assert '"module"' not in source and '"connection-string"' not in source


def test_admission_returning_after_deadline_never_releases_startup(harness, monkeypatch):
    import time

    from dpone.adapters import mssql_tds_coordinator_connection as module

    h = harness
    now = [0.0]
    original = module.TdsCoordinatorConnection

    def late(*args):
        factory = original(*args)
        now[0] = 11.0
        return factory

    monkeypatch.setattr(module, "TdsCoordinatorConnection", late)
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    h.args.update(startup_deadline=10.0, operation_deadline=10.0)
    assert run_coordinator(**h.args) == 1
    assert os.read(h.parent["startup"], 1) == b"" and "connect" not in h.events


def test_open_grant_writer_with_complete_frame_is_not_authorization(harness):
    h = harness
    h.args.update(startup_deadline=monotonic() + 0.2, operation_deadline=monotonic() + 0.2)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_coordinator, **h.args)
        _, grant = authenticate(h)
        frame = encode_message(encode_grant(h.credentials.identity, grant), max_payload=16384)
        write_worker_control(h.parent["grant"], frame, deadline=h.deadline, max_bytes=16388)
        # Deliberately retain the writer: a complete frame without EOF is incomplete.
        assert future.result(timeout=2) == 1
        assert os.read(h.parent["result"], 1) == b"" and not h.cursor.created
