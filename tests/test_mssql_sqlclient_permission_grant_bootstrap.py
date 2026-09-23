"""Synthetic SQL doubles exercise the fixed child transcript; no live claim."""

import os
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.app import mssql_sqlclient_permission_grant_bootstrap as bootstrap
from dpone.app.mssql_sqlclient_permission_grant_request import (
    encode_permission_credentials,
    encode_permission_launch_request,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    encode_permission_grant_evidence,
    encode_permission_grant_request,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    EVIDENCE_LIMIT,
    PermissionBoundary,
    PermissionWireBinding,
    encode_permission_message,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionWireKind as K,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionProfile
from dpone.contracts.mssql_tds_coordinator_authority import encode_authority
from dpone.contracts.mssql_tds_coordinator_codec import coordinator_identity_body
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_permission_grant_request import launch_request
from tests.test_mssql_sqlclient_permission_grant_wire import fixture, grant_body


@pytest.mark.parametrize(
    ("checks", "release_extra", "execute_extra", "fault"),
    [
        (0, False, False, None),
        (6, False, False, None),
        (0, True, False, None),
        (0, False, True, None),
        (0, False, False, "connect"),
        (0, False, False, "acquire"),
        (0, False, False, "execute"),
        (1, False, False, "hold_extra"),
        (1, False, False, "held"),
        (0, False, False, "close"),
    ],
)
def test_full_transcript_closes_sql_before_released(monkeypatch, tmp_path, checks, release_extra, execute_extra, fault):
    original, authority, grant, evidence = fixture()
    deadline = time.monotonic() + 3.0
    operation_ns = deadline_nanoseconds(deadline)
    value, _ = launch_request()
    pin = TdsBinaryPin(tmp_path / "pin", "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    value = replace(
        value,
        startup_deadline=deadline,
        operation_deadline=deadline,
        admission_sha256=sha256(admission).hexdigest(),
    )
    root = Path(bootstrap.__file__).resolve().parents[2]
    startup = TdsCoordinatorStartup(
        original.startup.process, original.operation.implementation_sha256, str(root), b"n" * 32
    )
    binding = PermissionWireBinding(value.request, value.operation, startup, value.execution_owner, operation_ns)
    events = []

    from dpone.adapters import mssql_sqlclient_permission_grant as grant_module
    from dpone.adapters import mssql_tds_coordinator_connection as connection_module
    from dpone.adapters import mssql_tds_installation as installation
    from dpone.adapters import mssql_tds_process as process_module
    from dpone.adapters import mssql_tds_worker_guard as guard

    monkeypatch.setattr(guard, "install_worker_guard", lambda **kwargs: events.append("guard"))
    monkeypatch.setattr(installation, "worker_installation_digest", lambda root: value.operation.implementation_sha256)
    monkeypatch.setattr(process_module.LinuxTdsProcess, "identify", lambda pid: original.startup.process)

    class Connection:
        def close(self):
            events.append("connection-close")

    class Factory:
        def __init__(self, build, profile):
            events.append("factory")

        def connect(self, material, *, deadline):
            events.append("connect")
            if fault == "connect":
                raise ValueError
            return Connection()

    class Sql:
        def __init__(self, connection, operation, owner, process):
            events.append("sql")

        def acquire(self, nonce, *, deadline):
            events.append("authority")
            if fault == "acquire":
                raise ValueError
            return authority

        def close(self):
            events.append("sql-close")

    class Grant:
        def __init__(self, sql):
            events.append("grant")

        def execute(self, request, actual, *, deadline):
            events.append("execute")
            if fault == "execute":
                raise ValueError
            assert actual == grant
            return evidence

        def require_held(self, *, deadline):
            events.append("held-check")
            if fault == "held":
                raise ValueError
            return evidence

        def close(self):
            events.append("grant-close")
            if fault == "close":
                raise ValueError

    monkeypatch.setattr(connection_module, "TdsCoordinatorConnection", Factory)
    monkeypatch.setattr(grant_module, "TdsCoordinatorSql", Sql)
    monkeypatch.setattr(grant_module, "SqlClientPermissionGrant", Grant)

    parent, child = socket.socketpair()
    parent.setblocking(False)
    child.setblocking(False)
    args = dict(
        expected_parent_pid=os.getppid(),
        address_space=1 << 30,
        startup_deadline=deadline,
        operation_deadline=deadline,
        channel_fd=child.detach(),
        launch_nonce=b"n" * 32,
        implementation_sha256=value.operation.implementation_sha256,
        admission=admission,
        public_request=encode_permission_launch_request(value),
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(bootstrap.run_permission_grant, **args)
        startup_payload = bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        request_payload = encode_permission_message(
            binding,
            K.REQUEST,
            1,
            {
                "operation": strict_json_object(canonical_json_bytes(coordinator_identity_body(binding.operation))),
                "execution_owner": asdict(binding.execution_owner),
                "request": strict_json_object(encode_permission_grant_request(binding.request)),
            },
        )
        assert startup_payload
        bootstrap._write(parent, request_payload, deadline)
        accepted = bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        assert b"REQUEST_ACCEPTED" in accepted and "connect" not in events
        credentials = encode_permission_credentials(value, binding=binding, request_payload=request_payload)
        bootstrap._write(parent, credentials, deadline)
        if fault in {"connect", "acquire"}:
            with pytest.raises(ValueError):
                bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
            assert future.result(timeout=2) == 1
            parent.close()
            return
        authority_payload = bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        assert encode_authority(authority) in authority_payload
        execute = encode_permission_message(binding, K.EXECUTE, 3, {"grant": grant_body(grant)})
        execute_frame = struct.pack("!I", len(execute)) + execute
        parent.sendall(execute_frame + (execute_frame if execute_extra else b""))
        if execute_extra or fault == "execute":
            with pytest.raises(ValueError):
                bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
            assert future.result(timeout=2) == 1
            assert events.count("execute") == int(fault == "execute")
            parent.close()
            return
        held = bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        assert encode_permission_grant_evidence(evidence) in held
        for offset, boundary in enumerate(list(PermissionBoundary)[:checks]):
            check = encode_permission_message(
                binding,
                K.CHECK_HELD,
                4 + offset,
                {
                    "boundary": boundary.value,
                    "evidence_sha256": sha256(encode_permission_grant_evidence(evidence)).hexdigest(),
                },
            )
            if fault == "hold_extra":
                release = encode_permission_message(
                    binding,
                    K.RELEASE,
                    4 + offset,
                    {"evidence_sha256": sha256(encode_permission_grant_evidence(evidence)).hexdigest()},
                )
                frames = tuple(struct.pack("!I", len(item)) + item for item in (check, release))
                parent.sendall(b"".join(frames))
                with pytest.raises(ValueError):
                    bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
                assert future.result(timeout=2) == 1
                assert events.count("held-check") == 0
                parent.close()
                return
            bootstrap._write(parent, check, deadline)
            if fault == "held":
                with pytest.raises(ValueError):
                    bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
                assert future.result(timeout=2) == 1
                parent.close()
                return
            assert b'"kind":"HELD"' in bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        assert events.count("held-check") == checks
        release = encode_permission_message(
            binding,
            K.RELEASE,
            4 + checks,
            {"evidence_sha256": sha256(encode_permission_grant_evidence(evidence)).hexdigest()},
        )
        framed = struct.pack("!I", len(release)) + release
        parent.sendall(framed + (framed if release_extra else b""))
        if release_extra or fault == "close":
            with pytest.raises(ValueError):
                bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
            assert future.result(timeout=2) == 1
            assert events.count("grant-close") == 1
        else:
            assert b"RELEASED" in bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
            assert events.count("grant-close") == 1
            assert future.result(timeout=2) == 0
    parent.close()
    assert events.count("execute") == 1


@pytest.mark.parametrize("mode", ["partial", "prequeued", "replay"])
def test_partial_credentials_fail_once_without_sql_or_resend(monkeypatch, tmp_path, mode):
    original, *_ = fixture()
    deadline = time.monotonic() + 2.0
    value, _ = launch_request()
    pin = TdsBinaryPin(tmp_path / "pin", "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    value = replace(
        value,
        startup_deadline=deadline,
        operation_deadline=deadline,
        admission_sha256=sha256(admission).hexdigest(),
    )
    root = Path(bootstrap.__file__).resolve().parents[2]
    startup = TdsCoordinatorStartup(
        original.startup.process, original.operation.implementation_sha256, str(root), b"n" * 32
    )
    binding = PermissionWireBinding(
        value.request, value.operation, startup, value.execution_owner, deadline_nanoseconds(deadline)
    )
    effects = []
    from dpone.adapters import mssql_tds_coordinator_connection as connection_module
    from dpone.adapters import mssql_tds_installation as installation
    from dpone.adapters import mssql_tds_process as process_module
    from dpone.adapters import mssql_tds_worker_guard as guard

    monkeypatch.setattr(guard, "install_worker_guard", lambda **kwargs: None)
    monkeypatch.setattr(installation, "worker_installation_digest", lambda root: value.operation.implementation_sha256)
    monkeypatch.setattr(process_module.LinuxTdsProcess, "identify", lambda pid: original.startup.process)
    monkeypatch.setattr(connection_module, "TdsCoordinatorConnection", lambda *args: effects.append("sql"))
    parent, child = socket.socketpair()
    parent.setblocking(False)
    child.setblocking(False)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            bootstrap.run_permission_grant,
            expected_parent_pid=os.getppid(),
            address_space=1 << 30,
            startup_deadline=deadline,
            operation_deadline=deadline,
            channel_fd=child.detach(),
            launch_nonce=b"n" * 32,
            implementation_sha256=value.operation.implementation_sha256,
            admission=admission,
            public_request=encode_permission_launch_request(value),
        )
        bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        request_payload = encode_permission_message(
            binding,
            K.REQUEST,
            1,
            {
                "operation": strict_json_object(canonical_json_bytes(coordinator_identity_body(binding.operation))),
                "execution_owner": asdict(binding.execution_owner),
                "request": strict_json_object(encode_permission_grant_request(binding.request)),
            },
        )
        request_frame = struct.pack("!I", len(request_payload)) + request_payload
        if mode == "prequeued":
            parent.sendall(request_frame + struct.pack("!I", 100) + b"partial")
            with pytest.raises(ValueError):
                bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
        else:
            parent.sendall(request_frame)
            assert b"REQUEST_ACCEPTED" in bootstrap._read(parent, EVIDENCE_LIMIT, deadline)
            if mode == "partial":
                parent.sendall((100).to_bytes(4, "big") + b"partial")
            else:
                credentials = encode_permission_credentials(value, binding=binding, request_payload=request_payload)
                frame = struct.pack("!I", len(credentials)) + credentials
                parent.sendall(frame + frame)
        if mode != "prequeued":
            parent.shutdown(socket.SHUT_WR)
        assert future.result(timeout=2) == 1
    parent.close()
    assert effects == []


def test_invalid_public_request_performs_no_sql(monkeypatch):
    effects = []
    from dpone.adapters import mssql_tds_coordinator_connection as connection_module
    from dpone.adapters import mssql_tds_worker_guard as guard

    monkeypatch.setattr(guard, "install_worker_guard", lambda **kwargs: None)
    monkeypatch.setattr(connection_module, "TdsCoordinatorConnection", lambda *args: effects.append("sql"))
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    deadline = time.monotonic() + 1
    assert (
        bootstrap.run_permission_grant(
            expected_parent_pid=os.getppid(),
            address_space=1 << 30,
            startup_deadline=deadline,
            operation_deadline=deadline,
            channel_fd=right.detach(),
            launch_nonce=b"n" * 32,
            implementation_sha256="a" * 64,
            admission=b"{}",
            public_request=b"{}",
        )
        == 1
    )
    left.close()
    assert effects == []
