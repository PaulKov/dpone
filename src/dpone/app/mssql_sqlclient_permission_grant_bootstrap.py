"""Fixed guarded child for one retained permission grant transcript."""

from __future__ import annotations

import os
import select
import socket
import struct
import time
from hashlib import sha256
from pathlib import Path


def _read(channel: socket.socket, limit: int, deadline: float) -> bytes:
    retained = bytearray()
    while len(retained) < 4:
        if time.monotonic() >= deadline:
            raise TimeoutError
        try:
            part = channel.recv(4 - len(retained))
        except BlockingIOError:
            continue
        if not part:
            raise ValueError
        retained.extend(part)
    size = struct.unpack("!I", retained)[0]
    if not 0 < size <= limit:
        raise ValueError
    body = bytearray()
    while len(body) < size:
        if time.monotonic() >= deadline:
            raise TimeoutError
        try:
            part = channel.recv(size - len(body))
        except BlockingIOError:
            continue
        if not part:
            raise ValueError
        body.extend(part)
    return bytes(body)


def _write(channel: socket.socket, payload: bytes, deadline: float) -> None:
    remaining = memoryview(struct.pack("!I", len(payload)) + payload)
    while remaining:
        if time.monotonic() >= deadline:
            raise TimeoutError
        try:
            count = channel.send(remaining)
        except BlockingIOError:
            continue
        if count <= 0:
            raise ValueError
        remaining = remaining[count:]


def _queued(channel: socket.socket) -> bool:
    """Observe already queued protocol bytes without consuming or retaining them."""
    readable, _, failed = select.select((channel,), (), (channel,), 0)
    if failed:
        raise ValueError
    if not readable:
        return False
    try:
        return bool(channel.recv(1, socket.MSG_PEEK))
    except BlockingIOError:
        return False


def run_permission_grant(
    *,
    expected_parent_pid: int,
    address_space: int,
    startup_deadline: float,
    operation_deadline: float,
    channel_fd: int,
    launch_nonce: bytes,
    implementation_sha256: str,
    admission: bytes,
    public_request: bytes,
) -> int:
    """Run the fixed state machine once; every SQL effect remains child-owned."""
    channel = connection = sql = grant_actor = None
    close_attempted = False
    try:
        from dpone.adapters import mssql_sqlclient_permission_grant as grant_adapter
        from dpone.adapters import mssql_tds_coordinator_connection as connection_adapter
        from dpone.adapters.mssql_tds_installation import worker_installation_digest
        from dpone.adapters.mssql_tds_process import LinuxTdsProcess
        from dpone.adapters.mssql_tds_worker_guard import install_worker_guard
        from dpone.app import mssql_sqlclient_permission_grant_request as request_codec
        from dpone.contracts import mssql_sqlclient_permission_grant_wire as wire

        SqlClientPermissionGrant = grant_adapter.SqlClientPermissionGrant
        TdsCoordinatorSql = grant_adapter.TdsCoordinatorSql
        TdsCoordinatorConnection = connection_adapter.TdsCoordinatorConnection
        decode_connection_admission = connection_adapter.decode_connection_admission
        encode_connection_admission = connection_adapter.encode_connection_admission
        decode_permission_credentials = request_codec.decode_permission_credentials
        decode_permission_launch_request = request_codec.decode_permission_launch_request
        encode_permission_grant_evidence = wire.encode_permission_grant_evidence
        CREDENTIAL_LIMIT, EVIDENCE_LIMIT = wire.CREDENTIAL_LIMIT, wire.EVIDENCE_LIMIT
        PermissionBoundary, PermissionWireBinding = wire.PermissionBoundary, wire.PermissionWireBinding
        PermissionWireState, K = wire.PermissionWireState, wire.K
        _grant, encode_permission_message = wire._grant, wire.encode_permission_message
        encode_authority, TdsCoordinatorStartup = wire.encode_authority, wire.TdsCoordinatorStartup
        encode_startup, strict_json_object = wire.encode_startup, wire.strict_json_object
        deadline_nanoseconds = request_codec._deadline

        install_worker_guard(expected_parent_pid=expected_parent_pid, max_address_space_bytes=address_space)
        deadline_nanoseconds(startup_deadline)
        operation_ns = deadline_nanoseconds(operation_deadline)
        if startup_deadline > operation_deadline or os.getppid() != expected_parent_pid:
            raise ValueError
        if os.get_blocking(channel_fd):
            raise ValueError
        channel = socket.socket(fileno=channel_fd)
        if channel.family != socket.AF_UNIX or channel.type != socket.SOCK_STREAM:
            raise ValueError
        channel.setblocking(False)
        root = Path(__file__).resolve().parents[2]
        if worker_installation_digest(root) != implementation_sha256:
            raise ValueError
        launch = decode_permission_launch_request(public_request)
        if (
            launch["operation"].implementation_sha256 != implementation_sha256
            or launch["startup_deadline"] != startup_deadline
            or launch["operation_deadline"] != operation_deadline
            or launch["admission_sha256"] != sha256(admission).hexdigest()
        ):
            raise ValueError
        build, profile = decode_connection_admission(admission)
        if encode_connection_admission(build, profile) != admission:
            raise ValueError
        process = LinuxTdsProcess.identify(os.getpid())
        startup = TdsCoordinatorStartup(process, implementation_sha256, str(root), launch_nonce)
        binding = PermissionWireBinding(
            launch["request"], launch["operation"], startup, launch["execution_owner"], operation_ns
        )
        state = PermissionWireState(binding)

        startup_payload = encode_permission_message(
            binding, K.STARTUP, 0, {"startup": strict_json_object(encode_startup(startup))}
        )
        state.accept(startup_payload, direction="CHILD_TO_PARENT")
        _write(channel, startup_payload, startup_deadline)
        request_payload = _read(channel, EVIDENCE_LIMIT, operation_deadline)
        state.accept(request_payload, direction="PARENT_TO_CHILD")
        if _queued(channel):
            raise ValueError
        accepted = encode_permission_message(
            binding, K.REQUEST_ACCEPTED, 1, {"request_payload_sha256": sha256(request_payload).hexdigest()}
        )
        state.accept(accepted, direction="CHILD_TO_PARENT")
        _write(channel, accepted, operation_deadline)
        private = _read(channel, CREDENTIAL_LIMIT, operation_deadline)
        try:
            material, session_nonce = decode_permission_credentials(
                private, binding=binding, request_payload=request_payload, profile=profile
            )
            state.consume_credentials(payload_size=len(private))
        finally:
            del private
        if _queued(channel):
            raise ValueError

        factory = TdsCoordinatorConnection(build, profile)
        try:
            connection = factory.connect(material, deadline=operation_deadline)
        finally:
            del material
        sql = TdsCoordinatorSql(connection, binding.operation, binding.execution_owner, process)
        authority = sql.acquire(session_nonce, deadline=operation_deadline)
        authority_body = strict_json_object(encode_authority(authority))
        authority_payload = encode_permission_message(binding, K.AUTHORITY, 2, {"authority": authority_body})
        state.accept(authority_payload, direction="CHILD_TO_PARENT")
        _write(channel, authority_payload, operation_deadline)

        execute_payload = _read(channel, EVIDENCE_LIMIT, operation_deadline)
        execute = state.accept(execute_payload, direction="PARENT_TO_CHILD")
        if _queued(channel):
            raise ValueError
        grant = _grant(strict_json_object(execute.body)["grant"])
        grant_actor = SqlClientPermissionGrant(sql)
        evidence = grant_actor.execute(binding.request, grant, deadline=operation_deadline)
        evidence_bytes = encode_permission_grant_evidence(evidence)
        evidence_sha256 = sha256(evidence_bytes).hexdigest()
        held_payload = encode_permission_message(
            binding, K.PERMISSION_HELD, 3, {"evidence": strict_json_object(evidence_bytes)}
        )
        state.accept(held_payload, direction="CHILD_TO_PARENT")
        _write(channel, held_payload, operation_deadline)

        ordinal = 4
        while True:
            payload = _read(channel, EVIDENCE_LIMIT, operation_deadline)
            message = state.accept(payload, direction="PARENT_TO_CHILD")
            if _queued(channel):
                raise ValueError
            body = strict_json_object(message.body)
            if message.kind is K.RELEASE:
                close_attempted = True
                grant_actor.close()
                released = encode_permission_message(binding, K.RELEASED, ordinal, {"evidence_sha256": evidence_sha256})
                state.accept(released, direction="CHILD_TO_PARENT")
                _write(channel, released, operation_deadline)
                channel.shutdown(socket.SHUT_WR)
                return 0
            if message.kind is not K.CHECK_HELD or body["boundary"] != list(PermissionBoundary)[ordinal - 4].value:
                raise ValueError
            current = grant_actor.require_held(deadline=operation_deadline)
            if current != evidence:
                raise ValueError
            held = encode_permission_message(binding, K.HELD, ordinal, {**body, "authority": authority_body})
            state.accept(held, direction="CHILD_TO_PARENT")
            _write(channel, held, operation_deadline)
            ordinal += 1
    except BaseException:
        return 1
    finally:
        target = grant_actor if grant_actor is not None else sql if sql is not None else connection
        if target is not None and not close_attempted:
            try:
                target.close()
            except BaseException:
                pass
        if channel is not None:
            try:
                channel.close()
            except BaseException:
                pass


def main() -> int:
    """Parse only fixed nonsecret argv; credentials always arrive on the socket."""
    import argparse
    import base64

    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    for name, kind in (
        ("parent", int),
        ("address-space", int),
        ("startup-deadline", float),
        ("operation-deadline", float),
        ("channel-fd", int),
        ("launch-nonce", str),
        ("implementation-sha256", str),
        ("admission", str),
        ("public-request", str),
    ):
        parser.add_argument("--" + name, required=True, type=kind)
    try:
        args = parser.parse_args()
        return run_permission_grant(
            expected_parent_pid=args.parent,
            address_space=args.address_space,
            startup_deadline=args.startup_deadline,
            operation_deadline=args.operation_deadline,
            channel_fd=args.channel_fd,
            launch_nonce=bytes.fromhex(args.launch_nonce),
            implementation_sha256=args.implementation_sha256,
            admission=args.admission.encode("ascii"),
            public_request=base64.urlsafe_b64decode(args.public_request.encode("ascii")),
        )
    except BaseException:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
