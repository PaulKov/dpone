"""Fixed child handshake and finite read-only OBSERVE dispatch.

The source shim installs the worker guard before importing this module. Secret
material is validated only in private memory after the nonsecret request ACK.
"""

import os
import socket
from pathlib import Path

from dpone.adapters.mssql_sqlclient_observe_session import SqlClientObserveSession
from dpone.adapters.mssql_sqlclient_observe_transport import read_frame, write_frame
from dpone.adapters.mssql_tds_coordinator_connection import (
    decode_connection_admission,
)
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_worker_guard import install_worker_guard
from dpone.contracts.mssql_sqlclient_observe import (
    ERROR,
    MAX_REQUEST_PAYLOAD_BYTES,
    decode_request,
)
from dpone.contracts.mssql_sqlclient_observe_handshake import decode_credentials, encode_request_accepted
from dpone.contracts.mssql_sqlclient_observe_wire import decode_command, response_frames
from dpone.contracts.mssql_tds_coordinator_ipc import encode_startup
from dpone.contracts.mssql_tds_operation_models import (
    CONTROL_PAYLOAD_BYTES,
    MAX_CREDENTIAL_PAYLOAD_BYTES,
    MAX_PAYLOAD_BYTES,
    TdsCoordinatorStartup,
)
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds, deadline_seconds


def run_observe(
    *,
    expected_parent_pid: int,
    address_space: int,
    startup_deadline: float,
    operation_deadline: float,
    channel_fd: int,
    launch_nonce: bytes,
    implementation_sha256: str,
    admission: bytes,
) -> int:
    channel = session = None
    try:
        install_worker_guard(expected_parent_pid=expected_parent_pid, max_address_space_bytes=address_space)
        deadline_nanoseconds(startup_deadline)
        deadline_nanoseconds(operation_deadline)
        if startup_deadline > operation_deadline:
            raise ValueError(ERROR)
        channel = socket.socket(fileno=channel_fd)
        if os.get_blocking(channel_fd) or channel.family != socket.AF_UNIX or channel.type != socket.SOCK_STREAM:
            raise ValueError(ERROR)
        channel.setblocking(False)
        root = Path(__file__).resolve().parents[2]
        if worker_installation_digest(root) != implementation_sha256:
            raise ValueError(ERROR)
        startup = TdsCoordinatorStartup(
            LinuxTdsProcess.identify(os.getpid()), implementation_sha256, str(root), launch_nonce
        )
        _, profile = decode_connection_admission(admission)
        write_frame(channel, encode_startup(startup), deadline=startup_deadline, limit=CONTROL_PAYLOAD_BYTES)
        request = decode_request(read_frame(channel, deadline=operation_deadline, limit=MAX_REQUEST_PAYLOAD_BYTES))
        if deadline_seconds(request.operation_deadline_ns) != operation_deadline or os.getppid() != expected_parent_pid:
            raise ValueError(ERROR)
        ack = encode_request_accepted(request, startup)
        write_frame(channel, ack, deadline=operation_deadline, limit=CONTROL_PAYLOAD_BYTES)
        payload = read_frame(channel, deadline=operation_deadline, limit=MAX_CREDENTIAL_PAYLOAD_BYTES)
        try:
            credentials = decode_credentials(payload, request, startup, profile)
        finally:
            del payload
        try:
            session = SqlClientObserveSession(request, credentials, startup, admission)
        finally:
            del credentials
        write_frame(channel, session.authority_payload(), deadline=operation_deadline, limit=CONTROL_PAYLOAD_BYTES)
        sequence = 1
        while True:
            cmd = decode_command(
                read_frame(channel, deadline=operation_deadline, limit=CONTROL_PAYLOAD_BYTES),
                request,
                launch_nonce,
                sequence,
            )
            items = session.dispatch(cmd)
            for response in response_frames(cmd, items):
                write_frame(channel, response, deadline=operation_deadline, limit=MAX_PAYLOAD_BYTES)
            sequence += 1
    except BaseException:
        return 1
    finally:
        if session is not None:
            try:
                session.close()
            except BaseException:
                pass
        if channel is not None:
            channel.close()


def main() -> int:
    import argparse

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
    ):
        parser.add_argument("--" + name, type=kind, required=True)
    args = parser.parse_args()
    return run_observe(
        expected_parent_pid=args.parent,
        address_space=args.address_space,
        startup_deadline=args.startup_deadline,
        operation_deadline=args.operation_deadline,
        channel_fd=args.channel_fd,
        launch_nonce=bytes.fromhex(args.launch_nonce),
        implementation_sha256=args.implementation_sha256,
        admission=args.admission.encode("utf-8"),
    )


if __name__ == "__main__":
    raise SystemExit(main())
