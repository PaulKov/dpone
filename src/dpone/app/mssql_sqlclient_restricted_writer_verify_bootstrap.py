"""Fixed guarded child for one restricted-writer read-only VERIFY."""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import time

from dpone.adapters.mssql_sqlclient_restricted_writer_verify import SqlClientRestrictedWriterVerify
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import read_frame as _read
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import write_frame as _write
from dpone.adapters.mssql_tds_coordinator_connection import TdsCoordinatorConnection, decode_connection_admission
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterVerifySqlOperations,
    decode_probe_authorization,
    decode_verify_credentials,
    decode_verify_launch_request,
    encode_verify_child_result,
    encode_verify_opening,
    encode_verify_startup,
)


def run_restricted_writer_verify(
    *,
    expected_parent_pid: int,
    startup_deadline: float,
    operation_deadline: float,
    channel_fd: int,
    admission: bytes,
    public_request: bytes,
    implementation_sha256: str,
) -> int:
    channel = connection = None
    try:
        if os.getppid() != expected_parent_pid or time.monotonic() >= startup_deadline:
            raise ValueError
        launch = decode_verify_launch_request(public_request)
        request = launch["request"]
        if (
            request.implementation_sha256 != implementation_sha256
            or launch["startup_deadline"] != startup_deadline
            or launch["operation_deadline"] != operation_deadline
            or launch["admission_sha256"] != hashlib.sha256(admission).hexdigest()
        ):
            raise ValueError
        build, profile = decode_connection_admission(admission)
        if profile is not launch["profile"]:
            raise ValueError
        process = LinuxTdsProcess.identify(os.getpid())
        channel = socket.socket(fileno=channel_fd)
        channel.setblocking(False)
        _write(
            channel,
            encode_verify_startup(process),
            startup_deadline,
        )
        private = _read(channel, operation_deadline)
        try:
            material, nonce = decode_verify_credentials(private, public_payload=public_request, process=process)
        finally:
            del private
        factory = TdsCoordinatorConnection(build, profile)
        try:
            connection = factory.connect(material, deadline=operation_deadline)
        finally:
            del material
        verifier = SqlClientRestrictedWriterVerify(connection, RestrictedWriterVerifySqlOperations())
        opening = verifier.open_writer_session(request, nonce, deadline=operation_deadline)
        opening_payload = encode_verify_opening(opening)
        _write(channel, opening_payload, operation_deadline)
        authorization = _read(channel, operation_deadline)
        decode_probe_authorization(authorization, opening_payload=opening_payload)
        result = verifier.execute_authorized(opening, deadline=operation_deadline)
        closing, connection = connection, None
        closing.close()
        _write(channel, encode_verify_child_result(result), operation_deadline)
        channel.shutdown(socket.SHUT_WR)
        closing_channel, channel = channel, None
        closing_channel.close()
        return 0
    except BaseException:
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                pass
        if channel is not None:
            try:
                channel.close()
            except BaseException:
                pass


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    for name, kind in (
        ("parent", int),
        ("address-space", int),
        ("startup-deadline", float),
        ("operation-deadline", float),
        ("channel-fd", int),
        ("implementation-sha256", str),
        ("admission", str),
        ("public-request", str),
    ):
        parser.add_argument("--" + name, required=True, type=kind)
    try:
        args = parser.parse_args()
        return run_restricted_writer_verify(
            expected_parent_pid=args.parent,
            startup_deadline=args.startup_deadline,
            operation_deadline=args.operation_deadline,
            channel_fd=args.channel_fd,
            admission=args.admission.encode("ascii"),
            public_request=base64.urlsafe_b64decode(args.public_request.encode("ascii")),
            implementation_sha256=args.implementation_sha256,
        )
    except BaseException:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
