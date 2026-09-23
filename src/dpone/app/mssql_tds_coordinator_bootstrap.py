"""Fixed guarded CREATE child: admission precedes startup and all credentials.

All driver work and cleanup belongs to this child. Parent pidfd interruption is
still required for stalled SQL. No error can emit a second frame or invent a
pre-grant result; an already observed result survives a later nonzero exit.
"""

from __future__ import annotations

import math
import os
import stat
import sys
import time


def run_coordinator(
    *,
    expected_parent_pid: int,
    address_space: int,
    startup_deadline: float,
    operation_deadline: float,
    startup_fd: int,
    credentials_fd: int,
    authority_fd: int,
    grant_fd: int,
    result_fd: int,
    launch_nonce: bytes,
    implementation_sha256: str,
    admission: bytes,
) -> int:
    """Execute the fixed five-pipe protocol once; only nonsecret launch inputs enter."""
    from dpone.adapters.mssql_tds_worker_guard import install_worker_guard

    install_worker_guard(expected_parent_pid=expected_parent_pid, max_address_space_bytes=address_space)
    # The shim also source-loads this guard before importing the dpone package.
    from pathlib import Path

    from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
    from dpone.adapters.mssql_tds_coordinator_connection import TdsCoordinatorConnection, decode_connection_admission
    from dpone.adapters.mssql_tds_coordinator_create import TdsCoordinatorCreate, TdsCreateUnknown
    from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
    from dpone.adapters.mssql_tds_installation import worker_installation_digest
    from dpone.adapters.mssql_tds_process import LinuxTdsProcess
    from dpone.app.mssql_tds_coordinator_request import (
        decode_create_response,
        decode_credentials,
        decode_grant,
        encode_create_response,
        failed_response,
        successful_response,
    )
    from dpone.contracts.mssql_tds_api import TdsAttemptError, encode_authority, encode_message
    from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup

    descriptors = (startup_fd, credentials_fd, authority_fd, grant_fd, result_fd)
    if len(set(descriptors)) != 5 or any(type(fd) is not int or fd < 0 for fd in descriptors):
        return 1
    for fd in descriptors:
        if not stat.S_ISFIFO(os.fstat(fd).st_mode) or os.get_blocking(fd):
            return 1
    owned = set(descriptors)
    sql = connection = creator = response = None
    exit_code = 1

    def before(deadline: float) -> None:
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or time.monotonic() >= deadline:
            raise ValueError("mssql_native.tds_coordinator_deadline")

    def send(fd: int, body: bytes, limit: int, deadline: float) -> None:
        owned.remove(fd)
        try:
            before(deadline)
            write_worker_control(fd, encode_message(body, max_payload=limit), deadline=deadline, max_bytes=limit + 4)
        finally:
            os.close(fd)

    def receive(fd: int, limit: int, deadline: float) -> bytes:
        owned.remove(fd)
        try:
            before(deadline)
            return read_worker_message(fd, deadline=deadline, max_payload=limit)
        finally:
            os.close(fd)

    try:
        before(operation_deadline)
        before(startup_deadline)
        startup_budget = min(startup_deadline, operation_deadline)
        package_root = Path(__file__).resolve().parents[2]
        actual_implementation = worker_installation_digest(package_root)
        if actual_implementation != implementation_sha256:
            raise ValueError("mssql_native.tds_coordinator_source_mismatch")
        process = LinuxTdsProcess.identify(os.getpid())
        startup = TdsCoordinatorStartup(process, actual_implementation, str(package_root), launch_nonce)
        build, profile = decode_connection_admission(admission)
        factory = TdsCoordinatorConnection(build, profile)
        # No startup acknowledgement exists until real binary admission succeeds.
        send(startup_fd, encode_startup(startup), 16384, startup_budget)
        body = receive(credentials_fd, 1048576, startup_budget)
        try:
            credentials = decode_credentials(body, startup=startup, profile=profile)
        finally:
            del body
        identity, owner, request, nonce = (
            credentials.identity,
            credentials.execution_owner,
            credentials.request,
            credentials.session_nonce,
        )
        before(operation_deadline)
        try:
            connection = factory.connect(credentials.connection_material, deadline=operation_deadline)
        finally:
            del credentials
        sql = TdsCoordinatorSql(connection, identity, owner, process)
        authority = sql.acquire(nonce, deadline=operation_deadline)
        send(authority_fd, encode_authority(authority), 16384, operation_deadline)
        grant = decode_grant(receive(grant_fd, 16384, operation_deadline), identity=identity, authority=authority)
        creator = TdsCoordinatorCreate(sql)
        try:
            evidence = creator.execute(request, grant, deadline=operation_deadline)
            response = successful_response(evidence)
            exit_code = 0
        except TdsCreateUnknown as error:
            response = (
                successful_response(error.evidence)
                if error.evidence is not None
                else failed_response(identity, grant, authority, TdsAttemptError.DRIVER)
            )
        except BaseException:
            response = failed_response(identity, grant, authority, TdsAttemptError.DRIVER)
        # Retain the exact typed observation before encoding/output/cleanup.
        encoded = encode_create_response(response)
        decode_create_response(encoded, request=request, identity=identity, grant=grant, authority=authority)
        send(result_fd, encoded, 262144, operation_deadline)
    except BaseException:
        exit_code = 1
    finally:
        try:
            if creator is not None:
                creator.close()
            elif sql is not None:
                sql.close()
            elif connection is not None:
                connection.close()
        except BaseException:
            exit_code = 1
        # Detached send/read descriptors are never retried even after lost close ACK.
        for fd in tuple(owned):
            owned.remove(fd)
            try:
                os.close(fd)
            except BaseException:
                exit_code = 1
    return exit_code


def main() -> int:
    """Fixed internal CLI: only FDs, limits, source/build admission and nonce."""
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    for name, kind in (
        ("parent", int),
        ("address-space", int),
        ("startup-deadline", float),
        ("operation-deadline", float),
        ("startup-fd", int),
        ("credentials-fd", int),
        ("authority-fd", int),
        ("grant-fd", int),
        ("result-fd", int),
        ("launch-nonce", str),
        ("implementation-sha256", str),
        ("admission", str),
    ):
        parser.add_argument("--" + name, type=kind, required=True)
    try:
        args = parser.parse_args()
        nonce = bytes.fromhex(args.launch_nonce)
        if nonce.hex() != args.launch_nonce:
            return 1
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
        return run_coordinator(
            expected_parent_pid=args.parent,
            address_space=args.address_space,
            startup_deadline=args.startup_deadline,
            operation_deadline=args.operation_deadline,
            startup_fd=args.startup_fd,
            credentials_fd=args.credentials_fd,
            authority_fd=args.authority_fd,
            grant_fd=args.grant_fd,
            result_fd=args.result_fd,
            launch_nonce=nonce,
            implementation_sha256=args.implementation_sha256,
            admission=args.admission.encode("utf-8"),
        )
    except BaseException:
        return 1


if __name__ == "__main__":
    sys.exit(main())
