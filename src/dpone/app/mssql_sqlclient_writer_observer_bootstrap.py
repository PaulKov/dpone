"""Child entrypoint for the finite contained P10d observer protocol."""

from __future__ import annotations

import os
import resource
import socket
from pathlib import Path

from dpone.adapters import mssql_sqlclient_writer_observer_process as observer_process
from dpone.adapters.mssql_sqlclient_writer_observer_session import SqlClientWriterObserverSession
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_process import LinuxTdsProcess

ERROR = "mssql_native.sqlclient_writer_observer_bootstrap_unknown"


def run_writer_observer(
    *,
    parent: int,
    address_space: int,
    channel_fd: int,
    startup_deadline: float,
    operation_deadline: float,
    launch_nonce: bytes,
    implementation_sha256: str,
    admission: bytes,
) -> int:
    channel = socket.socket(fileno=channel_fd)
    try:
        if (
            os.getppid() != parent
            or type(address_space) is not int
            or not 0 < address_space < 2**63
            or worker_installation_digest(Path(__file__).resolve().parents[2]) != implementation_sha256
        ):
            raise ValueError(ERROR)
        resource.setrlimit(resource.RLIMIT_AS, (address_space, address_space))
        decode_connection_admission(admission)
        channel.setblocking(False)
        observer_process.serve_writer_observer_protocol(
            channel,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            launch_nonce=launch_nonce,
            implementation_sha256=implementation_sha256,
            package_root=str(Path(__file__).resolve().parents[2]),
            process_identity=LinuxTdsProcess.identify(os.getpid()),
            admission=admission,
            session_factory=SqlClientWriterObserverSession,
        )
        return 0
    except BaseException:
        return 1
    finally:
        channel.close()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    for name, kind in (
        ("parent", int),
        ("address-space", int),
        ("channel-fd", int),
        ("startup-deadline", float),
        ("operation-deadline", float),
        ("launch-nonce", str),
        ("implementation-sha256", str),
        ("admission", str),
    ):
        parser.add_argument("--" + name, required=True, type=kind)
    args = parser.parse_args()
    return run_writer_observer(
        parent=args.parent,
        address_space=args.address_space,
        channel_fd=args.channel_fd,
        startup_deadline=args.startup_deadline,
        operation_deadline=args.operation_deadline,
        launch_nonce=bytes.fromhex(args.launch_nonce),
        implementation_sha256=args.implementation_sha256,
        admission=args.admission.encode(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
