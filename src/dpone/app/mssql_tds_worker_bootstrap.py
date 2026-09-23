"""Fixed Linux worker entrypoint; optional imports follow guards and private IPC.

The supervisor owns durable registration, credential release and all SQL/object
verification. This child never reads credentials from argv or environment, and
never emits SDK diagnostics. Main is invoked only in a newly spawned process.
"""

from __future__ import annotations

import os
import sys
import time


def _copy_job(job):
    """Compose the native decoder and pinned vendor adapter after admission."""
    if job.policy.backend != "mssql_python":
        raise ValueError("mssql_native.tds_backend_mismatch")
    from dpone.adapters.mssql_tds_input import NativeTdsInput
    from dpone.adapters.mssql_tds_sdk import admit_tds_sdk, copy_owned_stage
    from dpone.contracts.mssql_tds_api import TdsInputReceipt
    from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder

    admit_tds_sdk(input_mode=job.policy.input)
    source = NativeTdsInput(
        job.file,
        MssqlTdsRowDecoder(job.wire, input_mode=job.policy.input),
        input_mode=job.policy.input,
        batch_rows=job.policy.batch_rows,
        max_row_bytes=job.max_row_bytes,
    )
    connection = None
    try:
        if job.file.rows:
            from mssql_python import connect

            connection = connect(job.connection_string)
        target = ".".join(
            "[" + value.replace("]", "]]") + "]"
            for value in (job.identity.database, job.identity.schema, job.identity.table)
        )
        return copy_owned_stage(
            connection,
            target,
            source,
            input_mode=job.policy.input,
            columns=tuple(column.name for column in job.wire.columns),
            expected=TdsInputReceipt(job.file.rows, job.file.encoded_bytes, job.file.file_sha256),
            batch_rows=job.policy.batch_rows,
            timeout_seconds=job.policy.operation_timeout_seconds,
        )
    finally:
        if connection is not None:
            connection.close()


def run_worker(
    *,
    expected_parent_pid: int,
    address_space: int,
    startup_deadline: float,
    operation_deadline: float,
    control_fd: int,
    startup_fd: int,
    result_fd: int,
) -> int:
    """Run once; absence of a valid result or nonzero exit means failed attempt."""
    from dpone.adapters.mssql_tds_worker_guard import install_worker_guard

    install_worker_guard(expected_parent_pid=expected_parent_pid, max_address_space_bytes=address_space)
    # Guard installation precedes all runtime/contract/vendor dependency imports.
    from dataclasses import asdict
    from pathlib import Path

    from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
    from dpone.adapters.mssql_tds_installation import worker_installation_digest
    from dpone.adapters.mssql_tds_process import LinuxTdsProcess
    from dpone.contracts.mssql_tds_api import TdsAttemptError, canonical_json_bytes, encode_message
    from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest, encode_result

    package_root = Path(__file__).resolve().parents[2]
    implementation = worker_installation_digest(package_root)
    identity = LinuxTdsProcess.identify(os.getpid())
    startup = encode_message(
        canonical_json_bytes(
            {
                "schema_version": 1,
                "process": asdict(identity),
                "implementation_sha256": implementation,
                "package_root": str(package_root),
            }
        ),
        max_payload=16384,
    )
    write_worker_control(startup_fd, startup, deadline=min(startup_deadline, operation_deadline), max_bytes=16388)
    os.close(startup_fd)
    # Natural control EOF prevents partial private request delivery from executing.
    body = read_worker_message(control_fd, deadline=min(startup_deadline, operation_deadline), max_payload=1024 * 1024)
    os.close(control_fd)
    from dpone.app.mssql_tds_worker_request import decode_job

    job = decode_job(body)
    del body
    if job.policy.max_worker_address_space_bytes != address_space or time.monotonic() >= operation_deadline:
        raise ValueError("mssql_native.tds_worker_policy_mismatch")
    if job.identity.implementation_sha256 != implementation:
        raise ValueError("mssql_native.tds_worker_implementation_mismatch")
    binding = attempt_identity_digest(job.identity)
    try:
        receipt = _copy_job(job)
        result = TdsWorkerResult(binding, receipt, None)
        exit_code = 0
    except Exception:
        result = TdsWorkerResult(binding, None, TdsAttemptError.DRIVER)
        exit_code = 1  # Unclassified failure is never automatically transient.
    write_worker_control(result_fd, encode_result(result), deadline=operation_deadline, max_bytes=16388)
    return exit_code


def main() -> int:
    """Internal executable entrypoint; args contain only FDs, limits and deadlines."""
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    for name, kind in (
        ("parent", int),
        ("address-space", int),
        ("startup-deadline", float),
        ("operation-deadline", float),
        ("control-fd", int),
        ("startup-fd", int),
    ):
        parser.add_argument("--" + name, type=kind, required=True)
    args = parser.parse_args()
    result_fd = os.dup(1)
    try:
        # Keep the result pipe private while suppressing SDK native stdout/stderr.
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
        for fd in (args.control_fd, args.startup_fd, result_fd):
            os.set_blocking(fd, False)
        return run_worker(
            expected_parent_pid=args.parent,
            address_space=args.address_space,
            startup_deadline=args.startup_deadline,
            operation_deadline=args.operation_deadline,
            control_fd=args.control_fd,
            startup_fd=args.startup_fd,
            result_fd=result_fd,
        )
    except BaseException:
        return 1
    finally:
        os.close(result_fd)


if __name__ == "__main__":
    sys.exit(main())
