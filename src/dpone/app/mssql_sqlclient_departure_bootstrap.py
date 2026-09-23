"""Fixed departure child; successful SQL close precedes any result frame.

The launcher must source-load the guard before importing dpone. This entrypoint
repeats guard installation. SQL calls remain synchronous: the parent retains
process containment authority and must authenticate result, zero exit and ACKs.
"""

from __future__ import annotations

import os
import stat
import sys
import time
from functools import partial

from dpone.app.mssql_sqlclient_departure_pipe_contract import (
    before_departure_deadline,
    close_departure_resources,
    send_departure_frame,
    valid_departure_descriptors,
)


def run_departure(
    *,
    expected_parent_pid: int,
    address_space: int,
    startup_deadline: float,
    operation_deadline: float,
    startup_fd: int,
    request_fd: int,
    result_fd: int,
    launch_nonce: bytes,
    implementation_sha256: str,
    admission: bytes,
) -> int:
    """Execute once over three inherited nonblocking pipes, without diagnostics.

    Valid distinct descriptor declarations transfer ownership to this invocation.
    Detach before close, including SQL close, so an ambiguous close is never retried.
    Invalid descriptor declarations remain the launcher's responsibility.
    """
    descriptors = (startup_fd, request_fd, result_fd)
    if not valid_departure_descriptors(descriptors):
        return 1
    owned = set(descriptors)
    connection = None
    exit_code = 1

    before = partial(before_departure_deadline, clock=time.monotonic)

    try:
        from dpone.adapters.mssql_tds_worker_guard import install_worker_guard

        install_worker_guard(expected_parent_pid=expected_parent_pid, max_address_space_bytes=address_space)
        from hashlib import sha256
        from pathlib import Path
        from typing import Any

        from dpone.adapters.mssql_sqlclient_create_exclusion import SqlClientCreateExclusionObserver
        from dpone.adapters.mssql_sqlclient_create_exclusion_composition import compose_create_exclusion_observer_v2
        from dpone.adapters.mssql_sqlclient_departure_process import MAX_REQUEST_FRAME
        from dpone.adapters.mssql_sqlclient_permission_grant_departure import (
            SqlClientPermissionGrantDepartureObserver,
        )
        from dpone.adapters.mssql_sqlclient_writer_settlement import SqlClientWriterSettlementObserver
        from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
        from dpone.adapters.mssql_tds_coordinator_connection import (
            TdsCoordinatorConnection,
            decode_connection_admission,
            encode_connection_admission,
        )
        from dpone.adapters.mssql_tds_installation import worker_installation_digest
        from dpone.adapters.mssql_tds_process import LinuxTdsProcess
        from dpone.app import mssql_sqlclient_restricted_writer_departure as restricted_writer_departure
        from dpone.app.mssql_sqlclient_departure_request import (
            decode_departure_credentials,
            decode_departure_credentials_v2,
            decode_observe_departure_credentials,
        )
        from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
            SqlClientObserveDepartureRequest,
            decode_observe_departure_request,
            decode_observe_departure_result,
            encode_observe_departure_result,
            make_observe_departure_result,
        )
        from dpone.contracts.mssql_tds_api import (
            SqlClientDepartureRequest,
            SqlClientDepartureRequestV2,
            SqlClientPermissionGrantDepartureCredentials,
            SqlClientPermissionGrantDepartureRequest,
            SqlClientWriterSettlementCredentials,
            SqlClientWriterSettlementRequest,
            canonical_json_bytes,
            decode_departure_result,
            decode_departure_result_v2,
            decode_permission_grant_departure_credentials,
            decode_writer_settlement_credentials,
            decode_writer_settlement_request,
            decode_writer_settlement_result,
            encode_departure_result,
            encode_departure_result_v2,
            encode_message,
            encode_permission_grant_departure_result,
            encode_writer_settlement_result,
            make_departure_result,
            make_departure_result_v2,
            make_writer_settlement_result,
            strict_json_object,
        )
        from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup
        from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
        from dpone.runtime.mssql_native_chunks_files import native_multiset_digest

        send = partial(
            send_departure_frame,
            owned=owned,
            clock=time.monotonic,
            write_control=write_worker_control,
            encode_message=encode_message,
        )

        deadline_nanoseconds(startup_deadline)
        operation_ns = deadline_nanoseconds(operation_deadline)
        if startup_deadline > operation_deadline:
            raise ValueError
        for fd in descriptors:
            if not stat.S_ISFIFO(os.fstat(fd).st_mode) or os.get_blocking(fd):
                raise ValueError
        before(startup_deadline)
        package_root = Path(__file__).resolve().parents[2]
        actual_implementation = worker_installation_digest(package_root)
        if actual_implementation != implementation_sha256:
            raise ValueError
        process = LinuxTdsProcess.identify(os.getpid())
        startup = TdsCoordinatorStartup(process, actual_implementation, str(package_root), launch_nonce)
        build, profile = decode_connection_admission(admission)
        canonical_admission = encode_connection_admission(build, profile)
        if type(admission) is not bytes or admission != canonical_admission:
            raise ValueError
        admission_sha256 = sha256(canonical_admission).hexdigest()
        factory = TdsCoordinatorConnection(build, profile)
        send(startup_fd, encode_startup(startup), 16384, startup_deadline)
        owned.remove(request_fd)
        try:
            body = read_worker_message(request_fd, deadline=operation_deadline, max_payload=MAX_REQUEST_FRAME)
        finally:
            os.close(request_fd)
        try:
            if type(body) is not bytes or not 0 < len(body) <= MAX_REQUEST_FRAME:
                raise ValueError
            schema = strict_json_object(body).get("schema")
            credentials: Any
            if schema == "dpone.sqlclient.permission-grant-departure-credentials.v1":
                credentials = decode_permission_grant_departure_credentials(
                    body,
                    startup=startup,
                    admission_sha256=admission_sha256,
                    startup_deadline=startup_deadline,
                    operation_deadline=operation_deadline,
                    max_address_space_bytes=address_space,
                )
            elif schema == "dpone.sqlclient.writer-settlement-credentials.v1":
                settlement_request = decode_writer_settlement_request(
                    canonical_json_bytes(strict_json_object(body)["request"])
                )
                credentials = decode_writer_settlement_credentials(body, request=settlement_request)
                plan = settlement_request.plan
                if (
                    plan.admission_sha256 != admission_sha256
                    or plan.startup_deadline != startup_deadline
                    or plan.operation_deadline != operation_deadline
                    or plan.max_address_space_bytes != address_space
                    or plan.implementation_sha256 != actual_implementation
                    or plan.package_root != str(package_root)
                    or settlement_request.startup != startup
                ):
                    raise ValueError
            elif schema == restricted_writer_departure.CREDENTIAL_SCHEMA:
                credentials = restricted_writer_departure.decode_child_credentials(
                    body,
                    startup=startup,
                    admission_sha256=admission_sha256,
                    startup_deadline=startup_deadline,
                    operation_deadline=operation_deadline,
                    max_address_space_bytes=address_space,
                )
            elif schema == "dpone.sqlclient.observe-departure-credentials.v1":
                # The parent retains independent verifier admission. Here the sole
                # parent envelope supplies plan consistency, checked against SQL.
                observe_request = decode_observe_departure_request(
                    canonical_json_bytes(strict_json_object(body)["request"])
                )
                held_observer_admission = observe_request.plan.observer_admission
                credentials = decode_observe_departure_credentials(
                    body,
                    startup=startup,
                    admission_sha256=admission_sha256,
                    startup_deadline=startup_deadline,
                    operation_deadline=operation_deadline,
                    max_address_space_bytes=address_space,
                    observer_admission=held_observer_admission,
                )
            elif schema == "dpone.sqlclient.departure-credentials.v2":
                credentials = decode_departure_credentials_v2(
                    body,
                    startup=startup,
                    admission_sha256=admission_sha256,
                    startup_deadline=startup_deadline,
                    operation_deadline=operation_deadline,
                    max_address_space_bytes=address_space,
                )
            elif schema == "dpone.sqlclient.departure-credentials.v1":
                credentials = decode_departure_credentials(
                    body,
                    startup=startup,
                    admission_sha256=admission_sha256,
                    startup_deadline=startup_deadline,
                    operation_deadline=operation_deadline,
                    max_address_space_bytes=address_space,
                )
            else:
                raise ValueError
        finally:
            del body
        request = credentials.request
        verifier_nonce = (
            credentials.session_nonce if type(credentials) is SqlClientPermissionGrantDepartureCredentials else None
        )
        if restricted_writer_departure.is_credentials(credentials):
            verifier_nonce = credentials.session_nonce
        connection_material = (
            credentials.material
            if type(credentials) is SqlClientWriterSettlementCredentials
            else credentials.connection_material
        )
        try:
            before(operation_deadline)
            connection = factory.connect(connection_material, deadline=operation_deadline)
        finally:
            del connection_material
            del credentials
        if type(request) is SqlClientPermissionGrantDepartureRequest:
            if type(verifier_nonce) is not bytes:
                raise ValueError
            grant_departure = SqlClientPermissionGrantDepartureObserver(connection, request).observe(verifier_nonce)
        elif restricted_writer_departure.is_request(request):
            restricted_writer_result = restricted_writer_departure.observe_child(connection, request, verifier_nonce)
        elif type(request) is SqlClientWriterSettlementRequest:
            connection.set_query_deadline(deadline=operation_deadline, clock=time.monotonic)
            settlement_observer = SqlClientWriterSettlementObserver(
                connection.cursor,
                management_admission=request.plan.management_admission,
                operation_deadline_ns=operation_ns,
                operation_deadline=operation_deadline,
                monotonic_ns=time.monotonic_ns,
                finalize_digest=native_multiset_digest,
                close_connection=lambda: None,
            )
            settlement_observation = settlement_observer.observe(
                writer_observation=request.plan.writer_observation,
                writer_admission=request.plan.writer_admission,
                stage=request.plan.stage,
                input_descriptor=request.plan.input_descriptor,
                expectation=request.plan.expectation,
                operation_deadline=operation_deadline,
            )
        elif type(request) is SqlClientObserveDepartureRequest:
            observe_observer = compose_create_exclusion_observer_v2(
                connection.cursor,
                admission=request.plan.management_admission,
                observer_admission=held_observer_admission,
                operation_deadline_ns=operation_ns,
                monotonic_ns=time.monotonic_ns,
            )
            observe_departure = observe_observer.observe_departure_v2(
                original=request.plan.original, database=request.plan.database, principal=request.plan.principal
            )
        elif type(request) is SqlClientDepartureRequestV2:
            observer_v2 = compose_create_exclusion_observer_v2(
                connection.cursor,
                admission=request.plan.creator_admission,
                observer_admission=request.plan.observer_admission,
                operation_deadline_ns=operation_ns,
                monotonic_ns=time.monotonic_ns,
            )
            departure_v2 = observer_v2.observe_departure_v2(
                original=request.plan.original, database=request.plan.database, principal=request.plan.principal
            )
        elif type(request) is SqlClientDepartureRequest:
            observer = SqlClientCreateExclusionObserver(
                connection.cursor,
                admission=request.plan.creator_admission,
                operation_deadline_ns=operation_ns,
                monotonic_ns=time.monotonic_ns,
            )
            departure = observer.observe_departure(
                original=request.plan.original, database=request.plan.database, principal=request.plan.principal
            )
        closing, connection = connection, None
        try:
            closing.close()
        finally:
            del closing
        before(operation_deadline)
        if type(request) is SqlClientPermissionGrantDepartureRequest:
            encoded = encode_permission_grant_departure_result(grant_departure, request)
            result_limit = 32768
        elif restricted_writer_departure.is_request(request):
            encoded = restricted_writer_departure.encode_child_result(restricted_writer_result, request)
            result_limit = restricted_writer_departure.RESULT_FRAME_LIMIT
        elif type(request) is SqlClientWriterSettlementRequest:
            encoded = encode_writer_settlement_result(
                make_writer_settlement_result(request, settlement_observation), request=request
            )
            decode_writer_settlement_result(encoded, request=request)
            result_limit = 512 * 1024
        elif type(request) is SqlClientObserveDepartureRequest:
            encoded = encode_observe_departure_result(
                make_observe_departure_result(request, observe_departure, observer_admission=held_observer_admission),
                request=request,
                observer_admission=held_observer_admission,
            )
            decode_observe_departure_result(encoded, request=request, observer_admission=held_observer_admission)
            result_limit = 32768
        elif type(request) is SqlClientDepartureRequestV2:
            encoded = encode_departure_result_v2(make_departure_result_v2(request, departure_v2), request=request)
            decode_departure_result_v2(encoded, request=request)
            result_limit = 32768
        elif type(request) is SqlClientDepartureRequest:
            encoded = encode_departure_result(make_departure_result(request, departure), request=request)
            decode_departure_result(encoded, request=request)
            result_limit = 32768
        send(result_fd, encoded, result_limit, operation_deadline)
        exit_code = 0
    except BaseException:
        exit_code = 1
    finally:
        remaining_connection = connection
        connection = None
        if not close_departure_resources(remaining_connection, owned):
            exit_code = 1
    return exit_code


def main() -> int:
    """Fixed internal CLI carrying only launch facts; no arbitrary module or SQL."""
    from dpone.app.mssql_sqlclient_departure_entrypoint import run_main

    return run_main()


if __name__ == "__main__":
    sys.exit(main())
