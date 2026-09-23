"""Materialize an independently contained P10d observer before writer claim."""

from __future__ import annotations

import math
from time import monotonic

from dpone.adapters import mssql_sqlclient_writer_observer_process as observer_process
from dpone.ports import mssql_sqlclient_credentials as credential_port
from dpone.ports.mssql_coordinator_worker_capabilities import (
    SqlClientWriterObserverCustody,
    TdsLaunchUnknown,
    create_sqlclient_writer_observer_custody,
)

ERROR = "mssql_native.sqlclient_writer_observer_composition_unknown"


def open_sqlclient_writer_observer(
    request: observer_process.WriterObserverRequest,
    *,
    profile: credential_port.SqlClientCredentialProfile,
    credential_custody: credential_port.PreloadedSqlClientCredentials,
    launcher: observer_process.PythonSqlClientWriterObserverLauncher,
    startup_timeout: float,
    termination_timeout: float,
) -> SqlClientWriterObserverCustody:
    """Connect and publish own incarnation; target observation remains pending."""
    if (
        type(request) is not observer_process.WriterObserverRequest
        or type(profile) is not credential_port.SqlClientCredentialProfile
        or type(credential_custody) is not credential_port.PreloadedSqlClientCredentials
        or type(launcher) is not observer_process.PythonSqlClientWriterObserverLauncher
        or profile.writer_admission is not request.observer_admission
    ):
        raise ValueError(ERROR)
    request.__post_init__()
    profile.__post_init__()
    credential_custody.assert_profile(profile)
    if any(
        type(value) is not float or not math.isfinite(value) or value <= 0
        for value in (startup_timeout, termination_timeout)
    ):
        raise ValueError(ERROR)
    operation_deadline = request.operation_deadline
    startup_deadline = min(operation_deadline, monotonic() + startup_timeout)
    cleanup_deadline = operation_deadline + termination_timeout
    try:
        observer_process.validate_writer_observer_deadline(startup_deadline)
        observer_process.validate_writer_observer_deadline(operation_deadline)
        observer_process.validate_writer_observer_deadline(cleanup_deadline)
    except ValueError:
        raise ValueError(ERROR) from None
    process = None
    try:
        process = launcher.spawn(
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            termination_timeout=termination_timeout,
        )
        protocol = observer_process.SqlClientWriterObserverParentProtocol(process, request)
        protocol.acknowledge_request()
        credentials = credential_custody.release_once(profile)
        try:
            incarnation = protocol.send_credentials_and_receive_ready(credentials)
        finally:
            del credentials
        backend = protocol.backend()
        return create_sqlclient_writer_observer_custody(
            backend,
            observer_admission=request.observer_admission,
            target_admission=request.target_admission,
            incarnation=incarnation,
            attempt_sha256=request.attempt_sha256,
            launch_sha256=request.launch_sha256,
            credential_custody=credential_custody,
            operation_deadline_ns=request.operation_deadline_ns,
            operation_deadline=operation_deadline,
            cleanup_deadline=cleanup_deadline,
        )
    except TdsLaunchUnknown as error:
        unresolved = error.launch
        try:
            unresolved.contain(deadline=cleanup_deadline)
            unresolved.close()
        except BaseException:
            pass
        credential_custody = None  # type: ignore[assignment]
        raise ValueError(ERROR) from None
    except BaseException:
        if process is not None:
            try:
                process.contain(deadline=cleanup_deadline)
            except BaseException:
                pass
            try:
                process.close()
            except BaseException:
                pass
        credential_custody = None  # type: ignore[assignment]
        raise ValueError(ERROR) from None


__all__ = ("open_sqlclient_writer_observer",)
