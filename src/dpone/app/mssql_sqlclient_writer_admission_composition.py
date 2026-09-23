"""Private composition boundary for zero-effect P10a writer admission."""

from collections.abc import Callable
from time import monotonic

from dpone.services.mssql_tds_restricted_writer_settlement import RestrictedWriterVerified
from dpone.services.mssql_tds_writer_admission import SqlClientWriterAdmitted, admit_sqlclient_writer


def admit_mssql_sqlclient_writer(
    terminal: RestrictedWriterVerified,
    *,
    startup_deadline: float,
    operation_deadline: float,
    termination_timeout_seconds: int,
    max_worker_address_space_bytes: int,
    clock: Callable[[], float] = monotonic,
) -> SqlClientWriterAdmitted:
    """Sample time once and delegate the complete post-claim policy to P10a."""
    now = clock()
    return admit_sqlclient_writer(
        terminal,
        now=now,
        startup_deadline=startup_deadline,
        operation_deadline=operation_deadline,
        termination_timeout_seconds=termination_timeout_seconds,
        max_worker_address_space_bytes=max_worker_address_space_bytes,
    )
