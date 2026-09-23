"""Safe preclaim projection for the contained P10d observer."""

from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_writer_observer_wire import WriterObserverRequest
from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant

ERROR = "mssql_native.sqlclient_writer_observer_request_invalid"


def sqlclient_writer_observer_request(
    pregrant: SqlClientWriterPreGrant,
    *,
    observer_admission: SqlClientObserverAdmission,
) -> WriterObserverRequest | None:
    """Return immutable observer facts without consuming the P10d claim."""
    if (
        not isinstance(pregrant, SqlClientWriterPreGrant)
        or type(observer_admission) is not SqlClientObserverAdmission
        or type(pregrant)._p10d_identity(pregrant) is not pregrant
    ):
        raise ValueError(ERROR)
    try:
        facts = type(pregrant)._p10d_observer_facts(pregrant, pregrant)
        if facts is None:
            return None
        if type(facts) is not tuple or len(facts) != 4:
            raise ValueError
        attempt, launch, target, deadline = facts
        return WriterObserverRequest(
            attempt_sha256=attempt,
            launch_sha256=launch,
            observer_admission=observer_admission,
            target_admission=target,
            operation_deadline_ns=deadline,
        )
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise ValueError(ERROR) from None


__all__ = ("sqlclient_writer_observer_request",)
