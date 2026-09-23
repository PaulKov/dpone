"""Private P10c composition with all secret material preloaded before claim."""

from collections.abc import Callable
from time import monotonic_ns

from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.services.mssql_tds_writer_contracts import SqlClientCredentialProfile, SqlClientCredentials
from dpone.services.mssql_tds_writer_launch import SqlClientWriterRegistered
from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant, prepare_sqlclient_writer_pregrant


def prepare_mssql_sqlclient_writer(
    registered: SqlClientWriterRegistered,
    *,
    profile: SqlClientCredentialProfile,
    credentials: SqlClientCredentials | None,
    session_nonce: bytes | None,
    clock_ns: Callable[[], int] = monotonic_ns,
) -> SqlClientWriterPreGrant:
    """Preload a value-only holder, then enter the one-shot P10c service."""
    supplier = None
    try:
        supplier = preload_sqlclient_credentials(profile, credentials)
    finally:
        credentials = None
    try:
        return prepare_sqlclient_writer_pregrant(
            registered,
            profile=profile,
            supplier=supplier,
            session_nonce=session_nonce,
            clock_ns=clock_ns,
        )
    except BaseException:
        supplier = None
        raise


__all__ = ("prepare_mssql_sqlclient_writer",)
