"""CREATE-session absence observations, never self-authenticating exclusion proofs."""

from dataclasses import dataclass, fields
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, coordinator_authority_digest


def _validate(value: Any, cls: type) -> None:
    if type(value) is not cls:
        raise ValueError("mssql_native.sqlclient_create_departure_invalid")
    cls(**{field.name: getattr(value, field.name) for field in fields(cls)})


def validate_create_departure_inputs(
    original: TdsRemoteSessionIdentity,
    database: TdsDatabaseObservation,
    admission: SqlClientObserverAdmission,
    principal: SqlClientDatabasePrincipal,
) -> None:
    """Compare a separately admitted creator tuple to the original CREATE digest.

    Revalidate nested values without converting scalar aliases. The observer's
    own identity and the SqlClient writer digest cannot replace these originals.
    The application still must authenticate provenance, CREATE success and local
    containment of the original one-shot process before relying on observations.
    """
    _validate(original, TdsRemoteSessionIdentity)
    _validate(database, TdsDatabaseObservation)
    _validate(principal, SqlClientDatabasePrincipal)
    _validate(admission, SqlClientObserverAdmission)
    for value, cls in (
        (admission.server, SqlClientServerAuthority),
        (admission.database, SqlClientDatabaseAuthority),
        (admission.login, SqlClientLoginAuthority),
        (admission.transport, SqlClientTransportAuthority),
    ):
        _validate(value, cls)
    server, db, login = admission.server, admission.database, admission.login
    if (database.name, database.database_id, str(database.database_guid)) != (
        db.database_name,
        db.database_id,
        db.database_guid,
    ):
        raise ValueError("mssql_native.sqlclient_create_departure_invalid")
    digest = coordinator_authority_digest(
        [
            server.server_name,
            server.machine_name,
            server.instance_name,
            server.physical_machine_name,
            db.database_name,
            db.database_id,
            UUID(db.database_guid),
            login.name,
            bytes.fromhex(login.sid),
            login.original_name,
            bytes.fromhex(login.original_sid),
            principal.name,
            principal.principal_id,
            bytes.fromhex(principal.sid),
        ]
    )
    if digest != original.authority_sha256:
        raise ValueError("mssql_native.sqlclient_create_departure_invalid")


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientCreateDeparture:
    """One validated C/S/R/T/C/S sweep under supplied creator expectations.

    Counts describe sequential DMV reads, not an atomic snapshot. This record
    neither attests helper exit nor proves future exclusion of other writers.
    No durable artifact schema or mutation permission is introduced here.
    """

    original: TdsRemoteSessionIdentity
    database: TdsDatabaseObservation
    admission: SqlClientObserverAdmission
    principal: SqlClientDatabasePrincipal
    counts: tuple[int, ...]

    def __post_init__(self) -> None:
        validate_create_departure_inputs(self.original, self.database, self.admission, self.principal)
        if (
            type(self.counts) is not tuple
            or len(self.counts) != 6
            or any(type(value) is not int or value != 0 for value in self.counts)
        ):
            raise ValueError("mssql_native.sqlclient_create_departure_invalid")
