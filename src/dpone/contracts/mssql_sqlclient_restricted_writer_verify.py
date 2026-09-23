"""Coordinator operations for restricted-writer VERIFY.

Value contracts and validation remain re-exported here for compatibility.
"""

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, cast
from uuid import UUID

from dpone.contracts.mssql_sqlclient_restricted_writer_verification import *  # noqa: F403
from dpone.contracts.mssql_sqlclient_restricted_writer_verification import (
    ERROR,
    STAGE_PERMISSIONS,
    RestrictedWriterSessionContext,
    SqlClientEffectivePermission,
    SqlClientEffectivePermissionFingerprint,
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientRestrictedWriterVerifyResult,
    SqlClientTokenFingerprint,
    SqlClientTokenRow,
    _exact,
    restricted_writer_fingerprint,
    restricted_writer_stage_fingerprint,
    validate_verify_opening,
    validate_verify_result,
)
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_sqlclient_stage_observation import (
    parse_stage_columns,
    parse_stage_object,
    quote_stage_identifier,
)
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorCredentialIntent,
    CoordinatorProcessRegistered,
    CoordinatorSessionRegistered,
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
)
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership, TdsProcessIdentity

__all__ = [
    "ERROR",
    "STAGE_PERMISSIONS",
    "RestrictedWriterSessionContext",
    "RestrictedWriterVerifyDomainOperations",
    "RestrictedWriterVerifyRegistration",
    "RestrictedWriterVerifyReservation",
    "SqlClientEffectivePermission",
    "SqlClientEffectivePermissionFingerprint",
    "SqlClientRestrictedWriterVerifyRequest",
    "SqlClientRestrictedWriterVerifyResult",
    "SqlClientTokenFingerprint",
    "SqlClientTokenRow",
    "restricted_writer_fingerprint",
    "restricted_writer_stage_fingerprint",
    "validate_verify_opening",
    "validate_verify_result",
    "verify_request_digest",
]


class RestrictedWriterVerifyDomainOperations:
    snapshot_type = TdsCoordinatorSnapshot

    def validate_request(self, value: object) -> SqlClientRestrictedWriterVerifyRequest:
        _exact(value, SqlClientRestrictedWriterVerifyRequest)
        checked = cast(SqlClientRestrictedWriterVerifyRequest, value)
        checked.__post_init__()
        return checked

    def context(self, session: Any, row: tuple) -> RestrictedWriterSessionContext:
        values = list(row)
        for index, kind in ((0, "login"), (2, "original-login"), (6, "database"), (8, "user")):
            values[index] = restricted_writer_fingerprint(kind, values[index])
        return RestrictedWriterSessionContext(session, *values)

    def tokens(self, rows: tuple[tuple, ...]) -> tuple[SqlClientTokenFingerprint, ...]:
        return tuple(
            SqlClientTokenFingerprint(row[0], row[1], restricted_writer_fingerprint("token", row[2]), row[3], row[4])
            for row in rows
        )

    def permissions(self, rows: tuple[tuple, ...]) -> tuple[SqlClientEffectivePermissionFingerprint, ...]:
        return tuple(
            SqlClientEffectivePermissionFingerprint(
                None if row[0] is None else restricted_writer_fingerprint("permission-entity", row[0]),
                None if row[1] is None else restricted_writer_fingerprint("permission-subentity", row[1]),
                row[2],
            )
            for row in rows
        )

    def stage_target(self, request: object) -> str:
        checked = self.validate_request(request)
        return (
            quote_stage_identifier(checked.stage.schema_name) + "." + quote_stage_identifier(checked.stage.table_name)
        )

    def stage(
        self, request: object, database: tuple, schema: tuple, object_row: tuple, column_rows: tuple[tuple, ...]
    ) -> SqlClientStageIdentity:
        self.validate_request(request)
        observed = parse_stage_object(object_row)
        return SqlClientStageIdentity(
            database_guid=database[2],
            database_id=database[0],
            database_name=database[1],
            schema_id=schema[0],
            schema_name=schema[1],
            table_name=observed[1],
            object_id=observed[0],
            create_date=observed[2],
            owner_binding=observed[3],
            object_nonce=UUID(observed[4]),
            columns=parse_stage_columns(column_rows),
        )

    def result(self, **values: Any) -> SqlClientRestrictedWriterVerifyResult:
        values["stage_sha256"] = restricted_writer_stage_fingerprint(values.pop("stage"))
        values["closing_stage_sha256"] = restricted_writer_stage_fingerprint(values.pop("closing_stage"))
        return SqlClientRestrictedWriterVerifyResult(**values)

    def validate_result(self, request: object, result: object) -> None:
        _exact(result, SqlClientRestrictedWriterVerifyResult)
        validate_verify_result(self.validate_request(request), cast(SqlClientRestrictedWriterVerifyResult, result))

    def validate_opening(self, request: object, opening: object) -> None:
        _exact(opening, RestrictedWriterSessionContext)
        validate_verify_opening(self.validate_request(request), cast(RestrictedWriterSessionContext, opening))

    def process_registered(self, process: object, authentication_sha256: str) -> object:
        return CoordinatorProcessRegistered(cast(Any, process), authentication_sha256)

    def credential_intent(self) -> object:
        return CoordinatorCredentialIntent()

    def session_registered(self, opening: object) -> object:
        checked = cast(Any, opening)
        _exact(checked.context, RestrictedWriterSessionContext)
        return CoordinatorSessionRegistered(checked.context.session, checked.context.session.authority_sha256.hex())

    def expected_advance(self, state: object, event: object) -> object:
        checked = cast(Any, state)
        return advance_coordinator_state(checked, cast(Any, event), expected_phase=checked.phase)


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyReservation:
    """Opaque exact-origin reservation returned by the injected association."""

    operation_id: UUID
    request_sha256: str
    parent_sha256: str
    execution_owner: TdsAttemptOwnership

    def __post_init__(self) -> None:
        if type(self.operation_id) is not UUID or not self.operation_id.int:
            raise ValueError(ERROR)
        _hash(self.request_sha256)
        _hash(self.parent_sha256)
        _exact(self.execution_owner, TdsAttemptOwnership)

    @property
    def owner_fence(self) -> int:
        return self.execution_owner.fence


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyRegistration:
    reservation: RestrictedWriterVerifyReservation
    process: TdsProcessIdentity
    execution_owner: TdsAttemptOwnership

    def __post_init__(self) -> None:
        _exact(self.reservation, RestrictedWriterVerifyReservation)
        _exact(self.process, TdsProcessIdentity)
        _exact(self.execution_owner, TdsAttemptOwnership)
        if self.execution_owner is not self.reservation.execution_owner:
            raise ValueError(ERROR)


def verify_request_digest(payload: bytes) -> str:
    if type(payload) is not bytes or not payload:
        raise ValueError(ERROR)
    return sha256(b"dpone.sqlclient.restricted-writer-verify.v1\0" + payload).hexdigest()
