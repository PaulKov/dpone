"""Public launch envelope and the sole private credential frame for P9a."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, cast
from uuid import UUID

from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    RestrictedWriterCredentialSupplier as RestrictedWriterCredentialSupplier,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    RestrictedWriterVerifyLaunchRequest as RestrictedWriterVerifyLaunchRequest,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    decode_verify_credentials as decode_verify_credentials,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    decode_verify_launch_request as decode_verify_launch_request,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    encode_verify_credentials as encode_verify_credentials,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_envelope import (
    encode_verify_launch_request as encode_verify_launch_request,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_evidence_operations import (
    RestrictedWriterVerifyEvidenceOperations,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    ERROR,
    RestrictedWriterVerifyDomainOperations,
    RestrictedWriterVerifyRegistration,
    RestrictedWriterVerifyReservation,
    SqlClientEffectivePermission,
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientRestrictedWriterVerifyResult,
    SqlClientTokenRow,
    validate_verify_opening,
    validate_verify_result,
    verify_request_digest,
)
from dpone.contracts.mssql_tds_api import (
    OPENING_TYPE,
    canonical_verify_json,
    decode_probe_authorization,
    decode_verify_opening,
    decode_verify_process,
    decode_verify_result,
    encode_probe_authorization,
    encode_verify_opening,
    encode_verify_request,
    encode_verify_result,
    strict_verify_object,
)

EFFECTIVE_KEYS = frozenset(
    "subject login_token user_token server_permissions database_permissions restored_management".split()
)


def build_origin_request(
    grant: object,
    effective: object,
    *,
    operation_id: UUID,
    implementation_sha256: str,
) -> SqlClientRestrictedWriterVerifyRequest:
    """Build the sole P9 request projection from exact P8 and preparation facts."""
    if type(effective) is not dict or set(effective) != EFFECTIVE_KEYS:
        raise ValueError(ERROR)
    try:

        def tokens(name: str) -> tuple[SqlClientTokenRow, ...]:
            return tuple(SqlClientTokenRow(row[0], row[1], row[2], row[3], row[4]) for row in effective[name])

        def permissions(name: str) -> tuple[SqlClientEffectivePermission, ...]:
            return tuple(
                SqlClientEffectivePermission(
                    None if row[0] == "" else row[0],
                    None if row[1] == "" else row[1],
                    row[2],
                )
                for row in effective[name]
            )

        exact_grant = cast(Any, grant)
        return SqlClientRestrictedWriterVerifyRequest(
            parent=exact_grant.parent,
            stage=exact_grant.stage,
            writer=exact_grant.writer,
            writer_login=exact_grant.writer_login,
            login_token=tokens("login_token"),
            user_token=tokens("user_token"),
            server_permissions=permissions("server_permissions"),
            database_permissions=permissions("database_permissions"),
            operation_id=operation_id,
            implementation_sha256=implementation_sha256,
        )
    except (AttributeError, KeyError, IndexError, TypeError, ValueError):
        raise ValueError(ERROR) from None


def encode_verify_startup(process: object) -> bytes:
    return canonical_verify_json(
        {"schema": "dpone.sqlclient.restricted-writer-verify-startup.v1", "process": asdict(cast(Any, process))}
    )


def encode_verify_child_result(result: object) -> bytes:
    if type(result) is not SqlClientRestrictedWriterVerifyResult:
        raise ValueError(ERROR)
    return encode_verify_result(result)


class RestrictedWriterVerificationOperations(
    RestrictedWriterVerifyEvidenceOperations, RestrictedWriterVerifyDomainOperations
):
    """Single injected authority for P9 nominal codecs and DTO construction."""

    reservation_type = RestrictedWriterVerifyReservation
    registration_type = RestrictedWriterVerifyRegistration
    request_type = SqlClientRestrictedWriterVerifyRequest
    opening_type = OPENING_TYPE

    def encode_request(self, value: object) -> bytes:
        if type(value) is not SqlClientRestrictedWriterVerifyRequest:
            raise ValueError(ERROR)
        return encode_verify_request(value)

    def request_digest(self, payload: bytes) -> str:
        return verify_request_digest(payload)

    def validate_origin_request(self, request: object, grant: object, effective: object) -> None:
        """Rebuild the P9 request only from exact P8 grant and preparation facts."""
        if type(request) is not SqlClientRestrictedWriterVerifyRequest:
            raise ValueError(ERROR)
        expected = build_origin_request(
            grant,
            effective,
            operation_id=request.operation_id,
            implementation_sha256=request.implementation_sha256,
        )
        if request != expected:
            raise ValueError(ERROR)

    def origin_reservation(
        self, operation_id: object, request_sha256: str, parent_sha256: str, execution_owner: object
    ) -> RestrictedWriterVerifyReservation:
        return RestrictedWriterVerifyReservation(
            cast(Any, operation_id), request_sha256, parent_sha256, cast(Any, execution_owner)
        )

    def validate_result(self, request: object, result: object) -> None:
        if type(request) is not SqlClientRestrictedWriterVerifyRequest or type(result) is not (
            SqlClientRestrictedWriterVerifyResult
        ):
            raise ValueError(ERROR)
        validate_verify_result(request, result)

    def validate_opening(self, request: object, opening: object) -> None:
        if type(request) is not SqlClientRestrictedWriterVerifyRequest or type(opening) is not OPENING_TYPE:
            raise ValueError(ERROR)
        if opening.request_sha256 != verify_request_digest(encode_verify_request(request)):
            raise ValueError(ERROR)
        validate_verify_opening(request, cast(Any, opening.context))

    def encode_result(self, result: object) -> bytes:
        if type(result) is not SqlClientRestrictedWriterVerifyResult:
            raise ValueError(ERROR)
        return encode_verify_result(result)


class RestrictedWriterVerifyWireContract:
    """Nominal contract operations injected into the process-only launch adapter."""

    def validate_request(self, value: object) -> None:
        if type(value) is not SqlClientRestrictedWriterVerifyRequest:
            raise ValueError(ERROR)
        value.__post_init__()

    def decode_startup(self, payload: bytes) -> object:
        try:
            body = strict_verify_object(payload)
            if set(body) != {"schema", "process"} or body["schema"] != (
                "dpone.sqlclient.restricted-writer-verify-startup.v1"
            ):
                raise ValueError
            return decode_verify_process(body["process"])
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None

    def registration(
        self, reservation: RestrictedWriterVerifyReservation, process: object
    ) -> RestrictedWriterVerifyRegistration:
        return RestrictedWriterVerifyRegistration(reservation, cast(Any, process), reservation.execution_owner)

    def decode_result(self, payload: bytes):
        return decode_verify_result(payload)

    def decode_opening(self, payload: bytes):
        return decode_verify_opening(payload)

    def encode_authorization(self, opening_payload: bytes) -> bytes:
        return encode_probe_authorization(opening_payload)


class RestrictedWriterVerifySqlOperations(RestrictedWriterVerifyDomainOperations):
    """P9 SQL projection rules injected into the connection-only adapter."""

    def encode_request(self, value: object) -> bytes:
        return encode_verify_request(self.validate_request(value))

    def encode_result(self, result: object) -> bytes:
        if type(result) is not SqlClientRestrictedWriterVerifyResult:
            raise ValueError(ERROR)
        return encode_verify_result(result)

    def opening(self, request: object, context: object) -> object:
        checked = self.validate_request(request)
        self.validate_opening(checked, context)
        return OPENING_TYPE(verify_request_digest(encode_verify_request(checked)), cast(Any, context))

    def encode_opening(self, opening: object) -> bytes:
        if type(opening) is not OPENING_TYPE:
            raise ValueError(ERROR)
        return encode_verify_opening(opening)

    def validate_authorization(self, payload: bytes, *, opening_payload: bytes) -> None:
        decode_probe_authorization(payload, opening_payload=opening_payload)
