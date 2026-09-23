"""Canonical helper RESULT, local-exit and exclusion content, without authority.

RESULT requires an independently retained original request. Content references
identify artifact bytes; they do not prove writes, actual reaping or exclusion.
"""

from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import ERROR, _require_subject, require_payload
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_ipc import SqlClientDepartureRequest, SqlClientDepartureResult, _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    decode_departure_result,
    encode_departure_request,
    encode_departure_result,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape

_FAILURES = (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError)
_RESULT_SCHEMA = "dpone.sqlclient.departure-result-evidence.v1"
_LOCAL_SCHEMA = "dpone.sqlclient.departure-local-exit-evidence.v1"
_EXCLUSION_SCHEMA = "dpone.sqlclient.departure-exclusion-evidence.v1"


@dataclass(frozen=True, slots=True)
class SqlClientDepartureResultEvidence:
    """Result shape and credential-intent link; codecs bind the original request."""

    helper_id: UUID
    attempt_sha256: str
    credential_intent_sha256: str
    result: SqlClientDepartureResult

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.credential_intent_sha256)
            _typed(self.result, SqlClientDepartureResult)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureLocalExit:
    """Success-only supplied exit, never an assertion that a process was reaped."""

    helper_id: UUID
    attempt_sha256: str
    registration_sha256: str
    result_sha256: str
    exit: TdsChildExit

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.registration_sha256)
            _hash(self.result_sha256)
            if type(self.exit) is not TdsChildExit:
                raise ValueError
            _typed(self.exit.identity, TdsProcessIdentity)
            _typed(self.exit, TdsChildExit)
            if self.exit.exit_code != 0 or self.exit.reaped is not True:
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureExclusion:
    """Five exact CREATE/helper byte links; no settled, Prepared or permission flag."""

    helper_id: UUID
    attempt_sha256: str
    create_operation_sha256: str
    create_result_sha256: str
    create_local_exit_sha256: str
    result_sha256: str
    local_exit_sha256: str

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            for digest in (
                self.create_operation_sha256,
                self.create_result_sha256,
                self.create_local_exit_sha256,
                self.result_sha256,
                self.local_exit_sha256,
            ):
                _hash(digest)
        except _FAILURES:
            raise ValueError(ERROR) from None


def _result_subject(value: SqlClientDepartureResultEvidence, request: SqlClientDepartureRequest) -> None:
    encode_departure_request(request)
    if value.helper_id != request.plan.helper_id or value.attempt_sha256 != attempt_identity_digest(
        request.plan.attempt
    ):
        raise ValueError(ERROR)


def encode_result_evidence(value: SqlClientDepartureResultEvidence, *, request: SqlClientDepartureRequest) -> bytes:
    """Validate independent original request and creator facts before serialization."""
    try:
        _typed(value, SqlClientDepartureResultEvidence)
        _result_subject(value, request)
        payload = canonical_json_bytes(
            dict(
                schema=_RESULT_SCHEMA,
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                credential_intent_sha256=value.credential_intent_sha256,
                result=strict_json_object(encode_departure_result(value.result, request=request)),
            )
        )
        require_payload(payload, Kind.RESULT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_result_evidence(payload: bytes, *, request: SqlClientDepartureRequest) -> SqlClientDepartureResultEvidence:
    """Never infer the expected request from the incoming result or its digest."""
    try:
        require_payload(payload, Kind.RESULT)
        encode_departure_request(request)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _RESULT_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureResultEvidence, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["result"] = decode_departure_result(canonical_json_bytes(data["result"]), request=request)
        value = SqlClientDepartureResultEvidence(**data)
        if encode_result_evidence(value, request=request) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_local_exit(value: SqlClientDepartureLocalExit) -> bytes:
    """Require original exact process/zero-exit/true-reap scalars before asdict."""
    try:
        _typed(value, SqlClientDepartureLocalExit)
        data = asdict(value)
        data.update(schema=_LOCAL_SCHEMA, helper_id=str(value.helper_id))
        payload = canonical_json_bytes(data)
        require_payload(payload, Kind.LOCAL_EXIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_local_exit(payload: bytes) -> SqlClientDepartureLocalExit:
    """Decode only the closed canonical success shape; no process I/O occurs."""
    try:
        require_payload(payload, Kind.LOCAL_EXIT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _LOCAL_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureLocalExit, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        local = record_shape(TdsChildExit, data["exit"])
        local["identity"] = construct_record(TdsProcessIdentity, local["identity"])
        data["exit"] = TdsChildExit(**local)
        value = SqlClientDepartureLocalExit(**data)
        if encode_local_exit(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_exclusion(value: SqlClientDepartureExclusion) -> bytes:
    """Encode fixed original-artifact references without settlement assertions."""
    try:
        _typed(value, SqlClientDepartureExclusion)
        data = asdict(value)
        data.update(schema=_EXCLUSION_SCHEMA, helper_id=str(value.helper_id))
        payload = canonical_json_bytes(data)
        require_payload(payload, Kind.EXCLUSION)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_exclusion(payload: bytes) -> SqlClientDepartureExclusion:
    """Parent comparison with independently retained ACKs remains mandatory."""
    try:
        require_payload(payload, Kind.EXCLUSION)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _EXCLUSION_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureExclusion, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        value = SqlClientDepartureExclusion(**data)
        if encode_exclusion(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None
