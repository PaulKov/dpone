"""Three exact v2 nested evidence payloads within the shared six-kind protocol.

Registration, local exit, exclusion, actor ACKs and retention remain common.
These bytes establish consistency only; original producer facts are mandatory.
"""

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import ERROR, _require_subject, require_payload
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_ipc import _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import (
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequestV2,
    SqlClientDepartureResultV2,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_v2_codec import (
    decode_departure_plan_v2,
    decode_departure_request_v2,
    decode_departure_result_v2,
    encode_departure_plan_v2,
    encode_departure_request_v2,
    encode_departure_result_v2,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape

_FAILURES = (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError)
_LAUNCH_SCHEMA = "dpone.sqlclient.departure-launch-intent-evidence.v2"
_CREDENTIAL_SCHEMA = "dpone.sqlclient.departure-credential-intent-evidence.v2"
_RESULT_SCHEMA = "dpone.sqlclient.departure-result-evidence.v2"


@dataclass(frozen=True, slots=True)
class SqlClientDepartureLaunchIntentV2:
    """Validated original launch plan, without an outer duplicate identity."""

    plan: SqlClientDeparturePlanV2

    def __post_init__(self) -> None:
        try:
            _typed(self.plan, SqlClientDeparturePlanV2)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureCredentialIntentV2:
    """Nonsecret request tied to its validated subject and registration link."""

    helper_id: UUID
    attempt_sha256: str
    registration_sha256: str
    request: SqlClientDepartureRequestV2

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.registration_sha256)
            _typed(self.request, SqlClientDepartureRequestV2)
            if self.helper_id != self.request.plan.helper_id or self.attempt_sha256 != attempt_identity_digest(
                self.request.plan.attempt
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureResultEvidenceV2:
    """Result shape and credential-intent link; codecs bind the original request."""

    helper_id: UUID
    attempt_sha256: str
    credential_intent_sha256: str
    result: SqlClientDepartureResultV2

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.credential_intent_sha256)
            _typed(self.result, SqlClientDepartureResultV2)
        except _FAILURES:
            raise ValueError(ERROR) from None


def encode_launch_intent_v2(value: SqlClientDepartureLaunchIntentV2) -> bytes:
    """Revalidate the original plan before canonical nested-object encoding."""
    try:
        _typed(value, SqlClientDepartureLaunchIntentV2)
        payload = canonical_json_bytes(
            dict(schema=_LAUNCH_SCHEMA, plan=strict_json_object(encode_departure_plan_v2(value.plan)))
        )
        require_payload(payload, Kind.LAUNCH_INTENT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_launch_intent_v2(payload: bytes) -> SqlClientDepartureLaunchIntentV2:
    """Bound before strict parsing; accept only the complete canonical wrapper."""
    try:
        require_payload(payload, Kind.LAUNCH_INTENT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _LAUNCH_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureLaunchIntentV2, data)
        value = SqlClientDepartureLaunchIntentV2(decode_departure_plan_v2(canonical_json_bytes(data["plan"])))
        if encode_launch_intent_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_credential_intent_v2(value: SqlClientDepartureCredentialIntentV2) -> bytes:
    """Persist only the nonsecret request, never the private delivery envelope."""
    try:
        _typed(value, SqlClientDepartureCredentialIntentV2)
        payload = canonical_json_bytes(
            dict(
                schema=_CREDENTIAL_SCHEMA,
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                registration_sha256=value.registration_sha256,
                request=strict_json_object(encode_departure_request_v2(value.request)),
            )
        )
        require_payload(payload, Kind.CREDENTIAL_INTENT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_credential_intent_v2(payload: bytes) -> SqlClientDepartureCredentialIntentV2:
    """Require exact subject binding and canonical nested nonsecret request bytes."""
    try:
        require_payload(payload, Kind.CREDENTIAL_INTENT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _CREDENTIAL_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureCredentialIntentV2, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["request"] = decode_departure_request_v2(canonical_json_bytes(data["request"]))
        value = SqlClientDepartureCredentialIntentV2(**data)
        if encode_credential_intent_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def _result_subject(value: SqlClientDepartureResultEvidenceV2, request: SqlClientDepartureRequestV2) -> None:
    encode_departure_request_v2(request)
    if value.helper_id != request.plan.helper_id or value.attempt_sha256 != attempt_identity_digest(
        request.plan.attempt
    ):
        raise ValueError(ERROR)


def encode_result_evidence_v2(
    value: SqlClientDepartureResultEvidenceV2, *, request: SqlClientDepartureRequestV2
) -> bytes:
    """Validate independent original request and creator facts before serialization."""
    try:
        _typed(value, SqlClientDepartureResultEvidenceV2)
        _result_subject(value, request)
        payload = canonical_json_bytes(
            dict(
                schema=_RESULT_SCHEMA,
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                credential_intent_sha256=value.credential_intent_sha256,
                result=strict_json_object(encode_departure_result_v2(value.result, request=request)),
            )
        )
        require_payload(payload, Kind.RESULT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_result_evidence_v2(
    payload: bytes, *, request: SqlClientDepartureRequestV2
) -> SqlClientDepartureResultEvidenceV2:
    """Never infer the expected request from the incoming result or its digest."""
    try:
        require_payload(payload, Kind.RESULT)
        encode_departure_request_v2(request)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _RESULT_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureResultEvidenceV2, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["result"] = decode_departure_result_v2(canonical_json_bytes(data["result"]), request=request)
        value = SqlClientDepartureResultEvidenceV2(**data)
        if encode_result_evidence_v2(value, request=request) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None
