"""Canonical nonsecret launch, registration and credential-intent evidence.

References identify proposed artifact bytes, not authenticated observations or
ACKs. The owning parent must compare its actual launch and preceding receipts.
"""

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_evidence_types import ERROR, _require_subject, require_payload
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_ipc import SqlClientDeparturePlan, SqlClientDepartureRequest, _typed
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    decode_departure_plan,
    decode_departure_request,
    encode_departure_plan,
    encode_departure_request,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup, encode_startup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape

_FAILURES = (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError)
_LAUNCH_SCHEMA = "dpone.sqlclient.departure-launch-intent-evidence.v1"
_REGISTRATION_SCHEMA = "dpone.sqlclient.departure-registration-evidence.v1"
_CREDENTIAL_SCHEMA = "dpone.sqlclient.departure-credential-intent-evidence.v1"


@dataclass(frozen=True, slots=True)
class SqlClientDepartureLaunchIntent:
    """Validated original launch plan, without an outer duplicate identity."""

    plan: SqlClientDeparturePlan

    def __post_init__(self) -> None:
        try:
            _typed(self.plan, SqlClientDeparturePlan)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureRegistration:
    """Supplied startup and admission reference; no producer authentication."""

    helper_id: UUID
    attempt_sha256: str
    launch_intent_sha256: str
    startup: TdsCoordinatorStartup
    admission_sha256: str

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.launch_intent_sha256)
            _hash(self.admission_sha256)
            if type(self.startup) is not TdsCoordinatorStartup:
                raise ValueError
            _typed(self.startup.process, TdsProcessIdentity)
            _typed(self.startup, TdsCoordinatorStartup)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientDepartureCredentialIntent:
    """Nonsecret request tied to its validated subject and registration link."""

    helper_id: UUID
    attempt_sha256: str
    registration_sha256: str
    request: SqlClientDepartureRequest

    def __post_init__(self) -> None:
        try:
            _require_subject(self.helper_id, self.attempt_sha256)
            _hash(self.registration_sha256)
            _typed(self.request, SqlClientDepartureRequest)
            if self.helper_id != self.request.plan.helper_id or self.attempt_sha256 != attempt_identity_digest(
                self.request.plan.attempt
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


def encode_launch_intent(value: SqlClientDepartureLaunchIntent) -> bytes:
    """Revalidate the original plan before canonical nested-object encoding."""
    try:
        _typed(value, SqlClientDepartureLaunchIntent)
        payload = canonical_json_bytes(
            dict(schema=_LAUNCH_SCHEMA, plan=strict_json_object(encode_departure_plan(value.plan)))
        )
        require_payload(payload, Kind.LAUNCH_INTENT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_launch_intent(payload: bytes) -> SqlClientDepartureLaunchIntent:
    """Bound before strict parsing; accept only the complete canonical wrapper."""
    try:
        require_payload(payload, Kind.LAUNCH_INTENT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _LAUNCH_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureLaunchIntent, data)
        value = SqlClientDepartureLaunchIntent(decode_departure_plan(canonical_json_bytes(data["plan"])))
        if encode_launch_intent(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_registration(value: SqlClientDepartureRegistration) -> bytes:
    """Check original startup/process scalars before the existing startup encoder."""
    try:
        _typed(value, SqlClientDepartureRegistration)
        payload = canonical_json_bytes(
            dict(
                schema=_REGISTRATION_SCHEMA,
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                launch_intent_sha256=value.launch_intent_sha256,
                startup=strict_json_object(encode_startup(value.startup)),
                admission_sha256=value.admission_sha256,
            )
        )
        require_payload(payload, Kind.REGISTRATION)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_registration(payload: bytes) -> SqlClientDepartureRegistration:
    """Reject unknown fields and alternate UUID, nonce or JSON spellings."""
    try:
        require_payload(payload, Kind.REGISTRATION)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _REGISTRATION_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureRegistration, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["startup"] = decode_startup(canonical_json_bytes(data["startup"]))
        value = SqlClientDepartureRegistration(**data)
        if encode_registration(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_credential_intent(value: SqlClientDepartureCredentialIntent) -> bytes:
    """Persist only the nonsecret request, never the private delivery envelope."""
    try:
        _typed(value, SqlClientDepartureCredentialIntent)
        payload = canonical_json_bytes(
            dict(
                schema=_CREDENTIAL_SCHEMA,
                helper_id=str(value.helper_id),
                attempt_sha256=value.attempt_sha256,
                registration_sha256=value.registration_sha256,
                request=strict_json_object(encode_departure_request(value.request)),
            )
        )
        require_payload(payload, Kind.CREDENTIAL_INTENT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_credential_intent(payload: bytes) -> SqlClientDepartureCredentialIntent:
    """Require exact subject binding and canonical nested nonsecret request bytes."""
    try:
        require_payload(payload, Kind.CREDENTIAL_INTENT)
        data = strict_json_object(payload)
        if data.pop("schema", None) != _CREDENTIAL_SCHEMA:
            raise ValueError
        data = record_shape(SqlClientDepartureCredentialIntent, data)
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["request"] = decode_departure_request(canonical_json_bytes(data["request"]))
        value = SqlClientDepartureCredentialIntent(**data)
        if encode_credential_intent(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None
