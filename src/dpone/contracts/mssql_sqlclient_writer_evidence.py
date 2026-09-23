"""Full private writer observations; catalog identity never grants SQL capability."""

from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_evidence_binding import (
    SqlClientEvidenceBinding,
    decode_evidence_binding,
    encode_evidence_binding,
)
from dpone.contracts.mssql_sqlclient_evidence_types import ERROR, SqlClientEvidenceKind, require_payload
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientPrincipalResolution,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
    SqlClientWriterObservation,
    encode_session_authority,
)
from dpone.contracts.mssql_sqlclient_registration import (
    SqlClientCredentialIntentRecord,
    SqlClientRegistration,
    encode_credential_intent,
    encode_registration,
    validate_credential_intent,
)
from dpone.contracts.mssql_sqlclient_session_control import (
    SqlClientDatabasePrincipal,
    SqlClientSessionAnnouncement,
    decode_session_announcement,
    encode_session_announcement,
)
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, decode_session_identity, encode_session_identity
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape


def _authority(data: dict) -> SqlClientSessionAuthority:
    data = record_shape(SqlClientSessionAuthority, data)
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
        ("principal_resolution", SqlClientPrincipalResolution),
    ):
        data[name] = construct_record(cls, data[name])
    return SqlClientSessionAuthority(**data)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterObservationRecord:
    """Standalone consistency, not proof that references were acknowledged."""

    binding: SqlClientEvidenceBinding
    registration_sha256: str
    credential_intent_sha256: str
    capability_evidence_sha256: str
    announcement: SqlClientSessionAnnouncement
    remote_session: TdsRemoteSessionIdentity
    authority: SqlClientSessionAuthority
    resolved_database_principal: SqlClientDatabasePrincipal
    schema: str = "dpone.sqlclient.writer-observation.v1"

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.sqlclient.writer-observation.v1":
            raise ValueError(ERROR)
        decode_evidence_binding(encode_evidence_binding(self.binding))
        for value in (self.registration_sha256, self.credential_intent_sha256, self.capability_evidence_sha256):
            _hash(value)
        for nested, cls in (
            (self.announcement, SqlClientSessionAnnouncement),
            (self.remote_session, TdsRemoteSessionIdentity),
            (self.authority, SqlClientSessionAuthority),
            (self.resolved_database_principal, SqlClientDatabasePrincipal),
        ):
            if type(nested) is not cls:
                raise ValueError(ERROR)
            nested.__post_init__()
        announced = decode_session_announcement(encode_session_announcement(self.announcement))
        remote = decode_session_identity(encode_session_identity(self.remote_session))
        authority = _authority(strict_json_object(encode_session_authority(self.authority)))
        observation = SqlClientWriterObservation(remote, authority)
        if (
            observation.resolved_database_principal != self.resolved_database_principal
            or announced.launch_sha256 != self.binding.launch_sha256
            or announced.attempt_sha256 != self.binding.attempt_sha256
            or announced.nonce != remote.nonce.hex()
            or announced.session_id != remote.session_id
            or authority.database.database_name != self.binding.identity.database
        ):
            raise ValueError(ERROR)


def encode_writer_observation(record: SqlClientWriterObservationRecord) -> bytes:
    """Persist full authority using existing session/timestamp and authority codecs."""
    try:
        if type(record) is not SqlClientWriterObservationRecord:
            raise ValueError(ERROR)
        record.__post_init__()
        data = asdict(record)
        data["remote_session"] = strict_json_object(encode_session_identity(record.remote_session))
        body = canonical_json_bytes(data)
        require_payload(body, SqlClientEvidenceKind.WRITER_OBSERVATION)
        return body
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_writer_observation(payload: bytes) -> SqlClientWriterObservationRecord:
    """Explicit closed nested reconstruction; no catalog or capability inference."""
    try:
        require_payload(payload, SqlClientEvidenceKind.WRITER_OBSERVATION)
        data = record_shape(SqlClientWriterObservationRecord, strict_json_object(payload))
        data["binding"] = decode_evidence_binding(canonical_json_bytes(data["binding"]))
        data["announcement"] = decode_session_announcement(canonical_json_bytes(data["announcement"]))
        data["remote_session"] = decode_session_identity(canonical_json_bytes(data["remote_session"]))
        data["authority"] = _authority(data["authority"])
        data["resolved_database_principal"] = construct_record(
            SqlClientDatabasePrincipal, data["resolved_database_principal"]
        )
        record = SqlClientWriterObservationRecord(**data)
        if encode_writer_observation(record) != payload:
            raise ValueError(ERROR)
        return record
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def validate_writer_observation(
    record: SqlClientWriterObservationRecord,
    registration: SqlClientRegistration,
    credential_intent: SqlClientCredentialIntentRecord,
) -> None:
    """Compare separately acknowledged originals, including original nonce/capability."""
    record = decode_writer_observation(encode_writer_observation(record))
    validate_credential_intent(credential_intent, registration)
    if (
        credential_intent.input_empty
        or record.binding != registration.binding
        or record.registration_sha256 != sha256(encode_registration(registration)).hexdigest()
        or record.credential_intent_sha256 != sha256(encode_credential_intent(credential_intent)).hexdigest()
        or record.capability_evidence_sha256 != credential_intent.capability_evidence_sha256
        or record.announcement.nonce != credential_intent.session_nonce
    ):
        raise ValueError(ERROR)
