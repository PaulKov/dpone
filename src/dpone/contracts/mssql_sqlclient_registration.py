"""Registration and secret-free credential intent; neither proves an artifact ACK."""

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Literal

from dpone.contracts.mssql_sqlclient_evidence_binding import (
    SqlClientEvidenceBinding,
    decode_evidence_binding,
    encode_evidence_binding,
    validate_evidence_binding,
)
from dpone.contracts.mssql_sqlclient_evidence_types import ERROR, SqlClientEvidenceKind, require_payload
from dpone.contracts.mssql_sqlclient_input import (
    SqlClientInputDescriptor,
    decode_input_descriptor,
    encode_input_descriptor,
    input_descriptor_digest,
)
from dpone.contracts.mssql_sqlclient_launch import (
    SqlClientLaunch,
    SqlClientReady,
    decode_launch,
    decode_ready,
    encode_launch,
    encode_ready,
    validate_ready,
)
from dpone.contracts.mssql_tds_session import require_session_nonce
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import record_shape


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientRegistration:
    """Technical originals; supervisor supplies actual startup and policy admission."""

    binding: SqlClientEvidenceBinding
    launch: SqlClientLaunch
    ready: SqlClientReady
    input: SqlClientInputDescriptor
    input_mode: str
    batch_rows: int
    max_input_batch_bytes: int
    schema: str = "dpone.sqlclient.registration.v1"

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.sqlclient.registration.v1":
            raise ValueError(ERROR)
        for value, cls in (
            (self.launch, SqlClientLaunch),
            (self.ready, SqlClientReady),
            (self.input, SqlClientInputDescriptor),
        ):
            if type(value) is not cls:
                raise ValueError(ERROR)
            value.__post_init__()
        binding = decode_evidence_binding(encode_evidence_binding(self.binding))
        launch = decode_launch(encode_launch(self.launch))
        ready = decode_ready(encode_ready(self.ready))
        source = decode_input_descriptor(encode_input_descriptor(self.input))
        validate_evidence_binding(
            binding,
            launch=launch,
            identity=binding.identity,
            ownership=binding.ownership,
            object_identity=binding.object_identity,
        )
        # Structural readiness binding only. No current-clock/observed-startup assertion.
        validate_ready(launch, ready, now_ns=0)
        if (
            source.fd != launch.descriptors.input
            or input_descriptor_digest(source) != launch.input_binding_sha256
            or source.expected.file_sha256 != binding.identity.file_sha256
            or type(self.input_mode) is not str
            or self.input_mode not in ("rows", "arrow")
        ):
            raise ValueError(ERROR)
        _integer(self.batch_rows, 1, 65536)
        _integer(self.max_input_batch_bytes, 1 << 20, 256 << 20)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientCredentialIntentRecord:
    """Contains no credential object, host, username, password or secret hash."""

    binding: SqlClientEvidenceBinding
    registration_sha256: str
    job_binding_sha256: str
    input_empty: bool
    session_nonce: str | None
    tls_profile: str | None
    capability_evidence_sha256: str
    schema: str = "dpone.sqlclient.credential-intent.v1"

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.sqlclient.credential-intent.v1":
            raise ValueError(ERROR)
        decode_evidence_binding(encode_evidence_binding(self.binding))
        for value in (self.registration_sha256, self.job_binding_sha256, self.capability_evidence_sha256):
            _hash(value)
        if type(self.input_empty) is not bool:
            raise ValueError(ERROR)
        if self.input_empty:
            if self.session_nonce is not None or self.tls_profile is not None:
                raise ValueError(ERROR)
        else:
            if type(self.session_nonce) is not str:
                raise ValueError(ERROR)
            _hash(self.session_nonce)
            require_session_nonce(bytes.fromhex(self.session_nonce))
            if type(self.tls_profile) is not str or self.tls_profile not in ("verified", "disposable_test"):
                raise ValueError(ERROR)


def _job_projection(
    registration: SqlClientRegistration,
    session_nonce: str | None,
    *,
    credentials_present: bool,
) -> dict:
    return {
        "schema_version": 1,
        "launch_sha256": registration.binding.launch_sha256,
        "identity": asdict(registration.binding.identity),
        "ownership": asdict(registration.binding.ownership),
        "object_identity": asdict(registration.binding.object_identity),
        "input": asdict(registration.input),
        "input_mode": registration.input_mode,
        "batch_rows": registration.batch_rows,
        "max_input_batch_bytes": registration.max_input_batch_bytes,
        "session_nonce": session_nonce,
        "credentials_present": credentials_present,
    }


def build_credential_intent(
    registration: SqlClientRegistration,
    *,
    session_nonce: bytes | None,
    tls_profile: Literal["verified", "disposable_test"] | None,
    capability_evidence_sha256: str,
) -> SqlClientCredentialIntentRecord:
    """Build the secret-free Job projection before credential release."""
    try:
        raw = encode_registration(registration)
        registration = decode_registration(raw)
        empty = registration.input.expected.rows == 0
        if empty != (session_nonce is None and tls_profile is None):
            raise ValueError(ERROR)
        if session_nonce is not None and type(session_nonce) is not bytes:
            raise ValueError(ERROR)
        nonce = None if session_nonce is None else session_nonce.hex()
        projection = _job_projection(registration, nonce, credentials_present=not empty)
        return SqlClientCredentialIntentRecord(
            binding=registration.binding,
            registration_sha256=sha256(raw).hexdigest(),
            job_binding_sha256=sha256(
                b"dpone.sqlclient.job-binding.v1\0" + canonical_json_bytes(projection)
            ).hexdigest(),
            input_empty=empty,
            session_nonce=nonce,
            tls_profile=tls_profile,
            capability_evidence_sha256=capability_evidence_sha256,
        )
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError):
        raise ValueError(ERROR) from None


def encode_registration(record: SqlClientRegistration) -> bytes:
    """Produce canonical technical bytes after deep reconstruction."""
    try:
        if type(record) is not SqlClientRegistration:
            raise ValueError(ERROR)
        record.__post_init__()
        body = canonical_json_bytes(asdict(record))
        require_payload(body, SqlClientEvidenceKind.REGISTRATION)
        return body
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_registration(payload: bytes) -> SqlClientRegistration:
    """Bound outer and nested bodies before their specific decoders run."""
    try:
        require_payload(payload, SqlClientEvidenceKind.REGISTRATION)
        data = record_shape(SqlClientRegistration, strict_json_object(payload))
        for name, decoder in (
            ("binding", decode_evidence_binding),
            ("launch", decode_launch),
            ("ready", decode_ready),
            ("input", decode_input_descriptor),
        ):
            data[name] = decoder(canonical_json_bytes(data[name]))
        record = SqlClientRegistration(**data)
        if encode_registration(record) != payload:
            raise ValueError(ERROR)
        return record
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def encode_credential_intent(record: SqlClientCredentialIntentRecord) -> bytes:
    """Encode an intent only; referenced ACK authenticity remains supervisor-owned."""
    try:
        if type(record) is not SqlClientCredentialIntentRecord:
            raise ValueError(ERROR)
        record.__post_init__()
        body = canonical_json_bytes(asdict(record))
        require_payload(body, SqlClientEvidenceKind.CREDENTIAL_INTENT)
        return body
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_credential_intent(payload: bytes) -> SqlClientCredentialIntentRecord:
    """Reject extras/aliases; registration comparison is an explicit second step."""
    try:
        require_payload(payload, SqlClientEvidenceKind.CREDENTIAL_INTENT)
        data = record_shape(SqlClientCredentialIntentRecord, strict_json_object(payload))
        data["binding"] = decode_evidence_binding(canonical_json_bytes(data["binding"]))
        record = SqlClientCredentialIntentRecord(**data)
        if encode_credential_intent(record) != payload:
            raise ValueError(ERROR)
        return record
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def validate_credential_intent(record: SqlClientCredentialIntentRecord, registration: SqlClientRegistration) -> None:
    """Compare acknowledged registration and reconstruct the existing Job projection.

    The caller supplies the original acknowledged registration. Reuses the
    existing Job binding domain without constructing or encoding a secret Job.
    Capability evidence authenticity is separately admitted by the supervisor.
    """
    record = decode_credential_intent(encode_credential_intent(record))
    raw = encode_registration(registration)
    registration = decode_registration(raw)
    data = _job_projection(registration, record.session_nonce, credentials_present=not record.input_empty)
    if (
        record.binding != registration.binding
        or record.registration_sha256 != sha256(raw).hexdigest()
        or record.input_empty != (registration.input.expected.rows == 0)
        or record.job_binding_sha256
        != sha256(b"dpone.sqlclient.job-binding.v1\0" + canonical_json_bytes(data)).hexdigest()
    ):
        raise ValueError(ERROR)
