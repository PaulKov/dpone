"""Credential-free exact P10f projection for one SqlClient native chunk."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_evidence_types import (
    SqlClientEvidenceKind,
    SqlClientEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
    stage_object_identity,
)
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsObjectIdentity
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.contracts.strict_record import construct_record

ERROR = "mssql_native.sqlclient_terminal_projection_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDirectoryCoordinate:
    """Stable attempt coordinate used to locate its durable directory."""

    target_key: str
    run_id: str
    ordinal: int
    attempt: int

    def __post_init__(self) -> None:
        try:
            _text(self.target_key)
            _text(self.run_id)
            _integer(self.ordinal)
            _integer(self.attempt, 0, 2)
        except (ValueError, TypeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeChunkProjection:
    """Immutable source-free authority projected from the exact P10f terminal.

    The record deliberately contains values only. It carries no descriptor,
    credential, process, actor, session, file path, or mutable writer authority.
    """

    attempt: TdsAttemptIdentity
    attempt_sha256: str
    stage: SqlClientStageIdentity
    object_identity: TdsObjectIdentity
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str
    typed_sum: int
    registration_receipt: SqlClientEvidenceReceipt
    verification_receipt: SqlClientEvidenceReceipt
    lifecycle_verification_sha256: str
    lifecycle_revision: int
    worker_build_sha256: str
    implementation_sha256: str
    helper_implementation_sha256: str
    directory_key: str
    directory_coordinate: SqlClientDirectoryCoordinate
    projection_sha256: str

    def __post_init__(self) -> None:
        try:
            if type(self.attempt) is not TdsAttemptIdentity:
                raise ValueError
            attempt = construct_record(TdsAttemptIdentity, asdict(self.attempt))
            if type(self.stage) is not SqlClientStageIdentity:
                raise ValueError
            stage = decode_stage_identity(encode_stage_identity(self.stage))
            if type(self.object_identity) is not TdsObjectIdentity:
                raise ValueError
            object_identity = construct_record(TdsObjectIdentity, asdict(self.object_identity))
            if type(self.verification_receipt) is not SqlClientEvidenceReceipt:
                raise ValueError
            if type(self.registration_receipt) is not SqlClientEvidenceReceipt:
                raise ValueError
            registration_receipt = construct_record(SqlClientEvidenceReceipt, asdict(self.registration_receipt))
            receipt = construct_record(SqlClientEvidenceReceipt, asdict(self.verification_receipt))
            if type(self.directory_coordinate) is not SqlClientDirectoryCoordinate:
                raise ValueError
            coordinate = construct_record(SqlClientDirectoryCoordinate, asdict(self.directory_coordinate))
            for digest in (
                self.attempt_sha256,
                self.file_sha256,
                self.typed_digest,
                self.lifecycle_verification_sha256,
                self.worker_build_sha256,
                self.implementation_sha256,
                self.helper_implementation_sha256,
                self.projection_sha256,
            ):
                _hash(digest)
            _integer(self.rows, 0, 2**63 - 1)
            _integer(self.encoded_bytes, 0, 2**63 - 1)
            _integer(self.typed_sum, 0, 2**256 - 1)
            _integer(self.lifecycle_revision, 1)
            _text(self.directory_key)
            expected_coordinate = SqlClientDirectoryCoordinate(
                target_key=attempt.target_key,
                run_id=attempt.run_id,
                ordinal=attempt.ordinal,
                attempt=attempt.attempt,
            )
            if (
                attempt_identity_digest(attempt) != self.attempt_sha256
                or attempt.file_sha256 != self.file_sha256
                or attempt.implementation_sha256 != self.implementation_sha256
                or (stage.database_name, stage.schema_name, stage.table_name, stage.owner_binding)
                != (attempt.database, attempt.schema, attempt.table, attempt.owner_binding)
                or stage_object_identity(stage) != object_identity
                or registration_receipt.kind is not SqlClientEvidenceKind.REGISTRATION
                or registration_receipt.attempt_sha256 != self.attempt_sha256
                or receipt.kind is not SqlClientEvidenceKind.VERIFICATION
                or receipt.attempt_sha256 != self.attempt_sha256
                or receipt.payload_sha256 != self.lifecycle_verification_sha256
                or coordinate != expected_coordinate
                or self.directory_key != directory_key(attempt)
                or self.projection_sha256 != _projection_digest(self)
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None


def _projection_digest(value: SqlClientNativeChunkProjection) -> str:
    """Bind every value without serializing stage UUID/datetime objects directly."""
    body = {
        "attempt_sha256": value.attempt_sha256,
        "attempt_identity_sha256": attempt_identity_digest(value.attempt),
        "stage_sha256": sha256(encode_stage_identity(value.stage)).hexdigest(),
        "object_identity": asdict(value.object_identity),
        "rows": value.rows,
        "encoded_bytes": value.encoded_bytes,
        "file_sha256": value.file_sha256,
        "typed_digest": value.typed_digest,
        "typed_sum": value.typed_sum,
        "registration_receipt": asdict(value.registration_receipt),
        "verification_receipt": asdict(value.verification_receipt),
        "lifecycle_verification_sha256": value.lifecycle_verification_sha256,
        "lifecycle_revision": value.lifecycle_revision,
        "worker_build_sha256": value.worker_build_sha256,
        "implementation_sha256": value.implementation_sha256,
        "helper_implementation_sha256": value.helper_implementation_sha256,
        "directory_key": value.directory_key,
        "directory_coordinate": asdict(value.directory_coordinate),
    }
    return sha256(b"dpone.sqlclient.native-chunk.v1\0" + canonical_json_bytes(body)).hexdigest()


def bind_sqlclient_native_chunk(**facts: object) -> SqlClientNativeChunkProjection:
    """Create one self-binding value after the service has validated terminal facts."""
    try:
        provisional = object.__new__(SqlClientNativeChunkProjection)
        for field, value in facts.items():
            if field == "projection_sha256":
                raise ValueError
            object.__setattr__(provisional, field, value)
        object.__setattr__(provisional, "projection_sha256", "0" * 64)
        digest = _projection_digest(provisional)
        return SqlClientNativeChunkProjection(**facts, projection_sha256=digest)  # type: ignore[arg-type]
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


__all__ = (
    "SqlClientDirectoryCoordinate",
    "SqlClientNativeChunkProjection",
    "bind_sqlclient_native_chunk",
)
