"""Canonical schema-v4 SqlClient chunk receipt shared by all consumers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from hashlib import sha256
from json import dumps
from typing import Any, cast

from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_sqlclient_evidence_types import (
    SqlClientEvidenceKind,
    SqlClientEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
    stage_object_identity,
)
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.mssql_tds_worker import TdsObjectIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record

ERROR = "mssql_native.sqlclient_native_chunk_receipt_invalid"
SCHEMA = "dpone.sqlclient.native-chunk-receipt.v1"
_STAGE_DOMAIN = b"dpone.sqlclient.native-stage-id.v1\0"
_CUSTODY_SCHEMA = "dpone.sqlclient.input-custody.v1"


def canonical_stage_id(value: TdsObjectIdentity) -> str:
    """Derive the scheduler stage id only from the canonical object identity."""
    try:
        if type(value) is not TdsObjectIdentity:
            raise ValueError
        checked = construct_record(TdsObjectIdentity, asdict(value))
        return sha256(_STAGE_DOMAIN + canonical_json_bytes(asdict(checked))).hexdigest()
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientInputCustodyReceipt:
    """Closed durable-input receipt embedded byte-identically in v4 evidence."""

    schema: str
    plan_sha256: str
    target_id: str
    run_id: str
    window_fingerprint: str
    attempt_id: str
    ordinal: int
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str
    durable_object_id: str
    durable_location_sha256: str
    custody_sha256: str

    def __post_init__(self) -> None:
        try:
            if self.schema != _CUSTODY_SCHEMA:
                raise ValueError
            for value in (
                self.target_id,
                self.run_id,
                self.window_fingerprint,
                self.attempt_id,
                self.durable_object_id,
            ):
                _text(value)
            for value in (
                self.plan_sha256,
                self.file_sha256,
                self.typed_digest,
                self.durable_location_sha256,
                self.custody_sha256,
            ):
                _hash(value)
            for number in (self.ordinal, self.rows, self.encoded_bytes):
                _integer(number, 0, 2**63 - 1)
            body = asdict(self)
            body.pop("custody_sha256")
            expected = sha256(_CUSTODY_SCHEMA.encode() + b"\0" + canonical_json_bytes(body)).hexdigest()
            if self.custody_sha256 != expected:
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None

    @classmethod
    def bind(cls, **facts: object) -> SqlClientInputCustodyReceipt:
        """Build the canonical self-binding receipt from validated custody facts."""
        body = {"schema": _CUSTODY_SCHEMA, **facts}
        digest = sha256(_CUSTODY_SCHEMA.encode() + b"\0" + canonical_json_bytes(body)).hexdigest()
        try:
            return cls(**body, custody_sha256=digest)  # type: ignore[arg-type]
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeChunkEvidence:
    """Closed evidence body carried by the generic native receipt."""

    plan_sha256: str
    window_fingerprint: str
    projection_sha256: str
    lifecycle_verification_sha256: str
    lifecycle_revision: int
    verification_payload_sha256: str
    registration_receipt: SqlClientEvidenceReceipt
    verification_receipt: SqlClientEvidenceReceipt
    input_custody: SqlClientInputCustodyReceipt
    stage_identity: SqlClientStageIdentity
    object_identity: TdsObjectIdentity
    typed_sum: int
    schema: str = SCHEMA

    def __post_init__(self) -> None:
        try:
            if self.schema != SCHEMA:
                raise ValueError
            _text(self.window_fingerprint)
            for value in (
                self.plan_sha256,
                self.projection_sha256,
                self.lifecycle_verification_sha256,
                self.verification_payload_sha256,
            ):
                _hash(value)
            _integer(self.lifecycle_revision, 1)
            _integer(self.typed_sum, 0, 2**256 - 1)
            for receipt, kind in (
                (self.registration_receipt, SqlClientEvidenceKind.REGISTRATION),
                (self.verification_receipt, SqlClientEvidenceKind.VERIFICATION),
            ):
                if type(receipt) is not SqlClientEvidenceReceipt:
                    raise ValueError
                receipt.__post_init__()
                if receipt.kind is not kind:
                    raise ValueError
            self.input_custody.__post_init__()
            if type(self.stage_identity) is not SqlClientStageIdentity:
                raise ValueError
            stage = decode_stage_identity(encode_stage_identity(self.stage_identity))
            if type(self.object_identity) is not TdsObjectIdentity:
                raise ValueError
            object_identity = construct_record(TdsObjectIdentity, asdict(self.object_identity))
            if (
                self.registration_receipt.attempt_sha256 != self.verification_receipt.attempt_sha256
                or self.verification_payload_sha256 != self.verification_receipt.payload_sha256
                or self.lifecycle_verification_sha256 != self.verification_receipt.payload_sha256
                or self.plan_sha256 != self.input_custody.plan_sha256
                or self.window_fingerprint != self.input_custody.window_fingerprint
                or stage_object_identity(stage) != object_identity
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None

    def to_mapping(self) -> dict[str, Any]:
        self.__post_init__()
        body = asdict(self)
        body["stage_identity"] = strict_json_object(encode_stage_identity(self.stage_identity))
        body["object_identity"] = asdict(self.object_identity)
        body["native_typed_sum"] = body.pop("typed_sum")
        return body

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SqlClientNativeChunkEvidence:
        try:
            expected = set(cls.__dataclass_fields__) - {"typed_sum"} | {"native_typed_sum"}
            if not isinstance(value, Mapping) or set(value) != expected:
                raise ValueError
            body = dict(value)
            body["typed_sum"] = body.pop("native_typed_sum")
            for name in ("registration_receipt", "verification_receipt"):
                raw = dict(body[name])
                raw["kind"] = SqlClientEvidenceKind(raw["kind"])
                body[name] = construct_record(SqlClientEvidenceReceipt, raw)
            body["input_custody"] = construct_record(SqlClientInputCustodyReceipt, body["input_custody"])
            body["stage_identity"] = decode_stage_identity(canonical_json_bytes(body["stage_identity"]))
            body["object_identity"] = construct_record(TdsObjectIdentity, body["object_identity"])
            result = cls(**body)
            if result.to_mapping() != dict(value):
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None


def bind_native_chunk_receipt(
    *,
    projection: SqlClientNativeChunkProjection,
    plan_sha256: str,
    window_fingerprint: str,
    attempt_id: str,
    custody: object,
) -> NativeChunkReceipt:
    """Bind P10f projection and durable custody into one generic receipt."""
    try:
        projection.__post_init__()
        custody_receipt = construct_record(SqlClientInputCustodyReceipt, asdict(cast(Any, custody)))
        evidence = SqlClientNativeChunkEvidence(
            plan_sha256=plan_sha256,
            window_fingerprint=window_fingerprint,
            projection_sha256=projection.projection_sha256,
            lifecycle_verification_sha256=projection.lifecycle_verification_sha256,
            lifecycle_revision=projection.lifecycle_revision,
            verification_payload_sha256=projection.verification_receipt.payload_sha256,
            registration_receipt=projection.registration_receipt,
            verification_receipt=projection.verification_receipt,
            input_custody=custody_receipt,
            stage_identity=projection.stage,
            object_identity=projection.object_identity,
            typed_sum=projection.typed_sum,
        )
        receipt = NativeChunkReceipt(
            ordinal=projection.attempt.ordinal,
            attempt_id=attempt_id,
            stage_id=canonical_stage_id(projection.object_identity),
            rows=projection.rows,
            encoded_bytes=projection.encoded_bytes,
            file_sha256=projection.file_sha256,
            typed_digest=projection.typed_digest,
            consumed_part_evidence=evidence.to_mapping(),
        )
        validate_native_chunk_receipt(receipt, projection=projection)
        return receipt
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def validate_native_chunk_receipt(
    receipt: NativeChunkReceipt, *, projection: SqlClientNativeChunkProjection | None = None
) -> SqlClientNativeChunkEvidence:
    """Decode and validate the same canonical bytes at every v4 boundary."""
    try:
        if type(receipt) is not NativeChunkReceipt:
            raise ValueError
        evidence = SqlClientNativeChunkEvidence.from_mapping(receipt.consumed_part_evidence)
        custody = evidence.input_custody
        if (
            receipt.ordinal != custody.ordinal
            or receipt.attempt_id != custody.attempt_id
            or (receipt.rows, receipt.encoded_bytes, receipt.file_sha256, receipt.typed_digest)
            != (custody.rows, custody.encoded_bytes, custody.file_sha256, custody.typed_digest)
        ):
            raise ValueError
        if projection is not None and (
            evidence.projection_sha256 != projection.projection_sha256
            or evidence.plan_sha256 != projection.attempt.plan_sha256
            or evidence.typed_sum != projection.typed_sum
            or evidence.registration_receipt != projection.registration_receipt
            or evidence.verification_receipt != projection.verification_receipt
            or evidence.stage_identity != projection.stage
            or evidence.object_identity != projection.object_identity
            or evidence.registration_receipt.attempt_sha256 != projection.attempt_sha256
            or custody.target_id != projection.attempt.target_key
            or custody.run_id != projection.attempt.run_id
            or receipt.stage_id != canonical_stage_id(projection.object_identity)
            or receipt.ordinal != projection.attempt.ordinal
            or receipt.attempt_id
            != f"{projection.attempt.run_id}-{projection.attempt.ordinal}-{projection.attempt.attempt}"
            or (receipt.rows, receipt.encoded_bytes, receipt.file_sha256, receipt.typed_digest)
            != (projection.rows, projection.encoded_bytes, projection.file_sha256, projection.typed_digest)
        ):
            raise ValueError
        return evidence
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def sqlclient_physical_stage(receipt: NativeChunkReceipt) -> SqlClientStageIdentity:
    """Return the exact validated physical stage without interpreting stage_id as SQL."""
    return validate_native_chunk_receipt(receipt).stage_identity


IMPORT_ERROR = "mssql_native.sqlclient_native_import_unknown"
ELIGIBILITY_SCHEMA = "dpone.sqlclient.failed-eligibility.v1"
SETTLEMENT_SCHEMA = "dpone.sqlclient.failed-settlement.v1"


def _import_digest(value: object) -> None:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(IMPORT_ERROR)


def _import_text(value: object) -> None:
    if type(value) is not str or not value:
        raise ValueError(IMPORT_ERROR)


def _import_canonical(value: dict) -> bytes:
    return dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def plan_sha256(plan: object) -> str:
    value = getattr(plan, "to_dict", None)
    if not callable(value) or type(document := value()) is not dict:
        raise ValueError(IMPORT_ERROR)
    return sha256(_import_canonical(document)).hexdigest()


def _import_binding(schema: str, body: dict) -> str:
    return sha256(schema.encode() + b"\0" + _import_canonical(body)).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFailedEligibility:
    schema: str
    plan_sha256: str
    target_id: str
    run_id: str
    window_fingerprint: str
    attempt_id: str
    lease_owner: str
    lease_fence: int
    lifecycle_phase: str
    publication_eligible: bool
    observation_sha256: str
    eligibility_sha256: str

    def __post_init__(self) -> None:
        for value in (self.target_id, self.run_id, self.window_fingerprint, self.attempt_id, self.lease_owner):
            _import_text(value)
        valid_state = self.schema == ELIGIBILITY_SCHEMA and self.lifecycle_phase == "failed"
        if not valid_state or type(self.lease_fence) is not int or self.lease_fence < 1:
            raise ValueError(IMPORT_ERROR)
        if self.publication_eligible is not False:
            raise ValueError(IMPORT_ERROR)
        for value in (self.plan_sha256, self.observation_sha256, self.eligibility_sha256):
            _import_digest(value)
        body = asdict(self)
        body.pop("eligibility_sha256")
        if self.eligibility_sha256 != _import_binding(ELIGIBILITY_SCHEMA, body):
            raise ValueError(IMPORT_ERROR)

    @classmethod
    def bind(cls, **facts: object) -> SqlClientFailedEligibility:
        body = {"schema": ELIGIBILITY_SCHEMA, **facts}
        return cls(**body, eligibility_sha256=_import_binding(ELIGIBILITY_SCHEMA, body))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFailedAttemptSettlement:
    schema: str
    eligibility_sha256: str
    operation_key: str
    durable_receipt_sha256: str
    settled: bool
    settlement_sha256: str

    def __post_init__(self) -> None:
        if self.schema != SETTLEMENT_SCHEMA or self.settled is not True:
            raise ValueError(IMPORT_ERROR)
        digests = self.eligibility_sha256, self.operation_key, self.durable_receipt_sha256, self.settlement_sha256
        for value in digests:
            _import_digest(value)
        body = asdict(self)
        body.pop("settlement_sha256")
        if self.settlement_sha256 != _import_binding(SETTLEMENT_SCHEMA, body):
            raise ValueError(IMPORT_ERROR)

    @classmethod
    def bind(cls, **facts: object) -> SqlClientFailedAttemptSettlement:
        body = {"schema": SETTLEMENT_SCHEMA, **facts}
        return cls(**body, settlement_sha256=_import_binding(SETTLEMENT_SCHEMA, body))  # type: ignore[arg-type]


def settlement_operation_key(eligibility: SqlClientFailedEligibility) -> str:
    eligibility.__post_init__()
    return sha256(
        b"dpone.sqlclient.failed-settlement-operation.v1\0" + eligibility.eligibility_sha256.encode()
    ).hexdigest()


SqlClientInputCustody = SqlClientInputCustodyReceipt


__all__ = (
    "settlement_operation_key",
    "plan_sha256",
    "SqlClientFailedEligibility",
    "SqlClientFailedAttemptSettlement",
    "SCHEMA",
    "SqlClientInputCustodyReceipt",
    "SqlClientInputCustody",
    "SqlClientNativeChunkEvidence",
    "bind_native_chunk_receipt",
    "canonical_stage_id",
    "sqlclient_physical_stage",
    "validate_native_chunk_receipt",
)
