"""Exact authority and receipt for contained SqlClient failed-attempt retirement."""

from __future__ import annotations

import base64
from dataclasses import InitVar, asdict, dataclass
from datetime import datetime
from enum import Enum, StrEnum
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptIdentity, TdsObjectIdentity
from dpone.contracts.strict_json import canonical_json_bytes

_ERROR = "mssql_native.sqlclient_failed_settlement_invalid"
_ISSUER = object()


def _digest(value: object) -> None:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(_ERROR)


def _text(value: object) -> None:
    if type(value) is not str or not value or any(ord(char) < 32 for char in value):
        raise ValueError(_ERROR)


def _canonical(value: object) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Enum):
        return value.value
    return value


def _binding(schema: str, value: Any, *, omit: str) -> str:
    body = asdict(value)
    body.pop(omit)
    return sha256(schema.encode() + b"\0" + canonical_json_bytes(_canonical(body))).hexdigest()


class SqlClientFailedAttemptDisposition(StrEnum):
    """Closed operator outcome; only retry-ready admits a successor."""

    RETRY_PENDING_SETTLEMENT = "retry_pending_settlement"
    RETRY_READY = "retry_ready"
    MANUAL_RECONCILIATION_REQUIRED = "manual_reconciliation_required"
    TERMINAL_INPUT_OR_POLICY = "terminal_input_or_policy"


@dataclass(frozen=True, slots=True)
class SqlClientFailedRetirementSubject:
    """Exact contained predecessor bound to current durable observations."""

    schema: str
    attempt: TdsAttemptIdentity
    stage: SqlClientStageIdentity | None
    object_identity: TdsObjectIdentity | None
    expected_object_nonce: UUID
    input_custody_sha256: str
    error: TdsAttemptError
    observation_sha256: str
    lifecycle_revision: int
    lifecycle_state_sha256: str
    directory_key: str
    directory_revision: int
    directory_state_sha256: str
    lease_owner: str
    lease_fence: int
    subject_sha256: str

    def __post_init__(self) -> None:
        if self.schema != "dpone.sqlclient.failed-retirement-subject.v2":
            raise ValueError(_ERROR)
        if (
            type(self.attempt) is not TdsAttemptIdentity
            or type(self.error) is not TdsAttemptError
            or type(self.expected_object_nonce) is not UUID
            or not self.expected_object_nonce.int
        ):
            raise ValueError(_ERROR)
        if (self.stage is None) != (self.object_identity is None):
            raise ValueError(_ERROR)
        if self.stage is not None and (
            type(self.stage) is not SqlClientStageIdentity
            or type(self.object_identity) is not TdsObjectIdentity
            or self.stage.object_id != self.object_identity.object_id
            or self.stage.object_nonce != self.expected_object_nonce
            or (self.stage.database_name, self.stage.schema_name, self.stage.table_name)
            != (self.attempt.database, self.attempt.schema, self.attempt.table)
        ):
            raise ValueError(_ERROR)
        if any(
            type(value) is not int or value < 1
            for value in (self.lifecycle_revision, self.directory_revision, self.lease_fence)
        ):
            raise ValueError(_ERROR)
        _text(self.directory_key)
        _text(self.lease_owner)
        for value in (
            self.input_custody_sha256,
            self.observation_sha256,
            self.lifecycle_state_sha256,
            self.directory_state_sha256,
            self.subject_sha256,
        ):
            _digest(value)
        if self.subject_sha256 != _binding(self.schema, self, omit="subject_sha256"):
            raise ValueError(_ERROR)

    @classmethod
    def bind(cls, **facts: object) -> SqlClientFailedRetirementSubject:
        schema = "dpone.sqlclient.failed-retirement-subject.v2"
        provisional = object.__new__(cls)
        for name, value in {"schema": schema, **facts}.items():
            object.__setattr__(provisional, name, value)
        object.__setattr__(provisional, "subject_sha256", "0" * 64)
        return cls(
            **facts,  # type: ignore[arg-type]
            schema=schema,
            subject_sha256=_binding(schema, provisional, omit="subject_sha256"),
        )


@dataclass(frozen=True, slots=True)
class SqlClientFailedRetirementAuthorization:
    """Observer-issued authority for one exact contained subject."""

    subject_sha256: str
    authority_sha256: str
    _issuer: InitVar[object] = None

    def __post_init__(self, _issuer: object) -> None:
        if _issuer is not _ISSUER:
            raise ValueError(_ERROR)
        _digest(self.subject_sha256)
        _digest(self.authority_sha256)
        expected = sha256(
            b"dpone.sqlclient.failed-retirement-authorization.v1\0" + self.subject_sha256.encode()
        ).hexdigest()
        if self.authority_sha256 != expected:
            raise ValueError(_ERROR)


def issue_failed_retirement_authorization(
    subject: SqlClientFailedRetirementSubject,
) -> SqlClientFailedRetirementAuthorization:
    """Issue authority only from the production observer after exact validation."""
    if type(subject) is not SqlClientFailedRetirementSubject:
        raise ValueError(_ERROR)
    digest = sha256(
        b"dpone.sqlclient.failed-retirement-authorization.v1\0" + subject.subject_sha256.encode()
    ).hexdigest()
    return SqlClientFailedRetirementAuthorization(subject.subject_sha256, digest, _ISSUER)


@dataclass(frozen=True, slots=True)
class SqlClientFailedRetirementRequest:
    """One exact subject and its observer-issued authority."""

    subject: SqlClientFailedRetirementSubject
    authorization: SqlClientFailedRetirementAuthorization

    def __post_init__(self) -> None:
        if (
            type(self.subject) is not SqlClientFailedRetirementSubject
            or type(self.authorization) is not SqlClientFailedRetirementAuthorization
            or self.authorization.subject_sha256 != self.subject.subject_sha256
        ):
            raise ValueError(_ERROR)

    @property
    def request_sha256(self) -> str:
        return sha256(
            b"dpone.sqlclient.failed-retirement-request.v1\0"
            + self.subject.subject_sha256.encode()
            + self.authorization.authority_sha256.encode()
        ).hexdigest()


def encode_failed_retirement_request(request: SqlClientFailedRetirementRequest) -> bytes:
    """Return canonical credential-free bytes for crash recovery."""
    if type(request) is not SqlClientFailedRetirementRequest:
        raise ValueError(_ERROR)
    subject = request.subject
    body = asdict(subject)
    body["attempt"] = asdict(subject.attempt)
    body["stage"] = None if subject.stage is None else base64.b64encode(encode_stage_identity(subject.stage)).decode()
    body["object_identity"] = None if subject.object_identity is None else asdict(subject.object_identity)
    body["expected_object_nonce"] = str(subject.expected_object_nonce)
    body["error"] = subject.error.value
    return canonical_json_bytes({"subject": body, "authority_sha256": request.authorization.authority_sha256})


def decode_failed_retirement_request(payload: bytes) -> SqlClientFailedRetirementRequest:
    """Strictly reconstruct only an originally self-binding v2 request."""
    import json

    try:
        root = json.loads(payload)
        if type(root) is not dict or set(root) != {"subject", "authority_sha256"} or type(root["subject"]) is not dict:
            raise ValueError
        body = dict(root["subject"])
        body["attempt"] = TdsAttemptIdentity(**body["attempt"])
        stage = body["stage"]
        body["stage"] = None if stage is None else decode_stage_identity(base64.b64decode(stage, validate=True))
        obj = body["object_identity"]
        body["object_identity"] = None if obj is None else TdsObjectIdentity(**obj)
        body["expected_object_nonce"] = UUID(body["expected_object_nonce"])
        body["error"] = TdsAttemptError(body["error"])
        subject = SqlClientFailedRetirementSubject(**body)
        request = SqlClientFailedRetirementRequest(subject, issue_failed_retirement_authorization(subject))
        if (
            request.authorization.authority_sha256 != root["authority_sha256"]
            or encode_failed_retirement_request(request) != payload
        ):
            raise ValueError
        return request
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True)
class SqlClientFailedAttemptSettlementReceipt:
    """Terminal durable proof that a successor may be admitted."""

    schema: str
    request_sha256: str
    retirement_receipt_sha256: str
    operation_key: str
    disposition: SqlClientFailedAttemptDisposition
    receipt_sha256: str

    def __post_init__(self) -> None:
        if (
            self.schema != "dpone.sqlclient.failed-attempt-settlement.v2"
            or self.disposition is not SqlClientFailedAttemptDisposition.RETRY_READY
        ):
            raise ValueError(_ERROR)
        for value in (self.request_sha256, self.retirement_receipt_sha256, self.operation_key, self.receipt_sha256):
            _digest(value)
        if self.receipt_sha256 != _binding(self.schema, self, omit="receipt_sha256"):
            raise ValueError(_ERROR)

    @classmethod
    def bind(cls, **facts: object) -> SqlClientFailedAttemptSettlementReceipt:
        schema = "dpone.sqlclient.failed-attempt-settlement.v2"
        provisional = object.__new__(cls)
        for name, value in {"schema": schema, **facts}.items():
            object.__setattr__(provisional, name, value)
        object.__setattr__(provisional, "receipt_sha256", "0" * 64)
        return cls(
            **facts,  # type: ignore[arg-type]
            schema=schema,
            receipt_sha256=_binding(schema, provisional, omit="receipt_sha256"),
        )


@dataclass(frozen=True, slots=True)
class SqlClientFailedRetirementReceipt:
    """Exact P10g terminal for a contained failed attempt."""

    schema: str
    request_sha256: str
    object_incarnation_sha256: str
    drop_settlement_sha256: str
    absence_sha256: str
    terminal_sha256: str
    directory_sha256: str
    capacity_sha256: str
    receipt_sha256: str

    def __post_init__(self) -> None:
        if self.schema != "dpone.sqlclient.failed-retirement-receipt.v1":
            raise ValueError(_ERROR)
        for value in (
            self.request_sha256,
            self.object_incarnation_sha256,
            self.drop_settlement_sha256,
            self.absence_sha256,
            self.terminal_sha256,
            self.directory_sha256,
            self.capacity_sha256,
            self.receipt_sha256,
        ):
            _digest(value)
        if self.receipt_sha256 != _binding(self.schema, self, omit="receipt_sha256"):
            raise ValueError(_ERROR)

    @classmethod
    def bind(cls, **facts: object) -> SqlClientFailedRetirementReceipt:
        schema = "dpone.sqlclient.failed-retirement-receipt.v1"
        provisional = object.__new__(cls)
        for name, value in {"schema": schema, **facts}.items():
            object.__setattr__(provisional, name, value)
        object.__setattr__(provisional, "receipt_sha256", "0" * 64)
        return cls(
            **facts,  # type: ignore[arg-type]
            schema=schema,
            receipt_sha256=_binding(schema, provisional, omit="receipt_sha256"),
        )


__all__ = (
    "SqlClientFailedAttemptDisposition",
    "SqlClientFailedAttemptSettlementReceipt",
    "SqlClientFailedRetirementAuthorization",
    "SqlClientFailedRetirementRequest",
    "SqlClientFailedRetirementReceipt",
    "SqlClientFailedRetirementSubject",
    "issue_failed_retirement_authorization",
    "encode_failed_retirement_request",
    "decode_failed_retirement_request",
)
