"""Closed ClickHouse requests whose identities confer no dispatch authority.

A protected gateway must claim the stable claim_key once before network I/O.
Changed payload/schema/generation bytes cannot reset that attempt/slot/ordinal
claim. Query IDs provide attribution, never deduplication or permission.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, ClassVar, TypeAlias, cast
from uuid import UUID

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import SnapshotPublicationIntent, SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_SCHEMA = "dpone.composition-clickhouse-dispatch.v1"
_MAX_DOCUMENT = 1024 * 1024
MAX_DISPATCH_PAYLOAD_BYTES = 1024 * 1024 * 1024


def _require(valid: bool, code: str) -> None:
    if not valid:
        raise CompositionAdmissionError("clickhouse_dispatch_" + code)


def _hash(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


def _scalar_type(value: str) -> bool:
    if value.startswith("Nullable(") and value.endswith(")"):
        value = value[9:-1]
    if re.fullmatch(r"(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|String|UUID|Date|Date32|DateTime)", value):
        return True
    if value == "DateTime('UTC')" or re.fullmatch(r"DateTime64\([0-9](?:, ?'UTC')?\)", value):
        return True
    decimal = re.fullmatch(r"Decimal\(([1-9][0-9]?), ?(0|[1-9][0-9]?)\)", value)
    if decimal:
        precision, scale = map(int, decimal.groups())
        return 1 <= precision <= 76 and scale <= precision
    fixed = re.fullmatch(r"FixedString\(([1-9][0-9]{0,6})\)", value)
    return fixed is not None and int(fixed[1]) <= 1048576


@dataclass(frozen=True, slots=True)
class ClickHouseDispatchColumn:
    """An exact Native field with closed scalar type grammar, no SQL clauses."""

    name: str
    type_name: str

    def __post_init__(self) -> None:
        _require(
            type(self.name) is str and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", self.name) is not None, "column"
        )
        _require(type(self.type_name) is str and len(self.type_name) <= 96 and _scalar_type(self.type_name), "type")


def _generation(value: CreateGenerationDispatch | InsertGenerationDispatch) -> None:
    _require(type(value.attempt) is CompositionAttemptIdentity and type(value.target) is SnapshotTarget, "subject")
    value.attempt.__post_init__()
    value.target.__post_init__()
    _require(value.target.guard_id in dict(value.attempt.guard_epochs), "guard")
    try:
        valid_uuid = str(UUID(value.generation_uuid)) == value.generation_uuid and UUID(value.generation_uuid).int != 0
    except (ValueError, TypeError, AttributeError):
        valid_uuid = False
    _require(valid_uuid, "generation_uuid")
    _require(type(value.columns) is tuple and 1 <= len(value.columns) <= 4096, "columns")
    for column in value.columns:
        _require(type(column) is ClickHouseDispatchColumn, "column_shape")
        column.__post_init__()
    _require(len({column.name for column in value.columns}) == len(value.columns), "duplicate_column")


class _Dispatch:
    operation: ClassVar[str]

    def to_bytes(self) -> bytes:
        self.__post_init__()
        document = canonical_json_bytes({"schema": _SCHEMA, "operation": self.operation, **asdict(cast(Any, self))})
        _require(len(document) <= _MAX_DOCUMENT, "document_budget")
        return document

    @property
    def dispatch_sha256(self) -> str:
        return _hash(self.to_bytes())

    @property
    def claim_key(self) -> str:
        """One immutable attempt/operation/physical slot/ordinal; never a retry key."""
        self.__post_init__()
        value = cast(ClickHouseDispatch, self)
        return _hash(
            canonical_json_bytes(
                {
                    "schema": "dpone.composition-clickhouse-dispatch-claim.v1",
                    "attempt_sha256": value.attempt.attempt_sha256,
                    "guard_id": value.target.guard_id,
                    "write_subject_sha256": value.target.write_subject_sha256,
                    "operation": self.operation,
                    "ordinal": value.ordinal,
                }
            )
        )

    @property
    def query_id(self) -> str:
        return "dpone-ch-" + self.claim_key[7:]

    def __post_init__(self) -> None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class CreateGenerationDispatch(_Dispatch):
    """Create one explicit UUID and schema with fixed MergeTree physical design."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    generation_uuid: str
    columns: tuple[ClickHouseDispatchColumn, ...]
    operation: ClassVar[str] = "CREATE_GENERATION"
    ordinal: ClassVar[int] = 0

    def __post_init__(self) -> None:
        _generation(self)


@dataclass(frozen=True, slots=True)
class InsertGenerationDispatch(_Dispatch):
    """Insert one exact immutable Native body; ordinal cannot be refingerprinted."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    generation_uuid: str
    columns: tuple[ClickHouseDispatchColumn, ...]
    payload_sha256: str
    payload_bytes: int
    ordinal: int
    operation: ClassVar[str] = "INSERT_GENERATION"

    def __post_init__(self) -> None:
        _generation(self)
        require_digest(self.payload_sha256)
        _require(
            type(self.payload_bytes) is int and 1 <= self.payload_bytes <= MAX_DISPATCH_PAYLOAD_BYTES, "payload_budget"
        )
        _require(type(self.ordinal) is int and 0 <= self.ordinal < 2**63, "ordinal")


@dataclass(frozen=True, slots=True)
class ExchangeSnapshotDispatch(_Dispatch):
    """Exchange only the exact independently prepared snapshot intent pair."""

    intent: SnapshotPublicationIntent
    operation: ClassVar[str] = "EXCHANGE_SNAPSHOT"
    ordinal: ClassVar[int] = 0

    def __post_init__(self) -> None:
        _require(type(self.intent) is SnapshotPublicationIntent, "intent")
        self.intent.__post_init__()

    @property
    def attempt(self) -> CompositionAttemptIdentity:
        return self.intent.attempt

    @property
    def target(self) -> SnapshotTarget:
        return self.intent.target

    @property
    def query_id(self) -> str:
        return self.intent.exchange_query_id


ClickHouseDispatch: TypeAlias = CreateGenerationDispatch | InsertGenerationDispatch | ExchangeSnapshotDispatch


def decode_clickhouse_dispatch(document: bytes, expected_sha256: str) -> ClickHouseDispatch:
    """Reopen bounded canonical originals; decoding cannot issue a dispatch claim."""
    try:
        require_digest(expected_sha256)
        _require(type(document) is bytes and 0 < len(document) <= _MAX_DOCUMENT, "document_budget")
        _require(_hash(document) == expected_sha256, "document_hash")
        body = strict_json_object(document)
        _require(body.pop("schema") == _SCHEMA, "schema")
        operation = body.pop("operation")
        if operation == "EXCHANGE_SNAPSHOT":
            original = canonical_json_bytes(
                {"schema": "dpone.composition-clickhouse-snapshot-intent.v1", **body["intent"]}
            )
            body["intent"] = SnapshotPublicationIntent.from_bytes(original, _hash(original))
            result: ClickHouseDispatch = ExchangeSnapshotDispatch(**body)
        else:
            attempt = body["attempt"]
            _require(
                type(attempt["guard_epochs"]) is list and all(type(pair) is list for pair in attempt["guard_epochs"]),
                "epochs",
            )
            attempt["guard_epochs"] = tuple(tuple(pair) for pair in attempt["guard_epochs"])
            body["attempt"] = CompositionAttemptIdentity(**attempt)
            body["target"] = SnapshotTarget(**body["target"])
            _require(type(body["columns"]) is list, "columns")
            body["columns"] = tuple(ClickHouseDispatchColumn(**value) for value in body["columns"])
            if operation == "CREATE_GENERATION":
                result = CreateGenerationDispatch(**body)
            elif operation == "INSERT_GENERATION":
                result = InsertGenerationDispatch(**body)
            else:
                raise CompositionAdmissionError("clickhouse_dispatch_operation")
        _require(result.to_bytes() == document, "canonical_document")
        return result
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("clickhouse_dispatch_document") from None
