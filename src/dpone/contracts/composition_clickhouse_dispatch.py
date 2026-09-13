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
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
)
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


@dataclass(frozen=True, slots=True)
class ClickHouseDispatchObservation:
    """Complete transport observation; root persists it before worker ACK.

    request_body_bytes counts payload octets actually accepted by socket.send,
    excluding HTTP headers, URI SQL, TLS records and TCP framing. It is not
    source-export bytes. No failed/partial observation is a terminal receipt.
    """

    dispatch_sha256: str
    claim_key: str
    query_id: str
    request_body_bytes: int
    response_body_bytes: int
    response_body_sha256: str
    response_framing: str


def _journal_require(value: bool, reason: str) -> None:
    if not value:
        raise CompositionAdmissionError("clickhouse_journal_" + reason)


@dataclass(frozen=True, slots=True)
class DispatchBinding:
    """Root-pinned single issued user and exact attempt/target, never credentials."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    gate_id: str

    def __post_init__(self) -> None:
        _journal_require(
            type(self.attempt) is CompositionAttemptIdentity and type(self.target) is SnapshotTarget, "binding"
        )
        self.attempt.__post_init__()
        self.target.__post_init__()
        try:
            valid = str(UUID(self.gate_id)) == self.gate_id and UUID(self.gate_id).int != 0
        except (ValueError, TypeError, AttributeError):
            valid = False
        _journal_require(valid and self.target.guard_id in dict(self.attempt.guard_epochs), "binding")

    @property
    def principal_id(self) -> str:
        return "clickhouse-user:" + self.gate_id

    def require_dispatch(self, dispatch: ClickHouseDispatch) -> None:
        _journal_require(
            type(dispatch) in {CreateGenerationDispatch, InsertGenerationDispatch, ExchangeSnapshotDispatch},
            "dispatch_shape",
        )
        dispatch.__post_init__()
        _journal_require((dispatch.attempt, dispatch.target) == (self.attempt, self.target), "dispatch_scope")
        if isinstance(dispatch, ExchangeSnapshotDispatch):
            _journal_require(dispatch.intent.publisher_principal.principal_id == self.principal_id, "publisher")

    def closing_document(self) -> bytes:
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-clickhouse-dispatch-closure.v1",
                "phase": "CLOSING",
                **asdict(self),
            }
        )

    def closed_document(
        self, links: tuple[tuple[str, str], ...], proofs: tuple[CompositionAttemptProof, CompositionAttemptProof]
    ) -> bytes:
        _journal_require(type(proofs) is tuple and len(proofs) == 2, "closure_proof_shape")
        issued = (CompositionProofAuthority("clickhouse", self.target.service_id, self.principal_id),)
        for proof, kind in zip(proofs, ("CLOSED_GATES", "QUIESCENCE"), strict=True):
            _journal_require(type(proof) is CompositionAttemptProof, "closure_proof_shape")
            proof.require_attempt(self.attempt)
            _journal_require(proof.kind == kind and proof.authorities == issued, "closure_proof_subject")
        body = strict_json_object(self.closing_document())
        body.update(phase="CLOSED", dispatch_terminals=links, proofs=tuple(proof.to_dict() for proof in proofs))
        document = canonical_json_bytes(body)
        _journal_require(len(document) <= 8388608, "closure_budget")
        return document


def terminal_document(dispatch: ClickHouseDispatch, observation: ClickHouseDispatchObservation) -> bytes:
    """Accept only this dispatch's complete, empty, synchronously framed response."""
    _journal_require(type(observation) is ClickHouseDispatchObservation, "terminal_shape")
    expected = dispatch.payload_bytes if isinstance(dispatch, InsertGenerationDispatch) else 0
    _journal_require(
        (observation.dispatch_sha256, observation.claim_key, observation.query_id)
        == (dispatch.dispatch_sha256, dispatch.claim_key, dispatch.query_id)
        and type(observation.request_body_bytes) is int
        and observation.request_body_bytes == expected
        and type(observation.response_body_bytes) is int
        and observation.response_body_bytes == 0
        and observation.response_body_sha256 == _hash(b"")
        and observation.response_framing in {"chunked", "content-length"},
        "terminal_observation",
    )
    return canonical_json_bytes({"schema": "dpone.composition-clickhouse-dispatch-terminal.v1", **asdict(observation)})


def require_terminal_document(dispatch: ClickHouseDispatch, digest: str, document: bytes) -> None:
    try:
        _journal_require(type(document) is bytes and 0 < len(document) <= 65536, "terminal_budget")
        _journal_require(_hash(document) == digest, "terminal_hash")
        body = strict_json_object(document)
        _journal_require(body.pop("schema") == "dpone.composition-clickhouse-dispatch-terminal.v1", "terminal_schema")
        observation = ClickHouseDispatchObservation(**body)
        _journal_require(terminal_document(dispatch, observation) == document, "terminal_original")
    except (TypeError, ValueError, KeyError, AttributeError):
        raise CompositionAdmissionError("clickhouse_journal_terminal_original") from None


@dataclass(frozen=True, slots=True)
class ClickHouseDispatchStatus:
    """Retained claim and optional complete transport original, never a permit.

    A terminal proves only the journaled transport observation. It does not
    establish generation contents, publication, or business success. Absence is
    represented by the reader returning None, which never authorizes dispatch.
    No new wire schema is introduced; these are the existing exact originals.
    """

    binding: DispatchBinding
    dispatch_sha256: str
    dispatch_document: bytes
    terminal: tuple[str, bytes] | None

    def __post_init__(self) -> None:
        _journal_require(type(self.binding) is DispatchBinding, "binding")
        self.binding.__post_init__()
        dispatch = decode_clickhouse_dispatch(self.dispatch_document, self.dispatch_sha256)
        self.binding.require_dispatch(dispatch)
        if self.terminal is not None:
            _journal_require(type(self.terminal) is tuple and len(self.terminal) == 2, "terminal_original")
            require_terminal_document(dispatch, *self.terminal)
