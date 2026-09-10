"""Exact whole-snapshot subjects and state rules; these records grant no authority.

Protected producers must reopen source, enrollment and issued-principal evidence.
Hashes identify canonical UTF-8 bytes, never prove closure or permission.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.composition_activation import CompositionAdmissionError, require_digest, require_text
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_physical import CompositionPhysicalDomain
from dpone.contracts.composition_proof import CompositionProofAuthority
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_INTENT_SCHEMA = "dpone.composition-clickhouse-snapshot-intent.v1"
_RECORD_SCHEMA = "dpone.composition-clickhouse-snapshot-record.v1"
_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
_TERMINAL = frozenset({"PUBLISHED", "NOT_PUBLISHED"})


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("snapshot_" + reason)


def _uuid(value: str) -> None:
    try:
        valid = str(UUID(value)) == value and UUID(value).int != 0
    except (ValueError, TypeError, AttributeError):
        valid = False
    _require(valid, "uuid")


def _integer(value: object, *, minimum: int = 0) -> None:
    _require(type(value) is int and minimum <= value <= 2**63 - 1, "counter")


def _document(value: object) -> bytes:
    encoded = canonical_json_bytes(value)
    _require(len(encoded) <= _MAX_DOCUMENT_BYTES, "document_budget")
    return encoded


def _digest(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


@dataclass(frozen=True, slots=True)
class SnapshotTarget:
    """Protected logical slot and private generation in one enrolled database."""

    service_id: str
    physical_subject_sha256: str
    database_id: str
    database: str
    target_table: str
    generation_table: str
    write_subject_sha256: str
    enrollment_sha256: str

    def __post_init__(self) -> None:
        for value in (self.service_id, self.database_id):
            _uuid(value)
        for value in (self.physical_subject_sha256, self.write_subject_sha256, self.enrollment_sha256):
            require_digest(value)
        for value in (self.database, self.target_table, self.generation_table):
            require_text(value, maximum=128)
            _require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", value) is not None, "name")
        _require("." not in self.database and self.target_table != self.generation_table, "names")

    @property
    def guard_id(self) -> str:
        return CompositionPhysicalDomain("clickhouse", self.service_id, self.physical_subject_sha256).guard_id


@dataclass(frozen=True, slots=True)
class SnapshotLimits:
    """Externally verified effective ceilings; runtime producers enforce counters."""

    max_rows: int
    max_source_bytes: int
    max_wire_bytes: int
    max_generation_bytes: int
    max_retained_bytes: int
    max_total_transient_bytes: int

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _integer(value, minimum=1)

    def require_storage(self, generation: int, old_target: int, retained: int) -> None:
        for value in (generation, old_target, retained):
            _integer(value)
        _require(
            generation <= self.max_generation_bytes
            and old_target + retained <= self.max_retained_bytes
            and generation + old_target + retained <= self.max_total_transient_bytes,
            "storage_budget",
        )


@dataclass(frozen=True, slots=True)
class SnapshotGeneration:
    """Complete independently reconciled B, including an explicit empty snapshot."""

    record_sha256: str
    source_snapshot_sha256: str
    content_sha256: str
    schema_sha256: str
    physical_sha256: str
    old_target_uuid: str
    new_generation_uuid: str
    rows: int
    source_bytes: int
    wire_bytes: int
    generation_bytes: int
    old_target_bytes: int
    retained_bytes: int

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if name.endswith("sha256"):
                require_digest(value)
            elif name.endswith("uuid"):
                _uuid(value)
            else:
                _integer(value)
        _require(self.old_target_uuid != self.new_generation_uuid, "uuid_pair")


@dataclass(frozen=True, slots=True)
class SnapshotPublicationIntent:
    """Immutable publication intent; full attempt bytes avoid legacy hash aliases."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    generation: SnapshotGeneration
    limits: SnapshotLimits
    ingest_principal: CompositionProofAuthority
    publisher_principal: CompositionProofAuthority
    closed_ingest_sha256: str

    def __post_init__(self) -> None:
        for value, kind in (
            (self.attempt, CompositionAttemptIdentity),
            (self.target, SnapshotTarget),
            (self.generation, SnapshotGeneration),
            (self.limits, SnapshotLimits),
            (self.ingest_principal, CompositionProofAuthority),
            (self.publisher_principal, CompositionProofAuthority),
        ):
            _require(type(value) is kind, "intent_shape")
            value.__post_init__()
        require_digest(self.closed_ingest_sha256)
        _require(self.target.guard_id in dict(self.attempt.guard_epochs), "guard")
        for principal in (self.ingest_principal, self.publisher_principal):
            _require((principal.connector, principal.service_id) == ("clickhouse", self.target.service_id), "principal")
            _uuid(principal.principal_id.removeprefix("clickhouse-user:"))
        _require(self.ingest_principal != self.publisher_principal, "principal_reuse")
        value, limits = self.generation, self.limits
        _require(
            value.rows <= limits.max_rows
            and value.source_bytes <= limits.max_source_bytes
            and value.wire_bytes <= limits.max_wire_bytes,
            "generation_budget",
        )
        limits.require_storage(value.generation_bytes, value.old_target_bytes, value.retained_bytes)

    def to_bytes(self) -> bytes:
        self.__post_init__()
        return _document({"schema": _INTENT_SCHEMA, **asdict(self)})

    @property
    def intent_sha256(self) -> str:
        return _digest(self.to_bytes())

    @property
    def exchange_query_id(self) -> str:
        """One deterministic, journaled attribution ID; not deduplication or auth."""
        return "dpone-snapshot-" + self.intent_sha256.removeprefix("sha256:")

    @classmethod
    def from_bytes(cls, document: bytes, expected_sha256: str) -> SnapshotPublicationIntent:
        try:
            value = _intent_from_dict(_read(document, expected_sha256, _INTENT_SCHEMA))
            _require(value.to_bytes() == document, "canonical_document")
            return value
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
            raise CompositionAdmissionError("snapshot_intent_document") from None


@dataclass(frozen=True, slots=True)
class SnapshotPublisherClosure:
    """Subject returned only after actual publisher closure and quiescence."""

    intent_sha256: str
    closed_gates_sha256: str
    quiescence_sha256: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            require_digest(value)


@dataclass(frozen=True, slots=True)
class SnapshotCatalogObservation:
    """Fresh complete protected catalog/content projection, never caller authority.

    Pair/schema/design tuples are ordered target-name then generation-name.
    Content and byte observations follow UUID B/A, independent of current names.
    Missing rows or unknown bytes remain None. Unsupported findings include all
    dependency, policy, writer, mutation, TTL, projection and design exclusions.
    The adapter must verify complete catalog visibility; an empty finding tuple
    alone cannot establish that it did so.
    """

    target: SnapshotTarget
    target_uuid: str | None
    generation_uuid: str | None
    database_engine: str
    table_engines: tuple[str | None, str | None]
    node_count: int
    replica_count: int
    schema_sha256: tuple[str | None, str | None]
    physical_sha256: tuple[str | None, str | None]
    unsupported_features: tuple[str, ...]
    generation_content_sha256: str | None
    generation_rows: int | None
    generation_bytes: int | None
    old_target_bytes: int | None
    retained_bytes: int | None
    catalog_evidence_sha256: str

    def __post_init__(self) -> None:
        _require(type(self.target) is SnapshotTarget, "observation_target")
        self.target.__post_init__()
        require_text(self.database_engine)
        require_digest(self.catalog_evidence_sha256)
        for value in (self.target_uuid, self.generation_uuid):
            if value is not None:
                _uuid(value)
        for values in (self.table_engines, self.schema_sha256, self.physical_sha256):
            _require(type(values) is tuple and len(values) == 2, "observation_pair")
            for value in values:
                if value is not None:
                    require_text(value)
        for value in (*self.schema_sha256, *self.physical_sha256, self.generation_content_sha256):
            if value is not None:
                require_digest(value)
        _require(type(self.unsupported_features) is tuple and len(self.unsupported_features) <= 128, "findings")
        for value in self.unsupported_features:
            require_text(value)
        _require(tuple(sorted(set(self.unsupported_features))) == self.unsupported_features, "findings")
        for counter in (self.node_count, self.replica_count):
            _integer(counter)
        for measured in (self.generation_rows, self.generation_bytes, self.old_target_bytes, self.retained_bytes):
            if measured is not None:
                _integer(measured)


def classify_snapshot(intent: SnapshotPublicationIntent, observation: SnapshotCatalogObservation) -> str:
    """Classify data only; caller must first prove publisher closure/quiescence."""
    _require(
        type(intent) is SnapshotPublicationIntent and type(observation) is SnapshotCatalogObservation,
        "classification_shape",
    )
    intent.__post_init__()
    observation.__post_init__()
    expected = intent.generation
    if (
        observation.target != intent.target
        or observation.database_engine != "Atomic"
        or observation.table_engines != ("MergeTree", "MergeTree")
        or (observation.node_count, observation.replica_count) != (1, 1)
        or observation.unsupported_features
        or observation.schema_sha256 != (expected.schema_sha256,) * 2
        or observation.physical_sha256 != (expected.physical_sha256,) * 2
        or observation.generation_content_sha256 != expected.content_sha256
        or observation.generation_rows != expected.rows
        or observation.generation_bytes is None
        or observation.old_target_bytes is None
        or observation.retained_bytes is None
    ):
        return "COMMIT_UNKNOWN"
    try:
        intent.limits.require_storage(
            observation.generation_bytes, observation.old_target_bytes, observation.retained_bytes
        )
    except CompositionAdmissionError:
        return "COMMIT_UNKNOWN"
    pair = observation.target_uuid, observation.generation_uuid
    if pair == (expected.old_target_uuid, expected.new_generation_uuid):
        return "NOT_PUBLISHED"
    if pair == (expected.new_generation_uuid, expected.old_target_uuid):
        return "PUBLISHED"
    return "COMMIT_UNKNOWN"


@dataclass(frozen=True, slots=True)
class SnapshotPublicationRecord:
    """Protected CAS state. Publication outcome is not an attempt terminal grant.

    PREPARED is revision 1 and its sole exchange claim is revision 2. Published
    or uncertain outcomes require that claim first, so start at revision 3.
    NOT_PUBLISHED may start at revision 2 when cancelling PREPARED directly.
    """

    intent: SnapshotPublicationIntent
    state: str = "PREPARED"
    revision: int = 1
    closure: SnapshotPublisherClosure | None = None
    observation: SnapshotCatalogObservation | None = None

    def __post_init__(self) -> None:
        _require(type(self.intent) is SnapshotPublicationIntent, "record_intent")
        self.intent.__post_init__()
        _require(
            type(self.state) is str and self.state in {"PREPARED", "EXCHANGE_INTENT", "COMMIT_UNKNOWN", *_TERMINAL},
            "state",
        )
        _integer(self.revision, minimum=1 if self.state == "PREPARED" else 2)
        _require(
            (self.state != "PREPARED" or self.revision == 1)
            and (self.state != "EXCHANGE_INTENT" or self.revision == 2)
            and (self.state not in {"PUBLISHED", "COMMIT_UNKNOWN"} or self.revision >= 3),
            "revision",
        )
        if self.closure is not None:
            _require(type(self.closure) is SnapshotPublisherClosure, "closure_shape")
            self.closure.__post_init__()
            _require(self.closure.intent_sha256 == self.intent.intent_sha256, "closure_subject")
        if self.observation is not None:
            _require(type(self.observation) is SnapshotCatalogObservation, "observation_shape")
            self.observation.__post_init__()
        if self.state in {"PREPARED", "EXCHANGE_INTENT"}:
            _require(self.closure is None and self.observation is None, "premature_evidence")
        if self.state in _TERMINAL:
            _require(self.closure is not None and self.observation is not None, "outcome_evidence")
            assert self.observation is not None
            _require(classify_snapshot(self.intent, self.observation) == self.state, "outcome_pair")

    def transition(
        self,
        state: str,
        *,
        closure: SnapshotPublisherClosure | None = None,
        observation: SnapshotCatalogObservation | None = None,
    ) -> SnapshotPublicationRecord:
        """Validate a proposed CAS replacement; this does not persist anything."""
        self.__post_init__()
        allowed = {
            "PREPARED": {"EXCHANGE_INTENT", "NOT_PUBLISHED"},
            "EXCHANGE_INTENT": {*_TERMINAL, "COMMIT_UNKNOWN"},
            "COMMIT_UNKNOWN": {*_TERMINAL, "COMMIT_UNKNOWN"},
        }
        _require(state in allowed.get(self.state, set()), "transition")
        return replace(self, state=state, revision=self.revision + 1, closure=closure, observation=observation)

    def to_bytes(self) -> bytes:
        self.__post_init__()
        return _document({"schema": _RECORD_SCHEMA, **asdict(self)})

    @property
    def record_sha256(self) -> str:
        return _digest(self.to_bytes())

    @classmethod
    def from_bytes(cls, document: bytes, expected_sha256: str) -> SnapshotPublicationRecord:
        try:
            body = _read(document, expected_sha256, _RECORD_SCHEMA)
            body["intent"] = _intent_from_dict(body["intent"])
            if body["closure"] is not None:
                body["closure"] = SnapshotPublisherClosure(**body["closure"])
            if body["observation"] is not None:
                raw = body["observation"]
                raw["target"] = SnapshotTarget(**raw["target"])
                for key in ("table_engines", "schema_sha256", "physical_sha256", "unsupported_features"):
                    _require(type(raw[key]) is list, "observation_array")
                    raw[key] = tuple(raw[key])
                body["observation"] = SnapshotCatalogObservation(**raw)
            value = cls(**body)
            _require(value.to_bytes() == document, "canonical_document")
            return value
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
            raise CompositionAdmissionError("snapshot_record_document") from None


def _intent_from_dict(body: dict[str, Any]) -> SnapshotPublicationIntent:
    raw = body["attempt"]
    _require(
        type(raw["guard_epochs"]) is list and all(type(pair) is list for pair in raw["guard_epochs"]), "guard_array"
    )
    raw["guard_epochs"] = tuple(tuple(pair) for pair in raw["guard_epochs"])
    body["attempt"] = CompositionAttemptIdentity(**raw)
    body["target"] = SnapshotTarget(**body["target"])
    body["generation"] = SnapshotGeneration(**body["generation"])
    body["limits"] = SnapshotLimits(**body["limits"])
    for key in ("ingest_principal", "publisher_principal"):
        body[key] = CompositionProofAuthority(**body[key])
    return SnapshotPublicationIntent(**body)


def _read(document: bytes, expected_sha256: str, schema: str) -> dict[str, Any]:
    require_digest(expected_sha256)
    _require(type(document) is bytes and len(document) <= _MAX_DOCUMENT_BYTES, "document")
    _require(_digest(document) == expected_sha256, "document_digest")
    body = strict_json_object(document)
    _require(body.pop("schema", None) == schema, "document_schema")
    return body
