"""Exact whole-snapshot subjects and state rules; these records grant no authority.

Protected producers must reopen source, enrollment and issued-principal evidence.
Hashes identify canonical UTF-8 bytes, never prove closure or permission.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from typing import Any

from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
    require_digest,
)
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionProofAuthority,
    encode_attempt_identity,
    require_composition_attempt_scope,
)
from dpone.contracts.composition_snapshot_subjects import (
    SnapshotCatalogObservation as SnapshotCatalogObservation,
)
from dpone.contracts.composition_snapshot_subjects import (
    SnapshotGeneration as SnapshotGeneration,
)
from dpone.contracts.composition_snapshot_subjects import (
    SnapshotLimits as SnapshotLimits,
)
from dpone.contracts.composition_snapshot_subjects import (
    SnapshotPublisherClosure as SnapshotPublisherClosure,
)
from dpone.contracts.composition_snapshot_subjects import (
    SnapshotTarget as SnapshotTarget,
)
from dpone.contracts.composition_snapshot_subjects import (
    _integer,
    _require,
    _uuid,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_INTENT_SCHEMA = "dpone.composition-clickhouse-snapshot-intent.v1"
_RECORD_SCHEMA = "dpone.composition-clickhouse-snapshot-record.v1"
_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
_TERMINAL = frozenset({"PUBLISHED", "NOT_PUBLISHED"})
_MIN_REVISION = {"PREPARED": 1, "EXCHANGE_INTENT": 2, "NOT_PUBLISHED": 2, "PUBLISHED": 3, "COMMIT_UNKNOWN": 3}


def _document(value: object) -> bytes:
    encoded = canonical_json_bytes(value)
    _require(len(encoded) <= _MAX_DOCUMENT_BYTES, "document_budget")
    return encoded


def _digest(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


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

    def require_prepared_subject(self, attempt: CompositionAttemptIdentity, generation_ref: str) -> None:
        """Match protected prepared evidence to the exact requested attempt and generation."""
        if (
            encode_attempt_identity(self.attempt) != encode_attempt_identity(attempt)
            or self.generation.record_sha256 != generation_ref
        ):
            raise CompositionAdmissionError("snapshot_prepared_subject")

    def require_parent_scope(self, occurrence: CompositionActivationOccurrence, *, recovery: bool) -> None:
        """Validate publication scope against an independently reopened parent occurrence.

        This comparison performs no authority lookup and grants no dispatch rights.
        """
        if occurrence.receipt.state not in ({"ACTIVE", "RETIRING"} if recovery else {"ACTIVE"}):
            raise CompositionAdmissionError("snapshot_occurrence_state")
        guards = require_composition_attempt_scope(occurrence, self.attempt)
        target = self.target
        workload = next(row for row in occurrence.request.workloads if row.workload_id == self.attempt.workload_id)
        resource = next(
            (row for row in occurrence.request.resources if row.guard_id == target.guard_id),
            None,
        )
        if (
            target.guard_id not in guards
            or resource is None
            or (
                resource.connector,
                resource.service_id,
                resource.physical_subject_sha256,
            )
            != ("clickhouse", target.service_id, target.physical_subject_sha256)
            or target.write_subject_sha256 not in resource.write_subjects
            or target.write_subject_sha256 not in workload.write_subjects
            or workload.execution_cell != "mssql_clickhouse_full_refresh_v1"
        ):
            raise CompositionAdmissionError("snapshot_parent_scope")

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
        _require(type(self.state) is str and self.state in _MIN_REVISION, "state")
        _integer(self.revision, minimum=1 if self.state == "PREPARED" else 2)
        minimum = _MIN_REVISION[self.state]
        exact = self.state in {"PREPARED", "EXCHANGE_INTENT"}
        _require(self.revision == minimum if exact else self.revision >= minimum, "revision")
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
