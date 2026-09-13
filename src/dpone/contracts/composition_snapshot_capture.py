"""Closed runtime snapshot originals; identifiers confer no executor authority.

A source capture is claimed once per complete parent attempt and target write.
The generation name and UUID are stable across crash recovery, but a retained
claim never grants permission to replay CREATE or INSERT. Generation identity
hashes a canonical document that deliberately excludes its own record digest.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import SnapshotGeneration, SnapshotLimits, SnapshotTarget
from dpone.contracts.composition_snapshot_materialization import CONTENT_VERSION
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_CAPTURE_METADATA_BYTES = 1024 * 1024
CAPTURE_PHASES = ("CLAIMED", "CAPTURED", "GENERATION_SEALED")


def capture_digest(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


def attempt_snapshot_target(target: SnapshotTarget, attempt: CompositionAttemptIdentity) -> SnapshotTarget:
    """Derive a permanent physical slot from the complete attempt digest."""
    target.__post_init__()
    attempt.__post_init__()
    name = target.target_table[:48] + "__dpone_" + attempt.attempt_sha256[7:]
    return replace(target, generation_table=name)


@dataclass(frozen=True, slots=True)
class SnapshotCaptureSubject:
    """Verified source/attempt binding recorded before source SQL or target DDL."""

    attempt: CompositionAttemptIdentity
    target: SnapshotTarget
    source_binding_sha256: str
    source_table: tuple[str, str, str]
    limits: SnapshotLimits

    def __post_init__(self) -> None:
        self.attempt.__post_init__()
        self.target.__post_init__()
        self.limits.__post_init__()
        require_digest(self.source_binding_sha256)
        if (
            self.target != attempt_snapshot_target(self.target, self.attempt)
            or self.target.guard_id not in dict(self.attempt.guard_epochs)
            or type(self.source_table) is not tuple
            or len(self.source_table) != 3
            or any(
                type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None
                for value in self.source_table
            )
        ):
            raise CompositionAdmissionError("snapshot_capture_subject")

    @property
    def generation_uuid(self) -> str:
        return str(uuid5(NAMESPACE_URL, "dpone.composition-snapshot-generation.v1:" + self.subject_sha256))

    @property
    def subject_sha256(self) -> str:
        return capture_digest(self.to_bytes())

    def to_bytes(self) -> bytes:
        self.__post_init__()
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-snapshot-capture-subject.v1",
                "content_version": CONTENT_VERSION,
                **asdict(self),
            }
        )

    @classmethod
    def from_bytes(cls, document: bytes) -> SnapshotCaptureSubject:
        try:
            body = strict_json_object(document)
            if (
                len(document) > MAX_CAPTURE_METADATA_BYTES
                or body.pop("schema") != "dpone.composition-snapshot-capture-subject.v1"
                or body.pop("content_version") != CONTENT_VERSION
            ):
                raise ValueError
            attempt = body["attempt"]
            attempt["guard_epochs"] = tuple(tuple(pair) for pair in attempt["guard_epochs"])
            body["attempt"] = CompositionAttemptIdentity(**attempt)
            body["target"] = SnapshotTarget(**body["target"])
            body["limits"] = SnapshotLimits(**body["limits"])
            body["source_table"] = tuple(body["source_table"])
            result = cls(**body)
            if result.to_bytes() != document:
                raise ValueError
            return result
        except (ValueError, KeyError, TypeError, AttributeError):
            raise CompositionAdmissionError("snapshot_capture_document") from None


def generation_document(generation: SnapshotGeneration) -> bytes:
    """Canonical original for a sealed generation, with no circular digest."""
    generation.__post_init__()
    fields = asdict(generation)
    fields.pop("record_sha256")
    return canonical_json_bytes({"schema": "dpone.composition-snapshot-generation.v1", **fields})


def generation_original(generation: SnapshotGeneration) -> SnapshotGeneration:
    """Set the record digest from its immutable non-self-referential original."""
    return replace(generation, record_sha256=capture_digest(generation_document(generation)))


def decode_generation(document: bytes, expected_sha256: str) -> SnapshotGeneration:
    try:
        body = strict_json_object(document)
        if (
            len(document) > MAX_CAPTURE_METADATA_BYTES
            or body.pop("schema") != "dpone.composition-snapshot-generation.v1"
            or capture_digest(document) != expected_sha256
        ):
            raise ValueError
        result = SnapshotGeneration(record_sha256=expected_sha256, **body)
        if generation_document(result) != document:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError):
        raise CompositionAdmissionError("snapshot_capture_generation") from None


@dataclass(frozen=True, slots=True)
class SnapshotCaptureRecord:
    """SQL-pinned metadata for the two immutable source files, before CREATE."""

    subject_sha256: str
    source_document_sha256: str
    payload_sha256: str
    content_sha256: str
    schema_sha256: str
    physical_sha256: str
    baseline_evidence_sha256: str
    old_target_uuid: str
    columns: tuple[tuple[str, str], ...]
    rows: int
    source_bytes: int
    wire_bytes: int
    old_target_bytes: int
    retained_bytes: int

    def __post_init__(self) -> None:
        from uuid import UUID

        from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
        from dpone.contracts.composition_snapshot_materialization import require_snapshot_columns

        for name, value in asdict(self).items():
            if name.endswith("_sha256"):
                require_digest(value)
        require_snapshot_columns(tuple(ClickHouseDispatchColumn(*c) for c in self.columns))
        if str(UUID(self.old_target_uuid)) != self.old_target_uuid or UUID(self.old_target_uuid).int == 0:
            raise CompositionAdmissionError("snapshot_capture_uuid")
        if any(
            type(value) is not int or not 0 <= value <= 2**63 - 1
            for value in (self.rows, self.source_bytes, self.wire_bytes, self.old_target_bytes, self.retained_bytes)
        ):
            raise CompositionAdmissionError("snapshot_capture_counter")

    def to_bytes(self) -> bytes:
        self.__post_init__()
        return canonical_json_bytes(
            {"schema": "dpone.composition-snapshot-captured.v1", "content_version": CONTENT_VERSION, **asdict(self)}
        )

    @classmethod
    def from_bytes(cls, document: bytes) -> SnapshotCaptureRecord:
        try:
            body = strict_json_object(document)
            if (
                len(document) > MAX_CAPTURE_METADATA_BYTES
                or body.pop("schema") != "dpone.composition-snapshot-captured.v1"
                or body.pop("content_version") != CONTENT_VERSION
            ):
                raise ValueError
            body["columns"] = tuple(tuple(c) for c in body["columns"])
            result = cls(**body)
            if result.to_bytes() != document:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError):
            raise CompositionAdmissionError("snapshot_capture_document") from None


def snapshot_generation_from_observation(
    subject: SnapshotCaptureSubject, captured: SnapshotCaptureRecord, observed: object
) -> SnapshotGeneration:
    """Seal B only when independent complete materialization matches source A→B."""
    from dpone.contracts.composition_snapshot import SnapshotCatalogObservation

    subject.__post_init__()
    captured.__post_init__()
    if type(observed) is not SnapshotCatalogObservation:
        raise CompositionAdmissionError("snapshot_capture_observation")
    observed.__post_init__()
    if (
        captured.subject_sha256 != subject.subject_sha256
        or observed.target != subject.target
        or observed.target_uuid != captured.old_target_uuid
        or observed.generation_uuid != subject.generation_uuid
        or observed.database_engine != "Atomic"
        or observed.table_engines != ("MergeTree", "MergeTree")
        or (observed.node_count, observed.replica_count) != (1, 1)
        or observed.unsupported_features
        or observed.schema_sha256 != (captured.schema_sha256,) * 2
        or observed.physical_sha256 != (captured.physical_sha256,) * 2
        or observed.generation_content_sha256 != captured.content_sha256
        or observed.generation_rows != captured.rows
        or observed.generation_bytes is None
        or observed.old_target_bytes is None
        or observed.retained_bytes is None
    ):
        raise CompositionAdmissionError("snapshot_capture_materialization")
    subject.limits.require_storage(observed.generation_bytes, observed.old_target_bytes, observed.retained_bytes)
    fields = dict(
        source_snapshot_sha256=captured.source_document_sha256,
        content_sha256=captured.content_sha256,
        schema_sha256=captured.schema_sha256,
        physical_sha256=captured.physical_sha256,
        old_target_uuid=captured.old_target_uuid,
        new_generation_uuid=subject.generation_uuid,
        rows=captured.rows,
        source_bytes=captured.source_bytes,
        wire_bytes=captured.wire_bytes,
        generation_bytes=observed.generation_bytes,
        old_target_bytes=observed.old_target_bytes,
        retained_bytes=observed.retained_bytes,
    )
    original = canonical_json_bytes({"schema": "dpone.composition-snapshot-generation.v1", **fields})
    return decode_generation(original, capture_digest(original))


def decode_generation_seal(
    subject: SnapshotCaptureSubject, captured: SnapshotCaptureRecord, document: bytes
) -> SnapshotGeneration:
    """Rebuild the seal from retained source and actual observation originals."""
    from dpone.contracts.composition_snapshot import SnapshotCatalogObservation

    try:
        body = strict_json_object(document)
        if (
            len(document) > MAX_CAPTURE_METADATA_BYTES
            or set(body)
            != {"schema", "generation", "generation_sha256", "closed_gates_sha256", "quiescence_sha256", "observation"}
            or body["schema"] != "dpone.composition-snapshot-generation-seal.v1"
        ):
            raise ValueError
        for field in ("generation_sha256", "closed_gates_sha256", "quiescence_sha256"):
            require_digest(body[field])
        value = dict(body["observation"])
        value["target"] = SnapshotTarget(**value["target"])
        for field in ("table_engines", "schema_sha256", "physical_sha256", "unsupported_features"):
            value[field] = tuple(value[field])
        observed = SnapshotCatalogObservation(**value)
        expected = snapshot_generation_from_observation(subject, captured, observed)
        generation = decode_generation(canonical_json_bytes(body["generation"]), body["generation_sha256"])
        if generation != expected or canonical_json_bytes(body) != document:
            raise ValueError
        return generation
    except (ValueError, TypeError, KeyError, AttributeError):
        raise CompositionAdmissionError("snapshot_capture_generation_seal") from None
