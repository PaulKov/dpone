"""Typed immutable evidence for canonical MSSQL R1 V3 stage artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    encode_artifact_rows,
    expect_bool,
    expect_enum,
    expect_int,
    expect_text,
    expect_tuple,
    require_canonical_text,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
)

_SCAN_EVIDENCE_DOMAIN = b"dpone-r1-typed-stage-scan-evidence-v1\0"
_STAGE_CELL_DOMAIN = b"dpone-r1-typed-stage-cell-v1\0"


class R1StageArtifactKindV1(StrEnum):
    BATCH_PAYLOAD = "batch_payload"
    XMIN_DELTA = "xmin_delta"
    XMIN_COMPLETE_KEYS = "xmin_complete_keys"


class R1StageCellStateV1(StrEnum):
    NULL = "null"
    VALUE = "value"


@dataclass(frozen=True, slots=True)
class R1StageBusinessColumnV1:
    source_ordinal: int
    stage_column: str
    target_ordinal: int
    target_column: str
    logical_type_id: str
    nullable: bool
    is_business_key: bool

    def __post_init__(self) -> None:
        require_positive(self.source_ordinal, "source_ordinal")
        require_identifier(self.stage_column, "stage_column")
        require_positive(self.target_ordinal, "target_ordinal")
        require_identifier(self.target_column, "target_column")
        require_canonical_text(self.logical_type_id, "logical_type_id", maximum_bytes=128)
        if not isinstance(self.nullable, bool) or not isinstance(self.is_business_key, bool):
            raise MssqlR1V3ContractError("stage column flags must be boolean")
        if self.is_business_key and (self.nullable or self.logical_type_id not in _SUPPORTED_KEY_TYPES):
            raise MssqlR1V3ContractError("business key must be one supported non-null scalar")

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return (
            self.source_ordinal,
            self.stage_column,
            self.target_ordinal,
            self.target_column,
            self.logical_type_id,
            self.nullable,
            self.is_business_key,
        )

    @classmethod
    def from_values(cls, values: object) -> R1StageBusinessColumnV1:
        items = expect_tuple(values, "business column", size=7)
        return cls(
            expect_int(items[0], "source_ordinal"),
            expect_text(items[1], "stage_column"),
            expect_int(items[2], "target_ordinal"),
            expect_text(items[3], "target_column"),
            expect_text(items[4], "logical_type_id"),
            expect_bool(items[5], "nullable"),
            expect_bool(items[6], "is_business_key"),
        )


@dataclass(frozen=True, slots=True)
class R1TypedStageCellV1:
    target_ordinal: int
    logical_type_id: str
    value_state: R1StageCellStateV1
    canonical_scalar_bytes: bytes

    def __post_init__(self) -> None:
        require_positive(self.target_ordinal, "target_ordinal")
        require_canonical_text(self.logical_type_id, "logical_type_id", maximum_bytes=128)
        if not isinstance(self.value_state, R1StageCellStateV1) or not isinstance(self.canonical_scalar_bytes, bytes):
            raise MssqlR1V3ContractError("typed stage cell has an invalid state or scalar payload")
        if self.value_state is R1StageCellStateV1.NULL and self.canonical_scalar_bytes != b"":
            raise MssqlR1V3ContractError("NULL stage cell requires an empty canonical scalar payload")

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return (self.target_ordinal, self.logical_type_id, self.value_state, self.canonical_scalar_bytes)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_STAGE_CELL_DOMAIN, self.canonical_values)

    @classmethod
    def from_values(cls, values: object) -> R1TypedStageCellV1:
        items = expect_tuple(values, "typed stage cell", size=4)
        return cls(
            expect_int(items[0], "target_ordinal"),
            expect_text(items[1], "logical_type_id"),
            expect_enum(R1StageCellStateV1, items[2], "value_state"),
            _expect_bytes(items[3], "canonical_scalar_bytes"),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1TypedStageCellV1:
        return cls.from_values(decode_canonical_bytes(payload, _STAGE_CELL_DOMAIN, field_count=4))


@dataclass(frozen=True, slots=True)
class R1TypedStageRowV1:
    artifact_kind: R1StageArtifactKindV1
    cells: tuple[R1TypedStageCellV1, ...]
    canonical_key_payload: bytes
    canonical_row_payload: bytes | None
    canonical_row_hash: bytes | None

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_kind, R1StageArtifactKindV1):
            raise MssqlR1V3ContractError("typed stage row has an unsupported artifact kind")
        if (
            not isinstance(self.cells, tuple)
            or not self.cells
            or not all(isinstance(cell, R1TypedStageCellV1) for cell in self.cells)
        ):
            raise MssqlR1V3ContractError("typed stage row requires ordered typed cells")
        if tuple(cell.target_ordinal for cell in self.cells) != tuple(range(1, len(self.cells) + 1)):
            raise MssqlR1V3ContractError("typed stage cells require contiguous target ordering")
        if not isinstance(self.canonical_key_payload, bytes) or not self.canonical_key_payload:
            raise MssqlR1V3ContractError("typed stage row requires a canonical key payload")
        complete = self.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
        if complete:
            if self.canonical_row_payload is not None or self.canonical_row_hash is not None:
                raise MssqlR1V3ContractError("complete-keys row forbids canonical row/hash payloads")
        elif not isinstance(self.canonical_row_payload, bytes) or not self.canonical_row_payload:
            raise MssqlR1V3ContractError("business stage row requires a canonical row payload")
        elif (
            not isinstance(self.canonical_row_hash, bytes)
            or self.canonical_row_hash != hashlib.sha256(self.canonical_row_payload).digest()
        ):
            raise MssqlR1V3ContractError("stored row hash differs from canonical row payload")

    @property
    def row_payload_for_artifact(self) -> bytes:
        return b"" if self.canonical_row_payload is None else self.canonical_row_payload


@dataclass(frozen=True, slots=True)
class R1BoundedStageChunkV1:
    artifact_kind: R1StageArtifactKindV1
    ordered_business_columns: tuple[R1StageBusinessColumnV1, ...]
    chunk_sequence: int
    rows: tuple[R1TypedStageRowV1, ...]

    def __post_init__(self) -> None:
        require_count(self.chunk_sequence, "chunk_sequence")
        if not isinstance(self.rows, tuple) or not 1 <= len(self.rows) <= 10_000:
            raise MssqlR1V3ContractError("stage chunk must contain 1..10000 canonical rows")
        if not all(isinstance(row, R1TypedStageRowV1) for row in self.rows):
            raise MssqlR1V3ContractError("stage chunk contains an invalid row")
        _validate_chunk_rows(self.artifact_kind, self.ordered_business_columns, self.rows)


@dataclass(frozen=True, slots=True)
class R1TypedStageScanEvidenceV1:
    artifact_kind: R1StageArtifactKindV1
    artifact_digest: bytes
    observed_row_count: int
    observed_payload_bytes: int
    typed_scan_rows: int
    canonical_reencode_matches: int
    row_hash_matches: int
    unique_key_count: int
    scan_contract_digest: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_kind, R1StageArtifactKindV1):
            raise MssqlR1V3ContractError("typed scan artifact kind is unsupported")
        require_digest(self.artifact_digest, "artifact_digest")
        require_digest(self.scan_contract_digest, "scan_contract_digest")
        for name in (
            "observed_row_count",
            "observed_payload_bytes",
            "typed_scan_rows",
            "canonical_reencode_matches",
            "row_hash_matches",
            "unique_key_count",
        ):
            require_count(getattr(self, name), name)
        count = self.observed_row_count
        expected_hashes = 0 if self.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else count
        if (self.typed_scan_rows, self.canonical_reencode_matches, self.row_hash_matches, self.unique_key_count) != (
            count,
            count,
            expected_hashes,
            count,
        ):
            raise MssqlR1V3ContractError("typed stage scan evidence is incomplete")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SCAN_EVIDENCE_DOMAIN,
            (
                self.artifact_kind,
                self.artifact_digest,
                self.observed_row_count,
                self.observed_payload_bytes,
                self.typed_scan_rows,
                self.canonical_reencode_matches,
                self.row_hash_matches,
                self.unique_key_count,
                self.scan_contract_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1TypedStageScanEvidenceV1:
        values = decode_canonical_bytes(payload, _SCAN_EVIDENCE_DOMAIN, field_count=9)
        return cls(expect_enum(R1StageArtifactKindV1, values[0], "artifact_kind"), *values[1:])  # type: ignore[arg-type]


class _SealedArtifact(Protocol):
    artifact_kind: R1StageArtifactKindV1
    schema_digest: bytes
    observed_row_count: int
    observed_payload_bytes: int
    artifact_digest: bytes
    maximum_key_bytes: int
    maximum_row_bytes: int


def artifact_digest_for_rows(
    artifact_kind: R1StageArtifactKindV1,
    schema_digest: bytes,
    rows: tuple[R1TypedStageRowV1, ...],
) -> tuple[bytes, int]:
    return encode_artifact_rows(
        artifact_kind.value,
        require_digest(schema_digest, "schema_digest"),
        tuple((row.canonical_key_payload, row.row_payload_for_artifact) for row in rows),
    )


def canonical_artifact_digest(
    manifest: _SealedArtifact,
    rows: tuple[R1TypedStageRowV1, ...],
) -> bytes:
    if len(rows) != manifest.observed_row_count:
        raise MssqlR1V3ContractError("artifact rows differ from the sealed row count")
    for row in rows:
        if row.artifact_kind is not manifest.artifact_kind:
            raise MssqlR1V3ContractError("artifact row kind differs from sealed manifest")
        if (
            len(row.canonical_key_payload) > manifest.maximum_key_bytes
            or len(row.row_payload_for_artifact) > manifest.maximum_row_bytes
        ):
            raise MssqlR1V3ContractError("canonical artifact payload exceeds sealed bounds")
    digest, size = artifact_digest_for_rows(manifest.artifact_kind, manifest.schema_digest, rows)
    if (digest, size) != (manifest.artifact_digest, manifest.observed_payload_bytes):
        raise MssqlR1V3ContractError("artifact rows differ from the sealed digest/count authority")
    return digest


def _validate_chunk_rows(
    kind: R1StageArtifactKindV1,
    columns: tuple[R1StageBusinessColumnV1, ...],
    rows: tuple[R1TypedStageRowV1, ...],
) -> None:
    if not isinstance(kind, R1StageArtifactKindV1) or not isinstance(columns, tuple) or not columns:
        raise MssqlR1V3ContractError("stage chunk requires exact kind and column authority")
    if not all(isinstance(column, R1StageBusinessColumnV1) for column in columns):
        raise MssqlR1V3ContractError("stage chunk column authority is invalid")
    keys = tuple(column for column in columns if column.is_business_key)
    if len(keys) != 1 or (kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS and columns != keys):
        raise MssqlR1V3ContractError("stage chunk has an invalid business-key shape")
    expected = tuple((column.target_ordinal, column.logical_type_id) for column in columns)
    for row in rows:
        observed = tuple((cell.target_ordinal, cell.logical_type_id) for cell in row.cells)
        if row.artifact_kind is not kind or observed != expected:
            raise MssqlR1V3ContractError("typed stage row differs from chunk column authority")
        for cell, column in zip(row.cells, columns, strict=True):
            if cell.value_state is R1StageCellStateV1.NULL and not column.nullable:
                raise MssqlR1V3ContractError("typed stage row contains NULL for a non-null column")


def _expect_bytes(value: object, field: str) -> bytes:
    if not isinstance(value, bytes):
        raise MssqlR1V3ContractError(f"{field} must be immutable bytes")
    return value


_SUPPORTED_KEY_TYPES = {
    "pg.int2-mssql.smallint.v1",
    "pg.int4-mssql.int.v1",
    "pg.int8-mssql.bigint.v1",
    "pg.uuid-mssql.uniqueidentifier.v1",
}


__all__ = [
    "R1BoundedStageChunkV1",
    "R1StageCellStateV1",
    "R1StageArtifactKindV1",
    "R1StageBusinessColumnV1",
    "R1TypedStageScanEvidenceV1",
    "R1TypedStageCellV1",
    "R1TypedStageRowV1",
    "artifact_digest_for_rows",
    "canonical_artifact_digest",
]
