"""Receipt-bound sealed-stage consumption evidence for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_int,
    expect_tuple,
    expect_uuid,
    parse_canonical_utc_text,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_staging import R1SealedStageManifestV1, R1StageArtifactKindV1
from dpone.contracts.mssql_r1_v3_transaction_authority import R1ReplayResourceStateV3

_CONSUMED_STAGE_DOMAIN = b"dpone-r1-consumed-sealed-stage-set-v3\0"
_REPLAY_ARTIFACT_DOMAIN = b"dpone-r1-effect-replay-observation-v3\0artifacts\0"
_REPLAY_CHECKPOINT_DOMAIN = b"dpone-r1-effect-replay-observation-v3\0checkpoint\0"


class MssqlStageConsumptionStateV3(StrEnum):
    SEALED = "sealed"
    CONSUMED = "consumed"


@dataclass(frozen=True, slots=True)
class MssqlConsumedSealedStageSetV3:
    transaction_id: UUID
    session_identity_digest: bytes
    effect_key: bytes
    sealed_request_digest: bytes
    consuming_receipt_id: UUID
    consuming_receipt_digest: bytes
    artifact_ids: tuple[UUID, ...]
    artifact_kinds: tuple[R1StageArtifactKindV1, ...]
    manifest_payloads: tuple[bytes, ...]
    manifest_digests: tuple[bytes, ...]
    sealed_at: tuple[datetime, ...]
    retention_until: tuple[datetime, ...]
    consumed_at: tuple[datetime, ...]
    projection_revisions: tuple[int, ...]
    states: tuple[MssqlStageConsumptionStateV3, ...]

    def __post_init__(self) -> None:
        require_uuid(self.transaction_id, "transaction_id")
        require_digest(self.session_identity_digest, "session_identity_digest")
        require_digest(self.effect_key, "effect_key")
        require_digest(self.sealed_request_digest, "sealed_request_digest")
        require_uuid(self.consuming_receipt_id, "consuming_receipt_id")
        require_digest(self.consuming_receipt_digest, "consuming_receipt_digest")
        expected_kinds = {
            (R1StageArtifactKindV1.BATCH_PAYLOAD,),
            (R1StageArtifactKindV1.XMIN_DELTA, R1StageArtifactKindV1.XMIN_COMPLETE_KEYS),
        }
        if self.artifact_kinds not in expected_kinds:
            raise MssqlR1V3ContractError("consumed stage set has an invalid kind/order")
        count = len(self.artifact_kinds)
        parallel = (
            self.artifact_ids,
            self.manifest_payloads,
            self.manifest_digests,
            self.sealed_at,
            self.retention_until,
            self.consumed_at,
            self.projection_revisions,
            self.states,
        )
        if any(len(values) != count for values in parallel):
            raise MssqlR1V3ContractError("consumed stage observation is partial")
        if len(set(self.artifact_ids)) != count:
            raise MssqlR1V3ContractError("consumed stage observation contains duplicate artifacts")
        for index, artifact_id in enumerate(self.artifact_ids):
            require_uuid(artifact_id, "artifact_id")
            manifest = R1SealedStageManifestV1.from_canonical_bytes(self.manifest_payloads[index])
            if (
                manifest.artifact_id,
                manifest.artifact_kind,
                manifest.effect_key,
                manifest.manifest_digest,
            ) != (artifact_id, self.artifact_kinds[index], self.effect_key, self.manifest_digests[index]):
                raise MssqlR1V3ContractError("consumed stage differs from exact sealed manifest")
            require_digest(self.manifest_digests[index], "manifest_digest")
            sealed = self.sealed_at[index]
            retention = self.retention_until[index]
            consumed = self.consumed_at[index]
            canonical_utc_text(sealed, "sealed_at")
            canonical_utc_text(retention, "retention_until")
            canonical_utc_text(consumed, "consumed_at")
            if retention < sealed or not sealed <= consumed <= retention:
                raise MssqlR1V3ContractError("stage lifecycle timestamps are inconsistent")
            require_positive(self.projection_revisions[index], "projection_revision")
            if self.states[index] is not MssqlStageConsumptionStateV3.CONSUMED:
                raise MssqlR1V3ContractError("effect stage observation must be CONSUMED")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CONSUMED_STAGE_DOMAIN,
            (
                self.transaction_id,
                self.session_identity_digest,
                self.effect_key,
                self.sealed_request_digest,
                self.consuming_receipt_id,
                self.consuming_receipt_digest,
                self.artifact_ids,
                self.artifact_kinds,
                self.manifest_payloads,
                self.manifest_digests,
                self.sealed_at,
                self.retention_until,
                self.consumed_at,
                self.projection_revisions,
                self.states,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlConsumedSealedStageSetV3:
        values = list(decode_canonical_bytes(payload, _CONSUMED_STAGE_DOMAIN, field_count=15))
        values[8] = tuple(expect_bytes(item, "manifest_payload") for item in expect_tuple(values[8], "manifests"))
        values[10] = tuple(
            parse_canonical_utc_text(item, "sealed_at") for item in expect_tuple(values[10], "sealed_at")
        )
        values[11] = tuple(
            parse_canonical_utc_text(item, "retention_until") for item in expect_tuple(values[11], "retention_until")
        )
        values[12] = tuple(
            parse_canonical_utc_text(item, "consumed_at") for item in expect_tuple(values[12], "consumed_at")
        )
        values[7] = tuple(
            expect_enum(R1StageArtifactKindV1, item, "artifact_kind")
            for item in expect_tuple(values[7], "artifact_kinds")
        )
        values[14] = tuple(
            expect_enum(MssqlStageConsumptionStateV3, item, "stage_state")
            for item in expect_tuple(values[14], "states")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ArtifactSetReplayObservationV3:
    artifact_ids: tuple[UUID, ...]
    manifest_digests: tuple[bytes, ...]
    states: tuple[R1ReplayResourceStateV3, ...]
    consuming_receipt_ids: tuple[UUID | None, ...]
    consuming_receipt_digests: tuple[bytes | None, ...]

    def __post_init__(self) -> None:
        count = len(self.artifact_ids)
        parallel = (self.manifest_digests, self.states, self.consuming_receipt_ids, self.consuming_receipt_digests)
        if not count or any(len(value) != count for value in parallel):
            raise MssqlR1V3ContractError("artifact replay observation is partial")
        if len(set(self.artifact_ids)) != count:
            raise MssqlR1V3ContractError("artifact replay observation contains duplicates")
        for artifact_id in self.artifact_ids:
            require_uuid(artifact_id, "artifact_id")
        for manifest_digest in self.manifest_digests:
            require_digest(manifest_digest, "manifest_digest")
        for state, receipt_id, receipt_digest in zip(
            self.states, self.consuming_receipt_ids, self.consuming_receipt_digests, strict=True
        ):
            if (receipt_id is None) != (receipt_digest is None):
                raise MssqlR1V3ContractError("artifact replay receipt identity is partial")
            if receipt_id is not None:
                require_uuid(receipt_id, "consuming_receipt_id")
            if receipt_digest is not None:
                require_digest(receipt_digest, "consuming_receipt_digest")
            consumed = receipt_id is not None and receipt_digest is not None
            if (state is R1ReplayResourceStateV3.CONSUMED) != consumed:
                raise MssqlR1V3ContractError("artifact replay lifecycle is inconsistent")
            if state not in {R1ReplayResourceStateV3.SEALED, R1ReplayResourceStateV3.CONSUMED}:
                raise MssqlR1V3ContractError("artifact replay state is invalid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _REPLAY_ARTIFACT_DOMAIN,
            (
                self.artifact_ids,
                self.manifest_digests,
                self.states,
                self.consuming_receipt_ids,
                self.consuming_receipt_digests,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> ArtifactSetReplayObservationV3:
        values = list(decode_canonical_bytes(payload, _REPLAY_ARTIFACT_DOMAIN, field_count=5))
        values[2] = tuple(
            expect_enum(R1ReplayResourceStateV3, item, "artifact_state")
            for item in expect_tuple(values[2], "artifact_states")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class CheckpointReplayObservationV3:
    writer_generation: int
    checkpoint_revision: int
    checkpoint_payload: bytes
    consuming_receipt_id: UUID | None

    def __post_init__(self) -> None:
        require_positive(self.writer_generation, "writer_generation")
        require_positive(self.checkpoint_revision, "checkpoint_revision")
        if not isinstance(self.checkpoint_payload, bytes) or not self.checkpoint_payload:
            raise MssqlR1V3ContractError("checkpoint observation requires exact payload bytes")
        if self.consuming_receipt_id is not None:
            require_uuid(self.consuming_receipt_id, "consuming_receipt_id")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _REPLAY_CHECKPOINT_DOMAIN,
            (self.writer_generation, self.checkpoint_revision, self.checkpoint_payload, self.consuming_receipt_id),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> CheckpointReplayObservationV3:
        values = decode_canonical_bytes(payload, _REPLAY_CHECKPOINT_DOMAIN, field_count=4)
        return cls(
            expect_int(values[0], "writer_generation"),
            expect_int(values[1], "checkpoint_revision"),
            expect_bytes(values[2], "checkpoint_payload"),
            None if values[3] is None else expect_uuid(values[3], "consuming_receipt_id"),
        )


__all__ = [
    "ArtifactSetReplayObservationV3",
    "CheckpointReplayObservationV3",
    "MssqlConsumedSealedStageSetV3",
    "MssqlStageConsumptionStateV3",
]
