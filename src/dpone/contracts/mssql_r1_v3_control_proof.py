"""Fresh target-local proof contracts for R1 V3 control operations."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_r1_v3_control import (
    MssqlOpenStageRecoveryReceiptV1,
    MssqlR1ControlOperationV1,
    MssqlR1ControlReceiptV1,
    MssqlR1V3ContractError,
    MssqlTargetPhysicalIdentityV1,
    build_control_effect_key,
    build_open_stage_recovery_effect_key,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    require_canonical_text,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
    require_uuid,
)

_CONTROL_PROOF_DOMAIN = b"dpone-r1-control-fresh-proof-v1\0"
_OPEN_RECOVERY_PROOF_DOMAIN = b"dpone-r1-open-stage-recovery-fresh-proof-v1\0"
_WRITER_HEAD_OBSERVATION_DOMAIN = b"dpone-r1-control-writer-head-observation-v1\0"


@dataclass(frozen=True, slots=True)
class MssqlR1WriterHeadControlObservationV1:
    head_revision: int
    head_digest: bytes

    def __post_init__(self) -> None:
        require_positive(self.head_revision, "head_revision")
        require_digest(self.head_digest, "head_digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_WRITER_HEAD_OBSERVATION_DOMAIN, (self.head_revision, self.head_digest))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1WriterHeadControlObservationV1:
        return cls(*decode_canonical_bytes(payload, _WRITER_HEAD_OBSERVATION_DOMAIN, field_count=2))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ControlFreshProofV1:
    receipt: MssqlR1ControlReceiptV1
    observed_registration_id: UUID
    observed_active_registration_revision: int
    observed_physical_authority_digest: bytes
    observed_schema_contract_digest: bytes
    observed_permission_contract_digest: bytes
    observed_writer_head: MssqlR1WriterHeadControlObservationV1
    observed_authority_set_digest: bytes | None
    observed_override_digest: bytes | None

    def __post_init__(self) -> None:
        require_uuid(self.observed_registration_id, "observed_registration_id")
        require_positive(self.observed_active_registration_revision, "observed_active_registration_revision")
        for name in (
            "observed_physical_authority_digest",
            "observed_schema_contract_digest",
            "observed_permission_contract_digest",
        ):
            require_digest(getattr(self, name), name)
        if not isinstance(self.observed_writer_head, MssqlR1WriterHeadControlObservationV1):
            raise MssqlR1V3ContractError("fresh control proof requires a typed writer head observation")
        for value in (self.observed_authority_set_digest, self.observed_override_digest):
            if value is not None:
                require_digest(value, "optional control observation digest")
        expected = (
            self.receipt.registration_id,
            self.receipt.committed_active_registration_revision,
            self.receipt.registered_physical_authority_digest,
            self.receipt.schema_contract_digest,
            self.receipt.permission_contract_digest,
            self.receipt.observed_writer_head_revision,
            self.receipt.observed_writer_head_digest,
            self.receipt.affected_authority_set_digest,
            self.receipt.affected_override_payload_digest,
        )
        observed = (
            self.observed_registration_id,
            self.observed_active_registration_revision,
            self.observed_physical_authority_digest,
            self.observed_schema_contract_digest,
            self.observed_permission_contract_digest,
            self.observed_writer_head.head_revision,
            self.observed_writer_head.head_digest,
            self.observed_authority_set_digest,
            self.observed_override_digest,
        )
        if observed != expected:
            raise MssqlR1V3ContractError("fresh control proof differs from committed receipt/state")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CONTROL_PROOF_DOMAIN,
            (
                self.receipt.canonical_bytes,
                self.observed_registration_id,
                self.observed_active_registration_revision,
                self.observed_physical_authority_digest,
                self.observed_schema_contract_digest,
                self.observed_permission_contract_digest,
                self.observed_writer_head.canonical_bytes,
                self.observed_authority_set_digest,
                self.observed_override_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ControlFreshProofV1:
        values = list(decode_canonical_bytes(payload, _CONTROL_PROOF_DOMAIN, field_count=9))
        values[0] = MssqlR1ControlReceiptV1.from_canonical_bytes(expect_bytes(values[0], "control_receipt"))
        values[6] = MssqlR1WriterHeadControlObservationV1.from_canonical_bytes(expect_bytes(values[6], "writer_head"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlOpenStageRecoveryFreshProofV1:
    receipt: MssqlOpenStageRecoveryReceiptV1
    observed_operation_epoch: int
    observed_operation_projection_revision: int
    observed_operation_state: str
    observed_abandoned_artifact_ids: tuple[UUID, ...]
    observed_new_artifact_ids: tuple[UUID, ...]
    observed_open_plan_set_digest: bytes
    observed_writer_head_digest: bytes | None

    def __post_init__(self) -> None:
        require_digest(self.observed_open_plan_set_digest, "observed_open_plan_set_digest")
        expected = (
            self.receipt.committed_operation_epoch,
            self.receipt.committed_operation_projection_revision,
            self.receipt.committed_operation_state,
            self.receipt.abandoned_artifact_ids,
            self.receipt.new_artifact_ids,
            self.receipt.new_open_plan_set_digest,
            self.receipt.observed_writer_head_digest,
        )
        observed = tuple(getattr(self, name) for name in tuple(self.__dataclass_fields__)[1:])
        if observed != expected:
            raise MssqlR1V3ContractError("fresh OPEN recovery proof differs from committed state")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OPEN_RECOVERY_PROOF_DOMAIN,
            (self.receipt.canonical_bytes, *(getattr(self, name) for name in tuple(self.__dataclass_fields__)[1:])),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlOpenStageRecoveryFreshProofV1:
        values = list(decode_canonical_bytes(payload, _OPEN_RECOVERY_PROOF_DOMAIN, field_count=8))
        values[0] = MssqlOpenStageRecoveryReceiptV1.from_canonical_bytes(
            expect_bytes(values[0], "open_recovery_receipt")
        )
        return cls(*values)  # type: ignore[arg-type]


__all__ = [
    "MssqlOpenStageRecoveryFreshProofV1",
    "MssqlOpenStageRecoveryReceiptV1",
    "MssqlR1ControlFreshProofV1",
    "MssqlR1ControlOperationV1",
    "MssqlR1ControlReceiptV1",
    "MssqlR1V3ContractError",
    "MssqlR1WriterHeadControlObservationV1",
    "MssqlTargetPhysicalIdentityV1",
    "build_control_effect_key",
    "build_open_stage_recovery_effect_key",
    "canonical_utc_text",
    "require_canonical_text",
    "require_count",
    "require_digest",
    "require_identifier",
    "require_positive",
    "require_uuid",
]
