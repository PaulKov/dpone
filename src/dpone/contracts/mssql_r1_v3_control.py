"""Target-local control and OPEN-stage recovery receipts for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    EFFECT_CONTRACT_VERSION,
    MssqlR1V3ContractError,
    MssqlTargetPhysicalIdentityV1,
    canonical_bytes,
    canonical_digest,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    parse_canonical_utc_text,
    require_canonical_text,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
    require_uuid,
)

_CONTROL_EFFECT_DOMAIN = b"dpone-r1-control-effect-v1\0"
_CONTROL_RECEIPT_DOMAIN = b"dpone-r1-control-receipt-v1\0"
_OPEN_RECOVERY_EFFECT_DOMAIN = b"dpone-r1-open-stage-recovery-v1\0"
_OPEN_RECOVERY_RECEIPT_DOMAIN = b"dpone-r1-open-stage-recovery-receipt-v1\0"


class MssqlR1ControlOperationV1(StrEnum):
    PROVISION = "provision"
    REGISTRATION_ROTATE = "registration_rotate"
    AUTHORITY_IMPORT = "authority_import"
    REVOKED_REGISTRATION_OVERRIDE_IMPORT = "revoked_registration_override_import"
    REGISTRATION_RETIRE = "registration_retire"


def build_control_effect_key(
    operation: MssqlR1ControlOperationV1,
    physical_object_coordinate_digest: bytes,
    input_payload_digest: bytes,
    expected_active_registration_revision: int | None,
) -> bytes:
    if not isinstance(operation, MssqlR1ControlOperationV1):
        raise MssqlR1V3ContractError("control operation is unsupported")
    require_digest(physical_object_coordinate_digest, "physical_object_coordinate_digest")
    require_digest(input_payload_digest, "input_payload_digest")
    if expected_active_registration_revision is not None:
        require_positive(expected_active_registration_revision, "expected_active_registration_revision")
    return canonical_digest(
        _CONTROL_EFFECT_DOMAIN,
        (
            EFFECT_CONTRACT_VERSION,
            operation,
            physical_object_coordinate_digest,
            input_payload_digest,
            expected_active_registration_revision,
        ),
    )


@dataclass(frozen=True, slots=True)
class MssqlR1ControlReceiptV1:
    control_receipt_id: UUID
    control_effect_key: bytes
    operation: MssqlR1ControlOperationV1
    physical_object_coordinate_digest: bytes
    registered_physical_authority_digest: bytes
    target_binding_uuid: UUID
    registration_id: UUID
    input_payload_digest: bytes
    verification_receipt_digest: bytes
    expected_active_registration_revision: int | None
    committed_active_registration_revision: int
    expected_writer_head_revision: int | None
    observed_writer_head_revision: int
    expected_writer_head_digest: bytes | None
    observed_writer_head_digest: bytes
    schema_contract_digest: bytes
    permission_contract_digest: bytes
    affected_authority_set_digest: bytes | None
    affected_override_payload_digest: bytes | None
    committed_at: datetime

    def __post_init__(self) -> None:
        for name in ("control_receipt_id", "target_binding_uuid", "registration_id"):
            require_uuid(getattr(self, name), name)
        for name in (
            "control_effect_key",
            "physical_object_coordinate_digest",
            "registered_physical_authority_digest",
            "input_payload_digest",
            "verification_receipt_digest",
            "observed_writer_head_digest",
            "schema_contract_digest",
            "permission_contract_digest",
        ):
            require_digest(getattr(self, name), name)
        if self.affected_authority_set_digest is not None:
            require_digest(self.affected_authority_set_digest, "affected_authority_set_digest")
        if self.affected_override_payload_digest is not None:
            require_digest(self.affected_override_payload_digest, "affected_override_payload_digest")
        if self.control_effect_key != build_control_effect_key(
            self.operation,
            self.physical_object_coordinate_digest,
            self.input_payload_digest,
            self.expected_active_registration_revision,
        ):
            raise MssqlR1V3ContractError("control effect key differs from immutable transition identity")
        self._validate_revisions()
        authority_import = self.operation is MssqlR1ControlOperationV1.AUTHORITY_IMPORT
        override_import = self.operation is MssqlR1ControlOperationV1.REVOKED_REGISTRATION_OVERRIDE_IMPORT
        if authority_import != (self.affected_authority_set_digest is not None):
            raise MssqlR1V3ContractError("affected authority set belongs only to authority import")
        if override_import != (self.affected_override_payload_digest is not None):
            raise MssqlR1V3ContractError("affected override belongs only to override import")
        if self.affected_authority_set_digest is not None and self.affected_override_payload_digest is not None:
            raise MssqlR1V3ContractError("control import payload kinds are mutually exclusive")
        canonical_utc_text(self.committed_at, "committed_at")

    def _validate_revisions(self) -> None:
        expected = self.expected_active_registration_revision
        committed = require_positive(
            self.committed_active_registration_revision,
            "committed_active_registration_revision",
        )
        if self.operation is MssqlR1ControlOperationV1.PROVISION:
            if expected is not None or committed != 1:
                raise MssqlR1V3ContractError("initial provision must create active registration revision 1")
        else:
            require_positive(expected, "expected_active_registration_revision")
            if self.operation in {
                MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
                MssqlR1ControlOperationV1.REGISTRATION_RETIRE,
            }:
                if committed != expected + 1:  # type: ignore[operator]
                    raise MssqlR1V3ContractError("registration transition revision must be adjacent")
            elif committed != expected:
                raise MssqlR1V3ContractError("authority import cannot change registration revision")
        observed_revision = require_positive(self.observed_writer_head_revision, "observed_writer_head_revision")
        require_digest(self.observed_writer_head_digest, "observed_writer_head_digest")
        if self.operation is MssqlR1ControlOperationV1.PROVISION:
            if self.expected_writer_head_revision is not None or self.expected_writer_head_digest is not None:
                raise MssqlR1V3ContractError("initial provision cannot name a predecessor writer head")
            if observed_revision != 1:
                raise MssqlR1V3ContractError("initial provision must observe writer head revision 1")
        else:
            expected_revision = require_positive(self.expected_writer_head_revision, "expected_writer_head_revision")
            expected_digest = require_digest(self.expected_writer_head_digest, "expected_writer_head_digest")
            if (expected_revision, expected_digest) != (observed_revision, self.observed_writer_head_digest):
                raise MssqlR1V3ContractError("control operation cannot mutate the business writer head")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CONTROL_RECEIPT_DOMAIN,
            (EFFECT_CONTRACT_VERSION, *(getattr(self, item.name) for item in fields(self))),
        )

    @property
    def receipt_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ControlReceiptV1:
        values = list(decode_canonical_bytes(payload, _CONTROL_RECEIPT_DOMAIN, field_count=21))
        if values[0] != EFFECT_CONTRACT_VERSION:
            raise MssqlR1V3ContractError("control receipt uses an unknown contract version")
        values[3] = expect_enum(MssqlR1ControlOperationV1, values[3], "control operation")
        values[20] = parse_canonical_utc_text(values[20], "committed_at")
        return cls(*values[1:])  # type: ignore[arg-type]


def build_open_stage_recovery_effect_key(
    operation_key: bytes,
    effect_key: bytes,
    old_artifact_set_digest: bytes,
    expected_operation_epoch: int,
    expected_operation_projection_revision: int,
) -> bytes:
    for name, value in (
        ("operation_key", operation_key),
        ("effect_key", effect_key),
        ("old_artifact_set_digest", old_artifact_set_digest),
    ):
        require_digest(value, name)
    require_positive(expected_operation_epoch, "expected_operation_epoch")
    require_positive(expected_operation_projection_revision, "expected_operation_projection_revision")
    return canonical_digest(
        _OPEN_RECOVERY_EFFECT_DOMAIN,
        (
            EFFECT_CONTRACT_VERSION,
            operation_key,
            effect_key,
            old_artifact_set_digest,
            expected_operation_epoch,
            expected_operation_projection_revision,
        ),
    )


@dataclass(frozen=True, slots=True)
class MssqlOpenStageRecoveryReceiptV1:
    recovery_receipt_id: UUID
    effect_key: bytes
    recovery_effect_key: bytes
    operation_key: bytes
    old_artifact_set_digest: bytes
    abandoned_artifact_ids: tuple[UUID, ...]
    expected_operation_epoch: int
    committed_operation_epoch: int
    expected_operation_projection_revision: int
    committed_operation_projection_revision: int
    expected_writer_head_digest: bytes | None
    observed_writer_head_digest: bytes | None
    new_artifact_ids: tuple[UUID, ...]
    new_open_plan_set_digest: bytes
    committed_at: datetime
    committed_operation_state: str = "ADMITTED"

    def __post_init__(self) -> None:
        require_uuid(self.recovery_receipt_id, "recovery_receipt_id")
        for name in (
            "effect_key",
            "recovery_effect_key",
            "operation_key",
            "old_artifact_set_digest",
            "new_open_plan_set_digest",
        ):
            require_digest(getattr(self, name), name)
        _validate_uuid_sequence(self.abandoned_artifact_ids, "abandoned_artifact_ids")
        _validate_uuid_sequence(self.new_artifact_ids, "new_artifact_ids")
        if not set(self.abandoned_artifact_ids).isdisjoint(self.new_artifact_ids):
            raise MssqlR1V3ContractError("OPEN recovery old and new artifact IDs must be disjoint")
        require_positive(self.expected_operation_epoch, "expected_operation_epoch")
        require_positive(self.committed_operation_epoch, "committed_operation_epoch")
        require_positive(self.expected_operation_projection_revision, "expected_operation_projection_revision")
        require_positive(self.committed_operation_projection_revision, "committed_operation_projection_revision")
        if self.committed_operation_epoch != self.expected_operation_epoch + 1:
            raise MssqlR1V3ContractError("OPEN recovery operation epoch must advance exactly once")
        if self.committed_operation_projection_revision != self.expected_operation_projection_revision + 1:
            raise MssqlR1V3ContractError("OPEN recovery operation projection revision must advance exactly once")
        if (self.expected_writer_head_digest is None) != (self.observed_writer_head_digest is None):
            raise MssqlR1V3ContractError("OPEN recovery writer-head proof must be all-or-none")
        if self.expected_writer_head_digest is not None:
            require_digest(self.expected_writer_head_digest, "expected_writer_head_digest")
            require_digest(self.observed_writer_head_digest, "observed_writer_head_digest")
            if self.expected_writer_head_digest != self.observed_writer_head_digest:
                raise MssqlR1V3ContractError("OPEN recovery cannot change the business writer head")
        if self.recovery_effect_key != build_open_stage_recovery_effect_key(
            self.operation_key,
            self.effect_key,
            self.old_artifact_set_digest,
            self.expected_operation_epoch,
            self.expected_operation_projection_revision,
        ):
            raise MssqlR1V3ContractError("OPEN recovery key differs from transition identity")
        if self.committed_operation_state != "ADMITTED":
            raise MssqlR1V3ContractError("OPEN recovery must commit operation state ADMITTED")
        canonical_utc_text(self.committed_at, "committed_at")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OPEN_RECOVERY_RECEIPT_DOMAIN,
            (EFFECT_CONTRACT_VERSION, *(getattr(self, item.name) for item in fields(self))),
        )

    @property
    def receipt_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlOpenStageRecoveryReceiptV1:
        values = list(decode_canonical_bytes(payload, _OPEN_RECOVERY_RECEIPT_DOMAIN, field_count=17))
        if values[0] != EFFECT_CONTRACT_VERSION:
            raise MssqlR1V3ContractError("OPEN recovery receipt uses an unknown contract version")
        values[15] = parse_canonical_utc_text(values[15], "committed_at")
        return cls(*values[1:])  # type: ignore[arg-type]


def _validate_uuid_sequence(values: tuple[UUID, ...], field: str) -> None:
    if not isinstance(values, tuple) or not values or len(set(values)) != len(values):
        raise MssqlR1V3ContractError(f"{field} must be a non-empty unique ordered UUID tuple")
    for value in values:
        require_uuid(value, field)


__all__ = [
    "MssqlOpenStageRecoveryReceiptV1",
    "MssqlR1ControlOperationV1",
    "MssqlR1ControlReceiptV1",
    "MssqlR1V3ContractError",
    "MssqlTargetPhysicalIdentityV1",
    "build_control_effect_key",
    "build_open_stage_recovery_effect_key",
    "canonical_bytes",
    "canonical_utc_text",
    "decode_canonical_bytes",
    "expect_bytes",
    "require_canonical_text",
    "require_count",
    "require_digest",
    "require_identifier",
    "require_positive",
    "require_uuid",
]
