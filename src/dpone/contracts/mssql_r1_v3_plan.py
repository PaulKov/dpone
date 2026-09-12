"""Self-contained executable mutation plans for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    MssqlTargetPhysicalIdentityV1,
    canonical_bytes,
    canonical_digest,
    decode_canonical_bytes,
    expect_enum,
    expect_int,
    expect_text,
    expect_tuple,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
    writer_head_predecessor_digest,
)
from dpone.contracts.mssql_r1_v3_mutation import (
    R1MutationResourceLimitsV1,
    validate_mutation_plan_generation_coordinates,
)
from dpone.contracts.mssql_r1_v3_staging import R1SealedStageManifestV1, R1StageArtifactKindV1
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

_BATCH_PLAN_DOMAIN = b"dpone-r1-batch-mutation-plan-v1\0"
_XMIN_PLAN_DOMAIN = b"dpone-r1-xmin-mutation-plan-v1\0"
_TEMPLATE_SET_DOMAIN = b"dpone-r1-mutation-template-set-v1\0"
_RENDERER_CONTRACT_DOMAIN = b"dpone-r1-mutation-renderer-contract-v1\0"


class R1MutationStepV1(StrEnum):
    BATCH_PUBLICATION = "batch_publication"
    DELETE = "delete"
    UPDATE = "update"
    INSERT = "insert"
    ROW_HASH = "row_hash"
    CHECKPOINT = "checkpoint"
    QUALITY = "quality"


@dataclass(frozen=True, slots=True)
class R1MutationTemplateSetV1:
    source_mode: SourceMode
    template_set_id: str
    equality_policy_id: str
    ordered_steps: tuple[R1MutationStepV1, ...]
    renderer_contract_version: str = "dpone-mssql-r1-renderer-contract-v1"

    def __post_init__(self) -> None:
        expected = {
            SourceMode.BATCH_FULL_REFRESH: (
                "dpone-mssql-r1-batch-template-set-v1",
                "dpone-replace-equality-v1",
                (R1MutationStepV1.BATCH_PUBLICATION, R1MutationStepV1.ROW_HASH, R1MutationStepV1.QUALITY),
            ),
            SourceMode.XMIN_CURRENT_STATE: (
                "dpone-mssql-r1-xmin-template-set-v1",
                "dpone-null-safe-equality-v1",
                (
                    R1MutationStepV1.DELETE,
                    R1MutationStepV1.UPDATE,
                    R1MutationStepV1.INSERT,
                    R1MutationStepV1.ROW_HASH,
                    R1MutationStepV1.CHECKPOINT,
                    R1MutationStepV1.QUALITY,
                ),
            ),
        }
        if not isinstance(self.source_mode, SourceMode) or self.source_mode not in expected:
            raise MssqlR1V3ContractError("mutation template source mode is unsupported")
        if (self.template_set_id, self.equality_policy_id, self.ordered_steps) != expected[self.source_mode]:
            raise MssqlR1V3ContractError("mutation template set is not the closed R1 sequence")
        if self.renderer_contract_version != "dpone-mssql-r1-renderer-contract-v1":
            raise MssqlR1V3ContractError("mutation renderer contract version is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _TEMPLATE_SET_DOMAIN,
            (
                self.source_mode,
                self.template_set_id,
                self.equality_policy_id,
                self.ordered_steps,
                self.renderer_contract_version,
            ),
        )

    @property
    def renderer_contract_digest(self) -> bytes:
        return canonical_digest(_RENDERER_CONTRACT_DOMAIN, (self.renderer_contract_version, self.canonical_bytes))

    @classmethod
    def batch_v1(cls) -> R1MutationTemplateSetV1:
        return cls(
            SourceMode.BATCH_FULL_REFRESH,
            "dpone-mssql-r1-batch-template-set-v1",
            "dpone-replace-equality-v1",
            (R1MutationStepV1.BATCH_PUBLICATION, R1MutationStepV1.ROW_HASH, R1MutationStepV1.QUALITY),
        )

    @classmethod
    def xmin_v1(cls) -> R1MutationTemplateSetV1:
        return cls(
            SourceMode.XMIN_CURRENT_STATE,
            "dpone-mssql-r1-xmin-template-set-v1",
            "dpone-null-safe-equality-v1",
            (
                R1MutationStepV1.DELETE,
                R1MutationStepV1.UPDATE,
                R1MutationStepV1.INSERT,
                R1MutationStepV1.ROW_HASH,
                R1MutationStepV1.CHECKPOINT,
                R1MutationStepV1.QUALITY,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1MutationTemplateSetV1:
        values = list(decode_canonical_bytes(payload, _TEMPLATE_SET_DOMAIN, field_count=5))
        values[0] = expect_enum(SourceMode, values[0], "source_mode")
        values[3] = tuple(
            expect_enum(R1MutationStepV1, item, "mutation_step") for item in expect_tuple(values[3], "ordered_steps")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class R1BatchMutationPlanV1:
    effect_key: bytes
    stage_manifest: R1SealedStageManifestV1
    target_identity: MssqlTargetPhysicalIdentityV1
    ordered_target_columns: tuple[str, ...]
    type_policy_digest: bytes
    hash_policy_digest: bytes
    probe_contract_digest: bytes
    template_set: R1MutationTemplateSetV1
    resource_limits: R1MutationResourceLimitsV1
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    predecessor_receipt_id: UUID | None = None
    predecessor_receipt_digest: bytes | None = None
    expected_writer_head_digest: bytes | None = None
    codec_version: str = "dpone-r1-batch-mutation-plan-v1"

    def __post_init__(self) -> None:
        _validate_common(self, R1StageArtifactKindV1.BATCH_PAYLOAD, SourceMode.BATCH_FULL_REFRESH, False)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _BATCH_PLAN_DOMAIN,
            (
                self.codec_version,
                self.effect_key,
                self.stage_manifest.canonical_bytes,
                self.target_identity.canonical_bytes,
                self.ordered_target_columns,
                self.type_policy_digest,
                self.hash_policy_digest,
                self.probe_contract_digest,
                self.template_set.canonical_bytes,
                self.resource_limits.canonical_values,
                self.expected_writer_generation,
                self.candidate_writer_generation,
                self.expected_head_revision,
                self.candidate_head_revision,
                self.predecessor_receipt_id,
                self.predecessor_receipt_digest,
                self.expected_writer_head_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1BatchMutationPlanV1:
        values = list(decode_canonical_bytes(payload, _BATCH_PLAN_DOMAIN, field_count=17))
        values[2] = R1SealedStageManifestV1.from_canonical_bytes(values[2])  # type: ignore[arg-type]
        values[3] = MssqlTargetPhysicalIdentityV1.from_canonical_bytes(values[3])  # type: ignore[arg-type]
        values[8] = R1MutationTemplateSetV1.from_canonical_bytes(values[8])  # type: ignore[arg-type]
        limits = expect_tuple(values[9], "resource_limits", size=4)
        values[9] = R1MutationResourceLimitsV1(*(expect_int(item, "resource_limit") for item in limits))
        return cls(*values[1:], codec_version=expect_text(values[0], "codec_version"))  # type: ignore[arg-type,misc]


@dataclass(frozen=True, slots=True)
class R1XminMutationPlanV1:
    effect_key: bytes
    delta_manifest: R1SealedStageManifestV1
    complete_keys_manifest: R1SealedStageManifestV1
    target_identity: MssqlTargetPhysicalIdentityV1
    ordered_target_columns: tuple[str, ...]
    type_policy_digest: bytes
    hash_policy_digest: bytes
    probe_contract_digest: bytes
    checkpoint_state_key_digest: bytes
    checkpoint_writer_generation: int
    previous_checkpoint_revision: int | None
    previous_checkpoint_payload: bytes | None
    previous_checkpoint_value: int | None
    candidate_checkpoint_revision: int
    checkpoint_candidate_payload: bytes
    checkpoint_candidate_value: int
    template_set: R1MutationTemplateSetV1
    resource_limits: R1MutationResourceLimitsV1
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    predecessor_receipt_id: UUID | None = None
    predecessor_receipt_digest: bytes | None = None
    expected_writer_head_digest: bytes | None = None
    codec_version: str = "dpone-r1-xmin-mutation-plan-v1"

    def __post_init__(self) -> None:
        _validate_common(self, R1StageArtifactKindV1.XMIN_DELTA, SourceMode.XMIN_CURRENT_STATE, True)
        if self.complete_keys_manifest.artifact_kind is not R1StageArtifactKindV1.XMIN_COMPLETE_KEYS:
            raise MssqlR1V3ContractError("XMin plan requires an exact complete-key manifest")
        delta_key = tuple(item for item in self.delta_manifest.ordered_business_columns if item.is_business_key)
        if self.complete_keys_manifest.ordered_business_columns != delta_key:
            raise MssqlR1V3ContractError("XMin complete-key mapping differs from delta key")
        shared = ("schema_digest", "catalog_contract_digest", "permission_contract_digest", "type_policy_digest")
        if any(getattr(self.delta_manifest, name) != getattr(self.complete_keys_manifest, name) for name in shared):
            raise MssqlR1V3ContractError("XMin manifests have different schema/type/catalog authority")
        require_digest(self.checkpoint_state_key_digest, "checkpoint_state_key_digest")
        if require_positive(self.checkpoint_writer_generation, "checkpoint_writer_generation") != (
            self.candidate_writer_generation
        ):
            raise MssqlR1V3ContractError("checkpoint writer generation differs from candidate generation")
        expected = (
            1
            if self.previous_checkpoint_revision is None
            else require_positive(self.previous_checkpoint_revision, "previous_checkpoint_revision") + 1
        )
        if require_positive(self.candidate_checkpoint_revision, "candidate_checkpoint_revision") != expected:
            raise MssqlR1V3ContractError("XMin checkpoint revision must be adjacent")
        previous_absent = self.previous_checkpoint_revision is None
        if previous_absent != (self.previous_checkpoint_payload is None) or previous_absent != (
            self.previous_checkpoint_value is None
        ):
            raise MssqlR1V3ContractError("XMin predecessor checkpoint pointer must be all-or-none")
        if self.previous_checkpoint_payload is not None and not self.previous_checkpoint_payload:
            raise MssqlR1V3ContractError("XMin predecessor checkpoint payload must be exact non-empty bytes")
        if self.previous_checkpoint_value is not None:
            require_count(self.previous_checkpoint_value, "previous_checkpoint_value")
        if not isinstance(self.checkpoint_candidate_payload, bytes) or not self.checkpoint_candidate_payload:
            raise MssqlR1V3ContractError("XMin checkpoint candidate payload must be exact non-empty bytes")
        require_count(self.checkpoint_candidate_value, "checkpoint_candidate_value")
        if (
            self.previous_checkpoint_value is not None
            and self.checkpoint_candidate_value < self.previous_checkpoint_value
        ):
            raise MssqlR1V3ContractError("XMin checkpoint candidate cannot regress")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _XMIN_PLAN_DOMAIN,
            (
                self.codec_version,
                self.effect_key,
                self.delta_manifest.canonical_bytes,
                self.complete_keys_manifest.canonical_bytes,
                self.target_identity.canonical_bytes,
                self.ordered_target_columns,
                self.type_policy_digest,
                self.hash_policy_digest,
                self.probe_contract_digest,
                self.checkpoint_state_key_digest,
                self.checkpoint_writer_generation,
                self.previous_checkpoint_revision,
                self.previous_checkpoint_payload,
                self.previous_checkpoint_value,
                self.candidate_checkpoint_revision,
                self.checkpoint_candidate_payload,
                self.checkpoint_candidate_value,
                self.template_set.canonical_bytes,
                self.resource_limits.canonical_values,
                self.expected_writer_generation,
                self.candidate_writer_generation,
                self.expected_head_revision,
                self.candidate_head_revision,
                self.predecessor_receipt_id,
                self.predecessor_receipt_digest,
                self.expected_writer_head_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> R1XminMutationPlanV1:
        values = list(decode_canonical_bytes(payload, _XMIN_PLAN_DOMAIN, field_count=26))
        values[2] = R1SealedStageManifestV1.from_canonical_bytes(values[2])  # type: ignore[arg-type]
        values[3] = R1SealedStageManifestV1.from_canonical_bytes(values[3])  # type: ignore[arg-type]
        values[4] = MssqlTargetPhysicalIdentityV1.from_canonical_bytes(values[4])  # type: ignore[arg-type]
        values[17] = R1MutationTemplateSetV1.from_canonical_bytes(values[17])  # type: ignore[arg-type]
        limits = expect_tuple(values[18], "resource_limits", size=4)
        values[18] = R1MutationResourceLimitsV1(*(expect_int(item, "resource_limit") for item in limits))
        return cls(*values[1:], codec_version=expect_text(values[0], "codec_version"))  # type: ignore[arg-type,misc]


MutationPlanV1 = R1BatchMutationPlanV1 | R1XminMutationPlanV1


def _validate_common(
    plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1,
    kind: R1StageArtifactKindV1,
    expected_mode: SourceMode,
    allow_same_generation: bool,
) -> None:
    for name in ("effect_key", "type_policy_digest", "hash_policy_digest", "probe_contract_digest"):
        require_digest(getattr(plan, name), name)
    manifest = plan.stage_manifest if isinstance(plan, R1BatchMutationPlanV1) else plan.delta_manifest
    if manifest.artifact_kind is not kind or not isinstance(plan.target_identity, MssqlTargetPhysicalIdentityV1):
        raise MssqlR1V3ContractError("mutation plan has an invalid stage/target identity")
    _validate_target_columns(plan.ordered_target_columns)
    if tuple(item.target_column for item in manifest.ordered_business_columns) != plan.ordered_target_columns:
        raise MssqlR1V3ContractError("mutation plan target columns differ from stage mapping")
    if manifest.type_policy_digest != plan.type_policy_digest:
        raise MssqlR1V3ContractError("mutation plan type policy differs from stage manifest")
    if not isinstance(plan.template_set, R1MutationTemplateSetV1) or plan.template_set.source_mode is not expected_mode:
        raise MssqlR1V3ContractError("mutation plan template set differs from source mode")
    if not isinstance(plan.resource_limits, R1MutationResourceLimitsV1):
        raise MssqlR1V3ContractError("mutation resource limits are invalid")
    if plan.codec_version != f"dpone-r1-{'xmin' if allow_same_generation else 'batch'}-mutation-plan-v1":
        raise MssqlR1V3ContractError("mutation plan codec is unsupported")
    validate_mutation_plan_generation_coordinates(
        plan.expected_writer_generation,
        plan.candidate_writer_generation,
        plan.expected_head_revision,
        plan.candidate_head_revision,
        allow_same_generation=allow_same_generation,
    )
    predecessor = (plan.predecessor_receipt_id, plan.predecessor_receipt_digest, plan.expected_writer_head_digest)
    if plan.expected_writer_generation is None:
        if predecessor != (None, None, None):
            raise MssqlR1V3ContractError("initial mutation plan cannot name a predecessor head")
    elif any(value is None for value in predecessor):
        raise MssqlR1V3ContractError("mutation plan predecessor head proof is partial")
    else:
        receipt_id, receipt_digest, _ = predecessor
        assert receipt_id is not None and receipt_digest is not None and plan.expected_head_revision is not None
        if plan.expected_writer_head_digest != writer_head_predecessor_digest(
            plan.target_identity,
            plan.expected_writer_generation,
            plan.expected_head_revision,
            receipt_id,
            receipt_digest,
        ):
            raise MssqlR1V3ContractError("mutation plan predecessor head digest is invalid")


def _validate_target_columns(columns: tuple[str, ...]) -> None:
    if not isinstance(columns, tuple) or not columns or len(set(columns)) != len(columns):
        raise MssqlR1V3ContractError("ordered target columns must be a non-empty unique tuple")
    for index, column in enumerate(columns):
        require_identifier(column, f"ordered_target_columns[{index}]")


__all__ = [
    "MutationPlanV1",
    "R1BatchMutationPlanV1",
    "R1MutationStepV1",
    "R1MutationTemplateSetV1",
    "R1XminMutationPlanV1",
    "writer_head_predecessor_digest",
]
