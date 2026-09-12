"""Generation-authority and recoverable mutation contracts for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields

from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthorityPurposeV2,
    MssqlGenerationAuthorityRefV2,
    MssqlGenerationAuthoritySetV2,
)
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bool,
    expect_enum,
    require_count,
    require_digest,
    require_positive,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode


@dataclass(frozen=True, slots=True)
class R1MutationResourceLimitsV1:
    maximum_rows: int
    maximum_payload_bytes: int
    maximum_target_log_bytes: int
    maximum_apply_seconds: int

    def __post_init__(self) -> None:
        for item in fields(self):
            require_positive(getattr(self, item.name), item.name)

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return tuple(getattr(self, item.name) for item in fields(self))


@dataclass(frozen=True, slots=True)
class MssqlBatchGenerationTransitionV3:
    """Validate one of the six approved Batch generation/authority cells."""

    effect_key: bytes
    previous_writer_mode: SourceMode | None
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    payload_row_count: int
    source_snapshot_digest: bytes
    artifact_set_digest: bytes
    mutation_plan_digest: bytes
    recovery_identity_digest: bytes
    authority_set: MssqlGenerationAuthoritySetV2
    explicit_rebaseline: bool = False

    def __post_init__(self) -> None:
        for name in (
            "effect_key",
            "source_snapshot_digest",
            "artifact_set_digest",
            "mutation_plan_digest",
            "recovery_identity_digest",
        ):
            require_digest(getattr(self, name), name)
        require_count(self.payload_row_count, "payload_row_count")
        if not isinstance(self.authority_set, MssqlGenerationAuthoritySetV2):
            raise MssqlR1V3ContractError("Batch authority set is invalid")
        if not isinstance(self.explicit_rebaseline, bool):
            raise MssqlR1V3ContractError("explicit_rebaseline must be boolean")
        self._validate_transition()
        if self.authority_set.purposes != self.required_purposes:
            raise MssqlR1V3ContractError("Batch generation authority set differs from the closed matrix")
        for authority in self.authority_set.refs:
            expected = (
                self.effect_key,
                self.source_snapshot_digest,
                self.artifact_set_digest,
                self.mutation_plan_digest,
                self.expected_writer_generation,
                self.candidate_writer_generation,
                self.expected_head_revision,
                self.candidate_head_revision,
                self.recovery_identity_digest,
            )
            if authority.canonical_values[2:11] != expected:
                raise MssqlR1V3ContractError("Batch authority does not bind the exact transition")

    def _validate_transition(self) -> None:
        require_positive(self.candidate_writer_generation, "candidate_writer_generation")
        require_positive(self.candidate_head_revision, "candidate_head_revision")
        predecessor_absent = self.expected_writer_generation is None and self.expected_head_revision is None
        if predecessor_absent:
            if self.explicit_rebaseline:
                raise MssqlR1V3ContractError("initial Batch transition cannot be an explicit rebaseline")
            if self.previous_writer_mode is not None or (
                self.candidate_writer_generation,
                self.candidate_head_revision,
            ) != (1, 1):
                raise MssqlR1V3ContractError("initial Batch transition must begin at generation 1 revision 1")
            return
        if self.expected_writer_generation is None or self.expected_head_revision is None:
            raise MssqlR1V3ContractError("Batch predecessor identity must be all-or-none")
        if self.previous_writer_mode not in {SourceMode.BATCH_FULL_REFRESH, SourceMode.XMIN_CURRENT_STATE}:
            raise MssqlR1V3ContractError("Batch predecessor writer mode is required")
        if self.previous_writer_mode is SourceMode.XMIN_CURRENT_STATE and self.explicit_rebaseline:
            raise MssqlR1V3ContractError("XMin-to-Batch handoff has one non-explicit encoding")
        require_positive(self.expected_writer_generation, "expected_writer_generation")
        require_positive(self.expected_head_revision, "expected_head_revision")
        if self.candidate_writer_generation != self.expected_writer_generation + 1:
            raise MssqlR1V3ContractError("Batch must advance an adjacent content generation")
        if self.candidate_head_revision != 1:
            raise MssqlR1V3ContractError("Batch generation must begin at head revision 1")

    @property
    def required_purposes(self) -> tuple[MssqlGenerationAuthorityPurposeV2, ...]:
        required: list[MssqlGenerationAuthorityPurposeV2] = []
        if self.expected_writer_generation is None:
            required.append(MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER)
        elif self.previous_writer_mode is SourceMode.XMIN_CURRENT_STATE or self.explicit_rebaseline:
            required.append(MssqlGenerationAuthorityPurposeV2.REBASELINE)
        if self.payload_row_count == 0:
            required.append(MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH)
        return tuple(required)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-batch-generation-transition-v3\0",
            (
                self.effect_key,
                self.previous_writer_mode,
                self.expected_writer_generation,
                self.candidate_writer_generation,
                self.expected_head_revision,
                self.candidate_head_revision,
                self.payload_row_count,
                self.source_snapshot_digest,
                self.artifact_set_digest,
                self.mutation_plan_digest,
                self.recovery_identity_digest,
                self.authority_set.canonical_bytes,
                self.explicit_rebaseline,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlBatchGenerationTransitionV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-batch-generation-transition-v3\0", field_count=13))
        values[1] = _source_mode(values[1])
        values[11] = MssqlGenerationAuthoritySetV2.from_canonical_bytes(values[11])  # type: ignore[arg-type]
        values[12] = expect_bool(values[12], "explicit_rebaseline")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlXminGenerationTransitionV3:
    """Validate initial, handoff, rebaseline, ordinary and empty XMin authority."""

    effect_key: bytes
    previous_writer_mode: SourceMode | None
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    complete_key_count: int
    source_snapshot_digest: bytes
    artifact_set_digest: bytes
    mutation_plan_digest: bytes
    recovery_identity_digest: bytes
    authority_set: MssqlGenerationAuthoritySetV2
    explicit_rebaseline: bool = False

    def __post_init__(self) -> None:
        for name in (
            "effect_key",
            "source_snapshot_digest",
            "artifact_set_digest",
            "mutation_plan_digest",
            "recovery_identity_digest",
        ):
            require_digest(getattr(self, name), name)
        require_count(self.complete_key_count, "complete_key_count")
        if not isinstance(self.authority_set, MssqlGenerationAuthoritySetV2):
            raise MssqlR1V3ContractError("XMin authority set is invalid")
        if not isinstance(self.explicit_rebaseline, bool):
            raise MssqlR1V3ContractError("explicit_rebaseline must be boolean")
        self._validate_transition()
        if self.authority_set.purposes != self.required_purposes:
            raise MssqlR1V3ContractError("XMin generation authority set differs from the closed matrix")
        expected = (
            self.effect_key,
            self.source_snapshot_digest,
            self.artifact_set_digest,
            self.mutation_plan_digest,
            self.expected_writer_generation,
            self.candidate_writer_generation,
            self.expected_head_revision,
            self.candidate_head_revision,
            self.recovery_identity_digest,
        )
        if any(authority.canonical_values[2:11] != expected for authority in self.authority_set.refs):
            raise MssqlR1V3ContractError("XMin authority does not bind the exact transition")

    def _validate_transition(self) -> None:
        require_positive(self.candidate_writer_generation, "candidate_writer_generation")
        require_positive(self.candidate_head_revision, "candidate_head_revision")
        if self.expected_writer_generation is None and self.expected_head_revision is None:
            if self.explicit_rebaseline:
                raise MssqlR1V3ContractError("initial XMin transition cannot be an explicit rebaseline")
            if self.previous_writer_mode is not None or (
                self.candidate_writer_generation,
                self.candidate_head_revision,
            ) != (1, 1):
                raise MssqlR1V3ContractError("initial XMin transition must begin at generation 1 revision 1")
            return
        if self.expected_writer_generation is None or self.expected_head_revision is None:
            raise MssqlR1V3ContractError("XMin predecessor identity must be all-or-none")
        require_positive(self.expected_writer_generation, "expected_writer_generation")
        require_positive(self.expected_head_revision, "expected_head_revision")
        rebaseline = self.previous_writer_mode is SourceMode.BATCH_FULL_REFRESH or self.explicit_rebaseline
        if self.previous_writer_mode not in {SourceMode.BATCH_FULL_REFRESH, SourceMode.XMIN_CURRENT_STATE}:
            raise MssqlR1V3ContractError("XMin predecessor writer mode is required")
        if self.previous_writer_mode is SourceMode.BATCH_FULL_REFRESH and self.explicit_rebaseline:
            raise MssqlR1V3ContractError("Batch-to-XMin handoff has one non-explicit encoding")
        expected_candidate = (
            (self.expected_writer_generation + 1, 1)
            if rebaseline
            else (self.expected_writer_generation, self.expected_head_revision + 1)
        )
        if (self.candidate_writer_generation, self.candidate_head_revision) != expected_candidate:
            raise MssqlR1V3ContractError("XMin transition is not adjacent")

    @property
    def required_purposes(self) -> tuple[MssqlGenerationAuthorityPurposeV2, ...]:
        required: list[MssqlGenerationAuthorityPurposeV2] = []
        if self.expected_writer_generation is None:
            required.append(MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER)
        elif self.previous_writer_mode is SourceMode.BATCH_FULL_REFRESH or self.explicit_rebaseline:
            required.append(MssqlGenerationAuthorityPurposeV2.REBASELINE)
        if self.complete_key_count == 0:
            required.append(MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH)
        return tuple(required)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-r1-xmin-generation-transition-v3\0",
            (
                self.effect_key,
                self.previous_writer_mode,
                self.expected_writer_generation,
                self.candidate_writer_generation,
                self.expected_head_revision,
                self.candidate_head_revision,
                self.complete_key_count,
                self.source_snapshot_digest,
                self.artifact_set_digest,
                self.mutation_plan_digest,
                self.recovery_identity_digest,
                self.authority_set.canonical_bytes,
                self.explicit_rebaseline,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlXminGenerationTransitionV3:
        values = list(decode_canonical_bytes(payload, b"dpone-r1-xmin-generation-transition-v3\0", field_count=13))
        values[1] = _source_mode(values[1])
        values[11] = MssqlGenerationAuthoritySetV2.from_canonical_bytes(values[11])  # type: ignore[arg-type]
        values[12] = expect_bool(values[12], "explicit_rebaseline")
        return cls(*values)  # type: ignore[arg-type]


def validate_mutation_plan_generation_coordinates(
    expected_generation: int | None,
    candidate_generation: int,
    expected_head_revision: int | None,
    candidate_head_revision: int,
    *,
    allow_same_generation: bool,
) -> None:
    """Validate the two closed content/head transitions available to mutation plans."""

    require_positive(candidate_generation, "candidate_writer_generation")
    require_positive(candidate_head_revision, "candidate_head_revision")
    if expected_generation is None and expected_head_revision is None:
        if (candidate_generation, candidate_head_revision) != (1, 1):
            raise MssqlR1V3ContractError("initial mutation plan must begin at generation 1 revision 1")
        return
    if expected_generation is None or expected_head_revision is None:
        raise MssqlR1V3ContractError("mutation-plan predecessor identity must be all-or-none")
    require_positive(expected_generation, "expected_writer_generation")
    require_positive(expected_head_revision, "expected_head_revision")
    next_generation = candidate_generation == expected_generation + 1 and candidate_head_revision == 1
    same_generation = (
        allow_same_generation
        and candidate_generation == expected_generation
        and candidate_head_revision == expected_head_revision + 1
    )
    if not (next_generation or same_generation):
        raise MssqlR1V3ContractError("mutation-plan generation transition must be adjacent")


def _source_mode(value: object) -> SourceMode | None:
    return None if value is None else expect_enum(SourceMode, value, "previous_writer_mode")


__all__ = [
    "MssqlBatchGenerationTransitionV3",
    "MssqlGenerationAuthorityPurposeV2",
    "MssqlGenerationAuthorityRefV2",
    "MssqlGenerationAuthoritySetV2",
    "MssqlXminGenerationTransitionV3",
    "R1MutationResourceLimitsV1",
    "validate_mutation_plan_generation_coordinates",
]
