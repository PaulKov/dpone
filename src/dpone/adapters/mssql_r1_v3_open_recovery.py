"""Typed adapter boundary for target-only recovery of expired OPEN stages.

The shared V3 authority schema is intentionally not duplicated here. Its
backend owns the SERIALIZABLE physical/binding/operation/artifact lock order,
receipt and sealed-intent absence proofs, loader quiescence, stage abandonment,
operation/head CAS and immutable receipt persistence in one transaction.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlOpenStageRecoveryFreshProofV1,
    MssqlOpenStageRecoveryReceiptV1,
    MssqlR1V3ContractError,
    build_open_stage_recovery_effect_key,
    canonical_utc_text,
    require_digest,
    require_identifier,
    require_positive,
)

from .mssql_r1_v3_stage_schema import open_stage_plan_set_digest, render_stage_object_plan

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_staging import R1OpenStagePlanV1


class MssqlR1V3OpenStageRecoveryError(RuntimeError):
    """Expired OPEN state is not safely reusable or its outcome is ambiguous."""


def recovery_artifact_ids_are_disjoint(receipt: MssqlOpenStageRecoveryReceiptV1) -> bool:
    """Return whether an OPEN recovery receipt cannot reuse abandoned IDs."""

    return set(receipt.abandoned_artifact_ids).isdisjoint(receipt.new_artifact_ids)


@dataclass(frozen=True, slots=True)
class MssqlR1V3OpenStageRecoveryCandidate:
    """Fresh target observation used only as an optimistic CAS predecessor."""

    operation_key: bytes
    effect_key: bytes
    old_artifact_set_digest: bytes
    expected_operation_epoch: int
    expected_operation_projection_revision: int
    expected_writer_head_digest: bytes | None
    old_plans: tuple[R1OpenStagePlanV1, ...]
    observed_at_server_time: datetime

    def __post_init__(self) -> None:
        for name in ("operation_key", "effect_key", "old_artifact_set_digest"):
            require_digest(getattr(self, name), name)
        require_positive(self.expected_operation_epoch, "expected_operation_epoch")
        require_positive(
            self.expected_operation_projection_revision,
            "expected_operation_projection_revision",
        )
        if self.expected_writer_head_digest is not None:
            require_digest(self.expected_writer_head_digest, "expected_writer_head_digest")
        canonical_utc_text(self.observed_at_server_time, "observed_at_server_time")
        if (
            not self.old_plans
            or any(
                plan.effect_key != self.effect_key or plan.owner_epoch != self.expected_operation_epoch
                for plan in self.old_plans
            )
            or len({plan.target_binding_uuid for plan in self.old_plans}) != 1
        ):
            raise MssqlR1V3ContractError("OPEN recovery plans differ from the expected effect/epoch")
        if open_stage_plan_set_digest(self.old_plans) != self.old_artifact_set_digest:
            raise MssqlR1V3ContractError("OPEN recovery plans differ from the expected artifact set")


@dataclass(frozen=True, slots=True)
class MssqlR1V3OpenStageRecoveryCommand:
    """Exact transition that a backend must commit atomically or not at all."""

    candidate: MssqlR1V3OpenStageRecoveryCandidate
    receipt: MssqlOpenStageRecoveryReceiptV1
    new_plans: tuple[R1OpenStagePlanV1, ...]
    stage_schema: str

    def __post_init__(self) -> None:
        require_identifier(self.stage_schema, "stage_schema")
        old_ids = tuple(plan.artifact_id for plan in self.candidate.old_plans)
        new_ids = tuple(plan.artifact_id for plan in self.new_plans)
        expected = (
            self.candidate.operation_key,
            self.candidate.effect_key,
            self.candidate.old_artifact_set_digest,
            old_ids,
            self.candidate.expected_operation_epoch,
            self.candidate.expected_operation_projection_revision,
            self.candidate.expected_writer_head_digest,
        )
        observed = (
            self.receipt.operation_key,
            self.receipt.effect_key,
            self.receipt.old_artifact_set_digest,
            self.receipt.abandoned_artifact_ids,
            self.receipt.expected_operation_epoch,
            self.receipt.expected_operation_projection_revision,
            self.receipt.expected_writer_head_digest,
        )
        if observed != expected:
            raise MssqlR1V3ContractError("OPEN recovery receipt differs from its CAS predecessor")
        if not set(old_ids).isdisjoint(new_ids):
            raise MssqlR1V3ContractError("OPEN recovery old and new artifact identities must be disjoint")
        if (
            new_ids != self.receipt.new_artifact_ids
            or any(
                plan.effect_key != self.receipt.effect_key
                or plan.owner_epoch != self.receipt.committed_operation_epoch
                or render_stage_object_plan(plan, self.stage_schema).ddl_digest != plan.exact_stage_ddl_digest
                for plan in self.new_plans
            )
            or open_stage_plan_set_digest(self.new_plans) != self.receipt.new_open_plan_set_digest
        ):
            raise MssqlR1V3ContractError("OPEN recovery successor plans differ from the exact receipt")


class MssqlR1V3OpenStageRecoveryBackend(Protocol):
    """Target-local authority owner; implementations must use fresh sessions."""

    def inspect_expired_open(
        self,
        operation_key: bytes,
        effect_key: bytes,
        old_artifact_set_digest: bytes,
    ) -> MssqlR1V3OpenStageRecoveryCandidate: ...

    def commit_recovery(
        self,
        command: MssqlR1V3OpenStageRecoveryCommand,
    ) -> MssqlOpenStageRecoveryFreshProofV1:
        """Atomically abandon, CAS +1, persist, commit, then return fresh proof."""
        ...

    def probe_recovery_fresh(self, recovery_effect_key: bytes) -> MssqlOpenStageRecoveryFreshProofV1 | None: ...


class MssqlR1V3OpenStageRecoveryAdapter:
    """Build and verify an adjacent recovery transition around an injected UoW."""

    def __init__(
        self,
        backend: MssqlR1V3OpenStageRecoveryBackend,
        *,
        stage_schema: str = "dpone_stage",
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._backend = backend
        self._stage_schema = require_identifier(stage_schema, "stage_schema")
        self._uuid = uuid_factory

    @property
    def stage_schema(self) -> str:
        return self._stage_schema

    def recover_expired_open(
        self,
        operation_key: bytes,
        effect_key: bytes,
        old_artifact_set_digest: bytes,
    ) -> MssqlOpenStageRecoveryReceiptV1:
        for name, value in (
            ("operation_key", operation_key),
            ("effect_key", effect_key),
            ("old_artifact_set_digest", old_artifact_set_digest),
        ):
            require_digest(value, name)
        candidate = self._backend.inspect_expired_open(operation_key, effect_key, old_artifact_set_digest)
        self._require_candidate_identity(candidate, operation_key, effect_key, old_artifact_set_digest)
        command = self._build_command(candidate)
        try:
            proof: MssqlOpenStageRecoveryFreshProofV1 | None = self._backend.commit_recovery(command)
        except Exception as exc:
            proof = self._backend.probe_recovery_fresh(command.receipt.recovery_effect_key)
            if proof is None:
                raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_OUTCOME_UNKNOWN") from exc
        if proof is None:
            raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_OUTCOME_UNKNOWN")
        self._require_exact_proof(command, proof)
        return proof.receipt

    def probe_recovery_fresh(self, recovery_effect_key: bytes) -> MssqlOpenStageRecoveryFreshProofV1 | None:
        require_digest(recovery_effect_key, "recovery_effect_key")
        proof = self._backend.probe_recovery_fresh(recovery_effect_key)
        if proof is not None and (
            proof.receipt.recovery_effect_key != recovery_effect_key
            or not recovery_artifact_ids_are_disjoint(proof.receipt)
        ):
            raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_PROOF_CONFLICT")
        return proof

    def _build_command(
        self,
        candidate: MssqlR1V3OpenStageRecoveryCandidate,
    ) -> MssqlR1V3OpenStageRecoveryCommand:
        new_ids = tuple(self._uuid() for _ in candidate.old_plans)
        old_ids = tuple(item.artifact_id for item in candidate.old_plans)
        if len(set(new_ids)) != len(new_ids) or not set(old_ids).isdisjoint(new_ids):
            raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_ARTIFACT_ID_CONFLICT")
        new_epoch = candidate.expected_operation_epoch + 1
        new_plans = self._new_plans(candidate.old_plans, new_epoch, new_ids)
        recovery_key = build_open_stage_recovery_effect_key(
            candidate.operation_key,
            candidate.effect_key,
            candidate.old_artifact_set_digest,
            candidate.expected_operation_epoch,
            candidate.expected_operation_projection_revision,
        )
        receipt = MssqlOpenStageRecoveryReceiptV1(
            self._uuid(),
            candidate.effect_key,
            recovery_key,
            candidate.operation_key,
            candidate.old_artifact_set_digest,
            tuple(item.artifact_id for item in candidate.old_plans),
            candidate.expected_operation_epoch,
            new_epoch,
            candidate.expected_operation_projection_revision,
            candidate.expected_operation_projection_revision + 1,
            candidate.expected_writer_head_digest,
            candidate.expected_writer_head_digest,
            new_ids,
            open_stage_plan_set_digest(new_plans),
            candidate.observed_at_server_time,
        )
        return MssqlR1V3OpenStageRecoveryCommand(candidate, receipt, new_plans, self._stage_schema)

    def _new_plans(
        self,
        old_plans: tuple[R1OpenStagePlanV1, ...],
        epoch: int,
        artifact_ids: tuple[UUID, ...],
    ) -> tuple[R1OpenStagePlanV1, ...]:
        result = []
        for old, artifact_id in zip(old_plans, artifact_ids, strict=True):
            provisional = replace(old, artifact_id=artifact_id, owner_epoch=epoch)
            rendered = render_stage_object_plan(provisional, self._stage_schema)
            result.append(replace(provisional, exact_stage_ddl_digest=rendered.ddl_digest))
        return tuple(result)

    def _require_candidate_identity(
        self,
        candidate: MssqlR1V3OpenStageRecoveryCandidate,
        operation_key: bytes,
        effect_key: bytes,
        old_artifact_set_digest: bytes,
    ) -> None:
        identity = (candidate.operation_key, candidate.effect_key, candidate.old_artifact_set_digest)
        expected = (operation_key, effect_key, old_artifact_set_digest)
        wrong_renderer = any(
            render_stage_object_plan(plan, self._stage_schema).ddl_digest != plan.exact_stage_ddl_digest
            for plan in candidate.old_plans
        )
        if identity != expected or wrong_renderer:
            raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_CANDIDATE_CONFLICT")

    @staticmethod
    def _require_exact_proof(
        command: MssqlR1V3OpenStageRecoveryCommand,
        proof: MssqlOpenStageRecoveryFreshProofV1,
    ) -> None:
        if not isinstance(proof, MssqlOpenStageRecoveryFreshProofV1) or proof.receipt != command.receipt:
            raise MssqlR1V3OpenStageRecoveryError("DPONE_POSTGRES_MSSQL_OPEN_RECOVERY_PROOF_CONFLICT")


def bind_open_stage_recovery(
    recovery: MssqlR1V3OpenStageRecoveryAdapter | None,
    backend: MssqlR1V3OpenStageRecoveryBackend | None,
    stage_schema: str,
) -> MssqlR1V3OpenStageRecoveryAdapter | None:
    """Bind recovery to the same renderer authority as its stage provider."""

    schema = require_identifier(stage_schema, "stage_schema")
    if recovery is not None and backend is not None:
        raise ValueError("pass recovery or recovery_backend, not both")
    if recovery is not None and recovery.stage_schema != schema:
        raise ValueError("recovery and provider must share one stage schema authority")
    return (
        recovery
        if recovery is not None
        else (None if backend is None else MssqlR1V3OpenStageRecoveryAdapter(backend, stage_schema=schema))
    )


__all__ = [
    "MssqlR1V3OpenStageRecoveryAdapter",
    "MssqlR1V3OpenStageRecoveryBackend",
    "MssqlR1V3OpenStageRecoveryCandidate",
    "MssqlR1V3OpenStageRecoveryCommand",
    "MssqlR1V3OpenStageRecoveryError",
    "bind_open_stage_recovery",
    "recovery_artifact_ids_are_disjoint",
]
