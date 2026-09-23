"""Exact restartable P10g retirement orchestration for a verified native chunk."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkDropIntent,
    NativeChunkDropObservation,
    NativeChunkDropOutcome,
    NativeChunkLifecyclePhase,
    NativeChunkRetirementAuthorizationObserver,
    NativeChunkRetirementEffects,
    NativeChunkRetirementProgress,
    NativeChunkRetirementReceipt,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    NativeChunkRetirementState,
    native_chunk_object_incarnation_digest,
)

_Result = TypeVar("_Result")


class NativeChunkRetirementService:
    """Resume only the missing suffix; uncertain remote outcomes retain custody."""

    def __init__(
        self,
        state: NativeChunkRetirementState,
        effects: NativeChunkRetirementEffects,
        authorizations: NativeChunkRetirementAuthorizationObserver,
    ) -> None:
        self._state = state
        self._effects = effects
        self._authorizations = authorizations

    def retire(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementReceipt:
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError("mssql_native.chunk_retirement_request_invalid")
        progress = self._checked(request, self._call(request, self._state.observe, request))
        if not progress.work_sealed:
            progress = self._checked(request, self._call(request, self._state.seal_work, request))
        if self._phase(progress) is NativeChunkLifecyclePhase.VERIFIED:
            progress = self._checked(request, self._call(request, self._state.require_containment, request))
        if progress.containment is None:
            proof = self._call(request, self._effects.prove_containment, request)
            progress = self._checked(request, self._call(request, self._state.record_containment, request, proof))
        if progress.reservation is None:
            reservation = NativeChunkRetirementReservation.deterministic(request)
            progress = self._checked(request, self._call(request, self._state.reserve_retirement, request, reservation))
        created_intent = False
        if progress.drop_intent is None:
            if progress.reservation is None:
                raise ValueError("mssql_native.chunk_retirement_progress_invalid")
            intent = NativeChunkDropIntent(
                progress.reservation.operation_sha256, 0, progress.reservation.operation_sha256
            )
            progress = self._checked(request, self._call(request, self._state.record_drop_intent, request, intent))
            created_intent = True
        if progress.drop is None and created_intent:
            progress = self._drop(request, progress)
        elif progress.drop is None or progress.drop.outcome is NativeChunkDropOutcome.UNKNOWN:
            progress = self._reconcile_drop(request, progress)
        if progress.drop is not None and progress.drop.outcome is NativeChunkDropOutcome.NO_EFFECT:
            if (
                progress.drop.effect_attempt != 0
                or progress.drop.observation is not NativeChunkDropObservation.RECONCILED
            ):
                raise RuntimeError("mssql_native.chunk_retirement_drop_not_succeeded")
            if progress.reservation is None:
                raise ValueError("mssql_native.chunk_retirement_progress_invalid")
            retry = NativeChunkDropIntent(progress.reservation.operation_sha256, 1, progress.drop.proof_sha256)
            progress = self._checked(request, self._call(request, self._state.record_drop_intent, request, retry))
            progress = self._drop(request, progress)
        if progress.drop is None or progress.drop.outcome in (
            NativeChunkDropOutcome.UNKNOWN,
            NativeChunkDropOutcome.NO_EFFECT,
        ):
            raise RuntimeError("mssql_native.chunk_retirement_drop_not_succeeded")
        if progress.settlement is None:
            progress = self._settle(request, progress)
        settlement = progress.settlement
        if settlement is None:
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        if settlement.outcome is not NativeChunkDropOutcome.SUCCEEDED:
            raise RuntimeError("mssql_native.chunk_retirement_drop_not_succeeded")
        if progress.absence is None:
            absence = self._call(request, self._effects.observe_absence, request)
            if (
                absence.object_incarnation_sha256 != native_chunk_object_incarnation_digest(request)
                or absence.settlement_sha256 != settlement.proof_sha256
                or not absence.absent
            ):
                raise RuntimeError("mssql_native.chunk_retirement_absence_unproven")
            progress = self._checked(request, self._call(request, self._state.record_retired, request, absence))
        if not progress.admission_closed:
            progress = self._checked(request, self._call(request, self._state.close_admission, request))
        if progress.capacity is None:
            capacity = self._call(request, self._effects.observe_capacity, request)
            if not capacity.released:
                raise RuntimeError("mssql_native.chunk_retirement_capacity_unreleased")
            progress = self._checked(request, self._call(request, self._state.record_capacity, request, capacity))
        self._authorize(request)
        return self._receipt(request, progress)

    def _drop(
        self, request: NativeChunkRetirementRequest, progress: NativeChunkRetirementProgress
    ) -> NativeChunkRetirementProgress:
        reservation = progress.reservation
        intent = progress.drop_intent
        if reservation is None or intent is None:
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        incarnation = self._call(request, self._effects.observe_exact_incarnation, request)
        if incarnation != native_chunk_object_incarnation_digest(request):
            raise ValueError("mssql_native.chunk_retirement_incarnation_mismatch")
        drop = self._call(request, self._effects.drop_exact, request, reservation, intent)
        if (
            drop.operation_sha256 != reservation.operation_sha256
            or drop.effect_attempt != intent.effect_attempt
            or drop.observation is not NativeChunkDropObservation.INITIAL
        ):
            raise ValueError("mssql_native.chunk_retirement_drop_binding")
        return self._checked(request, self._call(request, self._state.record_drop_outcome, request, drop))

    def _reconcile_drop(
        self, request: NativeChunkRetirementRequest, progress: NativeChunkRetirementProgress
    ) -> NativeChunkRetirementProgress:
        if progress.reservation is None or progress.drop_intent is None:
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        drop = self._call(
            request,
            self._effects.reconcile_drop,
            request,
            progress.reservation,
            progress.drop_intent,
            progress.drop,
        )
        if (
            drop.operation_sha256 != progress.reservation.operation_sha256
            or drop.effect_attempt != progress.drop_intent.effect_attempt
            or drop.observation is not NativeChunkDropObservation.RECONCILED
        ):
            raise ValueError("mssql_native.chunk_retirement_drop_binding")
        return self._checked(request, self._call(request, self._state.record_drop_outcome, request, drop))

    def _settle(
        self, request: NativeChunkRetirementRequest, progress: NativeChunkRetirementProgress
    ) -> NativeChunkRetirementProgress:
        drop = progress.drop
        if drop is None or drop.outcome in (NativeChunkDropOutcome.UNKNOWN, NativeChunkDropOutcome.NO_EFFECT):
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        reservation = progress.reservation
        if reservation is None:
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        object_digest = native_chunk_object_incarnation_digest(request)
        settlement = self._call(
            request,
            self._effects.settle_drop,
            request,
            reservation,
            drop,
            object_digest,
        )
        if (
            settlement.drop_proof_sha256 != drop.proof_sha256
            or settlement.object_incarnation_sha256 != object_digest
            or settlement.outcome is not drop.outcome
        ):
            raise ValueError("mssql_native.chunk_retirement_settlement_binding")
        return self._checked(request, self._call(request, self._state.record_settlement, request, settlement))

    def _authorize(self, request: NativeChunkRetirementRequest) -> None:
        if not self._authorizations.verify(request.projection, request.authorization):
            raise ValueError("mssql_native.chunk_retirement_authorization_unobserved")

    def _call(
        self,
        request: NativeChunkRetirementRequest,
        callback: Callable[..., _Result],
        *args: object,
    ) -> _Result:
        self._authorize(request)
        return callback(*args)

    @staticmethod
    def _checked(
        request: NativeChunkRetirementRequest, progress: NativeChunkRetirementProgress
    ) -> NativeChunkRetirementProgress:
        if type(progress) is not NativeChunkRetirementProgress:
            raise ValueError("mssql_native.chunk_retirement_progress_invalid")
        if progress.projection_sha256 != request.projection.projection_sha256:
            raise ValueError("mssql_native.chunk_retirement_projection_mismatch")
        if progress.parent_authority_digest != request.authority.digest:
            raise ValueError("mssql_native.chunk_retirement_authority_mismatch")
        if progress.authorization_sha256 != request.authorization.authorization_sha256:
            raise ValueError("mssql_native.chunk_retirement_authorization_mismatch")
        expected_phases = (
            NativeChunkLifecyclePhase.VERIFIED,
            NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED,
            NativeChunkLifecyclePhase.CONTAINED,
            NativeChunkLifecyclePhase.RETIREMENT_REQUIRED,
            NativeChunkLifecyclePhase.RETIRED,
        )
        phases = tuple(proof.phase for proof in progress.lifecycle)
        if phases != expected_phases[: len(phases)]:
            raise ValueError("mssql_native.chunk_retirement_lifecycle_mismatch")
        for index, proof in enumerate(progress.lifecycle):
            if (
                proof.projection_sha256 != request.projection.projection_sha256
                or proof.authorization_sha256 != request.authorization.authorization_sha256
                or proof.directory_key != request.projection.directory_key
                or proof.authority_digest != request.authority.digest
                or proof.authority_fence != request.authority.fence
                or (index and proof.lifecycle_revision <= progress.lifecycle[index - 1].lifecycle_revision)
                or (index and proof.directory_revision < progress.lifecycle[index - 1].directory_revision)
            ):
                raise ValueError("mssql_native.chunk_retirement_lifecycle_mismatch")
        phase = phases[-1]
        if progress.containment is not None and phase not in expected_phases[2:]:
            raise ValueError("mssql_native.chunk_retirement_lifecycle_mismatch")
        if progress.reservation is not None and phase not in expected_phases[3:]:
            raise ValueError("mssql_native.chunk_retirement_lifecycle_mismatch")
        expected_reservation = NativeChunkRetirementReservation.deterministic(request)
        reservation = progress.reservation
        if reservation is not None and reservation != expected_reservation:
            raise ValueError("mssql_native.chunk_retirement_reservation_mismatch")
        containment = progress.containment
        if containment is not None and (
            containment.projection_sha256 != request.projection.projection_sha256
            or containment.authorization_sha256 != request.authorization.authorization_sha256
            or containment.directory_key != request.projection.directory_key
            or containment.authority_digest != request.authority.digest
            or containment.authority_fence != request.authority.fence
            or containment.lifecycle_revision != progress.lifecycle[2].lifecycle_revision
            or containment.directory_revision != progress.lifecycle[2].directory_revision
        ):
            raise ValueError("mssql_native.chunk_retirement_containment_binding")
        intent = progress.drop_intent
        if intent is not None and (reservation is None or intent.operation_sha256 != reservation.operation_sha256):
            raise ValueError("mssql_native.chunk_retirement_drop_intent_mismatch")
        drop = progress.drop
        if drop is not None and (
            reservation is None
            or intent is None
            or drop.operation_sha256 != reservation.operation_sha256
            or drop.effect_attempt != intent.effect_attempt
        ):
            raise ValueError("mssql_native.chunk_retirement_drop_binding")
        settlement = progress.settlement
        object_digest = native_chunk_object_incarnation_digest(request)
        if settlement is not None and (
            drop is None
            or reservation is None
            or settlement.drop_proof_sha256 != drop.proof_sha256
            or settlement.object_incarnation_sha256 != object_digest
            or settlement.outcome is not drop.outcome
            or settlement.projection_sha256 != request.projection.projection_sha256
            or settlement.authorization_sha256 != request.authorization.authorization_sha256
            or settlement.operation_id != reservation.operation_id
            or settlement.operation_sha256 != reservation.operation_sha256
            or settlement.effect_attempt != drop.effect_attempt
            or settlement.directory_key != request.projection.directory_key
            or settlement.directory_revision != progress.lifecycle[3].directory_revision
            or settlement.authority_digest != request.authority.digest
            or settlement.authority_fence != request.authority.fence
        ):
            raise ValueError("mssql_native.chunk_retirement_settlement_binding")
        absence = progress.absence
        if absence is not None and (
            settlement is None
            or settlement.outcome is not NativeChunkDropOutcome.SUCCEEDED
            or absence.object_incarnation_sha256 != object_digest
            or absence.settlement_sha256 != settlement.proof_sha256
            or absence.directory_revision != progress.lifecycle[3].directory_revision
            or absence.lifecycle_revision != progress.lifecycle[3].lifecycle_revision
        ):
            raise ValueError("mssql_native.chunk_retirement_absence_binding")
        terminal = progress.terminal
        if terminal is not None and (
            reservation is None
            or absence is None
            or terminal.projection_sha256 != request.projection.projection_sha256
            or terminal.authorization_sha256 != request.authorization.authorization_sha256
            or terminal.parent_authority_digest != request.authority.digest
            or terminal.operation_sha256 != reservation.operation_sha256
            or terminal.absence_sha256 != absence.proof_sha256
            or terminal.directory_revision != progress.lifecycle[-1].directory_revision
            or terminal.lifecycle_revision != progress.lifecycle[-1].lifecycle_revision
            or terminal.lifecycle_proof_sha256 != progress.lifecycle[-1].proof_sha256
            or phase is not NativeChunkLifecyclePhase.RETIRED
        ):
            raise ValueError("mssql_native.chunk_retirement_terminal_binding")
        directory = progress.directory
        if directory is not None and (
            reservation is None
            or terminal is None
            or directory.projection_sha256 != request.projection.projection_sha256
            or directory.parent_authority_digest != request.authority.digest
            or directory.operation_sha256 != reservation.operation_sha256
            or directory.terminal_sha256 != terminal.proof_sha256
        ):
            raise ValueError("mssql_native.chunk_retirement_directory_binding")
        if progress.capacity is not None and (
            directory is None or progress.capacity.directory_sha256 != directory.proof_sha256
        ):
            raise ValueError("mssql_native.chunk_retirement_capacity_binding")
        return progress

    @staticmethod
    def _phase(progress: NativeChunkRetirementProgress) -> NativeChunkLifecyclePhase:
        return progress.lifecycle[-1].phase

    @staticmethod
    def _receipt(
        request: NativeChunkRetirementRequest, progress: NativeChunkRetirementProgress
    ) -> NativeChunkRetirementReceipt:
        if (
            progress.settlement is None
            or progress.absence is None
            or progress.terminal is None
            or progress.directory is None
            or progress.capacity is None
        ):
            raise ValueError("mssql_native.chunk_retirement_progress_incomplete")
        projection = request.projection
        return NativeChunkRetirementReceipt(
            ordinal=projection.attempt.ordinal,
            attempt_id=f"{projection.attempt.run_id}-{projection.attempt.ordinal}-{projection.attempt.attempt}",
            parent_authority_digest=request.authority.digest,
            object_incarnation_sha256=progress.settlement.object_incarnation_sha256,
            verification_receipt_sha256=request.parent_verification_sha256,
            implementation_sha256=projection.implementation_sha256,
            drop_operation_sha256=progress.settlement.proof_sha256,
            absence_sha256=progress.absence.proof_sha256,
            terminal_sha256=progress.terminal.proof_sha256,
            directory_sha256=progress.directory.proof_sha256,
            capacity_sha256=progress.capacity.proof_sha256,
        )


__all__ = ("NativeChunkRetirementService",)
