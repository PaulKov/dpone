"""CAS-backed P10g retirement progress adapter."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkAbsenceProof,
    NativeChunkCapacityProof,
    NativeChunkContainmentProof,
    NativeChunkDropIntent,
    NativeChunkDropProof,
    NativeChunkLifecyclePhase,
    NativeChunkRetirementProgress,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    NativeChunkSettlementProof,
    bind_native_chunk_closed_directory,
    bind_native_chunk_lifecycle,
    bind_native_chunk_retired_terminal,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkClosedDirectory as NativeChunkClosedDirectory,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkDropObservation as NativeChunkDropObservation,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkDropOutcome as NativeChunkDropOutcome,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkLifecycleProof as NativeChunkLifecycleProof,
)
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkRetiredTerminal as NativeChunkRetiredTerminal,
)

LoadProgress = Callable[[str], tuple[int, NativeChunkRetirementProgress]]
CompareSwapProgress = Callable[[str, int, NativeChunkRetirementProgress], bool]


class CasSqlClientNativeRetirementState:
    """Persist monotonic progress through an injected durable compare-and-swap."""

    def __init__(self, *, load: LoadProgress, compare_swap: CompareSwapProgress) -> None:
        self._load = load
        self._compare_swap = compare_swap

    def observe(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._read(request)[1]

    def seal_work(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._change(request, lambda value: value if value.work_sealed else replace(value, work_sealed=True))

    def require_containment(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._advance(request, NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED)

    def record_containment(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkContainmentProof
    ) -> NativeChunkRetirementProgress:
        def mutation(value: NativeChunkRetirementProgress) -> NativeChunkRetirementProgress:
            if value.containment is not None:
                if value.containment != proof:
                    raise ValueError("mssql_sqlclient.retirement_state_conflict")
                return value
            return replace(self._with_phase(request, value, NativeChunkLifecyclePhase.CONTAINED), containment=proof)

        return self._change(request, mutation)

    def reserve_retirement(
        self, request: NativeChunkRetirementRequest, reservation: NativeChunkRetirementReservation
    ) -> NativeChunkRetirementProgress:
        def mutation(value: NativeChunkRetirementProgress) -> NativeChunkRetirementProgress:
            if value.reservation is not None:
                if value.reservation != reservation:
                    raise ValueError("mssql_sqlclient.retirement_state_conflict")
                return value
            return replace(
                self._with_phase(request, value, NativeChunkLifecyclePhase.RETIREMENT_REQUIRED),
                reservation=reservation,
            )

        return self._change(request, mutation)

    def record_drop_intent(
        self, request: NativeChunkRetirementRequest, intent: NativeChunkDropIntent
    ) -> NativeChunkRetirementProgress:
        return self._change(request, lambda value: replace(value, drop_intent=intent, drop=None))

    def record_drop_outcome(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkDropProof
    ) -> NativeChunkRetirementProgress:
        return self._change(request, lambda value: replace(value, drop=proof))

    def record_settlement(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkSettlementProof
    ) -> NativeChunkRetirementProgress:
        return self._change(request, lambda value: replace(value, settlement=proof))

    def record_retired(
        self, request: NativeChunkRetirementRequest, absence: NativeChunkAbsenceProof
    ) -> NativeChunkRetirementProgress:
        def mutation(value: NativeChunkRetirementProgress) -> NativeChunkRetirementProgress:
            if value.absence is not None:
                if value.absence != absence:
                    raise ValueError("mssql_sqlclient.retirement_state_conflict")
                return value
            advanced = self._with_phase(request, value, NativeChunkLifecyclePhase.RETIRED)
            reservation = advanced.reservation
            if reservation is None:
                raise ValueError("mssql_sqlclient.retirement_state_prefix")
            lifecycle = advanced.lifecycle[-1]
            terminal = bind_native_chunk_retired_terminal(
                projection_sha256=request.projection.projection_sha256,
                authorization_sha256=request.authorization.authorization_sha256,
                parent_authority_digest=request.authority.digest,
                operation_sha256=reservation.operation_sha256,
                absence_sha256=absence.proof_sha256,
                directory_revision=lifecycle.directory_revision,
                lifecycle_revision=lifecycle.lifecycle_revision,
                lifecycle_proof_sha256=lifecycle.proof_sha256,
            )
            return replace(advanced, absence=absence, terminal=terminal)

        return self._change(request, mutation)

    def close_admission(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        def mutation(value: NativeChunkRetirementProgress) -> NativeChunkRetirementProgress:
            if value.terminal is None or value.reservation is None:
                raise ValueError("mssql_sqlclient.retirement_state_prefix")
            directory = bind_native_chunk_closed_directory(
                projection_sha256=request.projection.projection_sha256,
                parent_authority_digest=request.authority.digest,
                operation_sha256=value.reservation.operation_sha256,
                terminal_sha256=value.terminal.proof_sha256,
            )
            return replace(value, admission_closed=True, directory=directory)

        return self._change(request, mutation)

    def record_capacity(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkCapacityProof
    ) -> NativeChunkRetirementProgress:
        return self._change(request, lambda value: replace(value, capacity=proof))

    def _advance(
        self, request: NativeChunkRetirementRequest, phase: NativeChunkLifecyclePhase
    ) -> NativeChunkRetirementProgress:
        return self._change(
            request,
            lambda value: value if value.lifecycle[-1].phase is phase else self._with_phase(request, value, phase),
        )

    @staticmethod
    def _with_phase(
        request: NativeChunkRetirementRequest,
        value: NativeChunkRetirementProgress,
        phase: NativeChunkLifecyclePhase,
    ) -> NativeChunkRetirementProgress:
        prior = value.lifecycle[-1]
        proof = bind_native_chunk_lifecycle(
            phase=phase,
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            directory_key=request.projection.directory_key,
            directory_revision=prior.directory_revision + 1,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            lifecycle_revision=prior.lifecycle_revision + 1,
        )
        return replace(value, lifecycle=(*value.lifecycle, proof))

    def _change(
        self,
        request: NativeChunkRetirementRequest,
        mutation: Callable[[NativeChunkRetirementProgress], NativeChunkRetirementProgress],
    ) -> NativeChunkRetirementProgress:
        key = self._key(request)
        for _ in range(8):
            revision, current = self._read(request)
            changed = mutation(current)
            if changed == current:
                return changed
            try:
                if self._compare_swap(key, revision, changed):
                    return changed
            except Exception as error:
                observed_revision, observed = self._read(request)
                if observed_revision > revision and observed == changed:
                    return observed
                raise RuntimeError("mssql_sqlclient.retirement_state_outcome_unknown") from error
        raise RuntimeError("mssql_sqlclient.retirement_state_contention")

    def _read(self, request: NativeChunkRetirementRequest) -> tuple[int, NativeChunkRetirementProgress]:
        revision, progress = self._load(self._key(request))
        if type(revision) is not int or revision < 0 or type(progress) is not NativeChunkRetirementProgress:
            raise ValueError("mssql_sqlclient.retirement_state_invalid")
        if (
            progress.projection_sha256 != request.projection.projection_sha256
            or progress.parent_authority_digest != request.authority.digest
            or progress.authorization_sha256 != request.authorization.authorization_sha256
        ):
            raise ValueError("mssql_sqlclient.retirement_state_binding")
        return revision, progress

    @staticmethod
    def _key(request: NativeChunkRetirementRequest) -> str:
        return request.projection.projection_sha256


__all__ = ("CasSqlClientNativeRetirementState",)
