"""Injected effect and state ports for exact source-free chunk retirement."""

# ruff: noqa: F401

from typing import Protocol

from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkContainmentProof,
    NativeChunkDropIntent,
    NativeChunkDropObservation,
    NativeChunkDropOutcome,
    NativeChunkDropProof,
    NativeChunkLifecyclePhase,
    NativeChunkLifecycleProof,
    NativeChunkRetirementAuthorization,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    bind_native_chunk_containment,
    bind_native_chunk_drop,
    bind_native_chunk_lifecycle,
    native_chunk_object_incarnation_digest,
    native_chunk_parent_evidence,
    native_chunk_parent_stage_id,
)
from dpone.contracts.mssql_native_chunk_retirement_evidence import (
    NativeChunkAbsenceProof,
    NativeChunkCapacityProof,
    NativeChunkClosedDirectory,
    NativeChunkRetiredTerminal,
    NativeChunkRetirementProgress,
    NativeChunkSettlementProof,
    bind_native_chunk_absence,
    bind_native_chunk_capacity,
    bind_native_chunk_closed_directory,
    bind_native_chunk_retired_terminal,
    bind_native_chunk_settlement,
)
from dpone.contracts.mssql_native_parent_journal import NativeChunkRetirementReceipt


class NativeChunkRetirementState(Protocol):
    """Durable fenced transitions for one exact attempt and directory."""

    def observe(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress: ...
    def seal_work(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress: ...
    def require_containment(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress: ...
    def record_containment(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkContainmentProof
    ) -> NativeChunkRetirementProgress: ...

    def reserve_retirement(
        self, request: NativeChunkRetirementRequest, reservation: NativeChunkRetirementReservation
    ) -> NativeChunkRetirementProgress: ...
    def record_drop_intent(
        self, request: NativeChunkRetirementRequest, intent: NativeChunkDropIntent
    ) -> NativeChunkRetirementProgress: ...
    def record_drop_outcome(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkDropProof
    ) -> NativeChunkRetirementProgress: ...
    def record_settlement(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkSettlementProof
    ) -> NativeChunkRetirementProgress: ...
    def record_retired(
        self, request: NativeChunkRetirementRequest, absence: NativeChunkAbsenceProof
    ) -> NativeChunkRetirementProgress: ...
    def close_admission(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress: ...
    def record_capacity(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkCapacityProof
    ) -> NativeChunkRetirementProgress: ...


class NativeChunkRetirementAuthorizationObserver(Protocol):
    """Read exact v4 parent journal authority without mutation."""

    def observe(self, projection: object) -> NativeChunkRetirementAuthorization | None: ...
    def verify(self, projection: object, authorization: NativeChunkRetirementAuthorization) -> bool: ...


class NativeChunkRetirementEffects(Protocol):
    """Closed observations and the only remote mutation used by retirement."""

    def prove_containment(self, request: NativeChunkRetirementRequest) -> NativeChunkContainmentProof: ...
    def observe_exact_incarnation(self, request: NativeChunkRetirementRequest) -> str: ...
    def drop_exact(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
    ) -> NativeChunkDropProof: ...
    def reconcile_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
        prior: NativeChunkDropProof | None,
    ) -> NativeChunkDropProof: ...
    def settle_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        drop: NativeChunkDropProof,
        object_incarnation_sha256: str,
    ) -> NativeChunkSettlementProof: ...
    def observe_absence(self, request: NativeChunkRetirementRequest) -> NativeChunkAbsenceProof: ...
    def observe_capacity(self, request: NativeChunkRetirementRequest) -> NativeChunkCapacityProof: ...


__all__ = tuple(name for name in globals() if name.startswith("NativeChunk") or name.startswith("bind_native"))
