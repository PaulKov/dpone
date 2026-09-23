"""Explicit callback adapter for P10g SQL Server retirement effects."""

from __future__ import annotations

from collections.abc import Callable

from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkAbsenceProof,
    NativeChunkCapacityProof,
    NativeChunkContainmentProof,
    NativeChunkDropIntent,
    NativeChunkDropProof,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    NativeChunkSettlementProof,
)

Containment = Callable[[NativeChunkRetirementRequest], NativeChunkContainmentProof]
Incarnation = Callable[[NativeChunkRetirementRequest], str]
Drop = Callable[
    [NativeChunkRetirementRequest, NativeChunkRetirementReservation, NativeChunkDropIntent], NativeChunkDropProof
]
Reconcile = Callable[
    [
        NativeChunkRetirementRequest,
        NativeChunkRetirementReservation,
        NativeChunkDropIntent,
        NativeChunkDropProof | None,
    ],
    NativeChunkDropProof,
]
Settle = Callable[
    [NativeChunkRetirementRequest, NativeChunkRetirementReservation, NativeChunkDropProof, str],
    NativeChunkSettlementProof,
]
Absence = Callable[[NativeChunkRetirementRequest], NativeChunkAbsenceProof]
Capacity = Callable[[NativeChunkRetirementRequest], NativeChunkCapacityProof]


class CallbackSqlClientNativeRetirementEffects:
    """Expose only named, injected effects; credentials never enter this adapter."""

    def __init__(
        self,
        *,
        prove_containment: Containment,
        observe_incarnation: Incarnation,
        drop: Drop,
        reconcile: Reconcile,
        settle: Settle,
        observe_absence: Absence,
        observe_capacity: Capacity,
    ) -> None:
        self._containment = prove_containment
        self._incarnation = observe_incarnation
        self._drop = drop
        self._reconcile = reconcile
        self._settle = settle
        self._absence = observe_absence
        self._capacity = observe_capacity

    def prove_containment(self, request: NativeChunkRetirementRequest) -> NativeChunkContainmentProof:
        return self._containment(request)

    def observe_exact_incarnation(self, request: NativeChunkRetirementRequest) -> str:
        return self._incarnation(request)

    def drop_exact(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
    ) -> NativeChunkDropProof:
        return self._drop(request, reservation, intent)

    def reconcile_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        intent: NativeChunkDropIntent,
        prior: NativeChunkDropProof | None,
    ) -> NativeChunkDropProof:
        return self._reconcile(request, reservation, intent, prior)

    def settle_drop(
        self,
        request: NativeChunkRetirementRequest,
        reservation: NativeChunkRetirementReservation,
        drop: NativeChunkDropProof,
        object_incarnation_sha256: str,
    ) -> NativeChunkSettlementProof:
        return self._settle(request, reservation, drop, object_incarnation_sha256)

    def observe_absence(self, request: NativeChunkRetirementRequest) -> NativeChunkAbsenceProof:
        return self._absence(request)

    def observe_capacity(self, request: NativeChunkRetirementRequest) -> NativeChunkCapacityProof:
        return self._capacity(request)


__all__ = ("CallbackSqlClientNativeRetirementEffects",)
