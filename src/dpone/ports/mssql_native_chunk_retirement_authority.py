"""Compatibility imports for canonical native chunk retirement authority contracts."""

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
from dpone.contracts.mssql_native_chunk_retirement_authority import (
    _issue_native_chunk_retirement_authorization as _issue_native_chunk_retirement_authorization,
)

__all__ = (
    "NativeChunkContainmentProof",
    "NativeChunkDropIntent",
    "NativeChunkDropObservation",
    "NativeChunkDropOutcome",
    "NativeChunkDropProof",
    "NativeChunkLifecyclePhase",
    "NativeChunkLifecycleProof",
    "NativeChunkRetirementAuthorization",
    "NativeChunkRetirementRequest",
    "NativeChunkRetirementReservation",
    "bind_native_chunk_containment",
    "bind_native_chunk_drop",
    "bind_native_chunk_lifecycle",
    "native_chunk_object_incarnation_digest",
    "native_chunk_parent_evidence",
    "native_chunk_parent_stage_id",
)
