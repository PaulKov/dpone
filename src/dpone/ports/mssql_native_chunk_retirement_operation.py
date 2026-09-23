"""Compatibility exports for native chunk retirement operation contracts."""

from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkContainmentProof,
    NativeChunkDropProof,
    NativeChunkRetirementReservation,
    bind_native_chunk_containment,
    bind_native_chunk_drop,
)

__all__ = (
    "NativeChunkContainmentProof",
    "NativeChunkDropProof",
    "NativeChunkRetirementReservation",
    "bind_native_chunk_containment",
    "bind_native_chunk_drop",
)
