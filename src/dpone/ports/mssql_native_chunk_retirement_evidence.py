"""Compatibility imports for canonical native chunk retirement evidence contracts."""

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

__all__ = (
    "NativeChunkAbsenceProof",
    "NativeChunkCapacityProof",
    "NativeChunkClosedDirectory",
    "NativeChunkRetiredTerminal",
    "NativeChunkRetirementProgress",
    "NativeChunkSettlementProof",
    "bind_native_chunk_absence",
    "bind_native_chunk_capacity",
    "bind_native_chunk_closed_directory",
    "bind_native_chunk_retired_terminal",
    "bind_native_chunk_settlement",
)
