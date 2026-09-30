"""Capability-oriented port for one supervised native stage launch."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.contracts.mssql_native_stage_writer import (
    NativeStageColumnMapping,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    NativeStageWriteGrant,
    NativeStageWriteOutcome,
)


class NativeStageWriter(Protocol):
    """Write one sealed input to one exact owned stage under one deadline."""

    def write(
        self,
        request: NativeStageWriteRequest,
        *,
        deadline: OperationDeadline,
    ) -> NativeStageWriteObservation: ...


class NativeStageBulkWriter(Protocol):
    """Write at most once per immutable grant and return process proof only."""

    def write(self, grant: NativeStageWriteGrant, *, rejects_path: Path) -> NativeStageWriteOutcome: ...


class NativeStageProcessProof(Protocol):
    """Vendor-neutral observation returned after supervising one stage process."""

    classification: str
    acknowledged: bool
    reaped: bool
    rows_copied: int | None


__all__ = [
    "BCP_STAGE_PROOF",
    "SQLCLIENT_SESSION_PROOF",
    "NativeStageBulkWriter",
    "NativeStageProcessProof",
    "NativeStageColumnMapping",
    "NativeStageWriter",
    "NativeStageWriteMetrics",
    "NativeStageWriteObservation",
    "NativeStageWriteRequest",
    "NativeStageWriteGrant",
    "NativeStageWriteOutcome",
    "OperationDeadline",
]
