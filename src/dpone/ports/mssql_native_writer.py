"""Capability-oriented port for one supervised native stage launch."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF, NativeStageWriteGrant, NativeStageWriteOutcome


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
    "NativeStageBulkWriter",
    "NativeStageProcessProof",
    "NativeStageWriteGrant",
    "NativeStageWriteOutcome",
]
