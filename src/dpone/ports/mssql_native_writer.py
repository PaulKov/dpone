"""Capability-oriented port for one supervised native stage launch."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF, NativeStageWriteGrant, NativeStageWriteOutcome


class NativeStageBulkWriter(Protocol):
    """Write at most once per immutable grant and return process proof only."""

    def write(self, grant: NativeStageWriteGrant, *, rejects_path: Path) -> NativeStageWriteOutcome: ...


__all__ = ["BCP_STAGE_PROOF", "NativeStageWriteGrant", "NativeStageWriteOutcome", "NativeStageBulkWriter"]
