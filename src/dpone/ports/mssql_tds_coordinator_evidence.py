"""Deadline-bounded immutable evidence gateway for one coordinator operation."""

from typing import Protocol

from dpone.contracts.mssql_tds_api import (
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
)


class TdsCoordinatorEvidenceGateway(Protocol):
    """Supervisor-owned gateway; backend writes occur exclusively on its actor."""

    @property
    def observation(self) -> TdsCoordinatorEvidenceObservation:
        """Last acknowledged immutable receipt, never a late backend result."""

    def write(self, record: TdsCoordinatorEvidenceRecord, *, deadline: float) -> TdsCoordinatorEvidenceReceipt:
        """Acknowledge one closed evidence kind once within an absolute deadline."""

    def close(self, *, deadline: float) -> None:
        """Bound waiting for teardown; live actors continue consuming pool capacity."""
