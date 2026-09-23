"""Deadline-bounded immutable evidence gateway for one coordinator operation."""

from typing import Protocol

from dpone.contracts.mssql_tds_api import (
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
)


class SqlClientEvidenceGateway(Protocol):
    """Supervisor-owned gateway; backend writes occur exclusively on its actor."""

    @property
    def observation(self) -> SqlClientEvidenceObservation:
        """Last acknowledged immutable receipt, never a late backend result."""

    def write(self, record: SqlClientEvidenceRecord, *, deadline: float) -> SqlClientEvidenceReceipt:
        """Acknowledge one closed evidence kind once within an absolute deadline."""

    def close(self, *, deadline: float) -> None:
        """Bound waiting for teardown; live actors continue consuming pool capacity."""
