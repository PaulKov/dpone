"""Narrow deadline-bounded gateway for permission parent evidence."""

from typing import Protocol

from dpone.contracts.mssql_tds_api import (
    PermissionGrantParentEvidenceObservation,
    PermissionGrantParentEvidenceReceipt,
    PermissionGrantParentEvidenceRecord,
)


class SqlClientPermissionGrantEvidenceGateway(Protocol):
    """Expose only the last ACK, one-shot write, and bounded teardown."""

    @property
    def observation(self) -> PermissionGrantParentEvidenceObservation:
        """Return the last fully checked ACK, never a late backend result."""

    def write(
        self, record: PermissionGrantParentEvidenceRecord, *, deadline: float
    ) -> PermissionGrantParentEvidenceReceipt:
        """Persist the next exact evidence kind within an absolute deadline."""

    def close(self, *, deadline: float) -> None:
        """Bound actor teardown without claiming completion of all evidence."""
