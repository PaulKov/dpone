"""One helper's bounded persistence gateway; no producer or SQL authority."""

from typing import Protocol

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_tds_api import (
    SqlClientDepartureEvidenceRecord,
    SqlClientObserveContainmentObservation,
    SqlClientObserveContainmentReceipt,
)


class SqlClientDepartureEvidenceGateway(Protocol):
    """Supervisor-owned actor gateway with one submission per evidence kind."""

    @property
    def observation(self) -> SqlClientDepartureEvidenceObservation:
        """Last fully acknowledged immutable snapshot, initially empty."""

    def write(self, record: SqlClientDepartureEvidenceRecord, *, deadline: float) -> SqlClientDepartureEvidenceReceipt:
        """Persist one closed kind within the original absolute deadline."""

    def close(self, *, deadline: float) -> None:
        """Bound actual actor teardown; retain shared-pool capacity while live."""


class SqlClientObserveContainmentEvidenceGateway(Protocol):
    """One original containment artifact; neither helper phase nor SQL authority."""

    @property
    def observation(self) -> SqlClientObserveContainmentObservation:
        """Last fully acknowledged receipt, initially absent."""

    def write(self, payload: bytes, *, deadline: float) -> SqlClientObserveContainmentReceipt:
        """Persist original reaped exit once within the original deadline."""

    def close(self, *, deadline: float) -> None:
        """Wait for local actor teardown without closing the original pool."""
