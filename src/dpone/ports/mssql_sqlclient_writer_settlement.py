"""Capability port for one contained P10f remote verification."""

from typing import Protocol

from dpone.contracts.mssql_sqlclient_writer_settlement_verifier import (
    SqlClientInputDescriptor,
    SqlClientObserverAdmission,
    SqlClientStageContentExpectation,
    SqlClientStageIdentity,
    SqlClientWriterObservationRecord,
    SqlClientWriterSettlementObservation,
    SqlClientWriterSettlementProvenance,
)
from dpone.contracts.mssql_tds_coordinator_authority import LOCK_RESOURCE as LOCK_RESOURCE
from dpone.contracts.mssql_tds_coordinator_authority import TdsLockObservation as TdsLockObservation


class SqlClientWriterSettlementVerifier(Protocol):
    """One-shot verifier contained outside the parent process."""

    def observe(
        self,
        *,
        writer_observation: SqlClientWriterObservationRecord,
        writer_admission: SqlClientObserverAdmission,
        stage: SqlClientStageIdentity,
        input_descriptor: SqlClientInputDescriptor,
        expectation: SqlClientStageContentExpectation,
        operation_deadline: float,
    ) -> SqlClientWriterSettlementObservation: ...

    def close(self) -> None: ...

    @property
    def provenance(self) -> SqlClientWriterSettlementProvenance: ...


__all__ = ("SqlClientWriterSettlementVerifier",)
