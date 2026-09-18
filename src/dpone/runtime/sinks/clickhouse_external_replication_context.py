"""Pure staged-lifecycle context for external ClickHouse publication."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.clickhouse_external_replication import ExternalPublicationRequest
from dpone.runtime.sinks.clickhouse_external_replication_receipt import ExternalReplicationReceipt


@dataclass(frozen=True, slots=True)
class ExternalStagedContext:
    """Carry only redacted immutable identity across governance boundaries."""

    request: ExternalPublicationRequest
    staged_receipt: ExternalReplicationReceipt
    candidate_name: str

    def __post_init__(self) -> None:
        if self.staged_receipt.phase != "STAGED":
            raise ValueError("external staged context requires STAGED authority")
        if self.request.operation_id != self.staged_receipt.operation_id:
            raise ValueError("external staged context operation differs")
        if self.request.generation_id != self.staged_receipt.generation_id:
            raise ValueError("external staged context generation differs")
        if not self.candidate_name:
            raise ValueError("external staged context candidate is missing")


@dataclass(frozen=True, slots=True)
class ExternalStagedValidation:
    """Bind validation to the exact authority version and logical generation."""

    operation_id: str
    generation_id: str
    authority_version: int


__all__ = ["ExternalStagedContext", "ExternalStagedValidation"]
