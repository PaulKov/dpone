"""Native transfer fault-injection workflow contracts.

This module is connector-free. Concrete tools/adapters inject real source,
sink and state operations, while the workflow owns the reusable certification
sequence: inject a controlled failure, retry, then evaluate resume invariants.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint
from dpone.strategy_intelligence.resume_certification import (
    NativeTransferResumeCertificationRequest,
    NativeTransferResumeCertificationResult,
    NativeTransferResumeCertificationService,
)


class FaultInjectionStage(StrEnum):
    """Supported native transfer fault-injection stages."""

    AFTER_EXPORT = "after_export"
    DURING_LOAD = "during_load"
    BEFORE_FINALIZER = "before_finalizer"


@dataclass(frozen=True)
class FaultInjectionSnapshot:
    """Observable transfer state at one fault-injection boundary."""

    checkpoints: tuple[PartitionCheckpoint, ...]
    row_count: int
    duplicate_rows: int
    state_committed: bool


@dataclass(frozen=True)
class NativeTransferFaultInjectionScenario:
    """One native transfer fault-injection scenario."""

    run_id: str
    source_type: str
    sink_type: str
    strategy: str
    failure_stage: FaultInjectionStage
    expected_rows: int
    query_hash: str
    schema_hash: str


class FaultInjectionOperations(Protocol):
    """Injected runtime operations for a concrete native transfer path."""

    def inject_failure(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        """Execute until ``stage`` and stop before state commit."""

    def retry(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        """Retry the transfer and return post-retry observable state."""


class NativeTransferFaultInjectionWorkflow:
    """Run a controlled failure + retry and certify the result."""

    def __init__(self, certification_service: NativeTransferResumeCertificationService | None = None) -> None:
        self._certification_service = certification_service or NativeTransferResumeCertificationService()

    def run(
        self,
        scenario: NativeTransferFaultInjectionScenario,
        operations: FaultInjectionOperations,
    ) -> NativeTransferResumeCertificationResult:
        before_retry = operations.inject_failure(scenario.failure_stage)
        after_retry = operations.retry(scenario.failure_stage)
        return self._certification_service.certify(
            NativeTransferResumeCertificationRequest(
                run_id=scenario.run_id,
                source_type=scenario.source_type,
                sink_type=scenario.sink_type,
                strategy=scenario.strategy,
                failure_stage=scenario.failure_stage.value,
                expected_rows=scenario.expected_rows,
                actual_rows_after_retry=after_retry.row_count,
                duplicate_rows_after_retry=after_retry.duplicate_rows,
                state_committed_before_retry=before_retry.state_committed,
                state_committed_after_retry=after_retry.state_committed,
                query_hash=scenario.query_hash,
                schema_hash=scenario.schema_hash,
                checkpoints_before_retry=before_retry.checkpoints,
                checkpoints_after_retry=after_retry.checkpoints,
            )
        )
