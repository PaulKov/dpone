"""Capability ports for the R1 target-local SQL Server unit of work.

Session lifecycle belongs exclusively to ``MssqlTransactionSessionPort``.
Every other port receives the same opaque transaction handle and must never
open, commit, roll back, or close a database session.
"""

from __future__ import annotations

from typing import Protocol, TypeAlias

from dpone.contracts.mssql_target_authority_v2 import (
    MAX_SQL_BIGINT,
    MssqlArtifactProofV2,
    MssqlBatchEffectDraftV2,
    MssqlBatchEffectReceiptV2,
    MssqlEffectReceiptHeaderV2,
    MssqlEffectReceiptV2,
    MssqlEffectRequestV2,
    MssqlGenerationAuthorityClaimV1,
    MssqlMutationResultV2,
    MssqlQualityEvidenceV2,
    MssqlReceiptContractError,
    MssqlReceiptProofRequestV2,
    MssqlReceiptProofV2,
    MssqlSealedIntentV2,
    MssqlTargetAdmissionV2,
    MssqlTargetHeadV2,
    MssqlTargetIdentityV2,
    MssqlXminCheckpointTransitionV2,
    MssqlXminEffectDraftV2,
    MssqlXminEffectReceiptV2,
    MssqlXminMutationResultV2,
    ReceiptKind,
    ReceiptProofOutcome,
    SourceMode,
    WriterMode,
    build_receipt,
)
from dpone.ports.mssql_target_authority_v2_pre_source import MssqlPreSourceAuthorityPort
from dpone.ports.mssql_target_authority_v2_preparation import MssqlTargetAuthorityPreparationPort

MssqlTransactionHandle: TypeAlias = object


class MssqlTransactionSessionPort(Protocol):
    """Own one physical SQL Server session and its local transaction."""

    def begin(self) -> MssqlTransactionHandle:
        """Begin the configured fully durable local transaction."""

    def assert_active(self, handle: MssqlTransactionHandle) -> None:
        """Fail unless ``handle`` is the active transaction on this session."""

    def commit(self, handle: MssqlTransactionHandle) -> None:
        """Commit; any raised outcome after dispatch is treated as ambiguous."""

    def rollback(self, handle: MssqlTransactionHandle) -> None:
        """Roll back an active transaction before commit dispatch."""

    def close(self) -> None:
        """Discard the physical session."""


class MssqlTransactionSessionFactoryPort(Protocol):
    """Open a fresh physical SQL Server session."""

    def open(self) -> MssqlTransactionSessionPort:
        """Return a new, unstarted session configured for the R1 profile."""


class TargetCommitReceiptReaderPort(Protocol):
    """Classify an ambiguous outcome through a fresh target-only proof."""

    def probe_fresh(self, request: MssqlReceiptProofRequestV2) -> MssqlReceiptProofV2:
        """Return committed, known-not-committed, or unknown without source I/O."""


class TargetAuthorityTransactionPort(Protocol):
    """Operate on head, operation, staging, and receipts on one transaction."""

    def lock_and_reprove(
        self,
        handle: MssqlTransactionHandle,
        request: MssqlEffectRequestV2,
    ) -> MssqlTargetAdmissionV2:
        """Lock and re-prove target, head, operation epoch, and sealed intent."""

    def probe_receipt(
        self,
        handle: MssqlTransactionHandle,
        effect_key: bytes,
    ) -> MssqlEffectReceiptV2 | None:
        """Read the exact immutable receipt under the target lock."""

    def reprove_staging(
        self,
        handle: MssqlTransactionHandle,
        intent: MssqlSealedIntentV2,
    ) -> MssqlArtifactProofV2:
        """Re-hash the complete sealed artifact set before target mutation."""

    def append_receipt(
        self,
        handle: MssqlTransactionHandle,
        receipt: MssqlEffectReceiptV2,
    ) -> None:
        """Append one immutable header and its exact typed body."""

    def consume_staging(
        self,
        handle: MssqlTransactionHandle,
        intent: MssqlSealedIntentV2,
        receipt: MssqlEffectReceiptV2,
    ) -> None:
        """Transition every sealed artifact to the consuming receipt."""

    def advance_head(
        self,
        handle: MssqlTransactionHandle,
        receipt: MssqlEffectReceiptV2,
    ) -> MssqlTargetHeadV2:
        """CAS the operation and target head to the receipt outcome."""

    def read_back(
        self,
        handle: MssqlTransactionHandle,
        receipt: MssqlEffectReceiptV2,
    ) -> MssqlTargetAdmissionV2:
        """Read back the complete committed candidate authority before commit."""


class MssqlTargetIdentityAttestorPort(Protocol):
    """Recompute the exact closed physical target identity on one handle."""

    def attest(self, handle: MssqlTransactionHandle, identity: MssqlTargetIdentityV2) -> None:
        """Fail unless catalog and protected target identity match exactly."""


class MssqlStagingArtifactAttestorPort(Protocol):
    """Perform the mandatory key-ordered scan of every locked staging row."""

    def attest(
        self,
        handle: MssqlTransactionHandle,
        intent: MssqlSealedIntentV2,
    ) -> MssqlArtifactProofV2:
        """Return the logical byte proof produced from the exact artifact set."""


class TargetMutationPort(Protocol):
    """Apply the prepared Batch or XMin target effect on the injected handle."""

    def apply(
        self,
        handle: MssqlTransactionHandle,
        request: MssqlEffectRequestV2,
        staging: MssqlArtifactProofV2,
    ) -> MssqlMutationResultV2:
        """Apply target DML without controlling session lifecycle."""


class RowHashTransactionPort(Protocol):
    """Apply generation-scoped sidecar mutations atomically with target DML."""

    def apply(
        self,
        handle: MssqlTransactionHandle,
        request: MssqlEffectRequestV2,
        mutation: MssqlMutationResultV2,
    ) -> None:
        """Insert/update/delete exact canonical row-hash records."""


class TargetQualityTransactionPort(Protocol):
    """Run only closed, bounded, deterministic target-local probes."""

    def evaluate(
        self,
        handle: MssqlTransactionHandle,
        request: MssqlEffectRequestV2,
        mutation: MssqlMutationResultV2,
    ) -> MssqlQualityEvidenceV2:
        """Return typed quality evidence or fail the transaction."""


class SourceCheckpointTransactionPort(Protocol):
    """Append the XMin checkpoint transition on the same transaction."""

    def compare_and_set(
        self,
        handle: MssqlTransactionHandle,
        transition: MssqlXminCheckpointTransitionV2,
        receipt: MssqlEffectReceiptV2,
    ) -> None:
        """Require exact predecessor and append the adjacent checkpoint."""


class GenerationAuthorityVerifierPort(Protocol):
    """Verify detached one-shot authority before opening the target transaction."""

    def verify(self, claim: MssqlGenerationAuthorityClaimV1) -> None:
        """Fail unless the detached signature and canonical claim are valid."""


class GenerationAuthorityTransactionPort(Protocol):
    """Consume an already issued authority inside the target transaction."""

    def admit(
        self,
        handle: MssqlTransactionHandle,
        claim: MssqlGenerationAuthorityClaimV1,
        request: MssqlEffectRequestV2,
    ) -> None:
        """Recheck issued state, identity, expiry, and revocation snapshot."""

    def consume(
        self,
        handle: MssqlTransactionHandle,
        claim: MssqlGenerationAuthorityClaimV1,
        receipt: MssqlEffectReceiptV2,
    ) -> None:
        """Bind the one-shot authority to its consuming receipt."""


__all__ = [
    "GenerationAuthorityTransactionPort",
    "GenerationAuthorityVerifierPort",
    "MAX_SQL_BIGINT",
    "MssqlTransactionHandle",
    "MssqlStagingArtifactAttestorPort",
    "MssqlPreSourceAuthorityPort",
    "MssqlTargetAuthorityPreparationPort",
    "MssqlTargetIdentityAttestorPort",
    "MssqlTransactionSessionFactoryPort",
    "MssqlTransactionSessionPort",
    "MssqlArtifactProofV2",
    "MssqlBatchEffectDraftV2",
    "MssqlBatchEffectReceiptV2",
    "MssqlEffectReceiptHeaderV2",
    "MssqlEffectReceiptV2",
    "MssqlEffectRequestV2",
    "MssqlGenerationAuthorityClaimV1",
    "MssqlMutationResultV2",
    "MssqlQualityEvidenceV2",
    "MssqlReceiptContractError",
    "MssqlReceiptProofRequestV2",
    "MssqlReceiptProofV2",
    "MssqlSealedIntentV2",
    "MssqlTargetAdmissionV2",
    "MssqlTargetHeadV2",
    "MssqlTargetIdentityV2",
    "MssqlXminCheckpointTransitionV2",
    "MssqlXminEffectDraftV2",
    "MssqlXminEffectReceiptV2",
    "MssqlXminMutationResultV2",
    "ReceiptKind",
    "ReceiptProofOutcome",
    "RowHashTransactionPort",
    "SourceCheckpointTransactionPort",
    "SourceMode",
    "TargetAuthorityTransactionPort",
    "TargetCommitReceiptReaderPort",
    "TargetMutationPort",
    "TargetQualityTransactionPort",
    "WriterMode",
    "build_receipt",
]
