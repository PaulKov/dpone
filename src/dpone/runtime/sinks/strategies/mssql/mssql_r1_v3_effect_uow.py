"""One-session effect unit of work for sealed MSSQL R1 V3 Batch/XMin runs."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, get_args, runtime_checkable

from dpone._compat import StrEnum
from dpone.ports import mssql_r1_v3_effect_runtime as r1

_REPLAY_PROOF_TYPES = get_args(r1.EffectReplayProofV3)


class MssqlR1V3EffectOutcome(StrEnum):
    COMMITTED = "committed"
    COMMITTED_AFTER_FRESH_PROOF = "committed_after_fresh_proof"
    KNOWN_NOT_COMMITTED = "known_not_committed"


@dataclass(frozen=True, slots=True)
class MssqlR1V3EffectResult:
    outcome: MssqlR1V3EffectOutcome
    receipt: r1.MssqlR1EffectReceiptV3 | None

    def __post_init__(self) -> None:
        committed = self.outcome is not MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED
        if committed != (self.receipt is not None):
            raise ValueError("V3 effect result discriminator is inconsistent")


class MssqlR1V3CommitOutcomeUnknown(RuntimeError):
    """The target cannot prove whether the dispatched commit became durable."""

    code = "DPONE_POSTGRES_MSSQL_R1_V3_COMMIT_OUTCOME_UNKNOWN"
    safe_to_retry = False
    preserve_staging = True

    def __init__(self) -> None:
        super().__init__(self.code)


@dataclass(frozen=True, slots=True)
class MssqlR1V3RecoveryResult:
    """Closed result of an exact fresh-session proof."""

    receipt: r1.MssqlR1EffectReceiptV3 | None
    retry_same_sealed_request: bool

    def __post_init__(self) -> None:
        if (self.receipt is None) != self.retry_same_sealed_request:
            raise ValueError("recovery result must be either committed or safely retryable")


class MssqlR1V3EffectModePort(Protocol):
    source_mode: object

    def mutate(self, transaction: r1.MssqlR1TransactionV3, attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> None: ...

    def update_row_hashes(
        self, transaction: r1.MssqlR1TransactionV3, attempt: r1.MssqlR1EffectAttemptEnvelopeV3
    ) -> None: ...

    def evaluate(
        self, transaction: r1.MssqlR1TransactionV3, attempt: r1.MssqlR1EffectAttemptEnvelopeV3
    ) -> r1.MssqlBatchQualityEvidenceV3 | r1.MssqlXminQualityEvidenceV3: ...


@runtime_checkable
class MssqlR1V3CheckpointModePort(Protocol):
    """Mode capability required only by XMin effects."""

    def write_checkpoint(
        self, transaction: r1.MssqlR1TransactionV3, attempt: r1.MssqlR1EffectAttemptEnvelopeV3
    ) -> None: ...


class MssqlR1V3ReceiptBuilderPort(Protocol):
    def build(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
        admitted: r1.MssqlAdmittedGenerationAuthoritySetV3,
        quality: r1.MssqlBatchQualityEvidenceV3 | r1.MssqlXminQualityEvidenceV3,
    ) -> r1.MssqlR1EffectReceiptV3: ...


class MssqlR1V3TransactionalReceiptAppenderPort(Protocol):
    """Append a candidate receipt only on the active effect transaction."""

    def append(self, transaction: r1.MssqlR1TransactionV3, receipt: r1.MssqlR1EffectReceiptV3) -> None: ...


class MssqlR1V3FreshProofSessionPort(Protocol):
    """Read post-dispatch authority from one independently opened session."""

    def probe(self, request: r1.EffectReceiptProofRequestV3) -> r1.EffectReplayProofV3: ...

    def close(self) -> None: ...


class MssqlR1V3FreshProofSessionFactoryPort(Protocol):
    """Open a fresh target-only proof session after the old session is discarded."""

    def open(self) -> MssqlR1V3FreshProofSessionPort: ...


@runtime_checkable
class _KnownNotCommittedProof(Protocol):
    receipt_absent: bool


class MssqlR1V3EffectUnitOfWork:
    """Commit the closed target effect and all authority in one V3 session."""

    def __init__(
        self,
        *,
        sessions: r1.MssqlR1TransactionSessionFactoryV3Port,
        stage_attestor: r1.MssqlSealedStageAttestorV3Port,
        generation_authority: r1.MssqlGenerationAuthoritySetTransactionV3Port,
        stages: r1.MssqlSealedStageConsumptionV3Port,
        writer_fence: r1.MssqlTargetWriterFenceV3Port,
        candidate: r1.MssqlCandidateEffectProofV3Port,
        mode: MssqlR1V3EffectModePort,
        receipt_builder: MssqlR1V3ReceiptBuilderPort,
        receipt_appender: MssqlR1V3TransactionalReceiptAppenderPort,
        fresh_proofs: MssqlR1V3FreshProofSessionFactoryPort,
    ) -> None:
        if receipt_appender is fresh_proofs:
            raise ValueError("transactional receipt appender and fresh-proof factory must be distinct")
        self._sessions = sessions
        self._stage_attestor = stage_attestor
        self._generation_authority = generation_authority
        self._stages = stages
        self._writer_fence = writer_fence
        self._candidate = candidate
        self._mode = mode
        self._receipt_builder = receipt_builder
        self._receipt_appender = receipt_appender
        self._fresh_proofs = fresh_proofs

    def execute(self, attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> MssqlR1V3EffectResult:
        self._validate_attempt(attempt)
        session: r1.MssqlR1TransactionSessionV3Port | None = None
        transaction: r1.MssqlR1TransactionV3 | None = None
        commit_dispatched = False
        try:
            session = self._sessions.open()
            transaction = session.begin()
            self._assert_active(session, transaction)
            self._writer_fence.admit(transaction, attempt)
            for manifest in attempt.request.artifacts:
                evidence = self._stage_attestor.attest(transaction, manifest)
                if evidence != manifest.typed_scan_evidence:
                    raise r1.MssqlR1V3ContractError("fresh stage attestation differs from sealed manifest")
            admitted = self._generation_authority.resolve_and_admit(transaction, attempt)
            admitted.validate_for_attempt(attempt)
            self._mode.mutate(transaction, attempt)
            self._mode.update_row_hashes(transaction, attempt)
            quality = self._mode.evaluate(transaction, attempt)
            receipt = self._receipt_builder.build(transaction, attempt, admitted, quality)
            self._receipt_appender.append(transaction, receipt)
            self._write_checkpoint(transaction, attempt)
            self._stages.consume(transaction, attempt, receipt)
            self._generation_authority.consume(transaction, admitted, receipt)
            self._writer_fence.advance_head(transaction, attempt, receipt)
            proof = self._candidate.prove(transaction, attempt, receipt)
            if not isinstance(proof, r1.MssqlCandidateEffectProofV3):
                raise r1.MssqlR1V3ContractError("candidate provider returned a non-V3 proof")
            self._assert_active(session, transaction)
            proof.assert_for(transaction.binding, attempt, receipt)
            commit_dispatched = True
            session.dispatch_commit(transaction)
            return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.COMMITTED, receipt)
        except Exception as error:
            if commit_dispatched:
                discarded_session = session
                assert discarded_session is not None
                self._discard(session)
                session = None
                return self._classify_after_commit(attempt, error, discarded_session)
            if session is not None and transaction is not None:
                try:
                    session.rollback(transaction)
                except Exception as rollback_error:
                    raise MssqlR1V3CommitOutcomeUnknown from rollback_error
            raise
        finally:
            self._discard(session)

    def _classify_after_commit(
        self,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
        commit_error: Exception,
        discarded_session: r1.MssqlR1TransactionSessionV3Port,
    ) -> MssqlR1V3EffectResult:
        fresh_session: MssqlR1V3FreshProofSessionPort | None = None
        try:
            fresh_session = self._fresh_proofs.open()
            if fresh_session is discarded_session:
                fresh_session = None
                raise MssqlR1V3CommitOutcomeUnknown
            proof = fresh_session.probe(proof_request_for(attempt))
            recovery = classify_fresh_proof(attempt, proof)
        except Exception as proof_error:
            raise MssqlR1V3CommitOutcomeUnknown from proof_error
        finally:
            self._discard(fresh_session)
        if recovery.retry_same_sealed_request:
            return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED, None)
        if recovery.receipt is None:
            raise MssqlR1V3CommitOutcomeUnknown from commit_error
        return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.COMMITTED_AFTER_FRESH_PROOF, recovery.receipt)

    def _validate_attempt(self, attempt: object) -> None:
        if not isinstance(attempt, r1.MssqlR1EffectAttemptEnvelopeV3):
            raise r1.MssqlR1V3ContractError("effect UoW requires an exact V3 attempt")
        if attempt.request.identity.source_mode is not self._mode.source_mode:
            raise r1.MssqlR1V3ContractError("effect UoW mode differs from sealed request")

    def _write_checkpoint(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        is_xmin = isinstance(attempt.request.mutation_plan, r1.R1XminMutationPlanV1)
        mode = self._mode
        if is_xmin != isinstance(mode, MssqlR1V3CheckpointModePort):
            raise r1.MssqlR1V3ContractError("effect mode checkpoint capability differs from sealed request")
        if is_xmin:
            assert isinstance(mode, MssqlR1V3CheckpointModePort)
            mode.write_checkpoint(transaction, attempt)

    @staticmethod
    def _assert_active(
        session: r1.MssqlR1TransactionSessionV3Port,
        transaction: r1.MssqlR1TransactionV3,
    ) -> None:
        transaction.assert_active()
        session.assert_active(transaction)

    @staticmethod
    def _discard(session: r1.MssqlR1TransactionSessionV3Port | MssqlR1V3FreshProofSessionPort | None) -> None:
        if session is not None:
            with suppress(Exception):
                session.close()


def proof_request_for(attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> r1.EffectReceiptProofRequestV3:
    """Build the exact post-dispatch proof request for one attempt."""

    return r1.EffectReceiptProofRequestV3(
        attempt.request.digest,
        attempt.request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
    )


def classify_fresh_proof(
    attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    proof: r1.EffectReplayProofV3,
) -> MssqlR1V3RecoveryResult:
    """Classify only exact committed/non-committed fresh proof variants."""

    request = proof_request_for(attempt)
    if type(proof) is r1.CommittedEffectReplayProofV3:
        try:
            observed = r1.CommittedEffectReplayProofV3.from_canonical_bytes(proof.canonical_bytes)
        except (ValueError, TypeError, AttributeError) as exc:
            raise MssqlR1V3CommitOutcomeUnknown from exc
        coordinates = observed.proof_request
        epoch_delta = coordinates.operation_epoch - request.operation_epoch
        revision_delta = coordinates.operation_projection_revision - request.operation_projection_revision
        if (
            observed.sealed_request.canonical_bytes != attempt.request.canonical_bytes
            or coordinates.sealed_request_digest != request.sealed_request_digest
            or coordinates.effect_key != request.effect_key
            or coordinates.expected_override_id != request.expected_override_id
            or coordinates.expected_override_digest != request.expected_override_digest
            or epoch_delta < 0
            # Every epoch advance increments the operation projection at least once.
            or revision_delta < epoch_delta
        ):
            raise MssqlR1V3CommitOutcomeUnknown
        return MssqlR1V3RecoveryResult(observed.receipt, False)
    if type(proof) not in _REPLAY_PROOF_TYPES or proof.proof_request != request:
        raise MssqlR1V3CommitOutcomeUnknown
    if isinstance(proof, _KnownNotCommittedProof) and proof.receipt_absent is True:
        return MssqlR1V3RecoveryResult(None, True)
    raise MssqlR1V3CommitOutcomeUnknown


__all__ = [
    "MssqlR1V3CheckpointModePort",
    "MssqlR1V3CommitOutcomeUnknown",
    "MssqlR1V3EffectModePort",
    "MssqlR1V3EffectOutcome",
    "MssqlR1V3EffectResult",
    "MssqlR1V3EffectUnitOfWork",
    "MssqlR1V3FreshProofSessionFactoryPort",
    "MssqlR1V3FreshProofSessionPort",
    "MssqlR1V3RecoveryResult",
    "MssqlR1V3ReceiptBuilderPort",
    "MssqlR1V3TransactionalReceiptAppenderPort",
    "classify_fresh_proof",
    "proof_request_for",
]
