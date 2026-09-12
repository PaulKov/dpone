from __future__ import annotations

import ast
import inspect
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from dpone.adapters.mssql_r1_v3_effect import MssqlR1BatchEffectV3, MssqlR1XminEffectV3
from dpone.adapters.mssql_r1_v3_receipt import MssqlR1V3ReceiptBuilder
from dpone.adapters.mssql_r1_v3_transaction_execution import MssqlR1V3RenderedAdmissionAdapter
from dpone.contracts.mssql_r1_v3_candidate_proof import (
    MssqlCandidateEffectProofV3,
    MssqlCandidateOperationObservationV3,
    MssqlCandidateWriterHeadObservationV3,
)
from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_plan import R1MutationStepV1
from dpone.contracts.mssql_r1_v3_quality import (
    MssqlBatchQualityEvidenceV3,
    MssqlXminQualityEvidenceV3,
)
from dpone.contracts.mssql_r1_v3_receipt import MssqlR1ReceiptObservationV3
from dpone.contracts.mssql_r1_v3_replay import (
    CommittedEffectReplayProofV3,
    KnownNotCommittedEffectReplayProofV3,
    UnknownEffectReplayProofV3,
)
from dpone.contracts.mssql_r1_v3_replay_observation import (
    EffectReceiptProofRequestV3,
    R1ReplayOperationStateV3,
    R1ReplayResourceStateV3,
    R1UnknownReplayReasonV3,
    WriterHeadReplayObservationV3,
)
from dpone.contracts.mssql_r1_v3_stage_consumption import (
    CheckpointReplayObservationV3,
    MssqlConsumedSealedStageSetV3,
    MssqlStageConsumptionStateV3,
)
from dpone.contracts.mssql_r1_v3_transaction_authority import MssqlConsumedGenerationAuthoritySetV3
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode
from dpone.ports import mssql_r1_v3 as broad_r1_ports
from dpone.ports import mssql_r1_v3_effect_runtime as effect_runtime_ports
from dpone.ports.mssql_r1_v3 import (
    MssqlR1PreSourceOutcomeV3,
    MssqlR1PreSourceStatusV3,
)
from dpone.runtime.sinks.strategies.mssql import mssql_r1_v3_batch, mssql_r1_v3_effect_uow
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_batch import MssqlR1BatchMutationProviderV3
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_effect_uow import (
    MssqlR1V3CommitOutcomeUnknown,
    MssqlR1V3EffectOutcome,
    MssqlR1V3EffectResult,
    MssqlR1V3EffectUnitOfWork,
    proof_request_for,
)
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_prepared import (
    MssqlR1PreparedEffectRunnerV3,
    MssqlR1V3PreparedRunnerError,
)
from tests.test_postgres_mssql_r1_v3_contracts import (
    D1,
    D3,
    NOW,
    _admitted,
    _batch_request,
    _operation,
    _resources,
    _transaction_binding,
)
from tests.test_postgres_mssql_r1_v3_quality_xmin import (
    _admission,
    _AuthorityResolver,
    _ExactTemplateVerifier,
    _fixture,
    _Gateway,
    _tamper,
)
from tests.test_postgres_mssql_r1_v3_quality_xmin import (
    _Transaction as _MutationTransaction,
)


def test_batch_provider_does_not_import_other_provider_implementations() -> None:
    tree = ast.parse(inspect.getsource(mssql_r1_v3_batch))
    imports = {imported.name for node in ast.walk(tree) if isinstance(node, ast.Import) for imported in node.names}
    imports.update(
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    forbidden = {
        "dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_quality",
        "dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_xmin",
    }

    assert imports.isdisjoint(forbidden)


def test_effect_runtime_uses_narrow_ports_with_compatible_broad_reexports() -> None:
    tree = ast.parse(inspect.getsource(mssql_r1_v3_effect_uow))
    imports = {
        (node.module, imported.name)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
        for imported in node.names
    }

    assert ("dpone.ports", "mssql_r1_v3") not in imports
    assert ("dpone.ports", "mssql_r1_v3_effect_runtime") in imports
    for name in (
        "MssqlR1TransactionV3",
        "MssqlR1TransactionSessionV3Port",
        "MssqlR1TransactionSessionFactoryV3Port",
        "MssqlSealedStageAttestorV3Port",
        "MssqlGenerationAuthoritySetTransactionV3Port",
        "MssqlSealedStageConsumptionV3Port",
        "MssqlCandidateEffectProofV3Port",
        "MssqlTargetWriterFenceV3Port",
    ):
        assert getattr(broad_r1_ports, name) is getattr(effect_runtime_ports, name)


class _FailingEvents(list[str]):
    def __init__(self, fail_at: str | None = None, fail_occurrence: int = 1) -> None:
        super().__init__()
        self.fail_at = fail_at
        self.fail_occurrence = fail_occurrence
        self._counts: dict[str, int] = {}

    def append(self, event: str) -> None:
        super().append(event)
        self._counts[event] = self._counts.get(event, 0) + 1
        if event == self.fail_at and self._counts[event] == self.fail_occurrence:
            raise RuntimeError(f"injected:{event}:{self.fail_occurrence}")

    def extend(self, events) -> None:
        for event in events:
            self.append(event)


class _DurableReceiptState:
    def __init__(self) -> None:
        self.committed: object | None = None


class _Transaction:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.binding = _transaction_binding()
        self.commit_dispatched = False
        self.invalidated = False

    @property
    def transaction_id(self) -> UUID:
        return self.binding.transaction_id

    def assert_active(self) -> None:
        if self.commit_dispatched or self.invalidated:
            raise AssertionError("old transaction accessed after commit dispatch")
        self.events.append("transaction_active")
        self.binding.assert_active()


class _Session:
    def __init__(
        self,
        events: list[str],
        durable: _DurableReceiptState,
        receipt_appender: _ReceiptAppender,
        *,
        fail_commit: bool = False,
        commit_persisted: bool = True,
        fail_rollback: bool = False,
        fail_close: bool = False,
    ) -> None:
        self.events = events
        self.durable = durable
        self.receipt_appender = receipt_appender
        self.fail_commit = fail_commit
        self.commit_persisted = commit_persisted
        self.fail_rollback = fail_rollback
        self.fail_close = fail_close
        self.transaction = _Transaction(events)

    def begin(self) -> _Transaction:
        self.events.append("begin")
        return self.transaction

    def assert_active(self, transaction: object) -> None:
        assert transaction is self.transaction
        if self.transaction.commit_dispatched or self.transaction.invalidated:
            raise AssertionError("old session accessed after commit dispatch")
        self.events.append("session_active")

    def dispatch_commit(self, transaction: object) -> None:
        assert transaction is self.transaction
        self.events.append("dispatch_commit")
        self.transaction.commit_dispatched = True
        if self.commit_persisted:
            self.durable.committed = self.receipt_appender.take(self.transaction)
        if self.fail_commit:
            raise OSError("lost commit response")

    def rollback(self, transaction: object) -> None:
        assert transaction is self.transaction
        if self.transaction.commit_dispatched or self.transaction.invalidated:
            raise AssertionError("old session rollback after commit dispatch")
        self.events.append("rollback")
        self.receipt_appender.rollback(self.transaction)
        if self.fail_rollback:
            raise OSError("rollback failed")

    def close(self) -> None:
        self.events.append("close")
        self.transaction.invalidated = True
        if self.fail_close:
            raise OSError("close failed after invalidation")


class _Sessions:
    def __init__(
        self,
        events: list[str],
        durable: _DurableReceiptState,
        receipt_appender: _ReceiptAppender,
        *,
        fail_commit: bool = False,
        commit_persisted: bool = True,
        fail_rollback: bool = False,
        fail_close: bool = False,
    ) -> None:
        self.events = events
        self.durable = durable
        self.receipt_appender = receipt_appender
        self.fail_commit = fail_commit
        self.commit_persisted = commit_persisted
        self.fail_rollback = fail_rollback
        self.fail_close = fail_close
        self.opened: list[_Session] = []

    def open(self) -> _Session:
        session = _Session(
            self.events,
            self.durable,
            self.receipt_appender,
            fail_commit=self.fail_commit,
            commit_persisted=self.commit_persisted,
            fail_rollback=self.fail_rollback,
            fail_close=self.fail_close,
        )
        self.fail_commit = False
        self.opened.append(session)
        return session


class _Mutation:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def mutate(self, transaction: object, attempt: object) -> None:
        self.events.append("batch")

    def apply_delta(self, transaction: object, attempt: object) -> None:
        self.events.extend(("delete", "update", "insert"))

    def update_row_hashes(self, transaction: object, attempt: object) -> None:
        self.events.append("row_hash")

    def write_checkpoint(self, transaction: object, attempt: object) -> None:
        self.events.append("checkpoint")


class _Quality:
    def __init__(self, events: list[str], attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.events = events
        self.attempt = attempt

    def evaluate(self, transaction: object, attempt: object):
        self.events.append("quality")
        if self.attempt.request.identity.source_mode is SourceMode.BATCH_FULL_REFRESH:
            count = self.attempt.request.artifacts[0].observed_row_count
            return MssqlBatchQualityEvidenceV3(D3, *(count,) * 8)
        return MssqlXminQualityEvidenceV3(D3, 10, 8, 8, 8, 2, 2, 2, 8, 13, 13, 12, 3, 2, 1, 6, 0, 0, 0, True)


class _Attestor:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def attest(self, transaction: object, manifest: object):
        self.events.append("attest")
        return manifest.typed_scan_evidence


class _Authority:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.admitted = None
        self.consumed = None

    def resolve_and_admit(self, transaction: _Transaction, attempt: MssqlR1EffectAttemptEnvelopeV3):
        self.events.append("authority_admit")
        self.admitted = _admitted(attempt.request, transaction.binding)
        return self.admitted

    def consume(self, transaction: object, admitted: object, receipt: object):
        self.events.append("authority_consume")
        self.consumed = MssqlConsumedGenerationAuthoritySetV3.from_admitted(
            admitted, receipt.header.receipt_id, receipt.digest
        )
        return self.consumed


class _Stages:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.consumed = None

    def consume(self, transaction: _Transaction, attempt: MssqlR1EffectAttemptEnvelopeV3, receipt: object):
        self.events.append("stage_consume")
        manifests = attempt.request.artifacts
        count = len(manifests)
        self.consumed = MssqlConsumedSealedStageSetV3(
            transaction.transaction_id,
            transaction.binding.session_identity_digest,
            attempt.request.identity.effect_key,
            attempt.request.digest,
            receipt.header.receipt_id,
            receipt.digest,
            tuple(item.artifact_id for item in manifests),
            tuple(item.artifact_kind for item in manifests),
            tuple(item.canonical_bytes for item in manifests),
            tuple(item.manifest_digest for item in manifests),
            (NOW,) * count,
            (NOW + timedelta(days=1),) * count,
            (NOW + timedelta(minutes=2),) * count,
            tuple(range(1, count + 1)),
            (MssqlStageConsumptionStateV3.CONSUMED,) * count,
        )
        return self.consumed


class _Fence:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def admit(self, transaction: object, attempt: object) -> None:
        self.events.append("fence")

    def advance_head(self, transaction: object, attempt: object, receipt: object) -> None:
        self.events.append("head_operation")


class _Observations:
    def __init__(self, events: list[str], mode: SourceMode) -> None:
        self.events = events
        self.mode = mode

    def observe(self, transaction: object, attempt: object, quality: object):
        self.events.append("receipt_observation")
        return MssqlR1ReceiptObservationV3(
            NOW + timedelta(minutes=2), 0 if self.mode is SourceMode.BATCH_FULL_REFRESH else None
        )


class _ReceiptBuilder:
    def __init__(self, events: list[str], mode: SourceMode) -> None:
        self.events = events
        self.delegate = MssqlR1V3ReceiptBuilder(_Observations(events, mode))

    def build(self, transaction: object, attempt: object, admitted: object, quality: object):
        receipt = self.delegate.build(transaction, attempt, admitted, quality)
        self.events.append("receipt_build")
        return receipt


class _ReceiptAppender:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.pending: dict[UUID, object] = {}
        self.receipt = None

    def append(self, transaction: _Transaction, receipt: object) -> None:
        self.events.append("receipt")
        self.receipt = receipt
        self.pending[transaction.transaction_id] = receipt

    def take(self, transaction: _Transaction) -> object:
        return self.pending.pop(transaction.transaction_id)

    def rollback(self, transaction: _Transaction) -> None:
        self.pending.pop(transaction.transaction_id, None)


class _FreshProofSession:
    def __init__(
        self,
        events: list[str],
        durable: _DurableReceiptState,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        proof_kind: str,
    ) -> None:
        self.events = events
        self.durable = durable
        self.attempt = attempt
        self.proof_kind = proof_kind
        self.handle = object()
        self.closed = False

    def probe(self, request: EffectReceiptProofRequestV3):
        self.events.append("fresh_probe")
        if self.proof_kind == "raise":
            raise OSError("fresh probe failed")
        if self.proof_kind == "mismatch":
            return SimpleNamespace(proof_request=replace(request, effect_key=b"x" * 32))
        if self.proof_kind == "unknown":
            return UnknownEffectReplayProofV3(request, R1UnknownReplayReasonV3.INCOMPLETE_OBSERVATION, D3)
        if self.proof_kind == "committed":
            receipt = self.durable.committed
            assert receipt is not None, "fresh committed proof cannot see transaction-local pending state"
            artifacts, authorities = _resources(self.attempt.request, R1ReplayResourceStateV3.CONSUMED, receipt)
            header = receipt.header
            return CommittedEffectReplayProofV3(
                request,
                self.attempt.request,
                receipt,
                _operation(self.attempt, R1ReplayOperationStateV3.COMMITTED, receipt),
                WriterHeadReplayObservationV3(
                    header.candidate_writer_generation,
                    header.candidate_writer_generation,
                    header.candidate_head_revision,
                    header.receipt_id,
                    receipt.digest,
                ),
                artifacts,
                authorities,
                _checkpoint(self.attempt, receipt.header.receipt_id),
            )
        artifacts, authorities = _resources(self.attempt.request, R1ReplayResourceStateV3.SEALED)
        return KnownNotCommittedEffectReplayProofV3(
            request,
            self.attempt.request,
            True,
            _operation(self.attempt, R1ReplayOperationStateV3.SEALED),
            None,
            artifacts,
            authorities,
        )

    def close(self) -> None:
        self.events.append("fresh_close")
        self.closed = True


class _FreshProofFactory:
    def __init__(
        self,
        events: list[str],
        durable: _DurableReceiptState,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        sessions: _Sessions,
        *,
        proof_kind: str = "committed",
    ) -> None:
        self.events = events
        self.durable = durable
        self.attempt = attempt
        self.sessions = sessions
        self.proof_kind = proof_kind
        self.opened: list[_FreshProofSession] = []

    def open(self) -> _FreshProofSession:
        self.events.append("fresh_open")
        assert self.sessions.opened and all(item.transaction.invalidated for item in self.sessions.opened)
        assert not hasattr(self.durable, "pending"), "fresh proof must not share transaction-local pending state"
        fresh = _FreshProofSession(self.events, self.durable, self.attempt, self.proof_kind)
        assert all(fresh is not old and fresh.handle is not old.transaction for old in self.sessions.opened)
        self.opened.append(fresh)
        return fresh


def _checkpoint(attempt: MssqlR1EffectAttemptEnvelopeV3, receipt_id: UUID):
    if attempt.request.identity.source_mode is SourceMode.BATCH_FULL_REFRESH:
        return None
    plan = attempt.request.mutation_plan
    return CheckpointReplayObservationV3(
        plan.checkpoint_writer_generation,
        plan.candidate_checkpoint_revision,
        plan.checkpoint_candidate_payload,
        receipt_id,
    )


class _Candidate:
    def __init__(self, events: list[str], authority: _Authority, stages: _Stages, *, foreign: bool = False) -> None:
        self.events = events
        self.authority = authority
        self.stages = stages
        self.foreign = foreign

    def prove(self, transaction: _Transaction, attempt: MssqlR1EffectAttemptEnvelopeV3, receipt: object):
        self.events.append("candidate")
        binding = replace(transaction.binding, factory_binding_token=b"f" * 16) if self.foreign else transaction.binding
        admitted = _admitted(attempt.request, binding) if self.foreign else self.authority.admitted
        consumed_authority = MssqlConsumedGenerationAuthoritySetV3.from_admitted(
            admitted, receipt.header.receipt_id, receipt.digest
        )
        consumed_stage = self.stages.consumed
        if self.foreign:
            consumed_stage = replace(
                consumed_stage,
                transaction_id=binding.transaction_id,
                session_identity_digest=binding.session_identity_digest,
            )
        header = receipt.header
        return MssqlCandidateEffectProofV3(
            binding,
            attempt.request.digest,
            attempt.canonical_bytes,
            attempt.digest,
            receipt.canonical_bytes,
            receipt.digest,
            MssqlCandidateOperationObservationV3(
                header.operation_key,
                header.effect_key,
                header.operation_epoch,
                header.operation_projection_revision,
                attempt.owner_id_digest,
                attempt.server_lease_expires_at,
                attempt.request.digest,
                header.receipt_id,
                receipt.digest,
            ),
            MssqlCandidateWriterHeadObservationV3(
                header.candidate_writer_generation,
                header.candidate_writer_generation,
                header.candidate_head_revision,
                header.receipt_id,
                receipt.digest,
                header.effect_key,
                header.operation_epoch,
                header.operation_projection_revision,
            ),
            header.candidate_writer_generation,
            header.candidate_writer_generation,
            _checkpoint(attempt, header.receipt_id),
            consumed_stage,
            consumed_authority,
            None,
            header.registration_id,
            header.registration_payload_digest,
            header.registered_physical_authority_digest,
            D3,
            1,
        )


def _build(
    attempt: MssqlR1EffectAttemptEnvelopeV3,
    events: list[str],
    *,
    fail: bool = False,
    foreign: bool = False,
    proof_kind: str = "committed",
    fail_rollback: bool = False,
    fail_close: bool = False,
):
    durable = _DurableReceiptState()
    receipt_appender = _ReceiptAppender(events)
    sessions = _Sessions(
        events,
        durable,
        receipt_appender,
        fail_commit=fail,
        commit_persisted=proof_kind != "known-not-committed",
        fail_rollback=fail_rollback,
        fail_close=fail_close,
    )
    authority = _Authority(events)
    stages = _Stages(events)
    mutation = _Mutation(events)
    quality = _Quality(events, attempt)
    mode = (
        MssqlR1BatchEffectV3(mutation, quality)
        if attempt.request.identity.source_mode is SourceMode.BATCH_FULL_REFRESH
        else MssqlR1XminEffectV3(mutation, quality)
    )
    fresh_proofs = _FreshProofFactory(
        events,
        durable,
        attempt,
        sessions,
        proof_kind=proof_kind,
    )
    uow = MssqlR1V3EffectUnitOfWork(
        sessions=sessions,
        stage_attestor=_Attestor(events),
        generation_authority=authority,
        stages=stages,
        writer_fence=_Fence(events),
        candidate=_Candidate(events, authority, stages, foreign=foreign),
        mode=mode,
        receipt_builder=_ReceiptBuilder(events, mode.source_mode),
        receipt_appender=receipt_appender,
        fresh_proofs=fresh_proofs,
    )
    return uow, sessions, fresh_proofs


@pytest.mark.parametrize("xmin", [False, True])
def test_effect_uow_orders_all_steps_before_exact_candidate_commit(xmin: bool) -> None:
    attempt = _fixture(xmin=xmin).attempt if xmin else _batch_request()[1]
    events: list[str] = []
    uow, sessions, _ = _build(attempt, events)

    result = uow.execute(attempt)

    mutation = ["delete", "update", "insert"] if xmin else ["batch"]
    checkpoint = ["checkpoint"] if xmin else []
    assert result.outcome is MssqlR1V3EffectOutcome.COMMITTED
    assert events == [
        "begin",
        "transaction_active",
        "session_active",
        "fence",
        *(["attest"] * len(attempt.request.artifacts)),
        "authority_admit",
        *mutation,
        "row_hash",
        "quality",
        "receipt_observation",
        "receipt_build",
        "receipt",
        *checkpoint,
        "stage_consume",
        "authority_consume",
        "head_operation",
        "candidate",
        "transaction_active",
        "session_active",
        "dispatch_commit",
        "close",
    ]
    assert len(sessions.opened) == 1


def test_foreign_candidate_binding_rolls_back_before_commit_dispatch() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, _, _ = _build(attempt, events, foreign=True)

    with pytest.raises(MssqlR1V3ContractError, match="trusted transaction binding"):
        uow.execute(attempt)

    assert "dispatch_commit" not in events
    assert events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize(
    ("boundary", "occurrence"),
    [
        ("transaction_active", 1),
        ("session_active", 1),
        ("fence", 1),
        ("attest", 1),
        ("attest", 2),
        ("authority_admit", 1),
        ("delete", 1),
        ("update", 1),
        ("insert", 1),
        ("row_hash", 1),
        ("quality", 1),
        ("receipt_observation", 1),
        ("receipt_build", 1),
        ("receipt", 1),
        ("checkpoint", 1),
        ("stage_consume", 1),
        ("authority_consume", 1),
        ("head_operation", 1),
        ("candidate", 1),
        ("transaction_active", 2),
        ("session_active", 2),
    ],
)
def test_every_predispatch_boundary_rolls_back_and_closes_once(boundary: str, occurrence: int) -> None:
    attempt = _fixture(xmin=True).attempt
    events = _FailingEvents(boundary, occurrence)
    uow, sessions, fresh_proofs = _build(attempt, events)

    with pytest.raises(RuntimeError, match=f"injected:{boundary}:{occurrence}"):
        uow.execute(attempt)

    assert events.count("rollback") == 1
    assert events.count("close") == 1
    assert "dispatch_commit" not in events
    assert "fresh_open" not in events
    assert fresh_proofs.opened == []
    assert sessions.opened[0].transaction.invalidated is True


def test_begin_failure_closes_once_without_rollback_or_fresh_probe() -> None:
    attempt = _batch_request()[1]
    events = _FailingEvents("begin")
    uow, sessions, fresh_proofs = _build(attempt, events)

    with pytest.raises(RuntimeError, match="injected:begin:1"):
        uow.execute(attempt)

    assert events == ["begin", "close"]
    assert sessions.opened[0].transaction.invalidated is True
    assert fresh_proofs.opened == []


def test_rollback_failure_becomes_unknown_and_still_closes_once() -> None:
    attempt = _batch_request()[1]
    events = _FailingEvents("quality")
    uow, sessions, _ = _build(attempt, events, fail_rollback=True)

    with pytest.raises(MssqlR1V3CommitOutcomeUnknown) as raised:
        uow.execute(attempt)

    assert isinstance(raised.value.__cause__, OSError)
    assert events.count("rollback") == 1
    assert events.count("close") == 1
    assert sessions.opened[0].transaction.invalidated is True
    assert "dispatch_commit" not in events
    assert "fresh_open" not in events


def test_close_failure_preserves_predispatch_error_and_invalidates_session() -> None:
    attempt = _batch_request()[1]
    events = _FailingEvents("quality")
    uow, sessions, _ = _build(attempt, events, fail_close=True)

    with pytest.raises(RuntimeError, match="injected:quality:1"):
        uow.execute(attempt)

    assert events.count("rollback") == 1
    assert events.count("close") == 1
    assert sessions.opened[0].transaction.invalidated is True


def test_close_failure_after_commit_dispatch_still_allows_distinct_fresh_proof() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, sessions, fresh_proofs = _build(attempt, events, fail=True, fail_close=True)

    result = uow.execute(attempt)

    assert result.outcome is MssqlR1V3EffectOutcome.COMMITTED_AFTER_FRESH_PROOF
    assert events.count("close") == 1
    assert len(fresh_proofs.opened) == 1
    assert sessions.opened[0].transaction.invalidated is True


def test_batch_has_no_checkpoint_capability_while_xmin_requires_it() -> None:
    batch_attempt = _batch_request()[1]
    batch_events: list[str] = []
    batch_uow, _, _ = _build(batch_attempt, batch_events)
    batch_uow.execute(batch_attempt)
    assert "checkpoint" not in batch_events

    xmin_attempt = _fixture(xmin=True).attempt
    xmin_events: list[str] = []
    xmin_uow, _, _ = _build(xmin_attempt, xmin_events)

    class MissingCheckpointMode:
        source_mode = SourceMode.XMIN_CURRENT_STATE
        mutation = _Mutation(xmin_events)
        quality = _Quality(xmin_events, xmin_attempt)

        def mutate(self, transaction: object, attempt: object) -> None:
            self.mutation.apply_delta(transaction, attempt)

        def update_row_hashes(self, transaction: object, attempt: object) -> None:
            self.mutation.update_row_hashes(transaction, attempt)

        def evaluate(self, transaction: object, attempt: object):
            return self.quality.evaluate(transaction, attempt)

    xmin_uow._mode = cast(object, MissingCheckpointMode())
    with pytest.raises(MssqlR1V3ContractError, match="checkpoint capability"):
        xmin_uow.execute(xmin_attempt)
    assert xmin_events[-2:] == ["rollback", "close"]


def test_lost_commit_uses_only_fresh_exact_replay_proof() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, sessions, _ = _build(attempt, events, fail=True)

    result = uow.execute(attempt)

    assert result.outcome is MssqlR1V3EffectOutcome.COMMITTED_AFTER_FRESH_PROOF
    assert events[-5:] == ["dispatch_commit", "close", "fresh_open", "fresh_probe", "fresh_close"]
    assert len(sessions.opened) == 1
    assert sessions.opened[0].transaction.invalidated is True


def test_ambiguous_fresh_proof_never_reuses_source_or_mutates_again() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, _, receipts = _build(attempt, events, fail=True)
    receipts.proof_kind = "unknown"

    with pytest.raises(MssqlR1V3CommitOutcomeUnknown):
        uow.execute(attempt)

    assert events.count("batch") == 1
    assert events[-5:] == ["dispatch_commit", "close", "fresh_open", "fresh_probe", "fresh_close"]


@pytest.mark.parametrize("proof_kind", ["mismatch", "raise"])
def test_invalid_fresh_proof_blocks_without_old_session_access(proof_kind: str) -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, sessions, fresh_proofs = _build(attempt, events, fail=True, proof_kind=proof_kind)

    with pytest.raises(MssqlR1V3CommitOutcomeUnknown):
        uow.execute(attempt)

    old = sessions.opened[0]
    assert old.transaction.invalidated is True
    assert len(fresh_proofs.opened) == 1
    with pytest.raises(AssertionError, match="old transaction accessed"):
        old.transaction.assert_active()
    with pytest.raises(AssertionError, match="old session rollback"):
        old.rollback(old.transaction)


def test_fresh_proof_factory_cannot_reuse_discarded_effect_session() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    uow, sessions, _ = _build(attempt, events, fail=True)

    class ReusingFactory:
        def open(self):
            return sessions.opened[0]

    uow._fresh_proofs = ReusingFactory()
    with pytest.raises(MssqlR1V3CommitOutcomeUnknown):
        uow.execute(attempt)

    assert events.count("close") == 1


def test_resume_does_not_take_over_source_required_open_state() -> None:
    class Preparation:
        takeovers = 0

        def pre_source(self, effect_key: bytes):
            return MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.SOURCE_REQUIRED)

        def take_over_sealed(self, effect_key: bytes, owner_id_digest: bytes):
            self.takeovers += 1
            raise AssertionError

    preparation = Preparation()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, preparation),
        unit_of_work=cast(object, SimpleNamespace()),
        owner_id_digest=D1,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )
    with pytest.raises(MssqlR1V3PreparedRunnerError, match="sealed_request_required"):
        runner.resume(D1)
    assert preparation.takeovers == 0


def test_committed_pre_source_replay_returns_without_uow() -> None:
    attempt = _batch_request()[1]
    receipt = _receipt_for(attempt)
    durable = _DurableReceiptState()
    durable.committed = receipt
    proof = _FreshProofSession([], durable, attempt, "committed").probe(proof_request_for(attempt))

    class Preparation:
        def pre_source(self, effect_key: bytes):
            assert effect_key == attempt.request.identity.effect_key
            return MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.REPLAY_COMMITTED, replay=proof)

    class UnitOfWork:
        calls = 0

        def execute(self, current: object):
            self.calls += 1
            raise AssertionError("committed pre-source replay must not open the UoW")

    unit_of_work = UnitOfWork()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, Preparation()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    result = runner.resume(attempt.request.identity.effect_key)
    assert result.receipt == receipt
    assert result.replayed is True
    assert unit_of_work.calls == 0


def test_blocked_pre_source_outcome_returns_reason_without_uow() -> None:
    class Preparation:
        def pre_source(self, effect_key: bytes):
            return MssqlR1PreSourceOutcomeV3(
                MssqlR1PreSourceStatusV3.BLOCKED,
                blocker="registration_revoked",
            )

    unit_of_work = SimpleNamespace(execute=lambda current: pytest.fail("blocked outcome must not open UoW"))
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, Preparation()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=D1,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match="postgres_mssql_r1.registration_revoked"):
        runner.resume(D1)


def test_sealed_result_distinguishes_direct_commit_from_fresh_replay() -> None:
    attempt = _batch_request()[1]
    direct_uow, _, _ = _build(attempt, [])
    fresh_uow, _, _ = _build(attempt, [], fail=True)
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, SimpleNamespace()),
        unit_of_work=direct_uow,
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    assert runner.run_sealed(attempt).replayed is False
    fresh_runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, SimpleNamespace()),
        unit_of_work=fresh_uow,
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )
    assert fresh_runner.run_sealed(attempt).replayed is True


def test_prepared_runner_rejects_other_closed_mode_before_uow() -> None:
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, SimpleNamespace()),
        unit_of_work=cast(object, SimpleNamespace()),
        owner_id_digest=D1,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )
    with pytest.raises(MssqlR1V3ContractError, match="mode differs"):
        runner.run_sealed(_fixture(xmin=True).attempt)


def test_sealed_resume_rejects_requested_effect_mismatch_before_uow() -> None:
    attempt = _batch_request()[1]

    class Preparation:
        def pre_source(self, effect_key: bytes):
            return MssqlR1PreSourceOutcomeV3(
                MssqlR1PreSourceStatusV3.SEALED_RESUME,
                sealed_request=attempt.request,
                attempt=attempt,
            )

    class UnitOfWork:
        calls = 0

        def execute(self, current: object):
            self.calls += 1
            raise AssertionError("UoW must not open for a foreign requested effect")

    unit_of_work = UnitOfWork()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, Preparation()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match="sealed_resume_effect_mismatch"):
        runner.resume(b"x" * 32)
    assert unit_of_work.calls == 0


def test_direct_sealed_attempt_rejects_foreign_owner_before_uow() -> None:
    attempt = _batch_request()[1]
    unit_of_work = SimpleNamespace(execute=lambda current: pytest.fail("UoW must not open for a foreign owner"))
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, SimpleNamespace()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=D1,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match="sealed_attempt_owner_mismatch"):
        runner.run_sealed(attempt)


def test_resumed_sealed_attempt_rejects_foreign_owner_before_uow() -> None:
    attempt = _batch_request()[1]

    class Preparation:
        def pre_source(self, effect_key: bytes):
            return MssqlR1PreSourceOutcomeV3(
                MssqlR1PreSourceStatusV3.SEALED_RESUME,
                sealed_request=attempt.request,
                attempt=attempt,
            )

    unit_of_work = SimpleNamespace(execute=lambda current: pytest.fail("foreign owner must not open UoW"))
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, Preparation()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=D1,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match="sealed_attempt_owner_mismatch"):
        runner.resume(attempt.request.identity.effect_key)


def test_batch_provider_executes_only_independently_readmitted_steps() -> None:
    fixture = _fixture(xmin=False)
    admission, _, verifier = _admission(fixture)
    gateway = _Gateway()
    provider = MssqlR1BatchMutationProviderV3(gateway, admission)

    provider.mutate(_MutationTransaction(), fixture.attempt)
    provider.update_row_hashes(_MutationTransaction(), fixture.attempt)

    assert tuple(step.value for step in gateway.steps) == ("batch_publication", "row_hash")
    assert verifier.calls == 2


def test_batch_provider_rejects_self_digested_sql_tamper_before_sql_io() -> None:
    fixture = _fixture(xmin=False)
    admission, _, _ = _admission(fixture)
    tampered = _tamper(fixture, R1MutationStepV1.BATCH_PUBLICATION, b"TRUNCATE TABLE [dbo].[orders]")
    gateway = _Gateway()

    with pytest.raises(ValueError, match="certified closed templates"):
        MssqlR1BatchMutationProviderV3(gateway, admission).mutate(_MutationTransaction(), tampered)

    assert gateway.steps == []


def test_batch_provider_rejects_foreign_renderer_authority_before_sql_io() -> None:
    fixture = _fixture(xmin=False)
    foreign = replace(fixture.authority, resolved_profile_digest=D1)
    resolver = _AuthorityResolver(foreign)
    verifier = _ExactTemplateVerifier(fixture)
    admission = MssqlR1V3RenderedAdmissionAdapter(resolver, verifier)
    gateway = _Gateway()

    with pytest.raises(ValueError, match="unknown signed environment profile"):
        MssqlR1BatchMutationProviderV3(gateway, admission).mutate(_MutationTransaction(), fixture.attempt)

    assert verifier.calls == 0
    assert gateway.steps == []


def test_known_noncommit_uses_adjacent_source_free_takeover() -> None:
    attempt = _batch_request()[1]
    events: list[str] = []
    unit_of_work, sessions, fresh_proofs = _build(
        attempt,
        events,
        fail=True,
        proof_kind="known-not-committed",
    )

    class Preparation:
        calls = 0

        def take_over_sealed(self, effect_key: bytes, owner_id_digest: bytes):
            self.calls += 1
            assert effect_key == attempt.request.identity.effect_key and owner_id_digest == attempt.owner_id_digest
            return replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            )

    preparation = Preparation()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, preparation),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    result = runner.run_sealed(attempt)
    assert result.replayed is False
    assert result.receipt.header.operation_epoch == attempt.operation_epoch + 1
    assert preparation.calls == 1
    assert len(sessions.opened) == 2
    assert len(fresh_proofs.opened) == 1
    assert events.count("batch") == 2


@pytest.mark.parametrize(
    ("case", "successor_factory", "message"),
    [
        (
            "request",
            lambda attempt: replace(
                attempt,
                request=_fixture(xmin=False, empty=True).attempt.request,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "owner",
            lambda attempt: replace(
                attempt,
                owner_id_digest=D1,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_attempt_owner_mismatch",
        ),
        (
            "epoch_same",
            lambda attempt: replace(
                attempt,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "epoch_skip",
            lambda attempt: replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 2,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "revision_same",
            lambda attempt: replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 1,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "revision_skip",
            lambda attempt: replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 2,
                server_lease_expires_at=attempt.server_lease_expires_at + timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "lease_same",
            lambda attempt: replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 1,
            ),
            "sealed_takeover_not_adjacent",
        ),
        (
            "lease_earlier",
            lambda attempt: replace(
                attempt,
                operation_epoch=attempt.operation_epoch + 1,
                operation_projection_revision=attempt.operation_projection_revision + 1,
                server_lease_expires_at=attempt.server_lease_expires_at - timedelta(seconds=1),
            ),
            "sealed_takeover_not_adjacent",
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_known_noncommit_rejects_nonadjacent_takeover(
    case: str,
    successor_factory,
    message: str,
) -> None:
    attempt = _batch_request()[1]

    class UnitOfWork:
        calls = 0

        def execute(self, current: object):
            self.calls += 1
            return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED, None)

    class Preparation:
        def take_over_sealed(self, effect_key: bytes, owner_id_digest: bytes):
            return successor_factory(attempt)

    unit_of_work = UnitOfWork()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, Preparation()),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match=message):
        runner.run_sealed(attempt)
    assert unit_of_work.calls == 1, case


def test_known_noncommit_takeover_limit_blocks_without_takeover() -> None:
    attempt = _batch_request()[1]

    class UnitOfWork:
        def execute(self, current: object):
            return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED, None)

    preparation = SimpleNamespace(
        take_over_sealed=lambda effect_key, owner_id_digest: pytest.fail("takeover exceeds configured limit")
    )
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, preparation),
        unit_of_work=cast(object, UnitOfWork()),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        maximum_takeovers=0,
    )

    with pytest.raises(MssqlR1V3PreparedRunnerError, match="known_noncommit_takeover_limit"):
        runner.run_sealed(attempt)


def test_known_noncommit_rejects_untyped_successor_before_retry() -> None:
    attempt = _batch_request()[1]

    class UnitOfWork:
        calls = 0

        def execute(self, current: object):
            self.calls += 1
            return MssqlR1V3EffectResult(MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED, None)

    preparation = SimpleNamespace(
        take_over_sealed=lambda effect_key, owner_id_digest: SimpleNamespace(request=attempt.request)
    )
    unit_of_work = UnitOfWork()
    runner = MssqlR1PreparedEffectRunnerV3(
        preparation=cast(object, preparation),
        unit_of_work=cast(object, unit_of_work),
        owner_id_digest=attempt.owner_id_digest,
        source_mode=SourceMode.BATCH_FULL_REFRESH,
    )

    with pytest.raises(MssqlR1V3ContractError, match="exact V3 attempt"):
        runner.run_sealed(attempt)
    assert unit_of_work.calls == 1


def _receipt_for(attempt: MssqlR1EffectAttemptEnvelopeV3):
    events: list[str] = []
    uow, _, _ = _build(attempt, events)
    return uow.execute(attempt).receipt
