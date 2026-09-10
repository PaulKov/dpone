"""Durable native prepared values and source-free replay preserve exact identity."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlReceiptMetrics,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, ConsumedPayloadPartEvidence
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.mssql_native_recovery import prepared_snapshot, restore_prepared
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService, NativePreparedStage
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    require_payload_evidence,
    require_source_lifecycle,
)


def prepared_fixture():
    digest = b"x" * 32
    request = MssqlAttemptRequest(
        InvocationIdentity("run", "process", "task"), digest, digest, "load", "db", "dbo", "target", "full_refresh"
    )
    attempt = MssqlTransactionAttempt(request, 1)
    operation_request = MssqlOperationRequest(digest, digest)
    operation = MssqlTransactionOperation(attempt, operation_request.operation_key(attempt), digest, digest, 1)
    admission = MssqlTransactionAdmission(operation=operation)
    evidence = ConsumedPayloadEvidence(
        (
            ConsumedPayloadPartEvidence(
                "native:00000000000000000000",
                "a" * 64,
                8,
                "b" * 64,
                "c" * 64,
                "d" * 64,
                "e" * 64,
                None,
                2,
                2,
            ),
        )
    ).with_native_rows(2, native_contract_sha256="f" * 64)
    stage = StagingTableArtifact(
        "stage",
        "owned",
        ("n",),
        object(),
        row_count=2,
        database="db",
        column_types={"n": "int"},
        target_column_types={"n": "int"},
        wire_schema=(("n", "int"),),
        consumed_payload_evidence=evidence,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    lifecycle = ExtractionLifecycleReceipt(now, "test.clock", extraction_completed_at=now)
    plan = MssqlTargetMutationPlan.from_admission(admission)
    return NativePreparedStage(stage, admission, lifecycle, plan, None)


def test_json_roundtrip_preserves_typed_receipt_and_mutation_identity():
    original = prepared_fixture()
    snapshot = json.loads(json.dumps(prepared_snapshot(original, recovery={})))
    restored = restore_prepared(
        snapshot, admission=original.admission, staging_manager=object(), interval=None, resources=()
    )
    assert restored.staging.consumed_payload_evidence == original.staging.consumed_payload_evidence
    assert restored.source_lifecycle == original.source_lifecycle
    assert restored.mutation_plan.digest == original.mutation_plan.digest


def test_new_operation_cannot_claim_old_prepared_table():
    original = prepared_fixture()
    operation = replace(original.admission.operation, operation_key=b"z" * 32)
    with pytest.raises(ValueError, match="operation_changed"):
        restore_prepared(
            prepared_snapshot(original, recovery={}),
            admission=MssqlTransactionAdmission(operation=operation),
            staging_manager=object(),
            interval=None,
            resources=(),
        )


def receipt_fixture(prepared):
    op = prepared.admission.operation
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return MssqlGenericCommitReceipt(
        op.receipt_id,
        op.operation_key,
        op.attempt.attempt_key,
        op.attempt.target_identity,
        1,
        op.scope_hash,
        1,
        op.owner_digest,
        op.attempt.route_fingerprint,
        "load",
        "full_refresh",
        prepared.mutation_plan.digest,
        prepared.mutation_plan.expected_before_sha256,
        prepared.mutation_plan.expected_after_sha256,
        now,
        now,
        require_payload_evidence(prepared.staging, staging_rows=2),
        require_source_lifecycle(prepared.source_lifecycle),
        MssqlReceiptMetrics(2, 0, 2, staging_rows=2),
    )


def test_published_recovery_uses_exact_target_receipt_without_stage_or_source_reads():
    prepared = prepared_fixture()
    receipt = receipt_fixture(prepared)
    admission = MssqlTransactionAdmission(replay_receipt=receipt)
    prepared = replace(prepared, admission=admission)
    events = []

    class Journal:
        @property
        def publication(self):
            return self

        def state(self):
            return {"phase": "publishing", "prepared": {"operation_key": receipt.operation_key.hex()}}

        def publication_confirmed(self, result):
            events.append(result)

    class Preparer:
        def restore(self, config, context, value):
            assert value is admission
            return prepared

        def reverify(self, prepared):
            raise AssertionError("Published replay must not depend on remaining stages")

    service = MssqlNativeStagedLoadService(object(), Preparer())
    context = SimpleNamespace(journal_factory=Journal, row_source=lambda: pytest.fail("source reopened"))
    result = service.resume(SimpleNamespace(load_strategy=LoadStrategy.FULL_REFRESH), context, admission)
    assert result.commit_receipt_id == receipt.receipt_id
    assert events == [{"receipt_id": receipt.receipt_id}]


def test_changed_committed_payload_cannot_be_reported_as_success():
    from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericCommitOutcomeUnknown

    prepared = prepared_fixture()
    receipt = receipt_fixture(prepared)
    receipt = replace(receipt, payload_evidence=replace(receipt.payload_evidence, manifest_sha256=b"z" * 32))
    admission = MssqlTransactionAdmission(replay_receipt=receipt)

    class Preparer:
        def restore(self, *args):
            return replace(prepared, admission=admission)

    context = SimpleNamespace(
        journal_factory=lambda: SimpleNamespace(
            publication=SimpleNamespace(
                state=lambda: {"phase": "published", "prepared": {"operation_key": receipt.operation_key.hex()}}
            )
        )
    )
    with pytest.raises(MssqlGenericCommitOutcomeUnknown):
        MssqlNativeStagedLoadService(object(), Preparer()).resume(object(), context, admission)
