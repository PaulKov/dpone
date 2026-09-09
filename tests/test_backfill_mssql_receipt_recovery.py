from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dpone.backfill.mssql_receipt_recovery import (
    MssqlBackfillReceiptRecoveryError,
    MssqlShadowInitialReceiptRecovery,
)
from dpone.backfill.state import BackfillChunkRecord, BackfillLedger, FileBackfillStateStore
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)


class _ReceiptState:
    def __init__(self, receipt: MssqlGenericCommitReceipt | None) -> None:
        self.receipt = receipt
        self.probes: list[object] = []

    def probe_receipt_fresh(self, operation: object) -> MssqlGenericCommitReceipt | None:
        self.probes.append(operation)
        return self.receipt

    def source_copy(self) -> None:
        raise AssertionError("receipt recovery must not read the source")

    def target_insert(self) -> None:
        raise AssertionError("receipt recovery must not mutate the target")


def _operation(*, strategy: str = "backfill") -> MssqlTransactionOperation:
    attempt_request = MssqlAttemptRequest(
        invocation=InvocationIdentity(
            run_id="dpone-backfill:campaign-a",
            process="initial",
            task_partition="dbo.orders:chunk-5",
        ),
        target_identity=b"c" * 32,
        route_fingerprint=b"f" * 32,
        load_id="load-committed",
        target_database="analytics_staging",
        target_schema="dbo",
        target_table="orders__shadow",
        strategy=strategy,
    )
    attempt = MssqlTransactionAttempt(request=attempt_request, generation=1)
    operation_request = MssqlOperationRequest(scope_hash=b"d" * 32, owner_digest=b"e" * 32)
    return MssqlTransactionOperation(
        attempt=attempt,
        operation_key=operation_request.operation_key(attempt),
        scope_hash=operation_request.scope_hash,
        owner_digest=operation_request.owner_digest,
        epoch=1,
    )


def _receipt(
    rows: int = 971_362,
    *,
    operation: MssqlTransactionOperation | None = None,
) -> MssqlGenericCommitReceipt:
    operation = operation or _operation()
    started = datetime(2026, 8, 23, 8, 15, tzinfo=UTC)
    return MssqlGenericCommitReceipt(
        receipt_id=operation.receipt_id,
        operation_key=operation.operation_key,
        attempt_key=operation.attempt.attempt_key,
        target_identity=operation.attempt.target_identity,
        generation=operation.attempt.generation,
        scope_hash=operation.scope_hash,
        operation_epoch=operation.epoch,
        owner_digest=operation.owner_digest,
        route_fingerprint=operation.attempt.route_fingerprint,
        load_id=operation.attempt.request.load_id,
        strategy=operation.attempt.request.strategy,
        mutation_plan_sha256=b"g" * 32,
        target_before_sha256=b"h" * 32,
        target_after_sha256=b"i" * 32,
        loaded_at_utc=started + timedelta(seconds=5),
        committed_at_utc=started + timedelta(seconds=6),
        payload_evidence=MssqlPayloadCommitEvidence(
            manifest_sha256=b"j" * 32,
            declared_rows=rows,
            actual_raw_rows=rows,
            actual_native_rows=rows,
            native_contract_sha256=b"k" * 32,
        ),
        source_lifecycle=MssqlSourceLifecycleEvidence(
            extraction_started_at_utc=started,
            extraction_completed_at_utc=started + timedelta(seconds=4),
            clock_authority="postgresql.transaction_timestamp",
            snapshot_acquired_at_utc=started + timedelta(seconds=1),
            snapshot_authority="postgresql.repeatable_read",
            source_token_sha256=b"l" * 32,
        ),
        metrics=MssqlReceiptMetrics(
            inserted_rows=rows,
            updated_rows=0,
            total_rows=rows,
            staging_rows=rows,
            replaced_rows=0,
            soft_deleted_rows=0,
            reactivated_rows=0,
            unchanged_rows=0,
            hard_deleted_rows=0,
        ),
    )


def _running_store(tmp_path: Path, *, owner: str = "worker-a") -> FileBackfillStateStore:
    store = FileBackfillStateStore(tmp_path)
    store.save(
        BackfillLedger(
            run_key="campaign-a",
            dataset="dbo.orders",
            inner_mode="incremental_append",
            chunk_config={},
            chunks=[BackfillChunkRecord(index=5, start="5", end="5", idempotency_key="campaign-a:5")],
        )
    )
    assert store.acquire_chunk_lease(
        "campaign-a",
        5,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    return store


def test_target_commit_receipt_promotes_ledger_without_source_copy_or_target_insert(tmp_path: Path) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    state = _ReceiptState(receipt)
    store = _running_store(tmp_path)

    evidence = MssqlShadowInitialReceiptRecovery(state).promote_owned(
        operation,
        store=store,
        run_key="campaign-a",
        chunk_index=5,
        owner="worker-a",
    )

    assert evidence is not None
    assert evidence.rows == 971_362
    assert evidence.load_id == "load-committed"
    assert evidence.already_projected is False
    assert state.probes == [operation]
    committed = store.load("campaign-a")
    assert committed is not None
    record = committed.chunk(5)
    assert record.status == "success"
    assert record.rows_extracted == record.rows_loaded == 971_362
    assert record.load_id == "load-committed"
    assert record.lease_owner is None
    assert record.lease_expires_at is None


def test_absent_receipt_keeps_running_chunk_uncommitted(tmp_path: Path) -> None:
    state = _ReceiptState(None)
    store = _running_store(tmp_path)

    assert (
        MssqlShadowInitialReceiptRecovery(state).promote_owned(
            _operation(),
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )
        is None
    )
    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"
    assert current.chunk(5).rows_loaded == 0


def test_receipt_metric_mismatch_fails_closed_without_ledger_promotion(tmp_path: Path) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    mismatched = replace(
        receipt,
        metrics=replace(receipt.metrics, inserted_rows=receipt.metrics.inserted_rows - 1),
    )
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="metrics_mismatch"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(mismatched)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


def test_receipt_projection_is_idempotent_but_rejects_conflicting_success(tmp_path: Path) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    store = _running_store(tmp_path)
    recovery = MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt))
    first = recovery.promote_owned(
        operation,
        store=store,
        run_key="campaign-a",
        chunk_index=5,
        owner="worker-a",
    )
    second = recovery.promote_owned(
        operation,
        store=store,
        run_key="campaign-a",
        chunk_index=5,
        owner="another-owner",
    )

    assert first is not None and first.already_projected is False
    assert second is not None and second.already_projected is True

    ledger = store.load("campaign-a")
    assert ledger is not None
    ledger.chunk(5).rows_loaded -= 1
    store.save(ledger)
    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="ledger_projection_mismatch"):
        recovery.promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="another-owner",
        )


def test_non_backfill_receipt_fails_closed_without_ledger_promotion(tmp_path: Path) -> None:
    operation = _operation(strategy="incremental_append")
    receipt = _receipt(operation=operation)
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="strategy_mismatch"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


def test_declared_raw_native_mismatch_fails_closed_without_ledger_promotion(tmp_path: Path) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    object.__setattr__(receipt.payload_evidence, "actual_raw_rows", receipt.payload_evidence.declared_rows - 1)
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="payload_count_mismatch"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


@pytest.mark.parametrize(
    ("container", "field", "invalid"),
    [
        ("payload", "declared_rows", -1),
        ("payload", "actual_native_rows", 1.5),
        ("metrics", "inserted_rows", -1),
        ("metrics", "staging_rows", True),
    ],
)
def test_corrupt_negative_or_non_integer_counts_fail_closed(
    tmp_path: Path,
    container: str,
    field: str,
    invalid: object,
) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    evidence = receipt.payload_evidence if container == "payload" else receipt.metrics
    object.__setattr__(evidence, field, invalid)
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="count_invalid"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


def test_shadow_receipt_rejects_cumulative_target_cardinality(tmp_path: Path) -> None:
    operation = _operation()
    receipt = _receipt(operation=operation)
    receipt = replace(receipt, metrics=replace(receipt.metrics, total_rows=receipt.metrics.total_rows * 2))
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="metrics_mismatch"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


def test_receipt_operation_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    operation = _operation()
    receipt = replace(_receipt(operation=operation), operation_key=b"z" * 32)
    store = _running_store(tmp_path)

    with pytest.raises(MssqlBackfillReceiptRecoveryError, match="operation_identity_mismatch"):
        MssqlShadowInitialReceiptRecovery(_ReceiptState(receipt)).promote_owned(
            operation,
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"


class _NativeCrash(BaseException):
    pass


class _CrashingReceiptState:
    def __init__(self, crash: BaseException) -> None:
        self.crash = crash

    def probe_receipt_fresh(self, _operation: MssqlTransactionOperation) -> MssqlGenericCommitReceipt | None:
        raise self.crash


def test_probe_base_exception_preserves_identity_and_never_mutates_ledger(tmp_path: Path) -> None:
    crash = _NativeCrash("native-child-crashed")
    store = _running_store(tmp_path)

    with pytest.raises(_NativeCrash) as caught:
        MssqlShadowInitialReceiptRecovery(_CrashingReceiptState(crash)).promote_owned(
            _operation(),
            store=store,
            run_key="campaign-a",
            chunk_index=5,
            owner="worker-a",
        )

    assert caught.value is crash
    current = store.load("campaign-a")
    assert current is not None
    assert current.chunk(5).status == "running"
    assert current.chunk(5).rows_loaded == 0
