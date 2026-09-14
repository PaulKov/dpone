"""Commit faults retain real source files and stages through public lifecycle APIs.

Real strategy, consumer, finalizer, native service and ownership scope execute.
Only external SQL/evidence/preparation collaborators are synthetic constructor DI;
these tests do not certify database transactions or operating-system signals.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
    operation_owner_digest,
    operation_scope_hash,
)
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.consumed_payload_evidence import (
    ConsumedPayloadEvidence,
    ConsumedPayloadPartEvidence,
    canonical_native_contract_sha256,
)
from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome, ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService, NativePreparedStage
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import MSSQLFullRefreshStrategy
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)
from dpone.runtime.sources.extract_result import ExtractResult

START = datetime(2026, 1, 1, tzinfo=UTC)
STAGE = "synthetic_db.stage.owned_attempt"


class SyntheticConnector:
    """Record transaction calls and simulate one configured COMMIT outcome."""

    def __init__(self) -> None:
        self.faults: dict[str, BaseException] = {}
        self.commit_effect = True
        self.committed = False
        self.events: list[str] = []

    def event(self, name: str) -> None:
        self.events.append(name)
        if error := self.faults.get(name):
            raise error

    def begin(self) -> None:
        self.event("begin")

    def table_exists(self, schema: str, table: str, *, database: str) -> bool:
        assert (database, schema, table) == ("synthetic_db", "dbo", "target")
        return True

    def execute_query(self, sql: str, _params: object = None) -> int:
        assert sql in {
            "SET XACT_ABORT ON",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "TRUNCATE TABLE [synthetic_db].[dbo].[target]",
        }, sql
        self.event(sql)
        return 0

    def get_records(self, sql: str, _params: object = None, *, as_dict: bool = False) -> list[Any]:
        if "COUNT_BIG(*)" in sql:
            return [(1,)]
        if sql.startswith("INSERT INTO [synthetic_db].[dbo].[target] WITH (TABLOCK)"):
            assert "FROM [synthetic_db].[stage].[owned_attempt]" in sql
            self.event("business_dml")
            return [{"__dpone__affected_rows": 1}]
        if sql == "SELECT @@SPID AS session_id":
            return [{"session_id": 57}]
        if sql == "SELECT SYSUTCDATETIME() AS loaded_at_utc":
            return [{"loaded_at_utc": START + timedelta(seconds=2)}]
        raise AssertionError(f"Unexpected synthetic query: {sql}")

    def commit_transaction(self) -> None:
        self.committed = self.commit_effect
        self.event("commit_attempted")

    def rollback(self) -> None:
        self.event("rollback")

    def close(self) -> None:
        self.event("close")

    def quote_identifier(self, value: str) -> str:
        return "[" + value.replace("]", "]]") + "]"


class SyntheticReceiptStore:
    """Create real receipts from actual finalizer inputs; simulate fresh reads."""

    def __init__(self, connector: SyntheticConnector) -> None:
        self.connector = connector
        self.pending: MssqlGenericCommitReceipt | None = None
        self.receipt_mode = "exact"

    def assert_current(self, _connector: object, _operation: object, *, require_unexpired_lease: bool) -> None:
        self.connector.events.append(f"assert_current:{require_unexpired_lease}")

    def probe_receipt(self, _operation: object, *, connector: object) -> None:
        assert connector is self.connector
        self.connector.events.append("existing_receipt_absent")

    def insert_receipt(
        self, _connector: object, operation: MssqlTransactionOperation, **values: Any
    ) -> MssqlGenericCommitReceipt:
        self.connector.events.append("insert_receipt")
        self.pending = MssqlGenericCommitReceipt(
            receipt_id=operation.receipt_id,
            operation_key=operation.operation_key,
            attempt_key=operation.attempt.attempt_key,
            target_identity=operation.attempt.target_identity,
            generation=operation.attempt.generation,
            scope_hash=operation.scope_hash,
            operation_epoch=operation.epoch,
            owner_digest=operation.owner_digest,
            route_fingerprint=operation.attempt.route_fingerprint,
            strategy=operation.attempt.request.strategy,
            committed_at_utc=START + timedelta(seconds=3),
            **values,
        )
        return self.pending

    def probe_receipt_fresh(self, _operation: object) -> MssqlGenericCommitReceipt | None:
        self.connector.event("fresh_receipt_probe")
        if not self.connector.committed or self.receipt_mode == "absent":
            return None
        assert self.pending is not None
        if self.receipt_mode == "mismatch":
            return replace(self.pending, mutation_plan_sha256=b"x" * 32)
        return self.pending


class SyntheticStagingManager:
    """Own exactly one fake table and record real artifact cleanup requests."""

    def __init__(self, connector: SyntheticConnector) -> None:
        self.connector = connector
        self.dropped: list[str] = []

    def finalize_native_evidence(
        self, raw: StagingTableArtifact, native: StagingTableArtifact, *, columns: Any
    ) -> None:
        self.connector.events.append("finalize_native_evidence")
        assert isinstance(raw.consumed_payload_evidence, ConsumedPayloadEvidence)
        contract = canonical_native_contract_sha256(
            columns,
            source_wire_contract_sha256s=[part.wire_contract_sha256 for part in raw.consumed_payload_evidence.parts],
        )
        native.consumed_payload_evidence = raw.consumed_payload_evidence.with_native_rows(
            1, native_contract_sha256=contract
        )

    def drop(self, artifact: StagingTableArtifact) -> None:
        assert artifact.qualified_name() == STAGE
        self.connector.events.append("drop_owned_stage")
        self.dropped.append(artifact.qualified_name())


class SyntheticInput:
    """Supply an owned typed stage at the normal materialization boundary."""

    def materialize(self, manager: SyntheticStagingManager, _config: LoadConfig, schema: Any) -> StagingTableArtifact:
        manager.connector.events.append("materialize")
        assert list(schema) == [("id", "bigint")]
        part = ConsumedPayloadPartEvidence(
            order_key="sequential:00000000000000000000",
            artifact_sha256="a" * 64,
            artifact_size_bytes=1,
            wire_contract_sha256="b" * 64,
            wire_schema_sha256="c" * 64,
            source_provenance_sha256="d" * 64,
            validated_schema_sha256="e" * 64,
            contract_validation_sha256=None,
            declared_rows=1,
            actual_raw_rows=1,
        )
        return StagingTableArtifact(
            database="synthetic_db",
            schema="stage",
            table="owned_attempt",
            columns=("id",),
            column_types={"id": "bigint"},
            target_column_types={"id": "bigint"},
            row_count=1,
            staging_manager=manager,
            consumed_payload_evidence=ConsumedPayloadEvidence((part,)),
            typed_file_ingestion=True,
            typed_file_deferred_native_evidence=True,
            direct_native_staging=True,
        )


def admission() -> MssqlTransactionAdmission:
    """Build a stable operation through existing public contract constructors."""
    request = MssqlAttemptRequest(
        invocation=InvocationIdentity("synthetic_run", "process", "synthetic:task"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="synthetic_load",
        target_database="synthetic_db",
        target_schema="dbo",
        target_table="target",
        strategy="full_refresh",
    )
    attempt = MssqlTransactionAttempt(request, generation=3)
    scope = MssqlOperationRequest(
        operation_scope_hash({"kind": "target_wide", "version": 1}), operation_owner_digest("owner")
    )
    return MssqlTransactionAdmission(
        operation=MssqlTransactionOperation(
            attempt=attempt,
            operation_key=scope.operation_key(attempt),
            scope_hash=scope.scope_hash,
            owner_digest=scope.owner_digest,
            epoch=2,
        )
    )


@pytest.fixture
def case(tmp_path: Path) -> Any:
    connector = SyntheticConnector()
    store = SyntheticReceiptStore(connector)
    manager = SyntheticStagingManager(connector)

    def record(name: str, *_args: Any, **_kwargs: Any) -> None:
        connector.event(name)

    factory = partial(
        MssqlGenericTransactionFinalizer,
        transaction_state=store,
        lock_acquirer=partial(record, "operation_lock"),
        target_lock_acquirer=partial(record, "target_lock"),
        target_identity_assertion=partial(record, "identity_check"),
        catalog_revalidator=partial(record, "catalog_check"),
        target_contract_validator=partial(record, "target_contract_check"),
    )
    strategy = MSSQLFullRefreshStrategy(
        connector,
        logging.getLogger(__name__),
        manager,
        SimpleNamespace(atomicity="target_atomic", provisioning="external"),
        transaction_finalizer_factory=factory,
    )
    config = LoadConfig(
        source_conn_id="synthetic_source",
        target_conn_id="synthetic_target",
        source_schema="dbo",
        source_table="source",
        target_schema="dbo",
        target_table="target",
        target_database="synthetic_db",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"source_type": "mssql", "sink_type": "mssql", "lineage": False},
    )
    lifecycle = ExtractionLifecycleAuthority()
    lifecycle.acquire(started_at=START)
    lifecycle.complete(completed_at=START + timedelta(seconds=1))
    path = tmp_path / "source.tsv"
    path.write_bytes(b"1\n")
    source = FileExportArtifact(str(path), ("id",), rows_exported=1)
    scope = OwnedPayloadScope.from_extract_result(
        ExtractResult(source, (("id", "bigint"),), extraction_lifecycle=lifecycle)
    )
    payload = LoadPayload(
        source,
        (("id", "bigint"),),
        mssql_transaction_admission=admission(),
        extraction_lifecycle=lifecycle,
        owned_payload_scope=scope,
    ).rebind(artifact=SyntheticInput())
    return SimpleNamespace(
        connector=connector,
        store=store,
        manager=manager,
        strategy=strategy,
        config=config,
        payload=payload,
        scope=scope,
        source=source,
        path=path,
    )


def assert_retained(case: Any, error: BaseException) -> None:
    """Use the same public terminal operation as the outer processor."""
    receipt = case.scope.terminate_for_error(error)
    assert receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert receipt.cleanup_succeeded
    assert case.scope.abort() is receipt  # Later cleanup cannot replace retention.
    case.source.cleanup()
    assert case.path.read_bytes() == b"1\n"
    assert not case.manager.dropped
    assert case.scope.target_commit_result is None
    events = case.connector.events
    assert events.count("commit_attempted") == events.count("business_dml") == 1
    assert events.count("insert_receipt") == 1
    assert events.index("insert_receipt") < events.index("commit_attempted")
    assert "rollback" not in events


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize(
    "kind,code", [(KeyboardInterrupt, "cancel"), (SystemExit, 23), (SystemExit, None), (SystemExit, "cancel")]
)
@pytest.mark.parametrize("boundary", ["commit_attempted", "close", "fresh_receipt_probe"])
def test_interruption_preserves_category_cause_exit_code_and_evidence(case, committed, kind, code, boundary):
    interrupt = kind(code)
    original_cause = ValueError("synthetic cancellation cause")
    interrupt.__cause__ = original_cause
    case.connector.commit_effect = committed
    case.connector.faults = {"commit_attempted": OSError("lost ACK"), boundary: interrupt}
    with pytest.raises(kind) as caught:
        case.strategy.load(case.config, case.payload)
    assert caught.value is interrupt
    assert caught.value.__cause__ is original_cause
    if kind is SystemExit:
        assert caught.value.code == code
    assert not isinstance(caught.value, Exception)
    assert case.connector.events.count("close") == 1
    assert case.connector.events.count("fresh_receipt_probe") == int(boundary == "fresh_receipt_probe")
    assert_retained(case, caught.value)


@pytest.mark.parametrize("close_kind", [OSError, KeyboardInterrupt, SystemExit])
def test_close_failure_cannot_replace_commit_cancellation(case, close_kind):
    interrupt = KeyboardInterrupt("cancel")
    case.connector.faults = {"commit_attempted": interrupt, "close": close_kind("close failed")}
    with pytest.raises(KeyboardInterrupt) as caught:
        case.strategy.load(case.config, case.payload)
    assert caught.value is interrupt
    assert case.connector.events.count("close") == 1
    assert any(close_kind.__name__ in note for note in caught.value.__notes__)
    assert "fresh_receipt_probe" not in case.connector.events
    assert_retained(case, caught.value)


@pytest.mark.parametrize("boundary", ["close", "fresh_receipt_probe"])
def test_reconciliation_failure_preserves_unknown_outcome_and_causal_chain(case, boundary):
    commit_error, recovery_error = OSError("lost ACK"), OSError("recovery failed")
    case.connector.faults = {"commit_attempted": commit_error, boundary: recovery_error}
    with pytest.raises(MssqlGenericCommitOutcomeUnknown) as caught:
        case.strategy.load(case.config, case.payload)
    assert caught.value.__cause__ is recovery_error
    assert recovery_error.__context__ is commit_error
    assert_retained(case, caught.value)


@pytest.mark.parametrize("receipt_mode", ["absent", "mismatch"])
def test_unproven_fresh_receipt_never_authorizes_cleanup(case, receipt_mode):
    case.store.receipt_mode = receipt_mode
    case.connector.faults["commit_attempted"] = OSError("lost ACK")
    with pytest.raises(MssqlGenericCommitOutcomeUnknown) as caught:
        case.strategy.load(case.config, case.payload)
    assert_retained(case, caught.value)


@pytest.mark.parametrize("lost_ack", [False, True])
def test_ordinary_commit_and_exact_receipt_recovery_release_once(case, lost_ack):
    if lost_ack:
        case.connector.faults["commit_attempted"] = OSError("lost ACK")
    result = case.strategy.load(case.config, case.payload)
    expected = AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE if lost_ack else AtomicCommitOutcome.COMMITTED
    assert result.commit_outcome is expected
    assert result.commit_receipt_id == case.payload.mssql_transaction_admission.operation.receipt_id
    assert result.inserted_rows == result.total_rows == 1
    case.scope.mark_target_committed(result)
    case.scope.success()
    assert not case.path.exists()
    assert case.manager.dropped == [STAGE]
    assert case.connector.events.count("commit_attempted") == 1
    assert case.connector.events.count("fresh_receipt_probe") == int(lost_ack)


@pytest.mark.parametrize("kind", [ValueError, KeyboardInterrupt, SystemExit])
def test_precommit_failures_keep_existing_abort_behavior(case, kind):
    error = kind("precommit")
    case.connector.faults["business_dml"] = error
    with pytest.raises(kind) as caught:
        case.strategy.load(case.config, case.payload)
    assert caught.value is error
    assert case.scope.terminate_for_error(error).outcome is ArtifactTerminalOutcome.ABORT
    assert not case.path.exists()
    assert case.manager.dropped == [STAGE]
    assert "commit_attempted" not in case.connector.events
    assert case.connector.events.count("rollback") == int(isinstance(error, Exception))


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("boundary", ["commit_attempted", "close", "fresh_receipt_probe"])
def test_native_service_refuses_abort_cleanup_or_second_publication_after_interrupt(case, kind, committed, boundary):
    case.connector.commit_effect = committed
    case.connector.faults = {"commit_attempted": OSError("lost ACK"), boundary: kind(29)}

    class Preparer:
        def stage(self, config, payload):
            staging = payload.artifact.materialize(case.manager, config, payload.schema)
            case.manager.finalize_native_evidence(
                staging,
                staging,
                columns=[{"wire_name": "id", "target_name": "id", "target_type": "bigint", "nullable": True}],
            )
            return NativePreparedStage(
                staging, payload.mssql_transaction_admission, payload.require_completed_extraction(), None, None
            )

        def reverify(self, prepared):
            case.connector.event("reverify_native_stage")

        def cleanup(self, prepared):
            prepared.staging.cleanup()

    service = MssqlNativeStagedLoadService(
        SimpleNamespace(_strategy_map={LoadStrategy.FULL_REFRESH: case.strategy}), Preparer()
    )
    handle = service.stage(case.config, case.payload)
    with pytest.raises(kind) as caught:
        service.finalize(case.config, handle)
    for action in (partial(service.finalize, case.config), service.abort, service.cleanup):
        with pytest.raises(RuntimeError, match="publication_unresolved"):
            action(handle)
    assert_retained(case, caught.value)
