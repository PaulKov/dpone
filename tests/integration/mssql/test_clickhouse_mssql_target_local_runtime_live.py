"""Synthetic ClickHouse rows through the complete target-local MSSQL runtime."""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_verification_identity import build_bcp_target_local_verification_identity
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


def _admission(database: str, table: str) -> MssqlTransactionAdmission:
    digest = sha256(f"synthetic:{database}:dbo:{table}".encode()).digest()
    request = MssqlAttemptRequest(
        InvocationIdentity("synthetic-run", "runtime-live", "single"),
        digest,
        digest,
        "synthetic-load",
        database,
        "dbo",
        table,
        LoadStrategy.FULL_REFRESH.value,
    )
    attempt = MssqlTransactionAttempt(request, 1)
    operation_request = MssqlOperationRequest(digest, digest)
    operation = MssqlTransactionOperation(
        attempt,
        operation_request.operation_key(attempt),
        operation_request.scope_hash,
        operation_request.owner_digest,
        1,
    )
    return MssqlTransactionAdmission(operation=operation)


class _AtomicLiveFinalizer:
    """Exercise real SQL publication while generic receipt fencing stays separate."""

    def __init__(self, strategy, _state_storage):
        self.strategy = strategy

    def finalize(self, _config, admission, handler, staging, *, staging_rows, **_kwargs):
        self.strategy.connector.begin()
        try:
            result = handler(staging)
            self.strategy.connector.commit_transaction()
        except BaseException:
            self.strategy.connector.rollback()
            raise
        return replace(
            result,
            staging_rows=staging_rows,
            commit_receipt_id=admission.operation.receipt_id,
            commit_outcome=AtomicCommitOutcome.COMMITTED,
        )


def test_clickhouse_mssql_target_local_runtime_orders_publication_and_cleanup(tmp_path) -> None:
    clickhouse = clickhouse_connector()
    target = mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table = f"dpone_runtime_source_{suffix}"
    target_table = f"dpone_runtime_target_{suffix}"
    target_id = f"synthetic-runtime-{suffix}"
    schema = (
        ("row_key", "bigint"),
        ("ratio", "float(53) nullable"),
        ("text_value", "nvarchar(max) nullable"),
        ("happened_at", "datetime2(6) nullable"),
    )
    wire = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format="mssql_native")
    plan = NativeChunkPlan(
        "synthetic-run", target_id, "synthetic-query", "synthetic-window", "synthetic-schema", wire.type_layout_hash
    )
    identity = build_bcp_target_local_verification_identity(plan, timeout_seconds=30)
    admission = _admission(target.database, target_table)
    mutation = MssqlTargetMutationPlan.from_admission(admission)
    config = LoadConfig(
        "source",
        "target",
        "default",
        source_table,
        "dbo",
        target_table,
        target_database=target.database,
        staging_database=target.database,
        staging_schema="dbo",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "lineage": False,
            "__dpone_load_identity": {"run_id": "synthetic-run", "load_id": "synthetic-load"},
            "native_transfer": {
                "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                "execution": {
                    "verification_backend": "target_local",
                    "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 1},
                    "native_chunks": {
                        "max_total_encoded_bytes": 8 << 20,
                        "stage_allocated_bytes_stop_threshold": 1 << 30,
                        "max_rows": 1024,
                        "max_bytes": 4 << 20,
                        "max_row_bytes": 1 << 20,
                        "max_pending": 1,
                        "max_staging_tables": 32,
                    },
                },
            },
        },
    )
    store = SQLiteWindowStore(tmp_path / "runtime-state.sqlite", clock=lambda: 1.0)
    observer = BoundedNativeDeliveryObserver()
    rows: list[tuple[object, ...]] = []
    bindings_seen: dict[str, object] = {}
    stages: dict[str, object] = {}
    order: list[str] = []
    source_entries = 0

    try:
        clickhouse.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC'))) ENGINE=Memory"
        )
        clickhouse.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999'),"
            "(2,0.0,'',NULL),(3,NULL,NULL,'2030-06-01 12:30:45.123456'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456')"
        )
        target.execute_query(
            f"CREATE TABLE [dbo].[{target_table}] ("
            "[row_key] bigint NOT NULL,[ratio] float NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL)"
        )
        target.execute_query(f"INSERT INTO [dbo].[{target_table}] VALUES (-1,NULL,N'before-publication',NULL)")

        @contextmanager
        def importer_connection():
            connector = mssql_connector()
            connector.get_records_iterator = lambda *_args, **_kwargs: pytest.fail(
                "target-local verification must not return business rows"
            )
            try:
                yield connector
            finally:
                connector.close()

        def bindings(_cfg, _owner, lease, cancelled):
            context = compose_native_stage_context(
                store=store,
                plan=plan,
                lease=lease,
                wire_contract=wire,
                limits=NativeChunkLimits(
                    max_total_encoded_bytes=8 << 20,
                    stage_allocated_bytes_stop_threshold=1 << 30,
                    max_rows=1024,
                    max_bytes=4 << 20,
                    max_row_bytes=1 << 20,
                    max_pending=1,
                    max_staging_tables=32,
                    parallelism=1,
                ),
                work_dir=tmp_path / "native-files",
                target_connector=target,
                importer_connection=importer_connection,
                bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
                database=target.database,
                schema="dbo",
                row_source=lambda: iter(rows),
                journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
                cancelled=cancelled,
                required_target_headroom_bytes=8 << 20,
                verification_identity=identity,
                target_local_timeout_seconds=30,
                observer=observer,
            )
            sink = MSSQLSink(
                target,
                transaction_finalizer_factory=_AtomicLiveFinalizer,
            )
            preparer = MssqlNativeStagePreparer(sink, lambda *_args: context)
            service = MssqlNativeStagedLoadService(sink, preparer)
            bindings_seen["context"] = context
            return NativeRuntimeBindings(service, context, admission, identity)

        @contextmanager
        def source(_cfg, _binding):
            nonlocal source_entries
            source_entries += 1
            fetched = clickhouse.get_records(
                f"SELECT row_key,ratio,text_value,happened_at FROM `{source_table}` ORDER BY row_key,happened_at"
            )
            rows[:] = [
                (key, ratio, text, happened.replace(tzinfo=None) if happened is not None else None)
                for key, ratio, text, happened in fetched
            ]
            now = datetime(2026, 1, 1, tzinfo=UTC)
            yield SimpleNamespace(
                schema=schema,
                relation_schema=None,
                relation_metadata=None,
                relation_dialect=None,
                target_projection=None,
                mssql_transaction_admission=admission,
                mssql_target_mutation_plan=mutation,
                require_completed_extraction=lambda: ExtractionLifecycleReceipt(
                    now, "synthetic.clickhouse", extraction_completed_at=now
                ),
            )

        def stage_names(journal):
            completed = journal.completed()
            publication = journal.publication.state()
            assert completed is not None and publication is not None
            raw = tuple(receipt.stage_id for receipt in completed.receipts)
            stage = publication["prepared"]["stage"]
            prepared = target.qualified_name(stage["schema"], stage["table"], database=stage["database"])
            return raw, prepared

        def assert_present(names):
            assert all(target.get_records("SELECT OBJECT_ID(?)", (name,))[0][0] is not None for name in names)

        def quality(_cfg, _handle, _lease):
            context = bindings_seen["context"]
            journal = context.journal_factory()
            assert journal.publication.state()["phase"] == "prepared"
            assert target.get_records(f"SELECT [row_key] FROM [dbo].[{target_table}]") == [(-1,)]
            raw, prepared = stage_names(journal)
            stages.update(raw=raw, prepared=prepared)
            assert_present((*raw, prepared))
            order.append("quality")

        def evidence(_cfg, _result, context, lease):
            journal = context.journal_factory()
            assert journal.publication.state()["phase"] == "published"
            receipt = journal.completed().receipts[0]
            aggregate = decode_target_digest_row(
                target.get_records(
                    build_target_digest_sql(f"[{target.database}].[dbo].[{target_table}]", wire, len(rows))
                )[0],
                expected_rows=len(rows),
            )
            assert aggregate.typed_digest == receipt.typed_digest
            store.save("cert/evidence", None, json.dumps({"rows": aggregate.rows}), lease)
            order.append("evidence")

        def advance_state(_cfg, _result, lease):
            context = bindings_seen["context"]
            assert context.journal_factory().publication.state()["phase"] == "evidence-complete"
            assert store.load("cert/evidence") is not None
            assert_present((*stages["raw"], stages["prepared"]))
            store.save("cert/checkpoint", None, json.dumps({"state": "advanced"}), lease)
            order.append("checkpoint")

        class AuditedCustody(NativeTargetCustody):
            def release(self, lease, invocation_key, reason, *, assert_release_authority):
                context = bindings_seen["context"]
                assert context.journal_factory().publication.state()["phase"] == "succeeded"
                assert store.load("cert/evidence") is not None and store.load("cert/checkpoint") is not None
                assert all(
                    target.get_records("SELECT OBJECT_ID(?)", (name,))[0][0] is None
                    for name in (*stages["raw"], stages["prepared"])
                )
                order.append("custody-release")
                return super().release(
                    lease,
                    invocation_key,
                    reason,
                    assert_release_authority=assert_release_authority,
                )

        runtime = NativeMssqlRuntime(
            store=store,
            target_id=target_id,
            bindings=bindings,
            source=source,
            preflight=lambda _cfg: None,
            quality=quality,
            evidence=evidence,
            advance_state=advance_state,
            custody_factory=AuditedCustody,
            v2_journal_admission=lambda journal, current: (
                isinstance(journal, NativeChunkJournalV2) and journal.identity == current
            ),
            lease_ttl=300,
            observer=observer,
        )

        result = runtime.run(config, owner="synthetic-runtime")
        assert result.status == "success" and result.extracted_rows == len(rows) and result.final_rows == len(rows)
        assert source_entries == 1
        assert order == ["quality", "evidence", "checkpoint", "custody-release"]
        context = bindings_seen["context"]
        assert context.journal_factory().publication.state()["phase"] == "succeeded"
        inspect_lease = store.acquire(target_id, "inspect", 60)
        custody = NativeTargetCustody(store, target_id).inspect(inspect_lease)
        assert custody.state == "clear" and custody.release_reason == "published_cleanup"
        store.release(inspect_lease)
        phases = [item["phase"] for item in observer.snapshot()["observations"]]
        assert {"raw_verify", "prepared_verify", "quality", "publish", "evidence", "checkpoint"} <= set(phases)
    finally:
        target.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]")
        clickhouse.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target.close()
        clickhouse.close()
