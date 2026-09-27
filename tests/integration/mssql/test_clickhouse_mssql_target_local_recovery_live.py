"""Live multi-chunk BCP concurrency and source-free recovery certification."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from threading import Event, Thread
from time import monotonic, sleep

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector
from tests.integration.mssql.mssql_target_local_p1_cases import digest_cases

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_verification_identity import build_bcp_target_local_verification_identity
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


def test_late_target_write_blocks_exact_stage_barrier_and_is_rejected(tmp_path) -> None:
    """A concurrent committed write becomes visible and invalidates the receipt."""

    case = digest_cases()[0]
    target = mssql_connector()
    target_id = f"synthetic-late-write-{uuid.uuid4().hex[:16]}"
    plan = NativeChunkPlan(
        "synthetic-late-write-run",
        target_id,
        "synthetic-query",
        "synthetic-window",
        "synthetic-schema",
        case.contract.type_layout_hash,
    )
    identity = build_bcp_target_local_verification_identity(plan, timeout_seconds=10)
    store = SQLiteWindowStore(tmp_path / "late-write.sqlite", clock=lambda: 1.0)
    lease = store.acquire(target_id, "controller", 300)
    NativeTargetCustody(store, target_id).claim(lease, identity.invocation_key)
    receipts = ()

    @contextmanager
    def importer_connection():
        connector = mssql_connector()
        try:
            yield connector
        finally:
            connector.close()

    context = compose_native_stage_context(
        store=store,
        plan=plan,
        lease=lease,
        wire_contract=case.contract,
        limits=NativeChunkLimits(
            max_total_encoded_bytes=8 << 20,
            stage_allocated_bytes_stop_threshold=1 << 30,
            max_rows=32,
            max_bytes=4 << 20,
            max_row_bytes=1 << 20,
            max_pending=1,
            max_staging_tables=32,
            parallelism=1,
        ),
        work_dir=tmp_path / "late-write-files",
        target_connector=target,
        importer_connection=importer_connection,
        bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
        database=target.database,
        schema="dbo",
        row_source=lambda: iter(case.rows),
        journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
        cancelled=Event(),
        required_target_headroom_bytes=8 << 20,
        verification_identity=identity,
        target_local_timeout_seconds=10,
    )
    inserted = Event()
    late_errors: list[BaseException] = []
    writer: Thread | None = None
    try:
        complete = context.executor.stage(plan, iter(case.rows), case.contract, lease)
        receipts = complete.receipts
        assert len(receipts) == 1
        receipt = receipts[0]
        columns = ", ".join(f"[{name}]" for name, _ in case.columns)
        values = ", ".join(case.sql_rows[0])

        def late_write() -> None:
            connector = mssql_connector()
            try:
                connector.begin()
                connector.execute_query(f"INSERT INTO {receipt.stage_id} ({columns}) VALUES ({values})")
                inserted.set()
                sleep(1.0)
                connector.commit_transaction()
            except BaseException as error:
                late_errors.append(error)
                inserted.set()
                connector.rollback()
            finally:
                connector.close()

        writer = Thread(target=late_write, daemon=True)
        writer.start()
        assert inserted.wait(timeout=5)
        started = monotonic()
        with pytest.raises(
            ValueError,
            match="target_digest_count_overflow|row_count_mismatch|typed_digest_mismatch",
        ):
            context.verify_receipts(receipts)
        assert monotonic() - started >= 0.75
        writer.join(timeout=5)
        assert not writer.is_alive()
        assert late_errors == []
    finally:
        if writer is not None:
            writer.join(timeout=5)
        for receipt in receipts:
            with context.executor.importer_factory() as importer:
                importer.drop_exact_owned(plan, receipt, lease)
        target.close()


def test_controller_restart_recovers_verified_bcp_chunks_without_writer_or_source(tmp_path) -> None:
    """Reopen durable EOF state under a new lease and only observe exact stages."""

    case = digest_cases()[0]
    target = mssql_connector()
    target_id = f"synthetic-recovery-{uuid.uuid4().hex[:16]}"
    plan = NativeChunkPlan(
        "synthetic-recovery-run",
        target_id,
        "synthetic-query",
        "synthetic-window",
        "synthetic-schema",
        case.contract.type_layout_hash,
    )
    identity = build_bcp_target_local_verification_identity(plan, timeout_seconds=30)
    store = SQLiteWindowStore(tmp_path / "controller-recovery.sqlite", clock=lambda: 1.0)
    first_lease = store.acquire(target_id, "first-controller", 300)
    custody = NativeTargetCustody(store, target_id)
    custody.claim(first_lease, identity.invocation_key)
    receipts = ()

    def context_for(lease, owner_target, *, allow_writer: bool):
        @contextmanager
        def importer_connection():
            connector = mssql_connector()
            try:
                yield connector
            finally:
                connector.close()

        return compose_native_stage_context(
            store=store,
            plan=plan,
            lease=lease,
            wire_contract=case.contract,
            limits=NativeChunkLimits(
                max_total_encoded_bytes=8 << 20,
                stage_allocated_bytes_stop_threshold=1 << 30,
                max_rows=3,
                max_bytes=4 << 20,
                max_row_bytes=1 << 20,
                max_pending=2,
                max_staging_tables=32,
                parallelism=2,
            ),
            work_dir=tmp_path / "controller-recovery-files",
            target_connector=owner_target,
            importer_connection=importer_connection,
            bcp_options_factory=(
                (lambda **values: BcpOptions(bcp_path=owner_target.bcp_path, **values))
                if allow_writer
                else (lambda **_values: pytest.fail("recovery must not launch BCP"))
            ),
            database=owner_target.database,
            schema="dbo",
            row_source=(lambda: iter(case.rows)) if allow_writer else (lambda: pytest.fail("source reopened")),
            journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
            cancelled=__import__("threading").Event(),
            required_target_headroom_bytes=8 << 20,
            verification_identity=identity,
            target_local_timeout_seconds=30,
        )

    try:
        first = context_for(first_lease, target, allow_writer=True)
        staged = first.executor.stage(plan, iter(case.rows), case.contract, first_lease)
        receipts = staged.receipts
        assert len(receipts) == 2
        durable_events = first.journal_factory().data["events"]
        store.release(first_lease)
        target.close()

        recovery_lease = store.acquire(target_id, "recovery-controller", 300)
        claim = custody.claim(recovery_lease, identity.invocation_key)
        assert claim.recovery_only is True
        recovered_target = mssql_connector()
        recovered = context_for(recovery_lease, recovered_target, allow_writer=False)

        complete = recovered.executor.recover(plan, recovery_lease)

        assert complete.receipts == receipts
        assert recovered.journal_factory().data["events"] == durable_events
        recovered.verify_receipts(receipts)
        for receipt in receipts:
            with recovered.executor.importer_factory() as importer:
                importer.drop_exact_owned(plan, receipt, recovery_lease)
        recovered_target.close()
    finally:
        for receipt in receipts:
            cleanup = mssql_connector()
            try:
                cleanup.execute_query(f"DROP TABLE IF EXISTS {receipt.stage_id}")
            finally:
                cleanup.close()
        target.close()


@pytest.mark.parametrize(("parallelism", "expected_import_workers"), [(1, 1), (2, 2)])
def test_real_bcp_two_chunk_invocation_obeys_import_concurrency(
    tmp_path, parallelism: int, expected_import_workers: int
) -> None:
    """Certify two real BCP chunks under one custody record in both modes."""

    clickhouse = clickhouse_connector()
    target = mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table = f"dpone_p1_chunks_{suffix}"
    target_id = f"synthetic-chunks-{suffix}"
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
    store = SQLiteWindowStore(tmp_path / "two-chunk-state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(target_id, "synthetic-owner", 300)
    custody = NativeTargetCustody(store, target_id)
    custody.claim(lease, identity.invocation_key)
    receipts = ()
    try:
        clickhouse.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC'))) ENGINE=Memory"
        )
        clickhouse.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999'),"
            "(2,0.0,'',NULL),(3,NULL,NULL,'2030-06-01 12:30:45.123456'),"
            "(4,2.5,'omega','2030-06-02 12:30:45.123456')"
        )
        fetched = clickhouse.get_records(
            f"SELECT row_key,ratio,text_value,happened_at FROM `{source_table}` ORDER BY row_key"
        )
        rows = [
            (key, ratio, text, happened.replace(tzinfo=None) if happened is not None else None)
            for key, ratio, text, happened in fetched
        ]

        @contextmanager
        def importer_connection():
            connector = mssql_connector()
            try:
                yield connector
            finally:
                connector.close()

        context = compose_native_stage_context(
            store=store,
            plan=plan,
            lease=lease,
            wire_contract=wire,
            limits=NativeChunkLimits(
                max_total_encoded_bytes=8 << 20,
                stage_allocated_bytes_stop_threshold=1 << 30,
                max_rows=2,
                max_bytes=4 << 20,
                max_row_bytes=1 << 20,
                max_pending=2,
                max_staging_tables=32,
                parallelism=parallelism,
                encoding_parallelism=parallelism,
                import_parallelism=parallelism,
            ),
            work_dir=tmp_path / "two-chunk-files",
            target_connector=target,
            importer_connection=importer_connection,
            bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
            database=target.database,
            schema="dbo",
            row_source=lambda: iter(rows),
            journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
            cancelled=__import__("threading").Event(),
            required_target_headroom_bytes=8 << 20,
            verification_identity=identity,
            target_local_timeout_seconds=30,
        )

        complete = context.executor.stage(plan, iter(rows), wire, lease)
        receipts = complete.receipts
        assert complete.rows == 4
        assert len(receipts) == 2
        context.verify_receipts(receipts)
        observations = context.journal_factory().data["observations"]
        import_workers = {item["worker"] for item in observations if item["phase"] == "import_verify"}
        assert len(import_workers) == expected_import_workers
        assert custody.inspect(lease).holder_invocation_key == identity.invocation_key
    finally:
        for receipt in receipts:
            with context.executor.importer_factory() as importer:
                importer.drop_exact_owned(plan, receipt, lease)
        clickhouse.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target.close()
        clickhouse.close()
