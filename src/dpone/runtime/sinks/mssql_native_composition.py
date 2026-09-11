"""Assemble concrete native staging capabilities at the application boundary."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from dpone.adapters.mssql_native_capacity import require_native_target_capacity
from dpone.adapters.mssql_native_guard import native_stage_writer_scope
from dpone.runtime.mssql_native_capacity import require_native_spool_capacity
from dpone.runtime.mssql_native_chunks import BoundedNativeChunks
from dpone.runtime.mssql_native_chunks_observations import NativeDeliverySession, delivery_session
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder

if TYPE_CHECKING:
    from dpone.ports.native_delivery_observer import NativeDeliveryObserver
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter
from dpone.runtime.sinks.mssql_native_prepare import NativeStageContext


def compose_native_stage_context(
    *,
    store: Any,
    plan: Any,
    lease: Any,
    wire_contract: Any,
    limits: Any,
    work_dir: Any,
    target_connector: Any,
    importer_connection: Callable[[], Any],
    bcp_options_factory: Callable[..., Any],
    database: str,
    schema: str,
    row_source: Callable[[], Any],
    journal_factory: Callable[[], Any],
    cancelled: Any,
    required_target_headroom_bytes: int,
    interval: Any = None,
    observer: NativeDeliveryObserver | NativeDeliverySession | None = None,
) -> NativeStageContext:
    """Wire independently owned importer sessions and real capacity/owner checks.

    ``importer_connection`` returns a context manager closing its dedicated
    connector. ``target_connector`` is the sink/finalizer's dedicated session in
    ``database``. Its ``open_session`` port supplies a separately owned preparation
    lock session, which survives closure of the finalizer's connection during
    commit acknowledgement recovery. The caller provides durable invocation and
    schema authorities, plus a row source invoked only after target-only recovery
    found no journal.
    """
    observations = delivery_session(observer)
    encoder = MssqlNativeEncoder(wire_contract, max_row_bytes=limits.max_row_bytes)

    @contextmanager
    def importer_factory() -> Iterator[MssqlNativeChunkImporter]:
        with importer_connection() as connector:
            current = connector.get_records("SELECT DB_NAME()")
            if not current or current[0][0] != database:
                raise ValueError("mssql_native.importer_database_mismatch")
            yield MssqlNativeChunkImporter(
                connector,
                observer=observations,
                options_factory=bcp_options_factory,
                database=database,
                schema=schema,
                columns=wire_contract.columns,
                encode_row=encoder.encode_row,
                assert_lease=store.assert_lease,
                mutation_scope=lambda current_plan, attempt, current_lease: native_stage_writer_scope(
                    connector, store, current_lease, current_plan.run_id + ":" + attempt
                ),
            )

    def verify(receipts: tuple[Any, ...]) -> None:
        with importer_factory() as importer:
            for receipt in receipts:
                importer.inspect(plan, receipt, lease)

    def cleanup(receipts: tuple[Any, ...]) -> None:
        with importer_factory() as importer:
            for receipt in receipts:
                importer.settle(plan, receipt.attempt_id, lease)

    def capacity(extra_tables: int) -> None:
        store.assert_lease(lease)
        current = target_connector.get_records("SELECT DB_NAME()")
        if not current or current[0][0] != database:
            raise ValueError("mssql_native.target_database_mismatch")
        require_native_spool_capacity(work_dir, limits)
        require_native_target_capacity(target_connector, limits, required_headroom_bytes=required_target_headroom_bytes)
        rows = target_connector.get_records("SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'dpone[_]native[_]%'")
        if not rows or type(rows[0][0]) is not int or rows[0][0] < 0:
            raise ValueError("mssql_native.stage_count_unavailable")
        if rows[0][0] + extra_tables > limits.max_staging_tables:
            raise ValueError("mssql_native.staging_table_limit_exceeded")

    @contextmanager
    def preparation_scope() -> Iterator[None]:
        store.assert_lease(lease)
        connector = target_connector.open_session(application_name="dpone-native-preparation")
        if connector is target_connector:
            raise ValueError("mssql_native.preparation_session_reused")
        try:
            current = connector.get_records("SELECT DB_NAME()")
            if not current or current[0][0] != database:
                raise ValueError("mssql_native.preparation_database_mismatch")
            with native_stage_writer_scope(connector, store, lease, "prepare:" + plan.run_id):
                yield
        finally:
            primary = sys.exc_info()[1]
            try:
                connector.close()
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note(f"native preparation session cleanup failed: {type(error).__name__}")

    return NativeStageContext(
        plan=plan,
        wire_contract=wire_contract,
        executor=BoundedNativeChunks(
            store=store, importer_factory=importer_factory, work_dir=work_dir, limits=limits, observer=observations
        ),
        observer=observations,
        lease=lease,
        row_source=row_source,
        verify_receipts=verify,
        cleanup_receipts=cleanup,
        capacity_check=capacity,
        journal_factory=journal_factory,
        preparation_scope=preparation_scope,
        interval=interval,
        max_row_bytes=limits.max_row_bytes,
        cancelled=cancelled,
    )
