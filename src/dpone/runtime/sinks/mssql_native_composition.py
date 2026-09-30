"""Assemble concrete native staging capabilities at the application boundary."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from dpone.adapters.mssql_native_capacity import require_native_target_capacity
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.adapters.mssql_native_guard import (
    native_exact_stage_barrier,
    native_stage_writer_scope,
    sqlclient_exact_stage_barrier,
)
from dpone.ports.mssql_native import SQLCLIENT_SESSION_PROOF, NativeChunkReceipt
from dpone.runtime.mssql_native_capacity import require_native_spool_capacity
from dpone.runtime.mssql_native_chunks import BoundedNativeChunks, WindowOutcomeUnknown
from dpone.runtime.mssql_native_chunks_files import discard_native_files, verify_native_file
from dpone.runtime.mssql_native_chunks_observations import NativeDeliverySession, delivery_session
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder

if TYPE_CHECKING:
    from dpone.ports.native_delivery_observer import NativeDeliveryObserver
from dpone.runtime.sinks.mssql_native_bcp_writer import MssqlNativeBcpWriter
from dpone.runtime.sinks.mssql_native_import import (
    MssqlNativeChunkImporter,
    native_attempt_table_name,
    native_stage_allocated_bytes,
)
from dpone.runtime.sinks.mssql_native_prepare import NativeStageContext
from dpone.runtime.sinks.mssql_native_target_digest import require_target_local_raw_layout
from dpone.runtime.sinks.mssql_native_target_local_import import NativeTargetLocalAttempt


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
    verification_identity: Any = None,
    target_local_timeout_seconds: int = 3600,
    native_stage_writer: Any = None,
    persisted_hash_layout: bool = False,
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
    if verification_identity is not None:
        require_target_local_raw_layout(wire_contract)
    encoder = MssqlNativeEncoder(wire_contract, max_row_bytes=limits.max_row_bytes)
    if verification_identity is not None and verification_identity.plan != plan:
        raise ValueError("mssql_native.invalid_v2_identity")
    if type(target_local_timeout_seconds) is not int or target_local_timeout_seconds < 1:
        raise ValueError("mssql_native.invalid_barrier_timeout")
    sqlclient_selected = (
        verification_identity is not None and verification_identity.writer_proof_capability == SQLCLIENT_SESSION_PROOF
    )
    if sqlclient_selected != (native_stage_writer is not None):
        raise ValueError("mssql_native.stage_writer_selection_mismatch")
    if type(persisted_hash_layout) is not bool or (persisted_hash_layout and not sqlclient_selected):
        raise ValueError("mssql_native.persisted_hash_layout_selection")
    v2_journal = (
        NativeChunkJournalV2(store, lease, verification_identity) if verification_identity is not None else None
    )
    custody = NativeTargetCustody(store, plan.target_id) if v2_journal is not None else None

    def recovery_bindings(admission: Any) -> Any:
        if verification_identity is None:
            return None
        from dpone.contracts.mssql_native_recovery_authority import compose_mssql_native_recovery_bindings

        return compose_mssql_native_recovery_bindings(
            admission,
            verification_identity=verification_identity,
            state_store=store,
            work_root=work_dir,
        )

    def selected_journal() -> Any:
        return v2_journal if v2_journal is not None else journal_factory()

    @contextmanager
    def importer_factory() -> Iterator[MssqlNativeChunkImporter]:
        with importer_connection() as connector:
            current = connector.get_records("SELECT DB_NAME()")
            if not current or current[0][0] != database:
                raise ValueError("mssql_native.importer_database_mismatch")
            target_local = None
            if v2_journal is not None and custody is not None:

                def launch(grant: Any, rejects: Any) -> Any:
                    options = bcp_options_factory(
                        file_format="native",
                        error_file=str(rejects),
                        trust_server_certificate=getattr(connector, "trust_server_certificate", "no") == "yes",
                    )
                    if options.file_format != "native" or options.error_file != str(rejects):
                        raise ValueError("mssql_native.native_options_required")
                    process = connector.bcp_import_process(
                        schema,
                        native_attempt_table_name(plan, grant.attempt_id),
                        str(grant.file_path),
                        options=options,
                        database=database,
                    )
                    return process.wait_supervised()

                writer = native_stage_writer if sqlclient_selected else MssqlNativeBcpWriter(launch)
                if sqlclient_selected:

                    def sqlclient_barrier(stage: str, grant: str, check: Callable[[], None]) -> Any:
                        return sqlclient_exact_stage_barrier(
                            connector,
                            stage,
                            grant_token_sha256=grant,
                            timeout_seconds=target_local_timeout_seconds,
                            assert_identity=check,
                        )

                    selected_barrier: Callable[..., Any] = sqlclient_barrier

                else:

                    def bcp_barrier(stage: str, check: Callable[[], None]) -> Any:
                        return native_exact_stage_barrier(
                            connector, stage, timeout_seconds=target_local_timeout_seconds, assert_identity=check
                        )

                    selected_barrier = bcp_barrier

                target_local = NativeTargetLocalAttempt(
                    v2_journal,
                    custody,
                    writer,
                    selected_barrier,
                    WindowOutcomeUnknown,
                    verify_native_file,
                    target_local_timeout_seconds,
                    timeout_seconds=target_local_timeout_seconds,
                    max_row_bytes=limits.max_row_bytes,
                )
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
                target_digest_contract=wire_contract if target_local is not None else None,
                target_local_attempt=target_local,
                persisted_hash_layout=persisted_hash_layout,
            )

    def verify(receipts: tuple[Any, ...]) -> None:
        def inspect(receipt: Any) -> None:
            with importer_factory() as importer:
                importer.inspect(plan, receipt, lease)

        workers = min(len(receipts), limits.effective_import_parallelism)
        if workers <= 1:
            for receipt in receipts:
                inspect(receipt)
            return
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dpone-native-verify") as executor:
            tuple(executor.map(inspect, receipts))

    def cleanup(receipts: tuple[Any, ...]) -> None:
        with importer_factory() as importer:
            for receipt in receipts:
                if v2_journal is None:
                    importer.settle(plan, receipt.attempt_id, lease)
                else:
                    importer.settle_published(plan, receipt, lease)

    def retire_failed_stage(journal: NativeChunkJournalV2) -> bool:
        """Retire only fully verified pre-EOF stages after durable exclusion."""
        if custody is None or verification_identity is None:
            return False
        projection = journal.data
        if projection is None:
            journal.begin()
            projection = journal.data
        if projection is None or projection["phase"] != "staging":
            return False
        if not projection["chunks"] and (projection["events"] or projection["nonces"]):
            return False
        if any(
            events[-1]["event"] not in {"VERIFIED", "PARTIAL_PROVED", "FAILED_RETIRABLE", "RETIRED"}
            for events in projection["events"].values()
        ):
            return False

        def assert_nonpublication() -> None:
            if journal.publication.state() is not None or journal.completed() is not None:
                raise ValueError("mssql_native.nonpublication_unproved")

        proof = sha256((verification_identity.invocation_key + ":pre_eof_nonpublication").encode()).hexdigest()
        journal.record_nonpublication(proof, assert_nonpublication=assert_nonpublication)
        for ordinal in sorted(map(int, projection["chunks"])):
            chunk = projection["chunks"][str(ordinal)]
            terminal = projection["events"][chunk["attempt_id"]][-1]
            if terminal["event"] == "RETIRED":
                continue
            with importer_factory() as importer:
                if chunk["receipt"] is None:
                    attempt_id = chunk["attempt_id"]
                    table = importer.table_name(plan, attempt_id)
                    artifact = chunk["file"]
                    receipt = NativeChunkReceipt(
                        ordinal,
                        attempt_id,
                        importer.qualified(table),
                        artifact["rows"],
                        artifact["encoded_bytes"],
                        artifact["file_sha256"],
                        artifact["typed_digest"],
                        {"native_object_id": terminal["stage_binding"]["object_id"]},
                    )
                else:
                    receipt = NativeChunkReceipt(**chunk["receipt"])
                journal.retire_verified(
                    ordinal,
                    receipt.attempt_id,
                    drop_exact_owned=lambda: importer.drop_exact_owned(plan, receipt, lease),
                )
        discard_native_files(work_dir / journal.key.rsplit("/", 1)[-1])

        def assert_retirement() -> None:
            current = journal.data
            if (
                current is None
                or current["publication"] is not None
                or not any(item.get("kind") == "pre_eof_nonpublication" for item in current["rollback_history"])
                or any(events[-1]["event"] != "RETIRED" for events in current["events"].values())
            ):
                raise ValueError("mssql_native.retirement_unproved")

        custody.release(
            lease,
            verification_identity.invocation_key,
            "nonpublication_all_stages_retired",
            assert_release_authority=assert_retirement,
        )
        return True

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

    def allocated_bytes() -> int:
        """Observe staging pressure through an independently owned session."""

        with importer_connection() as connector:
            if connector is target_connector:
                raise ValueError("mssql_native.allocation_session_reused")
            return native_stage_allocated_bytes(connector, database)

    return NativeStageContext(
        plan=plan,
        wire_contract=wire_contract,
        executor=BoundedNativeChunks(
            store=store,
            importer_factory=importer_factory,
            work_dir=work_dir,
            limits=limits,
            observer=observations,
            journal_factory=(lambda current_plan, current_lease: selected_journal())
            if v2_journal is not None
            else None,
            on_failed_stage=retire_failed_stage if v2_journal is not None else None,
            allocation_observer=allocated_bytes,
        ),
        observer=observations,
        lease=lease,
        row_source=row_source,
        verify_receipts=verify,
        cleanup_receipts=cleanup,
        capacity_check=capacity,
        journal_factory=selected_journal,
        preparation_scope=preparation_scope,
        interval=interval,
        max_row_bytes=limits.max_row_bytes,
        cancelled=cancelled,
        verification_identity=verification_identity,
        target_local_timeout_seconds=target_local_timeout_seconds,
        recovery_bindings=recovery_bindings if verification_identity is not None else None,
        persisted_hash_layout=persisted_hash_layout,
    )
