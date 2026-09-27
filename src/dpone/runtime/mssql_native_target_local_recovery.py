"""Source-free native recovery and exact-stage retirement after durable authority."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown
from dpone.runtime.mssql_native_chunks_files import EncodedNativeFile, discard_native_files


class NativeReextractRequired(WindowContractError):
    """Partial staging is settled; a new invocation must re-extract the source."""


def retire_exact_owned_stage(importer: Any, plan: Any, receipt: Any, lease: Any, *, timeout_seconds: int) -> None:
    """Drop the original object or prove it already absent under the writer fence.

    A reused stage name with a different object is never dropped or accepted as
    cleanup. The immutable receipt supplies the original SQL Server object ID.
    """
    table = importer.table_name(plan, receipt.attempt_id)
    qualified = importer.qualified(table)
    object_id = receipt.consumed_part_evidence.get("native_object_id")
    if receipt.stage_id != qualified or type(object_id) is not int or object_id < 1:
        raise ValueError("mssql_native.stage_identity_mismatch")
    connector = importer.connector

    def current_object_id() -> int | None:
        rows = connector.get_records("SELECT OBJECT_ID(?)", (qualified,))
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ValueError("mssql_native.object_identity_unavailable")
        value = rows[0][0]
        if value is not None and (type(value) is not int or value < 1):
            raise ValueError("mssql_native.object_identity_unavailable")
        return value

    with importer._mutation_scope(plan, receipt.attempt_id, lease):
        importer._assert_lease(lease)
        with connector.bounded_query_timeout(timeout_seconds):
            connector.begin()
            try:
                current = current_object_id()
                if current is not None:
                    if current != object_id:
                        raise ValueError("mssql_native.stage_identity_mismatch")
                    connector.get_records(f"SELECT TOP (1) 1 FROM {qualified} WITH (TABLOCKX, HOLDLOCK)")
                    importer._assert_stage_identity(plan, receipt.attempt_id, table, object_id)
                    connector.execute_query(f"DROP TABLE {qualified}")
                if current_object_id() is not None:
                    raise ValueError("mssql_native.stage_retirement_unproved")
                importer._assert_lease(lease)
                connector.commit_transaction()
            except BaseException:
                connector.rollback()
                raise


def recover_target_local_staging(
    journal: Any,
    plan: Any,
    lease: Any,
    work_dir: Path,
    importer_factory: Callable[[], AbstractContextManager[Any]],
    on_failed_stage: Callable[[Any], bool] | None,
) -> None:
    """Reobserve only positive terminals, then resume authorized retirement."""
    projection = journal.data
    if projection is None or projection["phase"] != "staging":
        raise WindowOutcomeUnknown("mssql_native.target_local_pre_eof_custody_retained")
    if not projection["chunks"] or any(
        item.get("kind") == "pre_eof_nonpublication" for item in projection["rollback_history"]
    ):
        if on_failed_stage is not None and on_failed_stage(journal):
            return
        raise WindowOutcomeUnknown("mssql_native.target_local_pre_eof_custody_retained")
    directory = work_dir / journal.key.rsplit("/", 1)[-1]
    with importer_factory() as importer:
        for ordinal in sorted(map(int, projection["chunks"])):
            chunk = projection["chunks"][str(ordinal)]
            attempt_id = chunk["attempt_id"]
            terminal = projection["events"][attempt_id][-1]
            if terminal["event"] == "VERIFIED" and chunk["phase"] == "verified":
                continue
            if terminal["event"] in {"FAILED_RETIRABLE", "RETIRED"}:
                continue
            if (
                terminal["event"] in {"WRITER_TERMINAL", "UNKNOWN", "VERIFIED"}
                and terminal["observation"]["writer_outcome"] == "success"
            ):
                artifact = chunk["file"]
                file = EncodedNativeFile(directory / f"{ordinal}.native", **artifact)
                importer.recover_positive(plan, file, attempt_id, lease)
                continue
            if terminal["event"] == "UNKNOWN":
                journal.retain_bcp_incident(ordinal, attempt_id)
            raise WindowOutcomeUnknown("mssql_native.target_local_pre_eof_custody_retained")
    if on_failed_stage is not None and on_failed_stage(journal):
        return
    raise WindowOutcomeUnknown("mssql_native.target_local_pre_eof_custody_retained")


def recover_native_chunks(service: Any, plan: Any, lease: Any) -> Any:
    """Recover a complete stage or settle an incomplete invocation source-free."""
    journal = service.journal_factory(plan, lease)
    journal.bind_limits(service.limits.to_dict())
    publication = journal.publication.state()
    if publication is not None and publication["phase"] not in ("preparing", "prepared"):
        raise WindowOutcomeUnknown("mssql_native.publication_requires_reconciliation")
    result = journal.completed()
    if service._target_local and result is None:
        recover_target_local_staging(
            journal, plan, lease, service.work_dir, service.importer_factory, service.on_failed_stage
        )
        raise NativeReextractRequired("mssql_native.reextract_required")
    with service.importer_factory() as importer:
        if result is not None:
            for receipt in result.receipts:
                service.store.assert_lease(lease)
                with service.observations.recorder().phase(
                    "raw_verify", reason="recovery", ordinal=receipt.ordinal, attempt_id=receipt.attempt_id
                ):
                    if importer.inspect(plan, receipt, lease) != receipt:
                        raise WindowContractError("mssql_native.recovered_stage_changed")
            service._capacity(importer)
            return result
        for attempt_id in journal.attempts():
            service.store.assert_lease(lease)
            importer.settle(plan, attempt_id, lease)
    service.store.assert_lease(lease)
    discard_native_files(service.work_dir / journal.key.rsplit("/", 1)[-1])
    if journal.data is not None and journal.data["phase"] == "staging":
        journal.reextract_required()
    raise NativeReextractRequired("mssql_native.reextract_required")
