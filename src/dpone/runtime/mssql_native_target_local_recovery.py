"""Source-free native recovery and exact-stage retirement after durable authority."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class NativeRecoveryFailures:
    """Preserve the executor's public failure types across recovery helpers."""

    contract: type[Exception]
    outcome_unknown: type[Exception]
    reextract: type[Exception]


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

    def assert_absence_visibility() -> None:
        # OBJECT_ID can also return NULL on metadata denial or error. Only a
        # principal with unfiltered database metadata may interpret NULL here.
        rows = connector.get_records("SELECT USER_NAME(), IS_SRVROLEMEMBER('sysadmin')")
        if len(rows) != 1 or len(rows[0]) != 2 or (rows[0][0] != "dbo" and rows[0][1] != 1):
            raise ValueError("mssql_native.stage_absence_visibility_unproved")

    with importer._mutation_scope(plan, receipt.attempt_id, lease):
        importer._assert_lease(lease)
        with connector.bounded_query_timeout(timeout_seconds):
            connector.begin()
            try:
                current = current_object_id()
                if current is None:
                    assert_absence_visibility()
                else:
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
    encoded_file_factory: Callable[..., Any],
    failures: NativeRecoveryFailures,
) -> None:
    """Reobserve only positive terminals, then resume authorized retirement."""
    projection = journal.data
    if projection is None or projection["phase"] != "staging":
        raise failures.outcome_unknown("mssql_native.target_local_pre_eof_custody_retained")
    if not projection["chunks"] or any(
        item.get("kind") == "pre_eof_nonpublication" for item in projection["rollback_history"]
    ):
        if on_failed_stage is not None and on_failed_stage(journal):
            return
        raise failures.outcome_unknown("mssql_native.target_local_pre_eof_custody_retained")
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
                file = encoded_file_factory(directory / f"{ordinal}.native", **artifact)
                importer.recover_positive(plan, file, attempt_id, lease)
                continue
            if terminal["event"] == "UNKNOWN":
                journal.retain_bcp_incident(ordinal, attempt_id)
            raise failures.outcome_unknown("mssql_native.target_local_pre_eof_custody_retained")
    if on_failed_stage is not None and on_failed_stage(journal):
        return
    raise failures.outcome_unknown("mssql_native.target_local_pre_eof_custody_retained")


def recover_native_chunks(
    service: Any,
    plan: Any,
    lease: Any,
    failures: NativeRecoveryFailures,
    *,
    encoded_file_factory: Callable[..., Any],
    discard_files: Callable[[Path], None],
) -> Any:
    """Recover a complete stage or settle an incomplete invocation source-free."""
    journal = service.journal_factory(plan, lease)
    journal.bind_limits(service.limits.to_dict())
    publication = journal.publication.state()
    if publication is not None and publication["phase"] not in ("preparing", "prepared"):
        raise failures.outcome_unknown("mssql_native.publication_requires_reconciliation")
    result = journal.completed()
    if service._target_local and result is None:
        recover_target_local_staging(
            journal,
            plan,
            lease,
            service.work_dir,
            service.importer_factory,
            service.on_failed_stage,
            encoded_file_factory,
            failures,
        )
        raise failures.reextract("mssql_native.reextract_required")
    with service.importer_factory() as importer:
        if result is not None:
            for receipt in result.receipts:
                service.store.assert_lease(lease)
                with service.observations.recorder().phase(
                    "raw_verify", reason="recovery", ordinal=receipt.ordinal, attempt_id=receipt.attempt_id
                ):
                    if importer.inspect(plan, receipt, lease) != receipt:
                        raise failures.contract("mssql_native.recovered_stage_changed")
            service._capacity(importer)
            return result
        for attempt_id in journal.attempts():
            service.store.assert_lease(lease)
            importer.settle(plan, attempt_id, lease)
    service.store.assert_lease(lease)
    discard_files(service.work_dir / journal.key.rsplit("/", 1)[-1])
    if journal.data is not None and journal.data["phase"] == "staging":
        journal.reextract_required()
    raise failures.reextract("mssql_native.reextract_required")
