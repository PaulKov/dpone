"""Source-free native recovery and exact-stage retirement after durable authority."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.sinks.mssql_native_object_authority import (
    read_exact_object_id,
    require_unfiltered_object_metadata,
)


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

    with importer._mutation_scope(plan, receipt.attempt_id, lease):
        importer._assert_lease(lease)
        with connector.bounded_query_timeout(timeout_seconds):
            connector.begin()
            try:
                current = read_exact_object_id(connector, qualified)
                if current is None:
                    require_unfiltered_object_metadata(
                        connector, diagnostic="mssql_native.stage_absence_visibility_unproved"
                    )
                else:
                    if current != object_id:
                        raise ValueError("mssql_native.stage_identity_mismatch")
                    connector.get_records(f"SELECT TOP (1) 1 FROM {qualified} WITH (TABLOCKX, HOLDLOCK)")
                    importer._assert_stage_identity(plan, receipt.attempt_id, table, object_id)
                    connector.execute_query(f"DROP TABLE {qualified}")
                if read_exact_object_id(connector, qualified) is not None:
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
            import_backend = getattr(getattr(journal, "identity", None), "import_backend", "bcp")
            recoverable_sqlclient = import_backend == "mssql_sqlclient" and terminal["event"] == "UNKNOWN"
            if recoverable_sqlclient or (
                terminal["event"] in {"WRITER_TERMINAL", "UNKNOWN", "QUIESCENT", "VERIFIED"}
                and terminal["observation"]["writer_outcome"] == "success"
            ):
                artifact = chunk["file"]
                file = encoded_file_factory(directory / f"{ordinal}.native", **artifact)
                recovered_receipt = importer.recover_positive(plan, file, attempt_id, lease)
                recovered = journal.data
                if recovered_receipt is None:
                    recovered_events = () if recovered is None else recovered["events"].get(attempt_id, ())
                    if not recovered_events or recovered_events[-1]["event"] != "PARTIAL_PROVED":
                        raise failures.outcome_unknown("mssql_native.recovery_postcondition_unproved")
                    continue
                recovered_chunk = None if recovered is None else recovered["chunks"].get(str(ordinal))
                recovered_events = () if recovered is None else recovered["events"].get(attempt_id, ())
                durable_receipt = None if recovered_chunk is None else recovered_chunk.get("receipt")
                recovered_payload = asdict(recovered_receipt) if is_dataclass(recovered_receipt) else None
                if (
                    recovered_chunk is None
                    or recovered_chunk["phase"] != "verified"
                    or not recovered_events
                    or recovered_events[-1]["event"] != "VERIFIED"
                    or recovered_payload != durable_receipt
                ):
                    raise failures.outcome_unknown("mssql_native.recovery_postcondition_unproved")
                continue
            if terminal["event"] == "UNKNOWN" and import_backend == "bcp":
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
