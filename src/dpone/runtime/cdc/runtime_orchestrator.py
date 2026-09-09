"""Generic CDC runtime apply orchestrator."""

from __future__ import annotations

from pathlib import Path

from dpone.readiness.cdc import CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCReader
from dpone.runtime.cdc.identity import CDCIdempotencyService
from dpone.runtime.cdc.poison import CdcPoisonClassifier, FileCdcPoisonQuarantine
from dpone.runtime.cdc.poison_models import CdcPoisonQuarantineReport
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimePolicy, CdcRuntimeRunReport, CdcRuntimeStream
from dpone.runtime.cdc.runtime_ports import CdcOffsetStore, CdcSinkApplier


class CdcRuntimeOrchestrator:
    """Run one bounded CDC read -> apply -> offset commit tick."""

    def __init__(
        self,
        *,
        idempotency: CDCIdempotencyService | None = None,
        poison_classifier: CdcPoisonClassifier | None = None,
        poison_quarantine: FileCdcPoisonQuarantine | None = None,
    ) -> None:
        self._idempotency = idempotency or CDCIdempotencyService()
        self._poison_classifier = poison_classifier or CdcPoisonClassifier()
        self._poison_quarantine = poison_quarantine or FileCdcPoisonQuarantine()

    def run_once(
        self,
        *,
        stream: CdcRuntimeStream,
        reader: CDCReader,
        offset_store: CdcOffsetStore,
        sink_applier: CdcSinkApplier,
        output_dir: str | Path,
        policy: CdcRuntimePolicy | None = None,
    ) -> CdcRuntimeRunReport:
        runtime_policy = policy or CdcRuntimePolicy()
        directory = Path(output_dir)
        reader.setup()
        start_offset = offset_store.load_offset(stream)
        batch = reader.read_batch(start_offset=start_offset, max_changes=runtime_policy.max_changes)
        poison = self._poison_classifier.classify(stream=stream, changes=batch.changes)
        poison_report: CdcPoisonQuarantineReport | None = None
        apply_batch = batch
        if poison.records:
            poison_report = self._poison_quarantine.write(output_dir=directory, stream=stream, records=poison.records)
            if runtime_policy.poison_mode == "quarantine_and_continue":
                apply_batch = CDCBatch(
                    changes=poison.clean_changes,
                    next_offset=batch.next_offset,
                    high_watermark=batch.high_watermark,
                )
            else:
                apply_batch = CDCBatch(
                    changes=tuple(), next_offset=batch.next_offset, high_watermark=batch.high_watermark
                )
        raw_idempotency = self._idempotency.evaluate(batch.changes, unique_key=stream.unique_key)
        idempotency = self._idempotency.evaluate(apply_batch.changes, unique_key=stream.unique_key)
        blockers: list[str] = []
        warnings: list[str] = []
        sink_receipt: CdcApplyReceipt | None = None
        if poison.records and runtime_policy.poison_mode == "fail_closed":
            blockers.append("cdc_runtime.poison_events")
        if poison.records and runtime_policy.poison_mode == "quarantine_and_continue":
            warnings.append("cdc_runtime.poison_events_quarantined")
        if runtime_policy.poison_mode == "fail_closed" and any(
            record.reason == "cdc_poison.duplicate_event" for record in poison.records
        ):
            blockers.append("cdc_runtime.duplicate_events")
        if runtime_policy.require_idempotent and not idempotency.passed:
            blockers.append("cdc_runtime.duplicate_events")
        if batch.row_count == 0 and not runtime_policy.commit_empty_batches:
            warnings.append("cdc_runtime.empty_batch")

        if not blockers and (apply_batch.row_count > 0 or runtime_policy.commit_empty_batches):
            sink_receipt = sink_applier.apply(stream=stream, batch=apply_batch)
            blockers.extend(_sink_blockers(sink_receipt))
            warnings.extend(sink_receipt.warnings)
            if batch.next_offset is None:
                blockers.append("cdc_runtime.next_offset_missing")

        committed = False
        poison_only_commit = bool(
            poison.records
            and runtime_policy.poison_mode == "quarantine_and_continue"
            and apply_batch.row_count == 0
            and batch.row_count > 0
        )
        if not blockers and (sink_receipt is not None or poison_only_commit) and batch.next_offset is not None:
            offset_store.save_offset(stream, batch.next_offset)
            committed = True

        report = _report(
            directory=directory,
            stream=stream,
            batch=batch,
            poison_report=poison_report,
            start_offset=start_offset,
            sink_receipt=sink_receipt,
            committed=committed,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
            metrics={
                "total_events": raw_idempotency.total_events,
                "unique_events": raw_idempotency.unique_events,
                "duplicate_events": raw_idempotency.duplicate_count,
                "max_changes": runtime_policy.max_changes,
                "poison_events": len(poison.records),
                "clean_events": len(poison.clean_changes),
                **(dict(sink_receipt.metrics) if sink_receipt else {}),
            },
        )
        report.write()
        return report


def _sink_blockers(receipt: CdcApplyReceipt) -> list[str]:
    blockers = list(receipt.blockers)
    if not receipt.passed and not blockers:
        blockers.append("cdc_runtime.sink_apply_failed")
    if not receipt.durable:
        blockers.append("cdc_runtime.sink_not_durable")
    return blockers


def _report(
    *,
    directory: Path,
    stream: CdcRuntimeStream,
    batch: CDCBatch,
    poison_report: CdcPoisonQuarantineReport | None,
    start_offset: CDCOffset | None,
    sink_receipt: CdcApplyReceipt | None,
    committed: bool,
    blockers: tuple[str, ...],
    warnings: tuple[str, ...],
    metrics: dict[str, object],
) -> CdcRuntimeRunReport:
    return CdcRuntimeRunReport(
        stream=stream,
        start_offset=start_offset,
        next_offset=batch.next_offset,
        high_watermark=batch.high_watermark,
        rows_read=batch.row_count,
        rows_applied=sink_receipt.rows_applied if sink_receipt else 0,
        rows_deleted=sink_receipt.rows_deleted if sink_receipt else 0,
        committed=committed,
        passed=not blockers,
        blockers=blockers,
        warnings=warnings,
        metrics=metrics,
        sink_receipt=sink_receipt,
        output_dir=str(directory),
        json_path=str(directory / "cdc_runtime_run.json"),
        markdown_path=str(directory / "cdc_runtime_run.md"),
        poison_quarantine=poison_report,
    )
