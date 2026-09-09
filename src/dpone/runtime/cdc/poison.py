"""CDC poison-event classification and file quarantine."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dpone.runtime.cdc.base import CDCChange, CDCOperation
from dpone.runtime.cdc.identity import CDCEventIdentityService
from dpone.runtime.cdc.poison_models import CdcPoisonDecision, CdcPoisonQuarantineReport, CdcPoisonRecord
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream

SUPPORTED_RUNTIME_OPERATIONS = frozenset({CDCOperation.INSERT, CDCOperation.UPDATE, CDCOperation.DELETE})


class CdcPoisonClassifier:
    """Classify CDC events that should not enter the sink apply path."""

    def __init__(self, *, identity_service: CDCEventIdentityService | None = None) -> None:
        self._identity = identity_service or CDCEventIdentityService()

    def classify(
        self,
        *,
        stream: CdcRuntimeStream,
        changes: Sequence[CDCChange],
    ) -> CdcPoisonDecision:
        seen: set[str] = set()
        clean: list[CDCChange] = []
        records: list[CdcPoisonRecord] = []
        for index, change in enumerate(changes):
            event_id = self._identity.event_id(change, unique_key=stream.unique_key)
            reason = _reason(change=change, event_id=event_id, seen=seen, unique_key=stream.unique_key)
            seen.add(event_id)
            if reason:
                records.append(_record(stream=stream, change=change, event_id=event_id, reason=reason, index=index))
            else:
                clean.append(change)
        return CdcPoisonDecision(clean_changes=tuple(clean), records=tuple(records))


class FileCdcPoisonQuarantine:
    """Write poison records beside the runtime report."""

    def write(
        self,
        *,
        output_dir: str | Path,
        stream: CdcRuntimeStream,
        records: Sequence[CdcPoisonRecord],
    ) -> CdcPoisonQuarantineReport:
        directory = Path(output_dir)
        report = CdcPoisonQuarantineReport(
            stream=stream,
            records=tuple(records),
            output_dir=str(directory),
            json_path=str(directory / "cdc_poison_quarantine.json"),
            markdown_path=str(directory / "cdc_poison_quarantine.md"),
        )
        report.write()
        return report


def _reason(
    *,
    change: CDCChange,
    event_id: str,
    seen: set[str],
    unique_key: Sequence[str],
) -> str | None:
    if event_id in seen:
        return "cdc_poison.duplicate_event"
    if change.operation not in SUPPORTED_RUNTIME_OPERATIONS:
        return "cdc_poison.unsupported_operation"
    if not unique_key:
        return "cdc_poison.unique_key_missing"
    before = change.before or {}
    for key in unique_key:
        value = change.data.get(key, before.get(key))
        if value is None:
            return "cdc_poison.unique_key_missing"
    return None


def _record(
    *,
    stream: CdcRuntimeStream,
    change: CDCChange,
    event_id: str,
    reason: str,
    index: int,
) -> CdcPoisonRecord:
    return CdcPoisonRecord(
        record_id=f"{stream.stream_id}:{change.position}:{index}",
        event_id=event_id,
        reason=reason,
        action="quarantine",
        replayable=True,
        summary=_summary(reason),
        change=change,
    )


def _summary(reason: str) -> str:
    return {
        "cdc_poison.duplicate_event": "Duplicate CDC event identity was quarantined",
        "cdc_poison.unique_key_missing": "CDC event is missing the configured unique key",
        "cdc_poison.unsupported_operation": "Unsupported CDC operation was quarantined",
    }.get(reason, "CDC event was quarantined")


__all__ = ["CdcPoisonClassifier", "FileCdcPoisonQuarantine", "SUPPORTED_RUNTIME_OPERATIONS"]
