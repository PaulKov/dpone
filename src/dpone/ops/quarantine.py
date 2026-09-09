"""File-backed quarantine store for bad rows and replay workflows."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.ids import new_ulid, utc_now_iso


@dataclass(frozen=True, slots=True)
class QuarantineEntry:
    quarantine_id: str
    run_id: str
    load_id: str
    row: dict[str, Any]
    reason: str
    diagnostics: dict[str, Any]
    created_at: str
    record_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class QuarantineExport:
    total_rows: int
    entries: tuple[QuarantineEntry, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"total_rows": self.total_rows, "entries": [item.to_dict() for item in self.entries]}


@dataclass(frozen=True, slots=True)
class QuarantineReplayResult:
    replayed_rows: int
    entries: tuple[QuarantineEntry, ...]
    applied: bool
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class _LegacyWriteReceipt:
    record_id: str
    reason_code: str


class QuarantineService:
    """Stores rejected rows in append-only JSONL files for safe replay."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def put(
        self,
        *,
        run_id: str,
        load_id: str,
        row: dict[str, Any],
        reason: str,
        diagnostics: dict[str, Any] | None = None,
    ) -> QuarantineEntry:
        entry = QuarantineEntry(
            quarantine_id=new_ulid(),
            run_id=run_id,
            load_id=load_id,
            row=dict(row),
            reason=reason,
            diagnostics=dict(diagnostics or {}),
            created_at=utc_now_iso(),
        )
        with self._path(run_id).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")
        return entry

    def export(self, *, run_id: str) -> QuarantineExport:
        entries = self._entries(run_id)
        return QuarantineExport(total_rows=len(entries), entries=tuple(entries))

    def write_rejected(
        self,
        *,
        run_id: str,
        load_id: str,
        row: Mapping[str, object],
        row_index: int,
        rejection: Any,
    ) -> _LegacyWriteReceipt:
        del row_index
        reason_code = str(getattr(rejection, "reason_code", "unknown.unclassified"))
        entry = self.put(
            run_id=run_id,
            load_id=load_id,
            row=dict(row),
            reason=reason_code,
            diagnostics=dict(getattr(rejection, "diagnostics", {})),
        )
        return _LegacyWriteReceipt(record_id=entry.quarantine_id, reason_code=reason_code)

    def finalize_run(self, run_id: str) -> Mapping[str, object]:
        exported = self.export(run_id=run_id)
        return {"record_count": exported.total_rows, "index_ref": None}

    def replay(self, *, run_id: str, yes: bool = False) -> QuarantineReplayResult:
        entries = self._entries(run_id)
        return QuarantineReplayResult(
            replayed_rows=0,
            entries=tuple(entries),
            applied=False,
            error_code="DPONE_DLQ_REPLAY_EXECUTOR_REQUIRED" if yes else None,
        )

    def _entries(self, run_id: str) -> list[QuarantineEntry]:
        canonical = self._canonical_entries(run_id)
        if canonical is not None:
            return canonical
        path = self._path(run_id)
        if not path.exists():
            return []
        entries: list[QuarantineEntry] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            payload = json.loads(line)
            entries.append(QuarantineEntry(**payload))
        return entries

    def _canonical_entries(self, run_id: str) -> list[QuarantineEntry] | None:
        if not (self.directory / "runs").exists():
            return None
        from dpone.ops.dlq_store import DlqFileStore

        records = DlqFileStore(self.directory).records(run_id)
        if not records:
            return None
        return [
            QuarantineEntry(
                quarantine_id=record.record_id,
                run_id=record.run_id,
                load_id=record.load_id,
                row=dict(record.payload.value) if isinstance(record.payload.value, dict) else {},
                reason=record.reason.code,
                diagnostics=dict(record.diagnostics),
                created_at=record.created_at,
                record_ref=record.record_ref,
            )
            for record in records
        ]

    def _path(self, run_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in run_id)
        return self.directory / f"{safe}.jsonl"
