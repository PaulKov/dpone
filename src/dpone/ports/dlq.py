"""Dependency-inversion ports for DLQ persistence and replay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dlq import DlqRecord, DlqRecordLocator, DlqWriteReceipt


@dataclass(frozen=True, slots=True)
class DlqRejection:
    """Connector-neutral rejection facts emitted by contract enforcement."""

    reason_code: str
    stage: str
    column: str | None
    diagnostics: Mapping[str, object]


class DlqWriter(Protocol):
    def write_rejected(
        self,
        *,
        run_id: str,
        load_id: str,
        row: Mapping[str, object],
        row_index: int,
        rejection: DlqRejection,
    ) -> DlqWriteReceipt: ...

    def finalize_run(self, run_id: str) -> Mapping[str, object]: ...


class DlqStore(Protocol):
    def append(self, record: DlqRecord) -> DlqRecord: ...

    def records(self, run_id: str) -> tuple[DlqRecord, ...]: ...

    def all_records(self) -> tuple[DlqRecord, ...]: ...

    def record(self, record_id: str) -> DlqRecord: ...

    def record_for_run(self, run_id: str, record_id: str) -> DlqRecord: ...

    def acknowledge(self, record: DlqRecord, *, plan_id: str, idempotency_key: str) -> None: ...

    def is_acknowledged(self, record: DlqRecord) -> bool: ...

    def delete(self, record_id: str) -> None: ...

    def delete_many(self, record_ids: tuple[str, ...]) -> None: ...

    def index_ref(self, run_id: str) -> str: ...

    def refresh_index(self, run_id: str) -> str: ...

    def replay_candidates(self, run_id: str) -> tuple[DlqRecordLocator, ...]: ...


class DlqRecordResolver(Protocol):
    @property
    def identity(self) -> str: ...

    def resolve(self, record_ref: str) -> Mapping[str, object]: ...


class DlqReplaySink(Protocol):
    @property
    def identity(self) -> str: ...

    def apply(self, row: Mapping[str, object], *, idempotency_key: str) -> None: ...


__all__ = ["DlqRecordResolver", "DlqRejection", "DlqReplaySink", "DlqStore", "DlqWriter"]
