"""Credential-free CDC runtime adapters for local and CI execution."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream


class JsonCdcBatchReader:
    """Read one bounded CDC batch from a local JSON artifact."""

    def __init__(
        self,
        *,
        path: str | Path,
        backend: CDCBackend,
        source_schema: str,
        source_table: str,
    ) -> None:
        self._path = Path(path)
        self._backend = backend
        self._source_schema = source_schema
        self._source_table = source_table

    def setup(self) -> None:
        return None

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        del start_offset
        payload = _read_mapping(self._path)
        changes = tuple(self._change(item) for item in _sequence(payload.get("changes"))[:max_changes])
        next_offset = _offset(payload.get("next_offset"), backend=self._backend)
        high_watermark = str(payload.get("high_watermark", next_offset.token if next_offset else ""))
        return CDCBatch(changes=changes, next_offset=next_offset, high_watermark=high_watermark or None)

    def _change(self, payload: object) -> CDCChange:
        item = _mapping(payload)
        return CDCChange(
            operation=CDCOperation(str(item["operation"]).strip().lower()),
            data=_mapping(item.get("data")),
            position=str(item["position"]),
            source_schema=str(item.get("source_schema") or self._source_schema),
            source_table=str(item.get("source_table") or self._source_table),
            transaction_id=str(item["transaction_id"]) if item.get("transaction_id") is not None else None,
            sequence=item.get("sequence"),
            before=_optional_mapping(item.get("before")),
            metadata=_mapping(item.get("metadata", {})),
        )


class FileCdcOffsetStore:
    """Persist one local CDC checkpoint JSON file."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def load_offset(self, stream: CdcRuntimeStream) -> CDCOffset | None:
        if not self._path.exists():
            return None
        payload = _read_mapping(self._path)
        if payload.get("stream_id") not in {None, stream.stream_id}:
            return None
        offset = payload.get("offset")
        return CDCOffset.from_state(_mapping(offset)) if offset else None

    def save_offset(self, stream: CdcRuntimeStream, offset: CDCOffset) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "stream_id": stream.stream_id,
            "pipeline_name": stream.pipeline_name,
            "offset": offset.to_state(),
        }
        self._path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


class LocalCdcSinkApplier:
    """Write sink-loadable CDC rows and a durable local receipt."""

    def __init__(self, output_dir: str | Path) -> None:
        self._output_dir = Path(output_dir)

    def apply(self, *, stream: CdcRuntimeStream, batch: CDCBatch) -> CdcApplyReceipt:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        rows = [dict(row) for row in batch.to_rows()]
        rows_path = self._output_dir / "cdc_sink_rows.json"
        rows_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        deleted = sum(1 for change in batch.changes if change.is_delete)
        receipt = CdcApplyReceipt(
            passed=True,
            durable=True,
            rows_applied=batch.row_count,
            rows_deleted=deleted,
            blockers=tuple(),
            warnings=tuple(),
            metrics={
                "sink": stream.sink,
                "target_dataset": stream.target_dataset,
                "artifact_rows": len(rows),
            },
            artifact_uri=str(rows_path),
        )
        receipt_path = self._output_dir / "cdc_sink_receipt.json"
        receipt_path.write_text(
            json.dumps(receipt.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return receipt


def _read_mapping(path: Path) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _mapping(payload)


def _offset(value: object, *, backend: CDCBackend) -> CDCOffset | None:
    if value is None:
        return None
    payload = dict(_mapping(value))
    payload.setdefault("backend", backend.value)
    return CDCOffset.from_state(payload)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC runtime JSON fields must be objects")


def _optional_mapping(value: object) -> Mapping[str, Any] | None:
    if value is None:
        return None
    return _mapping(value)


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    raise ValueError("CDC runtime JSON changes must be an array")
