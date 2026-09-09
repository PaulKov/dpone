"""Confined create-only filesystem adapter for canonical DLQ records."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.dlq import (
    DLQ_INDEX_SCHEMA,
    DlqRecord,
    DlqRecordLocator,
    canonical_fingerprint,
    is_canonical_sha256_digest,
)
from dpone.ops.dlq_store_io import (
    DlqIntegrityError,
    DlqPathError,
    DlqRecordTooLarge,
    DlqStoreError,
)
from dpone.ops.dlq_store_io import (
    atomic_json as _atomic_json,
)
from dpone.ops.dlq_store_io import (
    component as _component,
)
from dpone.ops.dlq_store_io import (
    confined as _confined,
)
from dpone.ops.dlq_store_io import (
    fsync_directory as _fsync_directory,
)
from dpone.ops.dlq_store_io import (
    json_bytes as _json_bytes,
)
from dpone.ops.dlq_store_io import (
    mkdir as _mkdir,
)
from dpone.ops.dlq_store_io import (
    read_json as _read_json,
)
from dpone.ops.ids import utc_now_iso


class DlqRecordNotFound(DlqStoreError):
    """A requested record does not exist in this store."""


class _RecordRepository:
    """Own create-only record files and their semantic validation."""

    def __init__(self, root: Path, *, max_record_bytes: int, max_diagnostic_bytes: int) -> None:
        self._root = root
        self._max_record_bytes = max_record_bytes
        self._max_diagnostic_bytes = max_diagnostic_bytes

    def append(self, record: DlqRecord) -> DlqRecord:
        self._validate(record)
        directory = self.run_directory(record.run_id) / "records"
        _mkdir(self._root, directory)
        path = _confined(self._root, directory / f"{_component(record.record_id)}.json")
        body = _json_bytes(record.to_dict())
        if len(body) > self._max_record_bytes:
            raise DlqRecordTooLarge(
                f"DPONE_DLQ_RECORD_TOO_LARGE: record bytes {len(body)} exceed {self._max_record_bytes}"
            )
        try:
            with path.open("xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as exc:
            existing = self.read(path)
            if existing.sha256 == record.sha256:
                return existing
            raise DlqIntegrityError("DPONE_DLQ_RECORD_CONFLICT: record_id already has different content") from exc
        _fsync_directory(path.parent)
        return record

    def files(self, run_id: str) -> tuple[DlqRecord, ...]:
        directory = _confined(self._root, self.run_directory(run_id) / "records")
        if not directory.exists():
            return ()
        if not directory.is_dir():
            raise DlqPathError("DPONE_DLQ_PATH_INVALID: records path is not a directory")
        records = tuple(self.read(_confined(self._root, path)) for path in sorted(directory.glob("*.json")))
        return tuple(sorted(records, key=lambda item: item.record_id))

    def for_run(self, run_id: str, record_id: str) -> DlqRecord:
        path = _confined(
            self._root,
            self.run_directory(run_id) / "records" / f"{_component(record_id)}.json",
        )
        if not path.exists():
            raise DlqRecordNotFound(f"DPONE_DLQ_RECORD_NOT_FOUND: {_component(record_id)}")
        return self.read(path)

    def require(self, record: DlqRecord) -> None:
        stored = self.for_run(record.run_id, record.record_id)
        if stored.sha256 != record.sha256:
            raise DlqIntegrityError("DPONE_DLQ_RECORD_SNAPSHOT_DRIFT: record differs from durable artifact")

    def delete(self, record: DlqRecord) -> None:
        path = _confined(
            self._root,
            self.run_directory(record.run_id) / "records" / f"{record.record_id}.json",
        )
        path.unlink(missing_ok=True)
        _fsync_directory(path.parent)

    def read(self, path: Path) -> DlqRecord:
        record = DlqRecord.from_dict(_read_json(self._root, path, max_bytes=self._max_record_bytes))
        self._validate(record)
        return record

    def run_directory(self, run_id: str) -> Path:
        return _confined(self._root, self._root / "runs" / _component(run_id))

    def _validate(self, record: DlqRecord) -> None:
        if record.sha256 != record.fingerprint():
            raise DlqIntegrityError("DPONE_DLQ_CHECKSUM_MISMATCH: record checksum does not match content")
        try:
            record.validate()
        except ValueError as exc:
            raise DlqIntegrityError(f"DPONE_DLQ_RECORD_INVALID: {exc}") from exc
        diagnostic_bytes = len(json.dumps(record.diagnostics, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        if diagnostic_bytes > self._max_diagnostic_bytes:
            raise DlqRecordTooLarge("DPONE_DLQ_DIAGNOSTICS_TOO_LARGE: diagnostics exceed configured byte limit")
        _component(record.run_id)
        _component(record.load_id)
        _component(record.record_id)


class _AcknowledgementRepository:
    """Own replay acknowledgements independently from immutable records."""

    def __init__(self, root: Path, records: _RecordRepository) -> None:
        self._root = root
        self._records = records

    def acknowledge(self, record: DlqRecord, *, plan_id: str, idempotency_key: str) -> None:
        self._records.require(record)
        directory = _confined(self._root, self._root / "acks")
        _mkdir(self._root, directory)
        path = _confined(self._root, directory / f"{_component(record.record_id)}.json")
        payload = {
            "schema": "dpone.dlq-replay-ack.v1",
            "record_id": record.record_id,
            "record_sha256": record.sha256,
            "plan_id": plan_id,
            "idempotency_key": idempotency_key,
            "acknowledged_at": utc_now_iso(),
        }
        try:
            with path.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            existing = _read_json(self._root, path, max_bytes=16_384)
            stable_fields = ("record_id", "record_sha256", "plan_id", "idempotency_key")
            if any(existing.get(field) != payload[field] for field in stable_fields):
                raise DlqIntegrityError("DPONE_DLQ_ACK_CONFLICT: acknowledgement content differs") from None
        _fsync_directory(directory)

    def is_acknowledged(self, record: DlqRecord) -> bool:
        path = _confined(self._root, self._root / "acks" / f"{_component(record.record_id)}.json")
        if not path.exists():
            return False
        payload = _read_json(self._root, path, max_bytes=16_384)
        if payload.get("record_sha256") != record.sha256:
            raise DlqIntegrityError("DPONE_DLQ_ACK_CHECKSUM_MISMATCH: acknowledgement references stale record")
        return True

    def delete(self, record_id: str) -> None:
        path = _confined(self._root, self._root / "acks" / f"{_component(record_id)}.json")
        path.unlink(missing_ok=True)
        if path.parent.exists():
            _fsync_directory(path.parent)


class _IndexRepository:
    """Own deterministic run indexes and their integrity checks."""

    def __init__(self, root: Path, records: _RecordRepository, *, max_index_bytes: int) -> None:
        self._root = root
        self._records = records
        self._max_index_bytes = max_index_bytes

    def ref(self, run_id: str) -> str:
        return f"file://{self._path(run_id)}"

    def refresh(self, run_id: str) -> str:
        directory = self._records.run_directory(run_id)
        _mkdir(self._root, directory)
        items = [
            {
                "id": record.record_id,
                "record_ref": record.record_ref,
                "reason_code": record.reason.code,
                "sha256": record.sha256,
                "expires_at": record.expires_at,
            }
            for record in self._records.files(run_id)
        ]
        stable = {"schema": DLQ_INDEX_SCHEMA, "run_id": run_id, "records": items}
        _atomic_json(
            self._root,
            self._path(run_id),
            {**stable, "fingerprint": canonical_fingerprint(stable)},
            max_bytes=self._max_index_bytes,
        )
        return self.ref(run_id)

    def validate(self, run_id: str, records: tuple[DlqRecord, ...]) -> None:
        if not self._path(run_id).exists():
            return
        payload = self._payload(run_id)
        items = payload.get("records")
        if not isinstance(items, list):
            raise DlqIntegrityError("DPONE_DLQ_INDEX_INVALID: records must be an array")
        indexed = [(str(item.get("id")), str(item.get("sha256"))) for item in items if isinstance(item, Mapping)]
        actual = [(record.record_id, record.sha256) for record in records]
        if indexed != actual:
            raise DlqIntegrityError("DPONE_DLQ_INDEX_DRIFT: index does not match record artifacts")

    def candidates(self, run_id: str) -> tuple[DlqRecordLocator, ...]:
        items = self._payload(run_id).get("records")
        if not isinstance(items, list):
            raise DlqIntegrityError("DPONE_DLQ_INDEX_INVALID: records must be an array")
        candidates: list[DlqRecordLocator] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise DlqIntegrityError("DPONE_DLQ_INDEX_INVALID: record locator must be an object")
            record_id = _component(str(item.get("id") or ""))
            record_ref = str(item.get("record_ref") or "")
            sha256 = str(item.get("sha256") or "")
            if not record_ref.startswith("dpone://") or not is_canonical_sha256_digest(sha256):
                raise DlqIntegrityError("DPONE_DLQ_INDEX_INVALID: record locator is invalid")
            candidates.append(DlqRecordLocator(record_id=record_id, record_ref=record_ref, sha256=sha256))
        return tuple(candidates)

    def _payload(self, run_id: str) -> Mapping[str, Any]:
        path = self._path(run_id)
        if not path.exists():
            raise DlqIntegrityError("DPONE_DLQ_INDEX_MISSING: replay requires a finalized run index")
        payload = _read_json(self._root, path, max_bytes=self._max_index_bytes)
        if payload.get("schema") != DLQ_INDEX_SCHEMA or payload.get("run_id") != run_id:
            raise DlqIntegrityError("DPONE_DLQ_INDEX_INVALID: schema or run identity does not match")
        expected = canonical_fingerprint({key: value for key, value in payload.items() if key != "fingerprint"})
        if payload.get("fingerprint") != expected:
            raise DlqIntegrityError("DPONE_DLQ_INDEX_CHECKSUM_MISMATCH: index checksum does not match content")
        return payload

    def _path(self, run_id: str) -> Path:
        return _confined(self._root, self._records.run_directory(run_id) / "index.json")


class DlqFileStore:
    """Thin facade over record, index, and acknowledgement repositories."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_record_bytes: int = 262_144,
        max_diagnostic_bytes: int = 16_384,
        max_index_bytes: int = 67_108_864,
    ) -> None:
        if min(max_record_bytes, max_diagnostic_bytes, max_index_bytes) <= 0:
            raise ValueError("DLQ storage byte limits must be positive")
        configured = Path(root)
        configured.mkdir(parents=True, exist_ok=True)
        self.root = configured
        self._root = configured.resolve()
        self.max_record_bytes = max_record_bytes
        self.max_diagnostic_bytes = max_diagnostic_bytes
        self.max_index_bytes = max_index_bytes
        self._records = _RecordRepository(
            self._root,
            max_record_bytes=max_record_bytes,
            max_diagnostic_bytes=max_diagnostic_bytes,
        )
        self._acks = _AcknowledgementRepository(self._root, self._records)
        self._indexes = _IndexRepository(self._root, self._records, max_index_bytes=max_index_bytes)

    def append(self, record: DlqRecord) -> DlqRecord:
        return self._records.append(record)

    def records(self, run_id: str) -> tuple[DlqRecord, ...]:
        records = self._records.files(run_id)
        self._indexes.validate(run_id, records)
        return records

    def all_records(self) -> tuple[DlqRecord, ...]:
        runs = _confined(self._root, self._root / "runs")
        if not runs.exists():
            return ()
        records: list[DlqRecord] = []
        for run_path in sorted(runs.iterdir()):
            safe_run = _confined(self._root, run_path)
            if safe_run.is_dir():
                records.extend(self.records(safe_run.name))
        return tuple(sorted(records, key=lambda item: item.record_id))

    def record(self, record_id: str) -> DlqRecord:
        wanted = _component(record_id)
        for record in self.all_records():
            if record.record_id == wanted:
                return record
        raise DlqRecordNotFound(f"DPONE_DLQ_RECORD_NOT_FOUND: {wanted}")

    def record_for_run(self, run_id: str, record_id: str) -> DlqRecord:
        return self._records.for_run(run_id, record_id)

    def acknowledge(self, record: DlqRecord, *, plan_id: str, idempotency_key: str) -> None:
        self._acks.acknowledge(record, plan_id=plan_id, idempotency_key=idempotency_key)

    def is_acknowledged(self, record: DlqRecord) -> bool:
        return self._acks.is_acknowledged(record)

    def delete(self, record_id: str) -> None:
        self.delete_many((record_id,))

    def delete_many(self, record_ids: tuple[str, ...]) -> None:
        records = {record.record_id: record for record in self.all_records()}
        requested = tuple(_component(record_id) for record_id in record_ids)
        missing = [record_id for record_id in requested if record_id not in records]
        if missing:
            raise DlqRecordNotFound(f"DPONE_DLQ_RECORD_NOT_FOUND: {missing[0]}")
        affected_runs: set[str] = set()
        for record_id in requested:
            record = records[record_id]
            self._records.delete(record)
            self._acks.delete(record_id)
            affected_runs.add(record.run_id)
        for run_id in sorted(affected_runs):
            self._indexes.refresh(run_id)

    def index_ref(self, run_id: str) -> str:
        return self._indexes.ref(run_id)

    def refresh_index(self, run_id: str) -> str:
        return self._indexes.refresh(run_id)

    def replay_candidates(self, run_id: str) -> tuple[DlqRecordLocator, ...]:
        return self._indexes.candidates(run_id)


__all__ = [
    "DlqFileStore",
    "DlqIntegrityError",
    "DlqPathError",
    "DlqRecordNotFound",
    "DlqRecordTooLarge",
    "DlqStoreError",
]
