"""Private attempt storage and durable immutable ClickHouse consumption events."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, BinaryIO

from dpone.runtime.immutable_local_tree import materialize_immutable_local_tree_at
from dpone.runtime.pinned_directory import PinnedDirectory
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.clickhouse_validated_file_models import (
    MAX_EVENT_BYTES,
    ClickHouseValidatedFilePolicy,
    FileConsumptionError,
)
from dpone.runtime.storage_policy import RuntimeStorageAdmissionError, RuntimeStoragePolicy, StoragePreflightService


def canonical_json(value: object) -> bytes:
    """Stable UTF-8 identities; no NaN, implicit encoders or path serialization."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


class ClickHouseFileAttemptJournal:
    """Own one pinned private directory; journal failure never grants success.

    Source files are never owned here. Closing releases descriptors only; named
    spools survive unknown execution for caller-controlled manual recovery.
    """

    def __init__(
        self,
        policy: ClickHouseValidatedFilePolicy,
        attempt_id: str,
        *,
        storage: StoragePreflightService,
        event_writer: Callable[..., str] = materialize_immutable_local_tree_at,
    ) -> None:
        if not re.fullmatch(r"[0-9a-f]{32}", attempt_id):
            raise ValueError("attempt_id must be UUID4 hex")
        self.policy, self.attempt_id, self.storage = policy, attempt_id, storage
        self._writer = event_writer
        self._bytes = 0
        self._sequence = 0
        self._previous_hash: str | None = None
        self._latest_bytes: bytes | None = None
        work = policy.work_directory.absolute()
        if any(part.is_symlink() for part in (work, *work.parents)):
            raise FileConsumptionError("resource_limit")
        try:
            self._admission = storage.require_spool_admission(
                RuntimeStoragePolicy(work_dir=work, min_free_bytes=policy.min_free_bytes)
            )
        except RuntimeStorageAdmissionError as exc:
            raise FileConsumptionError("resource_limit") from exc
        try:
            root = self._admission.directory
            root.require_identity()
            os.mkdir(attempt_id, mode=0o700, dir_fd=root.descriptor)
            os.fsync(root.descriptor)
            self.directory = self._admission.work_dir / attempt_id
            self._directory = PinnedDirectory.open(self.directory)
        except BaseException:
            self._admission.directory.close()
            raise

    def __enter__(self) -> ClickHouseFileAttemptJournal:
        return self

    def __exit__(self, *_args: object) -> None:
        self._directory.close()
        self._admission.directory.close()

    @property
    def latest(self) -> dict[str, Any] | None:
        """Return an independent snapshot; callers cannot mutate recorded history."""
        return json.loads(self._latest_bytes) if self._latest_bytes is not None else None

    def require_identity(self) -> None:
        self._admission.directory.require_identity()
        self._directory.require_identity()

    def reserve(self, amount: int) -> None:
        """Admit each actual spool/event byte against cap and refreshed free space."""
        self.require_identity()
        try:
            self._admission = self.storage.refresh_spool_admission(self._admission)
        except RuntimeStorageAdmissionError as exc:
            raise FileConsumptionError("resource_limit", phase="preparing") from exc
        if amount < 0 or self._bytes + amount > self.policy.max_spool_bytes or amount > self._admission.max_spool_bytes:
            raise FileConsumptionError("resource_limit", phase="preparing")
        self._bytes += amount

    def open_partial(self) -> BinaryIO:
        """Exclusively create a named private spool through its pinned directory."""
        self.require_identity()
        descriptor = os.open(
            "transport.partial",
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=self._directory.descriptor,
        )
        return os.fdopen(descriptor, "w+b")

    def seal(self, stream: BinaryIO) -> None:
        """Persist bytes and publish the final name without replacing another file."""
        self.require_identity()
        stream.flush()
        os.fsync(stream.fileno())
        descriptor = self._directory.descriptor
        os.link(
            "transport.partial",
            "transport.rowbinary",
            src_dir_fd=descriptor,
            dst_dir_fd=descriptor,
            follow_symlinks=False,
        )
        os.unlink("transport.partial", dir_fd=descriptor)
        os.fsync(descriptor)
        stream.seek(0)

    def release_spool(self) -> None:
        """Remove only fixed attempt-owned spool names; keep immutable events."""
        self.require_identity()
        for name in ("transport.partial", "transport.rowbinary"):
            try:
                os.unlink(name, dir_fd=self._directory.descriptor)
            except FileNotFoundError:
                pass
        os.fsync(self._directory.descriptor)

    def record(self, **changes: Any) -> dict[str, Any]:
        """Durably append one safe state snapshot, then advance sequence/hash."""
        record = self.latest or {
            "schema": "dpone.clickhouse-file-consumption.v1",
            "attempt_id": self.attempt_id,
            "semantic_id": None,
            "phase": "checking",
            "outcome": "running",
            "requested_mode": None,
            "resolved_mode": None,
            "source_identity": None,
            "derived_identity": None,
            "rows_prepared": None,
            "rows_observed": None,
            "query_id": None,
            "query_kind": None,
            "endpoint_identity": None,
            "owned_staging": None,
            "local_sender_state": None,
            "remote_execution_state": None,
            "cleanup": {"source": "unchanged", "spool": "not_created", "staging": "not_created"},
            "column": None,
            "row_ordinal": None,
            "blocker": None,
            "primary_error_code": None,
        }
        if set(changes) - set(record):
            raise ValueError("unknown consumption event field")
        record.update(changes)
        record.update(
            sequence=self._sequence + 1,
            previous_event_sha256=self._previous_hash,
            created_at_utc=datetime.now(UTC).isoformat(),
        )
        encoded = canonical_json(record)
        if len(encoded) > MAX_EVENT_BYTES:
            raise FileConsumptionError("resource_limit", phase=str(record["phase"]))
        self.reserve(len(encoded))
        try:
            self._writer(
                self._directory.descriptor,
                PurePosixPath("events", f"{self._sequence + 1:06d}"),
                {"attempt.json": encoded},
                error_root=self.directory,
            )
            self.require_identity()
        except (OSError, RuntimeError) as exc:
            raise FileConsumptionError("attempt_journal_unavailable", phase=str(record["phase"])) from exc
        self._sequence += 1
        self._previous_hash = hashlib.sha256(encoded).hexdigest()
        self._latest_bytes = encoded
        return json.loads(encoded)

    def attach_failure(self, error: BaseException, outcome: str) -> None:
        """Record cleanup observations and attach the latest durable event to the error."""
        try:
            self.record(
                phase="cleanup",
                outcome=outcome,
                blocker=getattr(error, "blocker", None),
                column=getattr(error, "details", {}).get("column"),
                row_ordinal=getattr(error, "details", {}).get("row_ordinal"),
                primary_error_code=getattr(error, "code", type(error).__name__),
                cleanup={
                    "source": "unchanged",
                    "spool": "released" if outcome == "failed_cleaned" else "retained_unknown",
                    "staging": "released" if outcome == "failed_cleaned" else "retained_unknown",
                },
            )
        except BaseException as journal_error:
            add_exception_note(error, f"attempt failure evidence unavailable:{type(journal_error).__name__}")
        details = dict(getattr(error, "details", {}) or {})
        details["validated_file_consumption"] = {**(self.latest or {}), "journal_directory": str(self.directory)}
        setattr(error, "details", details)
