"""Durable local journal for one desired-state publication occurrence."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path

from dpone.adapters.airflow_desired_state_checkpoint import (
    AtomicAirflowDesiredStateSnapshotWriter,
)
from dpone.contracts.airflow_desired_state_publish import (
    DesiredStatePublishCandidate,
    DesiredStatePublishIntent,
    DesiredStatePublishPreparation,
)
from dpone.contracts.airflow_desired_state_validation import (
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
)

MAX_PUBLISH_INTENT_BYTES = 8 * 1024


class FileDesiredStatePublishIntentStore:
    """Create once and reuse one bounded publish occurrence across retries."""

    def __init__(self, path: Path) -> None:
        if not path.name:
            raise ValueError("desired-state publish intent path must name a file")
        self._path = path

    def resolve(
        self,
        *,
        candidate_sha256: str,
        clock: Callable[[], datetime],
        occurrence_id_factory: Callable[[], object],
    ) -> DesiredStatePublishIntent:
        with _exclusive_intent_lock(self._path):
            existing = self._read()
            if existing is not None:
                if existing.candidate_sha256 != candidate_sha256:
                    raise ValueError("publish intent belongs to another candidate")
                return existing
            intent = DesiredStatePublishIntent(
                candidate_sha256=candidate_sha256,
                occurrence_id=str(occurrence_id_factory()),
                promoted_at=_format_utc(clock()),
            )
            AtomicAirflowDesiredStateSnapshotWriter(self._path).commit(intent.to_json_bytes())
            committed = self._read()
            if committed != intent:
                raise OSError("desired-state publish intent commit was not durable")
            return intent

    def _read(self) -> DesiredStatePublishIntent | None:
        try:
            size = self._path.stat().st_size
        except FileNotFoundError:
            return None
        if size <= 0 or size > MAX_PUBLISH_INTENT_BYTES:
            raise ValueError("desired-state publish intent size is invalid")
        body = self._path.read_bytes()
        if len(body) != size:
            raise ValueError("desired-state publish intent changed while being read")
        return DesiredStatePublishIntent.from_json_bytes(body)


class FileDesiredStatePublishPreparationStore:
    """Create once and reuse one bounded canonical preparation artifact."""

    def __init__(self, path: Path) -> None:
        if not path.name:
            raise ValueError("desired-state publish preparation path must name a file")
        self._path = path

    def resolve(
        self,
        *,
        candidate: DesiredStatePublishCandidate,
        preparation_factory: Callable[[], DesiredStatePublishPreparation],
    ) -> DesiredStatePublishPreparation:
        with _exclusive_intent_lock(self._path):
            existing = self._read()
            if existing is not None:
                if existing.candidate != candidate:
                    raise ValueError("publish preparation belongs to another candidate")
                return existing
            preparation = preparation_factory()
            if preparation.candidate != candidate:
                raise ValueError("publish preparation factory returned another candidate")
            AtomicAirflowDesiredStateSnapshotWriter(self._path).commit(preparation.to_json_bytes())
            committed = self._read()
            if committed != preparation:
                raise OSError("desired-state publish preparation commit was not durable")
            return preparation

    def read(self) -> DesiredStatePublishPreparation:
        preparation = self._read()
        if preparation is None:
            raise FileNotFoundError(self._path)
        return preparation

    def _read(self) -> DesiredStatePublishPreparation | None:
        try:
            size = self._path.stat().st_size
        except FileNotFoundError:
            return None
        if size <= 0 or size > MAX_AIRFLOW_DESIRED_STATE_BYTES:
            raise ValueError("desired-state publish preparation size is invalid")
        body = self._path.read_bytes()
        if len(body) != size:
            raise ValueError("desired-state publish preparation changed while being read")
        return DesiredStatePublishPreparation.from_json_bytes(body)


def commit_desired_state_publish_output(path: Path, body: bytes) -> None:
    """Atomically commit one already serialized publish CLI result."""

    AtomicAirflowDesiredStateSnapshotWriter(path).commit(body)


def _format_utc(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return an offset-aware datetime")
    normalized = value.astimezone(timezone.utc)  # noqa: UP017
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


@contextmanager
def _exclusive_intent_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path.parent / f".{path.name}.lock",
        os.O_CREAT | os.O_RDWR,
        0o600,
    )
    file_lock = import_module("fcntl")
    try:
        file_lock.flock(descriptor, file_lock.LOCK_EX)
        yield
    finally:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_UN)
        finally:
            os.close(descriptor)


__all__ = [
    "FileDesiredStatePublishIntentStore",
    "FileDesiredStatePublishPreparationStore",
    "MAX_PUBLISH_INTENT_BYTES",
    "commit_desired_state_publish_output",
]
