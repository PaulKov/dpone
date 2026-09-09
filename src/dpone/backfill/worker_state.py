"""Worker-scoped durable state-store port for parallel backfills."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol

from dpone.backfill.state import FileBackfillStateStore


class BackfillWorkerStateStoreFactory(Protocol):
    """Open one isolated state session for a fixed executor worker lane."""

    def __call__(self, worker_id: int) -> AbstractContextManager[FileBackfillStateStore]: ...


__all__ = ["BackfillWorkerStateStoreFactory"]
