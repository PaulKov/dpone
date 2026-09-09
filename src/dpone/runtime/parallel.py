"""Reusable bounded parallel execution primitives for runtime adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


class BoundedParallelMapExecutor:
    """Small ordered map executor shared by partitioned runtime paths.

    Source and sink adapters should not each hand-roll ``ThreadPoolExecutor``
    orchestration. This class centralizes the common bounded, ordered,
    fail-fast behavior used by native partition exports and partition loads.
    """

    def __init__(self, max_workers: int) -> None:
        self.max_workers = max(1, int(max_workers))

    def map(self, worker: Callable[[T], R], items: Iterable[T]) -> list[R]:
        item_list = list(items)
        if self.max_workers == 1 or len(item_list) <= 1:
            return [worker(item) for item in item_list]
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return list(executor.map(worker, item_list))


__all__ = ["BoundedParallelMapExecutor"]
