"""Parallel task execution helpers for API connectors."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from queue import Queue
from typing import Any, TypeVar

from dpone.runtime.connectors.api.config import ConcurrencyConfig
from dpone.runtime.connectors.api.rate_limit import ThreadSafeRateLimiter

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class TaskResult(tuple[T, Exception | None]):
    """Результат выполнения задачи: (результат, ошибка)."""

    pass


class ParallelTaskExecutor:
    """
    Универсальный исполнитель параллельных задач с rate limiting.

    Особенности:
    - Thread-safe rate limiting (общий для всех воркеров)
    - Поддержка preserve_order для детерминированных результатов
    - Fail-fast или сбор всех ошибок
    - Прогресс-логирование

    """

    def __init__(
        self,
        rate_limiter: ThreadSafeRateLimiter,
        config: ConcurrencyConfig,
        logger: logging.Logger | None = None,
    ):
        self.rate_limiter = rate_limiter
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

        # Статистика
        self._completed = 0
        self._errors = 0
        self._lock = threading.Lock()

    def map(
        self,
        func: Callable[[T], R],
        items: Sequence[T],
        total_hint: int | None = None,
    ) -> Iterator[tuple[R | None, Exception | None, T]]:
        """
        Параллельно применяет функцию к элементам с rate limiting.

        Args:
            func: Функция для применения к каждому элементу
            items: Последовательность элементов
            total_hint: Подсказка общего количества (для логирования)

        Yields:
            Tuple[result, error, original_item]
        """
        total = total_hint or len(items) if hasattr(items, "__len__") else None

        if self.config.max_workers <= 1:
            # Последовательное выполнение
            yield from self._sequential_map(func, items, total)
        else:
            # Параллельное выполнение
            yield from self._parallel_map(func, items, total)

    def _sequential_map(
        self,
        func: Callable[[T], R],
        items: Sequence[T],
        total: int | None,
    ) -> Iterator[tuple[R | None, Exception | None, T]]:
        """Последовательное выполнение (baseline)."""
        for item in items:
            self.rate_limiter.acquire()

            try:
                result = func(item)
                self._increment_completed()
                yield result, None, item
            except Exception as e:
                self._increment_errors()
                if self.config.fail_fast:
                    raise
                yield None, e, item

            self._log_progress(total)

    def _parallel_map(
        self,
        func: Callable[[T], R],
        items: Sequence[T],
        total: int | None,
    ) -> Iterator[tuple[R | None, Exception | None, T]]:
        """Параллельное выполнение с ThreadPoolExecutor."""

        def rate_limited_func(item: T) -> tuple[R, T]:
            """Обёртка с rate limiting."""
            self.rate_limiter.acquire()
            return func(item), item

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            if self.config.preserve_order:
                # Сохраняем порядок
                futures = [executor.submit(rate_limited_func, item) for item in items]

                for future in futures:
                    try:
                        result, item = future.result()
                        self._increment_completed()
                        yield result, None, item
                    except Exception as e:
                        self._increment_errors()
                        if self.config.fail_fast:
                            # Отменяем оставшиеся задачи
                            for f in futures:
                                f.cancel()
                            raise
                        yield None, e, None

                    self._log_progress(total)
            else:
                future_to_item: dict[Future, T] = {executor.submit(rate_limited_func, item): item for item in items}

                for future in as_completed(future_to_item):
                    item = future_to_item[future]
                    try:
                        result, _ = future.result()
                        self._increment_completed()
                        yield result, None, item
                    except Exception as e:
                        self._increment_errors()
                        if self.config.fail_fast:
                            executor.shutdown(wait=False, cancel_futures=True)
                            raise
                        yield None, e, item

                    self._log_progress(total)

    def _increment_completed(self) -> None:
        with self._lock:
            self._completed += 1

    def _increment_errors(self) -> None:
        with self._lock:
            self._errors += 1

    def _log_progress(self, total: int | None) -> None:
        """Логирует прогресс каждые N задач."""
        with self._lock:
            completed = self._completed + self._errors

        if completed % self.config.progress_interval == 0:
            if total:
                pct = (completed / total) * 100
                self.logger.info(f"   📦 Обработано {completed}/{total} cases ({pct:.1f}%)")
            else:
                self.logger.info(f"   📦 Обработано {completed} cases")

    @property
    def stats(self) -> dict[str, Any]:
        """Возвращает статистику выполнения."""
        with self._lock:
            return {
                "completed": self._completed,
                "errors": self._errors,
                "rate_limiter": self.rate_limiter.stats,
            }


class BoundedParallelStreamExecutor:
    """Reusable bounded streaming executor for API fan-out workloads.

    It keeps provider strategies from hand-rolling queue/thread orchestration
    while preserving backpressure and streaming semantics.
    """

    def __init__(
        self,
        config: ConcurrencyConfig,
        *,
        max_queue_size: int = 10_000,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.max_queue_size = max(1, max_queue_size)
        self.logger = logger or logging.getLogger(__name__)

    def stream_batches(
        self,
        batches: Iterator[Sequence[T]],
        worker: Callable[[T], Iterator[R]],
    ) -> Iterator[R]:
        if self.config.max_workers <= 1:
            for batch in batches:
                for item in batch:
                    yield from worker(item)
            return

        result_queue: Queue[R | object] = Queue(maxsize=self.max_queue_size)
        sentinel = object()

        def run_item(item: T) -> None:
            for result in worker(item):
                result_queue.put(result)

        def producer() -> None:
            try:
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    for batch in batches:
                        futures = [executor.submit(run_item, item) for item in batch]
                        for future in as_completed(futures):
                            future.result()
            finally:
                result_queue.put(sentinel)

        producer_thread = threading.Thread(target=producer, daemon=True)
        producer_thread.start()
        while True:
            item = result_queue.get()
            if item is sentinel:
                break
            yield item  # type: ignore[misc]
        producer_thread.join(timeout=5.0)


__all__ = ["BoundedParallelStreamExecutor", "ParallelTaskExecutor", "TaskResult"]
