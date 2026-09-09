"""Parallel message streaming helpers for Omnidesk incremental extraction."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any

from dpone.runtime.connectors.api.config import ConcurrencyConfig
from dpone.runtime.connectors.api.parallel import BoundedParallelStreamExecutor


class OmnideskParallelMessagesMixin:
    def _parallel_messages_generator(
        self,
        load_config: Any,
        lookback_days: int,
        exclude: set,
        parent_batch_size: int,
        parallel_workers: int,
        total_cases: int | None = None,
        message_counts: dict[int, int] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream messages through the shared bounded API fan-out executor."""

        message_counts = message_counts or {}
        error_count = 0
        error_lock = threading.Lock()
        processed_cases_count = 0
        processed_cases_lock = threading.Lock()
        skipped_messages_count = 0
        skipped_lock = threading.Lock()

        def fetch_messages(case_id: int) -> Iterator[dict[str, Any]]:
            nonlocal error_count, processed_cases_count, skipped_messages_count

            existing_count = message_counts.get(case_id, 0)
            message_idx = 0
            local_skipped = 0

            try:
                for message in self.connector.get_messages(case_id=case_id):
                    if message_idx < existing_count:
                        message_idx += 1
                        local_skipped += 1
                        continue

                    filtered = {key: value for key, value in message.items() if key not in exclude}
                    message_idx += 1
                    yield filtered

            except Exception as exc:
                self.logger.warning(f"Ошибка получения messages для case {case_id}: {exc}")
                with error_lock:
                    error_count += 1
            finally:
                with skipped_lock:
                    skipped_messages_count += local_skipped
                with processed_cases_lock:
                    processed_cases_count += 1
                    if processed_cases_count % 100 == 0:
                        if total_cases:
                            self.logger.info(
                                f"   📦 Обработано {processed_cases_count}/{total_cases} cases, "
                                f"пропущено: {skipped_messages_count}"
                            )
                        else:
                            self.logger.info(
                                f"   📦 Обработано {processed_cases_count} cases, пропущено: {skipped_messages_count}"
                            )

        executor = BoundedParallelStreamExecutor(
            ConcurrencyConfig(max_workers=parallel_workers, preserve_order=False, fail_fast=False),
            max_queue_size=10_000,
            logger=self.logger,
        )
        processed_messages = 0
        for item in executor.stream_batches(
            self._iter_parent_ids_batched(load_config, lookback_days, parent_batch_size),
            fetch_messages,
        ):
            yield item
            processed_messages += 1

        self.logger.info(
            f"🏁 Параллельный streaming завершён: "
            f"{processed_cases_count} cases, новых: {processed_messages}, "
            f"пропущено (уже в БД): {skipped_messages_count}, errors={error_count}"
        )


__all__ = ["OmnideskParallelMessagesMixin"]
