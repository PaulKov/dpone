"""
Стратегия INCREMENTAL_MERGE для Omnidesk API.

Особенности:
- Использует get_cases() напрямую для оптимальной производительности
- Streaming подход для любого объёма данных
- Адаптивное разбиение на чанки при большом периоде (max 30 дней)
- Автоматическое продолжение при превышении лимита страниц (450)
- Поддержка lookback_days для перезагрузки недавно обновлённых записей
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class OmnideskIncrementalMergeExtractStrategy(APIBaseStrategy):
    """
    INCREMENTAL_MERGE стратегия для Omnidesk (streaming).

    Особенности:
    - Использует get_cases() напрямую
    - Streaming подход для любого объёма данных
    - Адаптивное разбиение на чанки при большом периоде
    - Автоматическое продолжение при превышении лимита страниц

    Параметры в options:
    - resource: тип ресурса API (cases)
    - incremental_column: колонка для отслеживания (default: updated_at)
    - lookback_days: дней назад от MAX для страховки (default: 3)
    - full_refresh_days: за сколько дней при первой загрузке (default: 365)
    - exclude_columns: дополнительные колонки для исключения
    - batch_size: размер батча для streaming (default: 1000)
    - max_pages: лимит страниц (default: 450)
    """

    DEFAULT_EXCLUDE_COLUMNS = ["custom_fields", "locked_labels"]
    # Ограничения API Omnidesk
    MAX_PERIOD_DAYS = 15
    SAFE_PAGE_LIMIT = 450
    INCREMENTAL_COLUMN = "updated_at"

    def extract(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
    ) -> ExtractResult:
        """
        Извлекает данные из API для UPSERT (streaming).
        """
        options = self._get_options(load_config)
        resource = options.get("resource", "cases")
        incremental_column = options.get("incremental_column", self.INCREMENTAL_COLUMN)
        lookback_days = options.get("lookback_days", 3)
        max_pages = options.get("max_pages")
        exclude_columns = options.get("exclude_columns", [])
        batch_size = options.get("batch_size", 1000)

        self.logger.info(f"🔄 Стратегия: INCREMENTAL_MERGE (resource={resource}, column={incremental_column})")

        # Если нет state — full refresh
        if not last_state:
            self.logger.info("Нет инкрементального состояния. Full refresh.")
            return self._extract_full(load_config, resource, exclude_columns, max_pages)

        last_value = last_state.get("last_value")
        if not last_value:
            self.logger.info("last_value пустой. Full refresh.")
            return self._extract_full(load_config, resource, exclude_columns, max_pages)

        # Вычисляем from_time с lookback
        from_time = self._calculate_from_time(last_value, lookback_days)
        to_time = datetime.now(UTC)

        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE",
            {
                "Resource": resource,
                "Column": incremental_column,
                "Last_Value": str(last_value),
                "From_Time": str(from_time),
                "To_Time": str(to_time),
                "Lookback_Days": lookback_days,
                "Mode": "streaming",
            },
        )

        # Создаём streaming итератор
        iterator, first_record, schema = self._create_streaming_iterator(
            resource=resource,
            from_time=from_time,
            to_time=to_time,
            max_pages=max_pages,
            exclude_columns=exclude_columns,
            incremental_column=incremental_column,
        )

        if first_record is None:
            self.logger.info("API вернул 0 записей. Нет обновлений.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=last_state,
                force_full_refresh=False,
            )

        self.logger.info(f"📊 Streaming MERGE: схема определена ({len(schema)} колонок)")

        # Создаём streaming артефакт
        artifact = StreamingRowsArtifact(
            iterator=iterator,
            batch_size=batch_size,
        )

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=None,
            force_full_refresh=False,
        )

    def _extract_full(
        self,
        load_config: Any,
        resource: str,
        exclude_columns: list[str],
        max_pages: int | None = None,
    ) -> ExtractResult:
        """Full refresh для первой загрузки (streaming)."""
        options = self._get_options(load_config)
        full_refresh_days = options.get("full_refresh_days", 365)
        batch_size = options.get("batch_size", 1000)
        incremental_column = options.get("incremental_column", self.INCREMENTAL_COLUMN)

        from_time = datetime.now(UTC) - timedelta(days=full_refresh_days)
        to_time = datetime.now(UTC)

        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE_FULL",
            {
                "Resource": resource,
                "From_Time": str(from_time),
                "To_Time": str(to_time),
                "Days": full_refresh_days,
                "Mode": "streaming",
            },
        )

        iterator, first_record, schema = self._create_streaming_iterator(
            resource=resource,
            from_time=from_time,
            to_time=to_time,
            max_pages=max_pages,
            exclude_columns=exclude_columns,
            incremental_column=incremental_column,
        )

        if first_record is None:
            self.logger.info("API вернул 0 записей для full refresh.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=None,
                force_full_refresh=True,
            )

        self.logger.info(f"📊 Full refresh streaming: схема определена ({len(schema)} колонок)")

        # Создаём streaming артефакт
        artifact = StreamingRowsArtifact(
            iterator=iterator,
            batch_size=batch_size,
        )

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=None,
            force_full_refresh=True,
        )

    def _create_streaming_iterator(
        self,
        resource: str,
        from_time: datetime,
        to_time: datetime,
        max_pages: int | None = None,
        exclude_columns: list[str] | None = None,
        incremental_column: str | None = None,
    ) -> tuple[Iterator[dict[str, Any]], dict[str, Any] | None, list[tuple[str, str]]]:
        """
        Создаёт streaming итератор для Omnidesk с адаптивным разбиением.

        При большом периоде (> MAX_PERIOD_DAYS) разбивает на чанки.
        При превышении лимита страниц — автоматически создаёт продолжение.
        """
        exclude = set(self.DEFAULT_EXCLUDE_COLUMNS)
        if exclude_columns:
            exclude.update(exclude_columns)

        # Определяем колонку для сортировки и отслеживания
        track_column = incremental_column or self.INCREMENTAL_COLUMN
        sort_order = f"{track_column}_asc"

        # Определяем лимит страниц
        effective_max_pages = max_pages if max_pages else self.SAFE_PAGE_LIMIT

        # Разбиваем период на чанки
        total_days = (to_time - from_time).days
        initial_chunks: list[tuple[datetime, datetime]] = []

        if total_days <= self.MAX_PERIOD_DAYS:
            initial_chunks = [(from_time, to_time)]
        else:
            chunk_start = from_time
            while chunk_start < to_time:
                chunk_end = min(chunk_start + timedelta(days=self.MAX_PERIOD_DAYS), to_time)
                initial_chunks.append((chunk_start, chunk_end))
                chunk_start = chunk_end

        self.logger.info(
            f"🔍 Incremental {resource}: {total_days} дней, разбито на {len(initial_chunks)} чанков "
            f"(max {self.MAX_PERIOD_DAYS} дней, лимит {effective_max_pages} страниц, sort={sort_order})"
        )

        # Генератор с адаптивным разбиением
        def chunked_iterator() -> Iterator[dict[str, Any]]:
            pending_chunks: list[tuple[datetime, datetime, int]] = [(c[0], c[1], 1) for c in initial_chunks]
            processed_chunks = 0
            total_estimated = len(initial_chunks)

            while pending_chunks:
                chunk_from, chunk_to, original_idx = pending_chunks.pop(0)
                processed_chunks += 1
                chunk_days = (chunk_to - chunk_from).days

                self.logger.info(
                    f"📦 Чанк {processed_chunks}/{total_estimated}: "
                    f"{chunk_from.date()} → {chunk_to.date()} ({chunk_days} дней)"
                )

                chunk_count = 0
                last_track_value: datetime | None = None
                hit_page_limit = False
                max_records_at_limit = effective_max_pages * 100

                try:
                    for case in self.connector.get_cases(
                        from_time=chunk_from,
                        to_time=chunk_to,
                        max_pages=effective_max_pages,
                        sort=sort_order,
                    ):
                        filtered = {k: v for k, v in case.items() if k not in exclude}
                        chunk_count += 1

                        # Отслеживаем incremental_column для возможного продолжения
                        if track_column in case and case[track_column]:
                            try:
                                if isinstance(case[track_column], datetime):
                                    last_track_value = case[track_column]
                                    if last_track_value.tzinfo is None:
                                        last_track_value = last_track_value.replace(tzinfo=UTC)
                                elif isinstance(case[track_column], str):
                                    last_track_value = datetime.fromisoformat(case[track_column].replace("Z", "+00:00"))
                                    if last_track_value.tzinfo is None:
                                        last_track_value = last_track_value.replace(tzinfo=UTC)
                            except (ValueError, TypeError):
                                pass

                        yield filtered

                except Exception as e:
                    error_str = str(e)
                    if "max_page_is_500" in error_str or "page=501" in error_str:
                        hit_page_limit = True
                        self.logger.warning(
                            f"⚠️ Чанк превысил лимит страниц API (500). Извлечено {chunk_count} записей до ошибки."
                        )
                    else:
                        raise

                # Проверяем graceful stop
                if not hit_page_limit and chunk_count >= max_records_at_limit * 0.95:
                    hit_page_limit = True
                    self.logger.info(
                        f"   ℹ️ Достигнут лимит страниц ({chunk_count} записей ≈ "
                        f"{chunk_count // 100} страниц). Создаём продолжение."
                    )

                # Создаём продолжение если нужно
                if hit_page_limit and last_track_value:
                    continuation_from = last_track_value + timedelta(seconds=1)

                    if continuation_from < chunk_to:
                        self.logger.info(f"   🔄 Создаём продолжение: {continuation_from} → {chunk_to.date()}")
                        pending_chunks.insert(0, (continuation_from, chunk_to, original_idx))
                        total_estimated += 1

                self.logger.info(f"   ✅ Чанк {processed_chunks}: {chunk_count} записей")

        # Получаем первую запись для схемы
        iterator = chunked_iterator()
        try:
            first_record = next(iterator)
        except StopIteration:
            return iter([]), None, []

        schema = self._detect_schema_from_records([first_record])

        def full_iterator() -> Iterator[dict[str, Any]]:
            yield first_record
            yield from iterator

        return full_iterator(), first_record, schema
