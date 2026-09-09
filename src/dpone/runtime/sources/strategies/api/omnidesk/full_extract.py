"""
Стратегия FULL_REFRESH для Omnidesk API.

Особенности:
- Streaming: данные обрабатываются батчами, не грузятся в память целиком
- Разбивает период на чанки с учётом ограничений API (max 30 дней)
- Адаптивное разбиение: при превышении лимита страниц автоматически
  создаёт продолжение с того места, где остановились
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


class OmnideskFullExtractStrategy(APIBaseStrategy):
    """
    Стратегия полной выгрузки для Omnidesk (streaming).

    Особенности:
    - Streaming: данные обрабатываются батчами, не грузятся в память целиком
    - Автоматически исключает проблемные колонки
    - Разбивает период на чанки с учётом ограничений API
    - Адаптивное разбиение: если чанк превышает лимит страниц, автоматически
      разбивается на более мелкие части

    Параметры в options:
    - resource: тип ресурса API (cases)
    - full_refresh_days: за сколько дней выгружать (default: 365)
    - exclude_columns: дополнительные колонки для исключения
    - batch_size: размер батча для streaming (default: 1000)
    - max_pages: лимит страниц (default: 450)
    """

    DEFAULT_EXCLUDE_COLUMNS = ["custom_fields", "locked_labels"]
    DEFAULT_MAX_PERIOD_DAYS = 15
    SAFE_PAGE_LIMIT = 450
    MIN_CHUNK_DAYS = 1
    # Колонка для сортировки при full refresh (created_at для хронологического порядка)
    SORT_COLUMN = "created_at"

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        """Full refresh не использует state."""
        return None

    def extract(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
    ) -> ExtractResult:
        """
        Извлекает все данные за указанный период (streaming).
        """
        options = self._get_options(load_config)
        resource = options.get("resource", "cases")
        full_refresh_days = options.get("full_refresh_days", 365)
        max_pages = options.get("max_pages")
        exclude_columns = options.get("exclude_columns", [])
        batch_size = options.get("batch_size", 1000)
        max_period_days = options.get("max_period_days", self.DEFAULT_MAX_PERIOD_DAYS)

        from_time = datetime.now(UTC) - timedelta(days=full_refresh_days)
        to_time = datetime.now(UTC)

        self.logger.log_etl_progress(
            "API_FULL_EXTRACT",
            {
                "Resource": resource,
                "From_Time": str(from_time),
                "To_Time": str(to_time),
                "Days": full_refresh_days,
                "Mode": "streaming",
            },
        )

        # Получаем итератор и первую запись для определения схемы
        iterator, first_record, schema = self._create_streaming_iterator(
            resource=resource,
            from_time=from_time,
            to_time=to_time,
            max_pages=max_pages,
            exclude_columns=exclude_columns,
            max_period_days=max_period_days,
        )

        if first_record is None:
            self.logger.warning(f"Full refresh для {resource} вернул 0 записей.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=None,
                force_full_refresh=True,
            )

        self.logger.info(f"📊 Full refresh streaming: схема определена ({len(schema)} колонок)")

        # Создаём streaming артефакт с итератором
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
        sort_column: str | None = None,
        max_period_days: int | None = None,
    ) -> tuple[Iterator[dict[str, Any]], dict[str, Any] | None, list[tuple[str, str]]]:
        """
        Создаёт streaming итератор для Omnidesk.

        Разбивает период на чанки с адаптивным размером:
        - Начинает с max_period_days дней (конфигурируемо)
        - При превышении лимита страниц автоматически продолжает
        """
        chunk_days = max_period_days or self.DEFAULT_MAX_PERIOD_DAYS
        exclude = set(self.DEFAULT_EXCLUDE_COLUMNS)
        if exclude_columns:
            exclude.update(exclude_columns)

        # Определяем колонку для сортировки и отслеживания
        track_column = sort_column or self.SORT_COLUMN
        sort_order = f"{track_column}_asc"

        # Определяем лимит страниц для использования
        effective_max_pages = max_pages if max_pages else self.SAFE_PAGE_LIMIT

        # Разбиваем период на начальные чанки
        total_days = (to_time - from_time).days
        initial_chunks: list[tuple[datetime, datetime]] = []

        if total_days <= chunk_days:
            initial_chunks = [(from_time, to_time)]
        else:
            chunk_start = from_time
            while chunk_start < to_time:
                chunk_end = min(chunk_start + timedelta(days=chunk_days), to_time)
                initial_chunks.append((chunk_start, chunk_end))
                chunk_start = chunk_end

        self.logger.info(
            f"🔍 Full refresh {resource}: {total_days} дней, разбито на {len(initial_chunks)} чанков "
            f"(max {chunk_days} дней, лимит {effective_max_pages} страниц, sort={sort_order})"
        )

        # Генератор для streaming по чанкам с адаптивным разбиением
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

                        # Отслеживаем sort_column для возможного продолжения
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

        # Определяем схему по первой записи
        schema = self._detect_schema_from_records([first_record])

        # Создаём итератор: первая запись + остальные
        def full_iterator() -> Iterator[dict[str, Any]]:
            yield first_record
            yield from iterator

        return full_iterator(), first_record, schema
