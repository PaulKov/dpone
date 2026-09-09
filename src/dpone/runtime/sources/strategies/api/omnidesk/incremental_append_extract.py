"""
Стратегия INCREMENTAL_APPEND для Omnidesk API (Messages).

Особенности:
- Получает case_id из cases с updated_at > last_run (STREAMING по батчам)
- Для каждого case получает messages (streaming)
- Батчированная обработка parent_ids для экономии памяти
- Поддержка параллельных запросов через parallel_workers
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.sources.strategies.api.omnidesk.parallel_messages_mixin import OmnideskParallelMessagesMixin
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class OmnideskIncrementalAppendExtractStrategy(OmnideskParallelMessagesMixin, APIBaseStrategy):
    """
    INCREMENTAL_APPEND стратегия для Omnidesk Messages

    Особенности:
    - Получает case_id БАТЧАМИ (не все сразу в память)
    - Для каждого case получает messages (streaming)
    - Дедупликация по message_id при записи
    - Минимальное потребление памяти
    - Поддержка параллельных запросов (parallel_workers)

    Параметры в options:
    - resource: тип ресурса API (messages)
    - lookback_days: дней назад для поиска обновлённых родителей (default: 1)
    - parent_table: таблица родителя в sink (default: omnidesk_cases)
    - exclude_columns: дополнительные колонки для исключения
    - batch_size: размер батча для streaming (default: 1000)
    - parent_batch_size: размер батча parent_ids (default: 500)
    - parallel_workers: количество параллельных воркеров (default: 1)
    """

    DEFAULT_EXCLUDE_COLUMNS = ["attachments", "sent_at"]
    DEFAULT_PARENT_BATCH_SIZE = 500
    DEFAULT_PARALLEL_WORKERS = 1

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        """APPEND стратегия не использует state напрямую."""
        return None

    def extract(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
    ) -> ExtractResult:
        """
        Извлекает данные для APPEND (TRUE STREAMING — минимум памяти).

        Поддерживает параллельные запросы через parallel_workers.
        """
        options = self._get_options(load_config)
        resource = options.get("resource", "messages")
        lookback_days = options.get("lookback_days", 1)
        exclude_columns = options.get("exclude_columns", [])
        batch_size = options.get("batch_size", 1000)
        parent_batch_size = options.get("parent_batch_size", self.DEFAULT_PARENT_BATCH_SIZE)
        parallel_workers = options.get("parallel_workers", self.DEFAULT_PARALLEL_WORKERS)

        self.logger.info(
            f"🔄 Стратегия: INCREMENTAL_APPEND (resource={resource}, mode=streaming, workers={parallel_workers})"
        )

        # Считаем кол-во родительских записей
        parent_count = self._count_parent_ids(load_config, lookback_days)

        if parent_count == 0:
            self.logger.info("Нет родительских записей для загрузки. Пропускаем.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=last_state,
                force_full_refresh=False,
            )

        self.logger.info(f"📋 Найдено {parent_count} родительских записей")

        # Получаем count существующих messages для каждого case (для skip-логики)
        message_counts = self._get_message_counts_per_case(load_config, lookback_days)
        total_existing = sum(message_counts.values())

        self.logger.info(
            f"🔍 Загрузка messages для {parent_count} cases "
            f"(streaming, workers={parallel_workers}, existing_messages={total_existing})"
        )

        # Создаём TRUE STREAMING итератор
        iterator, first_record, schema = self._create_streaming_iterator(
            load_config=load_config,
            lookback_days=lookback_days,
            exclude_columns=exclude_columns,
            parent_batch_size=parent_batch_size,
            parallel_workers=parallel_workers,
            total_cases=parent_count,
            message_counts=message_counts,
        )

        if first_record is None:
            self.logger.info("Нет новых записей для APPEND.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=last_state,
                force_full_refresh=False,
            )

        self.logger.info(f"📊 Streaming APPEND: схема определена ({len(schema)} колонок)")

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

    def _count_parent_ids(
        self,
        load_config: Any,
        lookback_days: int,
    ) -> int:
        """Считает количество parent_ids без загрузки в память."""
        options = self._get_options(load_config)
        parent_table = options.get("parent_table", "omnidesk_cases")
        parent_schema = load_config.target_schema

        try:
            connector_class = self.sink_connector.__class__.__name__

            if "BigQuery" in connector_class:
                query = f"""
                    SELECT COUNT(DISTINCT case_id) as cnt
                    FROM `{parent_schema}.{parent_table}`
                    WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
                """
            else:
                query = f"""
                    SELECT COUNT(DISTINCT case_id) as cnt
                    FROM {parent_schema}.{parent_table}
                    WHERE updated_at >= CURRENT_DATE - INTERVAL '{lookback_days} day'
                """

            result = self.sink_connector.get_records(query, as_dict=True)
            return result[0]["cnt"] if result else 0
        except Exception as exc:
            self.logger.warning(f"Ошибка подсчёта case_ids: {exc}")
            return 0

    def _get_message_counts_per_case(
        self,
        load_config: Any,
        lookback_days: int,
    ) -> dict[int, int]:
        """
        Получает количество УЖЕ СУЩЕСТВУЮЩИХ messages для каждого case.

        Используется для skip-логики: пропускаем первые N messages,
        которые уже есть в target

        """
        options = self._get_options(load_config)
        parent_table = options.get("parent_table", "omnidesk_cases")
        parent_schema = load_config.target_schema
        target_table = load_config.target_table

        try:
            connector_class = self.sink_connector.__class__.__name__

            if "BigQuery" in connector_class:
                query = f"""
                    SELECT
                        oc.case_id,
                        COUNT(om.message_id) as count_messages
                    FROM `{parent_schema}.{parent_table}` oc
                    LEFT JOIN `{parent_schema}.{target_table}` om
                        ON oc.case_id = om.case_id
                    WHERE oc.updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
                    GROUP BY oc.case_id
                """
            else:
                query = f"""
                    SELECT
                        oc.case_id,
                        COUNT(om.message_id) as count_messages
                    FROM {parent_schema}.{parent_table} oc
                    LEFT JOIN {parent_schema}.{target_table} om
                        ON oc.case_id = om.case_id
                    WHERE oc.updated_at >= CURRENT_DATE - INTERVAL '{lookback_days} day'
                    GROUP BY oc.case_id
                """

            result = self.sink_connector.get_records(query, as_dict=True)
            return {row["case_id"]: row["count_messages"] for row in result} if result else {}

        except Exception as exc:
            self.logger.warning(f"Ошибка получения count_messages: {exc}")
            return {}

    def _iter_parent_ids_batched(
        self,
        load_config: Any,
        lookback_days: int,
        batch_size: int,
    ) -> Iterator[list[int]]:
        """
        GENERATOR: Возвращает case_id БАТЧАМИ (не все сразу в память).

        Использует OFFSET/LIMIT для PostgreSQL или LIMIT/OFFSET для BigQuery.
        """
        options = self._get_options(load_config)
        parent_table = options.get("parent_table", "omnidesk_cases")
        parent_schema = load_config.target_schema
        connector_class = self.sink_connector.__class__.__name__
        is_bigquery = "BigQuery" in connector_class

        offset = 0
        while True:
            try:
                if is_bigquery:
                    query = f"""
                        SELECT DISTINCT case_id
                        FROM `{parent_schema}.{parent_table}`
                        WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
                        ORDER BY case_id
                        LIMIT {batch_size} OFFSET {offset}
                    """
                else:
                    query = f"""
                        SELECT DISTINCT case_id
                        FROM {parent_schema}.{parent_table}
                        WHERE updated_at >= CURRENT_DATE - INTERVAL '{lookback_days} day'
                        ORDER BY case_id
                        LIMIT {batch_size} OFFSET {offset}
                    """

                result = self.sink_connector.get_records(query, as_dict=True)

                if not result:
                    break

                batch = [row["case_id"] for row in result]
                yield batch

                if len(batch) < batch_size:
                    break

                offset += batch_size

            except Exception as exc:
                self.logger.warning(f"Ошибка получения батча case_ids (offset={offset}): {exc}")
                break

    def _create_streaming_iterator(
        self,
        load_config: Any,
        lookback_days: int,
        exclude_columns: list[str] | None = None,
        parent_batch_size: int = 500,
        parallel_workers: int = 1,
        total_cases: int | None = None,
        message_counts: dict[int, int] | None = None,
    ) -> tuple[Iterator[dict[str, Any]], dict[str, Any] | None, list[tuple[str, str]]]:
        """
        Создаёт TRUE STREAMING итератор для messages.

        Особенности:
        - parent_ids загружаются БАТЧАМИ
        - messages для каждого case - generator (не list)
        - Минимальное потребление памяти
        - Поддержка параллельных запросов через parallel_workers
        """
        exclude = set(self.DEFAULT_EXCLUDE_COLUMNS)
        if exclude_columns:
            exclude.update(exclude_columns)

        # Создаём генератор messages (последовательный или параллельный)
        if parallel_workers > 1:
            iterator = self._parallel_messages_generator(
                load_config=load_config,
                lookback_days=lookback_days,
                exclude=exclude,
                parent_batch_size=parent_batch_size,
                parallel_workers=parallel_workers,
                total_cases=total_cases,
                message_counts=message_counts or {},
            )
        else:
            iterator = self._streaming_messages_generator(
                load_config=load_config,
                lookback_days=lookback_days,
                exclude=exclude,
                parent_batch_size=parent_batch_size,
                total_cases=total_cases,
                message_counts=message_counts or {},
            )

        # Получаем первую запись для схемы
        try:
            first_record = next(iterator)
        except StopIteration:
            return iter([]), None, []

        # Определяем схему
        schema = self._detect_schema_from_records([first_record])

        # Создаём итератор: первая запись + остальные
        def full_iterator() -> Iterator[dict[str, Any]]:
            yield first_record
            yield from iterator

        return full_iterator(), first_record, schema

    def _streaming_messages_generator(
        self,
        load_config: Any,
        lookback_days: int,
        exclude: set,
        parent_batch_size: int,
        total_cases: int | None = None,
        message_counts: dict[int, int] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        TRUE STREAMING генератор messages (последовательный режим).

        Логика skip:
        1. Получает батч case_id
        2. Для каждого case_id смотрит сколько messages УЖЕ в БД
        3. Пропускает первые N messages (уже существующие)
        4. Yield только НОВЫЕ messages
        """
        message_counts = message_counts or {}
        processed_cases = 0
        processed_messages = 0
        skipped_messages = 0

        for case_batch in self._iter_parent_ids_batched(load_config, lookback_days, parent_batch_size):
            for case_id in case_batch:
                # Сколько messages для этого case УЖЕ есть в БД
                existing_count = message_counts.get(case_id, 0)
                message_idx = 0

                try:
                    # get_messages возвращает generator — не копим в память
                    for message in self.connector.get_messages(case_id=case_id):
                        # Пропускаем первые N messages (уже в БД)
                        if message_idx < existing_count:
                            message_idx += 1
                            skipped_messages += 1
                            continue

                        filtered = {k: v for k, v in message.items() if k not in exclude}
                        yield filtered
                        processed_messages += 1
                        message_idx += 1

                except Exception as exc:
                    self.logger.warning(f"Ошибка получения messages для case {case_id}: {exc}")

                processed_cases += 1

                # Логируем прогресс каждые 100 cases
                if processed_cases % 100 == 0:
                    if total_cases:
                        pct = (processed_cases / total_cases) * 100
                        self.logger.info(
                            f"   📦 Обработано {processed_cases}/{total_cases} cases ({pct:.1f}%), "
                            f"новых: {processed_messages}, пропущено: {skipped_messages}"
                        )
                    else:
                        self.logger.info(
                            f"   📦 Обработано {processed_cases} cases, "
                            f"новых: {processed_messages}, пропущено: {skipped_messages}"
                        )

        self.logger.info(
            f"🏁 Streaming завершён: {processed_cases} cases, "
            f"новых messages: {processed_messages}, пропущено (уже в БД): {skipped_messages}"
        )
