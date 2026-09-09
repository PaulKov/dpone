"""Единая стратегия INCREMENTAL_APPEND для PostgreSQL."""

from __future__ import annotations

import itertools

from psycopg import sql

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class PostgresIncrementAppendStrategy(PostgresStrategyBase):
    """Стратегия INCREMENTAL_APPEND для PostgreSQL.

    Поведение зависит от флага load_config.only_new_rows:

    only_new_rows=True (INSERT NOT EXISTS):
    - Если таблица не существует - создает её и вставляет все данные
    - Если существует - вставляет только новые записи (по unique_key)
    - Требует unique_key для проверки дубликатов
    - Используется для: append-only таблиц (события, логи, истории)

    only_new_rows=False (INSERT ALL):
    - Если таблица не существует - создает её
    - Вставляет ВСЕ данные без проверок на дубликаты
    - Используется для: append-only таблиц где дубликаты возможны

    ⚠️ ВНИМАНИЕ: only_new_rows=False может привести к дубликатам!

    Режим micro_batch_commit (source.options.micro_batch_commit: true):
    - Коммит после каждого батча (batch_size записей)
    - При падении DAG продолжаем с места остановки (не с начала)
    - Требует only_new_rows=True для защиты от дубликатов
    - Идеально для долгих загрузок с миллионами записей
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с incremental_append стратегией."""
        only_new = load_config.only_new_rows

        # Валидация: only_new_rows требует unique_key
        if only_new and not load_config.unique_key:
            raise ValueError(
                "only_new_rows=True требует unique_key для проверки дубликатов. "
                "Либо укажите unique_key, либо установите only_new_rows=False."
            )

        # Проверяем режим micro_batch_commit
        micro_batch_commit = getattr(load_config, "micro_batch_commit", False)

        if micro_batch_commit:
            # Валидация: micro_batch_commit требует only_new_rows=True
            if not only_new:
                raise ValueError(
                    "micro_batch_commit=True требует only_new_rows=True для защиты от дубликатов. "
                    "При перезапуске записи могут дублироваться без проверки unique_key."
                )
            # Валидация: micro_batch_commit работает только со StreamingRowsArtifact
            if not isinstance(payload.artifact, StreamingRowsArtifact):
                self.logger.warning(
                    "micro_batch_commit работает только со StreamingRowsArtifact. Используем стандартный режим."
                )
            else:
                return self._load_with_micro_batch_commit(load_config, payload)

        def handler(staging):
            target_created = self._ensure_target_table(load_config, payload.schema)

            # Определяем режим вставки и логируем
            if target_created:
                action = "INSERT ALL (new table)"
                reason = "Target table created"
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
            elif not only_new:
                action = "INSERT ALL (⚠️ duplicates possible)"
                reason = "only_new_rows=False"
                inserted = self._insert_from_staging(load_config, staging, payload.schema)
            else:
                action = "INSERT NOT EXISTS (by unique_key)"
                reason = "only_new_rows=True + table exists"
                inserted = self._insert_new_only(load_config, staging, payload)

            # Логируем операцию
            self.logger.log_etl_progress(
                "PG_INCREMENT_APPEND",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Action": action,
                    "Reason": reason,
                    "Only_New_Rows": only_new,
                    "Unique_Key": str(load_config.unique_key) if load_config.unique_key else "None",
                    "Inserted": inserted,
                },
            )

            return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

        return self._consume_with_staging(load_config, payload, handler)

    def _load_with_micro_batch_commit(
        self,
        load_config,
        payload: LoadPayload,
    ) -> LoadResult:
        """
        Micro-batch commit: коммит после каждого батча.

        Flow:
        1. Создаём target таблицу (если нет)
        2. Создаём staging таблицу (одну на всё время)
        3. Для каждого батча:
           - INSERT batch → staging
           - INSERT staging → target WHERE NOT EXISTS
           - COMMIT
           - TRUNCATE staging
        4. Cleanup staging

        Преимущества:
        - При падении DAG продолжаем с места остановки
        - Память: O(batch_size) вместо O(total_records)
        - Прогресс виден в target сразу
        """
        artifact: StreamingRowsArtifact = payload.artifact
        batch_size = artifact._batch_size
        iterator = artifact._iterator

        self.logger.log_etl_progress(
            "MICRO_BATCH_START",
            {
                "Target": f"{load_config.target_schema}.{load_config.target_table}",
                "Mode": "micro_batch_commit",
                "Batch_Size": batch_size,
                "Unique_Key": str(load_config.unique_key),
            },
        )

        staging = self.staging_manager.create(load_config, payload.schema)

        try:
            self.connector.connection.commit()
        except Exception:
            pass

        total_inserted = 0
        total_skipped = 0
        batch_num = 0

        try:
            # Создаём target таблицу (если нет) - один раз в начале
            self.connector.begin()
            target_created = self._ensure_target_table(load_config, payload.schema)
            self.connector.commit_transaction()

            if target_created:
                self.logger.log_etl_progress(
                    "MICRO_BATCH_TARGET_CREATED",
                    {
                        "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    },
                )

            # Обрабатываем батчи
            while True:
                # Получаем следующий батч
                chunk = list(itertools.islice(iterator, batch_size))
                if not chunk:
                    break

                batch_num += 1
                batch_rows = len(chunk)

                # Начинаем транзакцию для этого батча
                self.connector.begin()

                try:
                    # 1. INSERT batch → staging
                    self.staging_manager.insert_rows(staging, chunk)

                    # 2. INSERT staging → target WHERE NOT EXISTS
                    inserted = self._insert_new_only(load_config, staging, payload)
                    skipped = batch_rows - inserted

                    # 3. COMMIT
                    self.connector.commit_transaction()

                    total_inserted += inserted
                    total_skipped += skipped

                    # 4. TRUNCATE staging (для следующего батча)
                    self._truncate_staging(staging)

                    # Логируем прогресс
                    self.logger.log_etl_progress(
                        "MICRO_BATCH_COMMITTED",
                        {
                            "Batch": batch_num,
                            "Batch_Rows": batch_rows,
                            "Inserted": inserted,
                            "Skipped": skipped,
                            "Total_Inserted": total_inserted,
                            "Total_Skipped": total_skipped,
                        },
                    )

                except Exception as e:
                    self.connector.rollback()
                    self.logger.log_etl_error(
                        f"Micro-batch {batch_num} failed: {e}",
                        {
                            "Batch": batch_num,
                            "Total_Inserted_Before_Fail": total_inserted,
                        },
                    )
                    raise

            # Финальная статистика
            self.logger.log_etl_progress(
                "MICRO_BATCH_COMPLETE",
                {
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Total_Batches": batch_num,
                    "Total_Inserted": total_inserted,
                    "Total_Skipped": total_skipped,
                    "Mode": "micro_batch_commit",
                },
            )

            lifecycle = artifact.extraction_lifecycle
            if lifecycle is not None:
                lifecycle.complete()

            return LoadResult(
                inserted_rows=total_inserted,
                updated_rows=0,
                total_rows=total_inserted,
                staging_rows=total_inserted + total_skipped,
            )

        finally:
            # Cleanup staging
            try:
                staging.cleanup()
            except Exception:
                pass

    def _truncate_staging(self, staging) -> None:
        """Очищает staging таблицу для следующего батча."""
        truncate_sql = sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
        )
        self.connector.execute_query(truncate_sql)

    def _insert_new_only(self, load_config, staging, payload: LoadPayload) -> int:
        """
        Вставляет только новые записи (по unique_key).

        Использует explicit type casting для решения проблемы несовпадения типов:
        - API/JSON возвращает все значения как строки (text)
        - Target таблица имеет INTEGER/BOOLEAN/TIMESTAMP колонки
        """
        # Получаем типы target таблицы (кэшируется)
        target_types = self._get_target_column_types(load_config)

        # Список колонок для INSERT (без cast)
        columns = self._select_columns(payload.schema)

        # SELECT с explicit cast к типам target
        typed_select = self._build_typed_select("s", payload.schema, target_types)

        condition = self._build_key_condition("s", "t", load_config.unique_key)

        insert_sql = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {}
            FROM {}.{} AS s
            WHERE NOT EXISTS (
                SELECT 1 FROM {}.{} AS t
                WHERE {}
            )
            """
        ).format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            columns,
            typed_select,
            sql.Identifier(staging.schema),
            sql.Identifier(staging.table),
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            condition,
        )

        inserted = self.connector.execute_query(insert_sql)

        # Логируем sample после insert
        if inserted > 0:
            self._log_target_sample(load_config, load_config.log_sample_rows)

        return inserted
