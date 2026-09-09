"""Стратегия полной перезагрузки для BigQuery."""

from __future__ import annotations

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.bigquery.bigquery_base import BigQueryStrategyBase
from dpone.runtime.sql_helpers import ExchangeQueries


class BigQueryFullRefreshStrategy(BigQueryStrategyBase):
    """Стратегия FULL_REFRESH для BigQuery.

    Поведение (overwrite_type=None или 'truncate_insert'):
    1. Если таблица не существует - создает её
    2. Если существует - удаляет все строки (TRUNCATE)
    3. Вставляет новые данные из staging

    Поведение (overwrite_type='exchange'):
    1. Данные уже загружены в staging таблицу {target_table}__tmp
    2. Если целевая таблица существует - переименовывает её в {target_table}__backup
    3. Переименовывает {target_table}__tmp в {target_table} (подмена)
    4. Удаляет {target_table}__backup
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с полной перезагрузкой."""
        columns = [col for col, _ in payload.schema]
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)

        # Проверяем режим overwrite_type
        overwrite_type = getattr(load_config, "overwrite_type", None)

        if overwrite_type == "exchange":
            # Exchange Pattern: атомарная замена через RENAME
            return self._load_with_exchange_pattern(load_config, payload, columns, fq_target)
        else:
            # Стандартный режим: TRUNCATE + INSERT (default для None и 'truncate_insert')
            return self._load_with_truncate(load_config, payload, columns, fq_target)

    def _load_with_truncate(
        self,
        load_config,
        payload: LoadPayload,
        columns: list,
        fq_target: str,
    ) -> LoadResult:
        """Загружает данные с TRUNCATE + INSERT (стандартный режим)."""
        # 1. Создаем целевую таблицу если нужно
        self._create_target_table_if_not_exists(load_config, payload.schema)

        # 2. Очищаем таблицу
        self._truncate_table(load_config)

        # 3. Вставляем данные из staging
        fq_staging = f"`{self.connector.project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"

        # Строим SELECT с PARSE_JSON() для JSON колонок
        include_tech = self._include_technical_columns(load_config)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=include_tech,
        )

        query = f"""
        INSERT INTO {fq_target} ({columns_str})
        SELECT {select_str}
        FROM {fq_staging}
        """

        self._log_full_refresh(fq_target, len(columns))

        inserted = self._execute_dml_query(query)

        # Валидация партиций после загрузки (только для ClickHouse source с партициями)
        partition_validation_results = self._validate_partitions_clickhouse(
            load_config=load_config,
            payload=payload,
            inserted=inserted,
        )

        if inserted and inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
            self._log_target_sample(fq_target, load_config.log_sample_rows)

        return LoadResult(
            inserted_rows=inserted or 0,
            updated_rows=0,
            total_rows=inserted or 0,
            deleted_lookback_rows=0,
            partition_validation_results=partition_validation_results if partition_validation_results else None,
        )

    def _load_with_exchange_pattern(
        self,
        load_config,
        payload: LoadPayload,
        columns: list,
        fq_target: str,
    ) -> LoadResult:
        """Загружает данные с Exchange Pattern (атомарная замена через RENAME).

        Оптимизированный подход БЕЗ CTAS:
        1. Данные уже в staging (table__tmp)
        2. Переименовываем table → table__backup (если существует)
        3. Переименовываем table__tmp → table (ATOMIC SWAP!)
        4. Удаляем table__backup
        """
        from google.cloud.exceptions import NotFound

        tmp_table = ExchangeQueries.get_tmp_table_name(load_config.target_table)  # table__tmp
        backup_table = ExchangeQueries.get_backup_table_name(load_config.target_table)  # table__backup
        table_existed = False

        try:
            # Шаг 1: Проверяем существование целевой таблицы
            table_existed = self._check_table_exists(load_config)

            # Шаг 2: Получаем количество строк из staging (table__tmp уже загружена в target_schema!)
            fq_staging = self._get_fq_table(load_config.target_schema, tmp_table)
            count_query = f"SELECT COUNT(*) AS row_count FROM {fq_staging}"
            result = self.connector.get_records(count_query)
            if result and len(result) > 0:
                inserted = int(result[0].get("row_count", 0)) if result[0].get("row_count") is not None else 0
            else:
                inserted = 0

            # Шаг 3: Добавляем технические колонки если их нет
            from dataclasses import replace

            temp_load_config = replace(load_config, target_table=tmp_table)
            self._ensure_technical_columns(temp_load_config)

            if table_existed:
                # ПУТЬ 1: Таблица СУЩЕСТВУЕТ → Exchange Pattern
                self._log_exchange_start(fq_target)

                # Шаг 4: Переименовываем старую таблицу в backup
                self._rename_table(load_config, load_config.target_table, backup_table)
                self._log_exchange_backup(load_config.target_schema, load_config.target_table, backup_table)
            else:
                # ПУТЬ 2: Таблицы НЕТ → простое RENAME (первая загрузка)
                self._log_full_refresh_create_new(fq_target)

            # Шаг 5: Переименовываем staging → целевая (ATOMIC SWAP!)
            self._rename_table(load_config, tmp_table, load_config.target_table)
            self._log_exchange_swap(load_config.target_schema, load_config.target_table, tmp_table)

            if table_existed:
                # Шаг 6: Удаляем backup таблицу
                self._drop_table(load_config, backup_table)
                self._log_exchange_cleanup(load_config.target_schema, backup_table)

            self._log_exchange_complete(fq_target, inserted)

            # Валидация партиций после загрузки (только для ClickHouse source с партициями)
            # Используем DRY метод из базового класса
            partition_validation_results = self._validate_partitions_clickhouse(
                load_config=load_config,
                payload=payload,
                inserted=inserted,
            )

            if (
                inserted
                and inserted > 0
                and hasattr(load_config, "log_sample_rows")
                and load_config.log_sample_rows > 0
            ):
                self._log_target_sample(fq_target, load_config.log_sample_rows)

            return LoadResult(
                inserted_rows=inserted or 0,
                updated_rows=0,
                total_rows=inserted or 0,
                deleted_lookback_rows=0,
                partition_validation_results=partition_validation_results if partition_validation_results else None,
            )

        except Exception as e:
            # Rollback: восстанавливаем из backup если что-то пошло не так
            try:
                # Если staging таблица осталась в target schema - удаляем её
                try:
                    temp_load_config = replace(load_config, target_table=tmp_table)
                    self._drop_table(temp_load_config, tmp_table)
                except Exception:
                    pass

                # Восстанавливаем из backup (если был)
                if table_existed:
                    try:
                        # Проверяем, существует ли backup
                        backup_exists = False
                        try:
                            backup_table_id = f"{self.connector.project_id}.{load_config.target_schema}.{backup_table}"
                            self.connector.connection.get_table(backup_table_id)
                            backup_exists = True
                        except NotFound:
                            pass

                        if backup_exists:
                            # Если целевая таблица существует, удаляем её
                            try:
                                self._drop_table(load_config, load_config.target_table)
                            except Exception:
                                pass

                            # Восстанавливаем из backup
                            self._rename_table(load_config, backup_table, load_config.target_table)
                            self._log_exchange_rollback(load_config.target_schema, load_config.target_table)
                    except Exception as rollback_error:
                        self._log_exchange_rollback_failed(rollback_error, e)
            except Exception as rollback_error:
                self._log_exchange_rollback_failed(rollback_error, e)

            raise
