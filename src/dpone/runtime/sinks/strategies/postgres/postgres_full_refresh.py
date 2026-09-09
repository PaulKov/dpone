"""Стратегия полной перезагрузки для PostgreSQL."""

from __future__ import annotations

from psycopg import sql

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase


class PostgresFullRefreshStrategy(PostgresStrategyBase):
    """Стратегия FULL_REFRESH для PostgreSQL.

    Поведение (overwrite_type=None или 'truncate_insert'):
    1. Если таблица не существует - создает её
    2. Если существует - очищает (TRUNCATE)
    3. Вставляет данные из staging

    Поведение (overwrite_type='exchange'):
    1. Создается staging таблица с данными
    2. Если целевая таблица существует - переименовывается в {table}__backup
    3. Staging переименовывается в целевую таблицу (атомарный swap)
    4. Удаляется backup таблица
    """

    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с полной перезагрузкой."""
        # Проверяем режим overwrite_type
        overwrite_type = getattr(load_config, "overwrite_type", None)

        # DEBUG: Логируем режим загрузки
        mode_display = {
            "exchange": "Exchange Pattern",
            "truncate_insert": "TRUNCATE + INSERT",
            None: "TRUNCATE + INSERT (default)",
        }.get(overwrite_type, "TRUNCATE + INSERT (default)")

        self.logger.log_etl_progress(
            "PG_FULL_REFRESH_MODE",
            {
                "Target": f"{load_config.target_schema}.{load_config.target_table}",
                "OverwriteType": overwrite_type or "truncate_insert (default)",
                "Mode": mode_display,
            },
        )

        if overwrite_type == "exchange":
            # Exchange Pattern: атомарная замена через RENAME
            return self._load_with_exchange_pattern(load_config, payload)
        else:
            # Стандартный режим: TRUNCATE + INSERT
            return self._load_with_truncate(load_config, payload)

    def _load_with_truncate(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с TRUNCATE + INSERT (стандартный режим)."""

        def handler(staging):
            # 1. Создаем target таблицу если не существует
            target_created = self._ensure_target_table(load_config, payload.schema)

            # 2. Очищаем таблицу (TRUNCATE) если она уже существовала
            if not target_created:
                truncate_sql = sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(load_config.target_schema),
                    sql.Identifier(load_config.target_table),
                )
                self.connector.execute_query(truncate_sql)

                self.logger.log_etl_progress(
                    "PG_TARGET_TRUNCATED",
                    {
                        "Target": f"{load_config.target_schema}.{load_config.target_table}",
                        "Reason": "Full refresh (overwrite_type=truncate_insert)",
                    },
                )

            # 3. Вставляем данные из staging
            inserted = self._insert_from_staging(load_config, staging, payload.schema)

            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=inserted,
            )

        return self._consume_with_staging(load_config, payload, handler)

    def _load_with_exchange_pattern(self, load_config, payload: LoadPayload) -> LoadResult:
        """Загружает данные с Exchange Pattern (атомарная замена через RENAME).

        Алгоритм:
        1. Staging таблица уже создана и заполнена данными
        2. Переименовываем target → target__backup (если существует)
        3. Переименовываем staging → target (атомарный swap)
        4. Удаляем backup таблицу
        """
        from dpone.runtime.sql_helpers import ExchangeQueries

        def handler(staging):
            target_schema = load_config.target_schema
            target_table = load_config.target_table
            staging_schema = staging.schema
            staging_table = staging.table
            backup_table = ExchangeQueries.get_backup_table_name(target_table)

            self.connector.begin()
            try:
                # Шаг 1: Проверяем существование target таблицы
                check_sql = sql.SQL(ExchangeQueries.pg_check_table_exists())
                exists = self.connector.get_records(check_sql, (target_schema, target_table))
                table_existed = exists and exists[0][0]

                if table_existed:
                    # ПУТЬ 1: Target существует → Exchange Pattern
                    self.logger.log_etl_progress(
                        "PG_EXCHANGE_START",
                        {
                            "Target": f"{target_schema}.{target_table}",
                            "Staging": f"{staging_schema}.{staging_table}",
                            "Mode": "Exchange Pattern (атомарная замена)",
                        },
                    )

                    # Переименовываем target → backup
                    rename_to_backup_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(target_table),
                        sql.Identifier(backup_table),
                    )
                    self.connector.execute_query(rename_to_backup_sql)

                    self.logger.log_etl_progress(
                        "PG_EXCHANGE_BACKUP",
                        {
                            "Original": f"{target_schema}.{target_table}",
                            "Backup": f"{target_schema}.{backup_table}",
                        },
                    )
                else:
                    # ПУТЬ 2: Target не существует → первая загрузка
                    self.logger.log_etl_progress(
                        "PG_EXCHANGE_FIRST_LOAD",
                        {
                            "Target": f"{target_schema}.{target_table}",
                            "Staging": f"{staging_schema}.{staging_table}",
                            "Mode": "First load (no backup needed)",
                        },
                    )

                # Шаг 2: Переименовываем staging → target (атомарный swap)
                if staging_schema != target_schema:
                    set_schema_sql = sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(
                        sql.Identifier(staging_schema),
                        sql.Identifier(staging_table),
                        sql.Identifier(target_schema),
                    )
                    self.connector.execute_query(set_schema_sql)

                    # Затем переименовываем (теперь staging в target_schema)
                    rename_staging_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(staging_table),
                        sql.Identifier(target_table),
                    )
                    self.connector.execute_query(rename_staging_sql)
                else:
                    # Staging уже в нужной схеме, просто переименовываем
                    rename_staging_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(staging_schema),
                        sql.Identifier(staging_table),
                        sql.Identifier(target_table),
                    )
                    self.connector.execute_query(rename_staging_sql)

                self.logger.log_etl_progress(
                    "PG_EXCHANGE_SWAP",
                    {
                        "Staging": f"{staging_schema}.{staging_table}",
                        "Target": f"{target_schema}.{target_table}",
                        "Status": "Swapped (atomic)",
                    },
                )

                # Шаг 3: Удаляем backup таблицу (если была)
                if table_existed:
                    drop_backup_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(backup_table),
                    )
                    self.connector.execute_query(drop_backup_sql)

                    self.logger.log_etl_progress(
                        "PG_EXCHANGE_CLEANUP",
                        {
                            "Backup": f"{target_schema}.{backup_table}",
                            "Status": "Dropped",
                        },
                    )

                # Получаем количество строк в новой target таблице
                count_sql = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(target_table),
                )
                result = self.connector.get_records(count_sql)
                inserted = result[0][0] if result else 0

                self.logger.log_etl_progress(
                    "PG_EXCHANGE_COMPLETE",
                    {
                        "Target": f"{target_schema}.{target_table}",
                        "Rows": inserted,
                        "Mode": "Exchange Pattern",
                    },
                )

                self.connector.commit_transaction()

                # Логируем sample если нужно
                if inserted > 0 and hasattr(load_config, "log_sample_rows") and load_config.log_sample_rows > 0:
                    self._log_target_sample(load_config, load_config.log_sample_rows)

                return LoadResult(
                    inserted_rows=inserted,
                    updated_rows=0,
                    total_rows=inserted,
                    staging_rows=inserted,
                )

            except Exception as e:
                # Rollback: восстанавливаем из backup если что-то пошло не так
                try:
                    self.connector.rollback()

                    # Если swap успел произойти, откатываем
                    if table_existed:
                        # Проверяем, существует ли target (может быть уже переименован)
                        check_new_target = self.connector.get_records(check_sql, (target_schema, target_table))
                        new_target_exists = check_new_target and check_new_target[0][0]

                        if new_target_exists:
                            # Target существует (staging был переименован), удаляем его
                            drop_new_target_sql = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                                sql.Identifier(target_schema),
                                sql.Identifier(target_table),
                            )
                            self.connector.execute_query(drop_new_target_sql)

                        # Восстанавливаем из backup
                        restore_sql = sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            sql.Identifier(target_schema),
                            sql.Identifier(backup_table),
                            sql.Identifier(target_table),
                        )
                        self.connector.execute_query(restore_sql)

                        self.logger.log_etl_progress(
                            "PG_EXCHANGE_ROLLBACK",
                            {
                                "Target": f"{target_schema}.{target_table}",
                                "Status": "Restored from backup",
                                "Error": str(e),
                            },
                        )
                except Exception as rollback_error:
                    self.logger.log_etl_progress(
                        "PG_EXCHANGE_ROLLBACK_FAILED",
                        {
                            "Error": str(rollback_error),
                            "Original_Error": str(e),
                        },
                    )

                raise

        return self._consume_with_staging(load_config, payload, handler)
