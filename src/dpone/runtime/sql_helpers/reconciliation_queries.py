"""SQL запросы для Reconciliation (отслеживание удалений).

ВАЖНО: Все tech таблицы (__rs и __deleted_log) создаются ТОЛЬКО в BigQuery.
BigQuery выступает как единое централизованное хранилище состояния для всех ETL процессов.
"""


class ReconciliationQueries:
    """Коллекция SQL запросов для reconciliation процесса"""

    @staticmethod
    def bq_prune_old_snapshots(project_id: str, tech_schema: str, rs_table: str) -> str:
        """Удаляет старые снэпшоты, оставляет только 2 последних (BigQuery).

        Оптимизация:
        - Материализация timestamp через CTE
        - Использует DELETE с подзапросом (BigQuery поддерживает WHERE NOT IN)
        - Partition pruning для партиционированных таблиц
        """
        return f"""
        DELETE FROM `{project_id}.{tech_schema}.{rs_table}`
        WHERE __dpone__loaded_at NOT IN (
            SELECT DISTINCT __dpone__loaded_at
            FROM `{project_id}.{tech_schema}.{rs_table}`
            ORDER BY __dpone__loaded_at DESC
            LIMIT 2
        )
        """

    @staticmethod
    def bq_get_snapshot_count(project_id: str, tech_schema: str, rs_table: str) -> str:
        """Получает количество уникальных timestamp в __rs таблице (BigQuery)."""
        return f"""
        SELECT COUNT(DISTINCT __dpone__loaded_at)
        FROM `{project_id}.{tech_schema}.{rs_table}`
        """

    @staticmethod
    def bq_soft_delete_update(
        project_id: str,
        target_schema: str,
        target_table: str,
        tech_schema: str,
        rs_table: str,
        unique_key_columns: list[str],
        meta_delete_dtm: str,
        meta_load_dtm: str,
    ) -> str:
        """Выполняет soft delete для удаленных записей (BigQuery).

        Логика:
        - Находит записи в предпоследнем snapshot, отсутствующие в последнем
        - Эти записи удалены в source → устанавливаем __dpone__deleted_at и __dpone__loaded_at

        Обновляет обе колонки для audit trail:
        - __dpone__deleted_at = CURRENT_TIMESTAMP() (время удаления)
        - __dpone__loaded_at = CURRENT_TIMESTAMP() (время последнего обновления записи)
        """
        # Условие для IN: сравнение tuple ключей
        if len(unique_key_columns) == 1:
            # Одиночный ключ: простое IN
            key_col = unique_key_columns[0]
            target_key = f"COALESCE(CAST(`{key_col}` AS STRING), '__NULL__')"
            snapshot_key_select = f"COALESCE(`{key_col}`, '__NULL__')"
        else:
            # Составной ключ: IN с STRUCT
            target_key = (
                "STRUCT("
                + ", ".join([f"COALESCE(CAST(`{k}` AS STRING), '__NULL__') AS `{k}`" for k in unique_key_columns])
                + ")"
            )
            snapshot_key_select = (
                "STRUCT(" + ", ".join([f"COALESCE(`{k}`, '__NULL__') AS `{k}`" for k in unique_key_columns]) + ")"
            )

        # ANTI JOIN для curr snapshot
        key_anti_join = " AND ".join(
            [f"COALESCE(prev.`{k}`, '__NULL__') = COALESCE(curr.`{k}`, '__NULL__')" for k in unique_key_columns]
        )

        return f"""
        UPDATE `{project_id}.{target_schema}.{target_table}`
        SET `{meta_delete_dtm}` = CURRENT_TIMESTAMP(),
            `{meta_load_dtm}` = CURRENT_TIMESTAMP()
        WHERE `{meta_delete_dtm}` IS NULL
          AND {target_key} IN (
              SELECT {snapshot_key_select}
              FROM `{project_id}.{tech_schema}.{rs_table}` prev
              WHERE prev.__dpone__loaded_at = (
                  SELECT DISTINCT __dpone__loaded_at
                  FROM `{project_id}.{tech_schema}.{rs_table}`
                  ORDER BY __dpone__loaded_at DESC
                  LIMIT 1 OFFSET 1
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM `{project_id}.{tech_schema}.{rs_table}` curr
                  WHERE curr.__dpone__loaded_at = (
                      SELECT DISTINCT __dpone__loaded_at
                      FROM `{project_id}.{tech_schema}.{rs_table}`
                      ORDER BY __dpone__loaded_at DESC
                      LIMIT 1
                  )
                  AND {key_anti_join}
              )
          )
        """

    @staticmethod
    def bq_get_deleted_keys(
        project_id: str, tech_schema: str, rs_table: str, unique_key_columns: list[str], meta_load_dtm: str
    ) -> str:
        """Получает удаленные ключи из BigQuery __rs (ANTI JOIN 2 последних снэпшотов).

        Логика:
        - Находит записи в предпоследнем snapshot, отсутствующие в последнем
        - Возвращает unique_key колонки для soft delete в target таблице

        Returns:
            SELECT unique_key колонок для удаленных записей
        """
        key_columns = ", ".join([f"prev.`{k}`" for k in unique_key_columns])

        # ANTI JOIN для поиска удаленных ключей
        key_anti_join = " AND ".join(
            [f"COALESCE(prev.`{k}`, '__NULL__') = COALESCE(curr.`{k}`, '__NULL__')" for k in unique_key_columns]
        )

        return f"""
        SELECT {key_columns}
        FROM `{project_id}.{tech_schema}.{rs_table}` prev
        WHERE prev.`{meta_load_dtm}` = (
            SELECT DISTINCT `{meta_load_dtm}`
            FROM `{project_id}.{tech_schema}.{rs_table}`
            ORDER BY `{meta_load_dtm}` DESC
            LIMIT 1 OFFSET 1
        )
        AND NOT EXISTS (
            SELECT 1
            FROM `{project_id}.{tech_schema}.{rs_table}` curr
            WHERE curr.`{meta_load_dtm}` = (
                SELECT DISTINCT `{meta_load_dtm}`
                FROM `{project_id}.{tech_schema}.{rs_table}`
                ORDER BY `{meta_load_dtm}` DESC
                LIMIT 1
            )
            AND {key_anti_join}
        )
        """

    @staticmethod
    def bq_insert_deleted_log(
        project_id: str, tech_schema: str, deleted_log_table: str, rs_table: str, unique_key_columns: list[str]
    ) -> str:
        """Логирует удаленные записи в __deleted_log таблицу (BigQuery).

        Логика:
        - Находит записи из предпоследнего snapshot, отсутствующие в последнем
        - Добавляет их в лог удалений с дедупликацией через NOT EXISTS
        """
        deleted_log_columns = ", ".join(
            [f"`{k}`" for k in unique_key_columns] + ["`__dpone__loaded_at`", "`__dpone__deleted_at`"]
        )

        # ANTI JOIN для поиска удаленных ключей (оба STRING)
        key_anti_join = " AND ".join(
            [f"COALESCE(prev.`{k}`, '__NULL__') = COALESCE(curr.`{k}`, '__NULL__')" for k in unique_key_columns]
        )

        # Условия для дедупликации
        dedup_conditions = " AND ".join(
            [f"COALESCE(d.`{k}`, '__NULL__') = COALESCE(prev.`{k}`, '__NULL__')" for k in unique_key_columns]
        )

        return f"""
        INSERT INTO `{project_id}.{tech_schema}.{deleted_log_table}`
            ({deleted_log_columns})
        SELECT {", ".join([f"prev.`{k}`" for k in unique_key_columns])},
               prev.__dpone__loaded_at,
               CURRENT_TIMESTAMP() AS __dpone__deleted_at
        FROM `{project_id}.{tech_schema}.{rs_table}` prev
        WHERE prev.__dpone__loaded_at = (
            SELECT DISTINCT __dpone__loaded_at
            FROM `{project_id}.{tech_schema}.{rs_table}`
            ORDER BY __dpone__loaded_at DESC
            LIMIT 1 OFFSET 1
        )
        AND NOT EXISTS (
            SELECT 1
            FROM `{project_id}.{tech_schema}.{rs_table}` curr
            WHERE curr.__dpone__loaded_at = (
                SELECT DISTINCT __dpone__loaded_at
                FROM `{project_id}.{tech_schema}.{rs_table}`
                ORDER BY __dpone__loaded_at DESC
                LIMIT 1
            )
            AND {key_anti_join}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM `{project_id}.{tech_schema}.{deleted_log_table}` d
            WHERE {dedup_conditions}
              AND d.__dpone__loaded_at = prev.__dpone__loaded_at
        )
        """
