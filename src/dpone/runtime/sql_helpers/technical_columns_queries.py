"""SQL запросы для работы с техническими колонками (__dpone__loaded_at, __dpone__deleted_at).

Технические колонки добавляются ВСЕГДА для всех таблиц:
- __dpone__loaded_at: дата и время загрузки/обновления записи (NOT NULL)
- __dpone__deleted_at: дата и время удаления записи (NULL = активная, NOT NULL = удалена)

Эти колонки используются для:
1. Аудита данных (когда запись была загружена)
2. Reconciliation (отслеживание удаленных записей через soft delete)
3. Временных срезов данных (as-of queries)
"""

from __future__ import annotations


class TechnicalColumnsQueries:
    """SQL запросы для работы с техническими колонками __dpone__loaded_at и __dpone__deleted_at."""

    # ===== PostgreSQL Queries =====

    @staticmethod
    def pg_check_columns_exist(
        schema: str,
        table: str,
    ) -> str:
        """SQL для проверки существования технических колонок (PostgreSQL).

        Args:
            schema: Схема таблицы
            table: Имя таблицы

        Returns:
            SQL запрос для проверки существования __dpone__loaded_at и __dpone__deleted_at

        Note:
            Запрос возвращает column_name для каждой найденной технической колонки.
            Используйте с параметрами: (schema, table, '__dpone__loaded_at', '__dpone__deleted_at')
        """
        return """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name IN (%s, %s)
        """

    @staticmethod
    def pg_add_load_dtm_column(
        schema: str,
        table: str,
    ) -> str:
        """SQL для добавления колонки __dpone__loaded_at (PostgreSQL).

        Создает колонку с DEFAULT CURRENT_TIMESTAMP.

        Args:
            schema: Схема таблицы
            table: Имя таблицы

        Returns:
            SQL запрос с использованием {} placeholders для sql.Identifier
        """
        return f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "__dpone__loaded_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP'

    @staticmethod
    def pg_add_delete_dtm_column(
        schema: str,
        table: str,
    ) -> str:
        """SQL для добавления колонки __dpone__deleted_at (PostgreSQL).

        Создает nullable колонку.

        Args:
            schema: Схема таблицы
            table: Имя таблицы

        Returns:
            SQL запрос с использованием {} placeholders для sql.Identifier
        """
        return (
            f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "__dpone__deleted_at" TIMESTAMP WITH TIME ZONE'
        )

    @staticmethod
    def pg_update_load_dtm_null_rows(
        schema: str,
        table: str,
    ) -> str:
        """SQL для заполнения NULL значений в __dpone__loaded_at (PostgreSQL).

        Обновляет существующие строки где __dpone__loaded_at IS NULL.

        Args:
            schema: Схема таблицы
            table: Имя таблицы

        Returns:
            SQL запрос для UPDATE
        """
        return f'UPDATE "{schema}"."{table}" SET "__dpone__loaded_at" = CURRENT_TIMESTAMP WHERE "__dpone__loaded_at" IS NULL'

    @staticmethod
    def pg_build_insert_with_technical_columns(
        target_schema: str,
        target_table: str,
        staging_schema: str,
        staging_table: str,
        data_columns: list[str],
    ) -> str:
        """Строит INSERT с заполнением технических колонок (PostgreSQL).

        Генерирует SQL:
        INSERT INTO target (col1, col2, ..., __dpone__loaded_at, __dpone__deleted_at)
        SELECT col1, col2, ..., CURRENT_TIMESTAMP, NULL
        FROM staging

        Args:
            target_schema: Схема целевой таблицы
            target_table: Целевая таблица
            staging_schema: Схема staging таблицы
            staging_table: Staging таблица
            data_columns: Список колонок данных (без технических)

        Returns:
            SQL запрос для INSERT с техническими колонками
        """
        # Формируем список всех колонок (данные + технические)
        all_columns = data_columns + ["__dpone__loaded_at", "__dpone__deleted_at"]
        columns_str = ", ".join([f'"{col}"' for col in all_columns])

        # Формируем SELECT: колонки данных + CURRENT_TIMESTAMP + NULL
        select_parts = [f'"{col}"' for col in data_columns]
        select_parts.append("CURRENT_TIMESTAMP")  # __dpone__loaded_at
        select_parts.append("NULL")  # __dpone__deleted_at
        select_str = ", ".join(select_parts)

        return f"""
        INSERT INTO "{target_schema}"."{target_table}" ({columns_str})
        SELECT {select_str}
        FROM "{staging_schema}"."{staging_table}"
        """

    @staticmethod
    def pg_build_select_with_technical_columns(
        data_columns: list[str],
        table_alias: str = "",
    ) -> tuple[str, str]:
        """Строит SELECT с техническими колонками для PostgreSQL.

        Args:
            data_columns: Список колонок данных
            table_alias: Алиас таблицы (опционально)

        Returns:
            Tuple[columns_str, select_str]: Строки для INSERT и SELECT
        """
        prefix = f"{table_alias}." if table_alias else ""

        # Все колонки (данные + технические)
        all_columns = data_columns + ["__dpone__loaded_at", "__dpone__deleted_at"]
        columns_str = ", ".join([f'"{col}"' for col in all_columns])

        # SELECT: колонки данных + CURRENT_TIMESTAMP + NULL
        select_parts = [f'{prefix}"{col}"' for col in data_columns]
        select_parts.append("CURRENT_TIMESTAMP")
        select_parts.append("NULL")
        select_str = ", ".join(select_parts)

        return (columns_str, select_str)

    # ===== BigQuery Queries =====

    @staticmethod
    def bq_update_load_dtm_null_rows(
        project_id: str,
        schema: str,
        table: str,
    ) -> str:
        """SQL для заполнения NULL значений в __dpone__loaded_at (BigQuery).

        Обновляет существующие строки где __dpone__loaded_at IS NULL.

        Args:
            project_id: BigQuery project ID
            schema: Схема (dataset) таблицы
            table: Имя таблицы

        Returns:
            SQL запрос для UPDATE
        """
        return f"""
        UPDATE `{project_id}.{schema}.{table}`
        SET `__dpone__loaded_at` = CURRENT_TIMESTAMP()
        WHERE `__dpone__loaded_at` IS NULL
        """

    @staticmethod
    def bq_build_insert_with_technical_columns(
        project_id: str,
        target_schema: str,
        target_table: str,
        staging_schema: str,
        staging_table: str,
        data_columns: list[str],
        json_columns: list[str] = None,
    ) -> str:
        """Строит INSERT с заполнением технических колонок (BigQuery).

        Генерирует SQL:
        INSERT INTO target (col1, col2, ..., __dpone__loaded_at, __dpone__deleted_at)
        SELECT col1, PARSE_JSON(json_col), ..., CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP)
        FROM staging

        Args:
            project_id: BigQuery project ID
            target_schema: Схема (dataset) целевой таблицы
            target_table: Целевая таблица
            staging_schema: Схема (dataset) staging таблицы
            staging_table: Staging таблица
            data_columns: Список колонок данных (без технических)
            json_columns: Список JSON колонок требующих PARSE_JSON() (опционально)

        Returns:
            SQL запрос для INSERT с техническими колонками
        """
        json_columns = json_columns or []

        # Формируем список всех колонок
        all_columns = data_columns + ["__dpone__loaded_at", "__dpone__deleted_at"]
        columns_str = ", ".join([f"`{col}`" for col in all_columns])

        # Формируем SELECT с обработкой JSON
        select_parts = []
        for col in data_columns:
            if col in json_columns:
                select_parts.append(f"PARSE_JSON(`{col}`)")
            else:
                select_parts.append(f"`{col}`")

        select_parts.append("CURRENT_TIMESTAMP()")  # __dpone__loaded_at
        select_parts.append("CAST(NULL AS TIMESTAMP)")  # __dpone__deleted_at
        select_str = ", ".join(select_parts)

        return f"""
        INSERT INTO `{project_id}.{target_schema}.{target_table}` ({columns_str})
        SELECT {select_str}
        FROM `{project_id}.{staging_schema}.{staging_table}`
        """

    @staticmethod
    def bq_build_select_with_technical_columns(
        data_columns: list[str],
        json_columns: list[str] = None,
        table_alias: str = "",
    ) -> tuple[str, str]:
        """Строит SELECT с техническими колонками для BigQuery.

        Args:
            data_columns: Список колонок данных
            json_columns: Список JSON колонок требующих PARSE_JSON()
            table_alias: Алиас таблицы (опционально)

        Returns:
            Tuple[columns_str, select_str]: Строки для INSERT и SELECT
        """
        json_columns = json_columns or []
        prefix = f"{table_alias}." if table_alias else ""

        # Добавляем технические колонки только если их еще нет
        tech_columns_to_add = []
        if "__dpone__loaded_at" not in data_columns:
            tech_columns_to_add.append("__dpone__loaded_at")
        if "__dpone__deleted_at" not in data_columns:
            tech_columns_to_add.append("__dpone__deleted_at")

        # Все колонки
        all_columns = data_columns + tech_columns_to_add
        columns_str = ", ".join([f"`{col}`" for col in all_columns])

        # SELECT с обработкой JSON
        select_parts = []
        for col in data_columns:
            if col in json_columns:
                # Для JSON колонок: PARSE_JSON() AS column_name (алиас обязателен для CREATE TABLE AS SELECT)
                select_parts.append(f"PARSE_JSON({prefix}`{col}`) AS `{col}`")
            else:
                select_parts.append(f"{prefix}`{col}`")

        # Добавляем значения для технических колонок (только те которых нет в data_columns)
        # ⚠️ ВАЖНО: Алиасы обязательны для CREATE TABLE AS SELECT в BigQuery
        if "__dpone__loaded_at" not in data_columns:
            select_parts.append("CURRENT_TIMESTAMP() AS `__dpone__loaded_at`")
        if "__dpone__deleted_at" not in data_columns:
            select_parts.append("CAST(NULL AS TIMESTAMP) AS `__dpone__deleted_at`")

        select_str = ", ".join(select_parts)

        return (columns_str, select_str)

    # ===== Универсальные утилиты =====

    @staticmethod
    def get_technical_column_names() -> list[str]:
        """Возвращает список имен технических колонок.

        Returns:
            ["__dpone__loaded_at", "__dpone__deleted_at"]
        """
        return ["__dpone__loaded_at", "__dpone__deleted_at"]

    @staticmethod
    def get_technical_column_definitions_pg() -> list[tuple[str, str]]:
        """Возвращает определения технических колонок для PostgreSQL.

        Returns:
            [("__dpone__loaded_at", "TIMESTAMP WITH TIME ZONE"),
             ("__dpone__deleted_at", "TIMESTAMP WITH TIME ZONE")]
        """
        return [
            ("__dpone__loaded_at", "TIMESTAMP WITH TIME ZONE"),
            ("__dpone__deleted_at", "TIMESTAMP WITH TIME ZONE"),
        ]

    @staticmethod
    def get_technical_column_definitions_bq() -> list[tuple[str, str]]:
        """Возвращает определения технических колонок для BigQuery.

        Returns:
            [("__dpone__loaded_at", "TIMESTAMP"),
             ("__dpone__deleted_at", "TIMESTAMP")]
        """
        return [
            ("__dpone__loaded_at", "TIMESTAMP"),
            ("__dpone__deleted_at", "TIMESTAMP"),
        ]
