"""SQL запросы для Exchange Pattern (атомарная замена таблиц).

Exchange Pattern - это метод атомарной замены таблицы с zero downtime:
1. Переименовываем старую таблицу в backup
2. Создаем новую таблицу
3. Удаляем backup
"""

from __future__ import annotations


class ExchangeQueries:
    """SQL запросы для Exchange Pattern."""

    # ===== PostgreSQL Queries =====

    @staticmethod
    def pg_check_table_exists() -> str:
        """SQL для проверки существования таблицы (PostgreSQL).

        Returns:
            SQL запрос для проверки существования таблицы.
            Используйте с параметрами: (schema, table)
        """
        return """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """

    @staticmethod
    def pg_rename_table(schema: str, from_table: str, to_table: str) -> str:
        """SQL для переименования таблицы (PostgreSQL).

        Args:
            schema: Схема таблицы
            from_table: Текущее имя таблицы
            to_table: Новое имя таблицы

        Returns:
            SQL для ALTER TABLE ... RENAME TO ...
        """
        return f"ALTER TABLE {schema}.{{}} RENAME TO {{}}"

    @staticmethod
    def pg_create_table_as(schema: str, table: str, query: str) -> str:
        """SQL для создания таблицы из запроса (PostgreSQL).

        Args:
            schema: Схема таблицы
            table: Имя таблицы
            query: SELECT запрос для заполнения таблицы

        Returns:
            SQL для CREATE TABLE ... AS ...
        """
        return f"CREATE TABLE {schema}.{{}} AS {{}}"

    @staticmethod
    def pg_drop_table(schema: str, table: str) -> str:
        """SQL для удаления таблицы (PostgreSQL).

        Args:
            schema: Схема таблицы
            table: Имя таблицы

        Returns:
            SQL для DROP TABLE IF EXISTS
        """
        return f"DROP TABLE IF EXISTS {schema}.{{}}"

    @staticmethod
    def get_backup_table_name(table: str) -> str:
        """Возвращает имя backup таблицы.

        Args:
            table: Имя оригинальной таблицы

        Returns:
            Имя backup таблицы (table__backup)
        """
        return f"{table}__backup"

    @staticmethod
    def get_tmp_table_name(table: str) -> str:
        """Возвращает имя временной таблицы.

        Args:
            table: Имя оригинальной таблицы

        Returns:
            Имя временной таблицы (table__tmp)
        """
        return f"{table}__tmp"

    # ===== BigQuery Queries =====

    @staticmethod
    def bq_check_table_exists(project_id: str, dataset: str, table: str) -> str:
        """SQL для проверки существования таблицы (BigQuery).

        Args:
            project_id: GCP project ID
            dataset: Dataset (схема)
            table: Имя таблицы

        Returns:
            SQL запрос для проверки существования таблицы.
        """
        return f"""
        SELECT COUNT(*) > 0
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{table}'
        """

    @staticmethod
    def bq_rename_table(project_id: str, dataset: str, from_table: str, to_table: str) -> str:
        """SQL для переименования таблицы (BigQuery).

        Args:
            project_id: GCP project ID
            dataset: Dataset (схема)
            from_table: Текущее имя таблицы
            to_table: Новое имя таблицы

        Returns:
            SQL для ALTER TABLE ... RENAME TO ...
        """
        return f"ALTER TABLE `{project_id}.{dataset}.{from_table}` RENAME TO `{to_table}`"

    @staticmethod
    def bq_drop_table(project_id: str, dataset: str, table: str) -> str:
        """SQL для удаления таблицы (BigQuery).

        Args:
            project_id: GCP project ID
            dataset: Dataset (схема)
            table: Имя таблицы

        Returns:
            SQL для DROP TABLE IF EXISTS
        """
        return f"DROP TABLE IF EXISTS `{project_id}.{dataset}.{table}`"
