"""SQL запросы для XMin state storage."""


class XMinStateQueries:
    """Коллекция SQL запросов для работы с XMin состоянием."""

    @staticmethod
    def merge_state(fq_table: str) -> str:
        """MERGE запрос для upsert состояния xmin.

        Args:
            fq_table: Fully qualified имя таблицы (project.dataset.table)

        Returns:
            SQL запрос с параметрами:
            - @source_schema
            - @source_table
            - @xmin_value
            - @is_initial
            - @wraparound_detected
            - @frozen_xid
        """
        return f"""
        MERGE `{fq_table}` AS tgt
        USING (
            SELECT
                @source_schema AS source_schema,
                @source_table AS source_table,
                @xmin_value AS xmin_value,
                @is_initial AS is_initial,
                @wraparound_detected AS wraparound_detected,
                @frozen_xid AS frozen_xid,
                CURRENT_TIMESTAMP() AS ts
        ) AS src
        ON tgt.source_schema = src.source_schema AND tgt.source_table = src.source_table
        WHEN MATCHED THEN UPDATE SET
            xmin_value = src.xmin_value,
            is_initial = src.is_initial,
            wraparound_detected = src.wraparound_detected,
            frozen_xid = src.frozen_xid,
            __dpone__updated_at = src.ts
        WHEN NOT MATCHED THEN INSERT (
            source_schema,
            source_table,
            xmin_value,
            is_initial,
            wraparound_detected,
            frozen_xid,
            __dpone__loaded_at,
            __dpone__updated_at
        ) VALUES (
            src.source_schema,
            src.source_table,
            src.xmin_value,
            src.is_initial,
            src.wraparound_detected,
            src.frozen_xid,
            src.ts,
            src.ts
        )
        """

    @staticmethod
    def load_state(fq_table: str) -> str:
        """SELECT запрос для загрузки состояния xmin.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @source_schema
            - @source_table
        """
        return f"""
        SELECT
            xmin_value,
            is_initial,
            wraparound_detected,
            frozen_xid,
            __dpone__loaded_at
        FROM `{fq_table}`
        WHERE source_schema = @source_schema AND source_table = @source_table
        LIMIT 1
        """

    @staticmethod
    def delete_state(fq_table: str) -> str:
        """DELETE запрос для удаления состояния xmin.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @source_schema
            - @source_table
        """
        return f"""
        DELETE FROM `{fq_table}`
        WHERE source_schema = @source_schema AND source_table = @source_table
        """
