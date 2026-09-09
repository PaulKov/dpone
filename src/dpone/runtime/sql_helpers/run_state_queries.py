"""SQL запросы для Run state storage."""


class RunStateQueries:
    """Коллекция SQL запросов для работы с состоянием выполнения ETL."""

    @staticmethod
    def merge_run_state(fq_table: str) -> str:
        """MERGE запрос для upsert состояния выполнения.

        Args:
            fq_table: Fully qualified имя таблицы (project.dataset.table)

        Returns:
            SQL запрос с параметрами:
            - @dag_id
            - @source_schema
            - @source_table
            - @target_schema
            - @target_table
            - @load_strategy
            - @execution_date
            - @state
            - @started_at
            - @ended_at
            - @duration_min
            - @error_message
            - @rows_read
            - @rows_written
            - @rows_updated
            - @rows_deleted
        """
        return f"""
        MERGE `{fq_table}` AS tgt
        USING (
            SELECT
                @dag_id AS dag_id,
                @source_schema AS source_schema,
                @source_table AS source_table,
                @target_schema AS target_schema,
                @target_table AS target_table,
                @load_strategy AS load_strategy,
                @execution_date AS execution_date,
                @state AS state,
                @started_at AS started_at,
                @ended_at AS ended_at,
                @duration_min AS duration_min,
                @error_message AS error_message,
                @rows_read AS rows_read,
                @rows_written AS rows_written,
                @rows_updated AS rows_updated,
                @rows_deleted AS rows_deleted,
                CURRENT_TIMESTAMP() AS ts
        ) AS src
        ON tgt.dag_id = src.dag_id AND tgt.execution_date = src.execution_date
        WHEN MATCHED THEN UPDATE SET
            state = src.state,
            started_at = src.started_at,
            ended_at = src.ended_at,
            duration_min = src.duration_min,
            error_message = src.error_message,
            rows_read = src.rows_read,
            rows_written = src.rows_written,
            rows_updated = src.rows_updated,
            rows_deleted = src.rows_deleted,
            __dpone__updated_at = src.ts
        WHEN NOT MATCHED THEN INSERT (
            dag_id, source_schema, source_table, target_schema, target_table,
            load_strategy, execution_date, state, started_at, ended_at,
            duration_min, error_message, rows_read, rows_written, rows_updated,
            rows_deleted, __dpone__loaded_at, __dpone__updated_at
        ) VALUES (
            src.dag_id, src.source_schema, src.source_table, src.target_schema,
            src.target_table, src.load_strategy, src.execution_date, src.state,
            src.started_at, src.ended_at, src.duration_min, src.error_message,
            src.rows_read, src.rows_written, src.rows_updated, src.rows_deleted,
            src.ts, src.ts
        )
        """

    @staticmethod
    def update_run_state(fq_table: str) -> str:
        """UPDATE запрос для обновления состояния выполнения.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @state
            - @ended_at
            - @duration_min
            - @error_message
            - @rows_read
            - @rows_written
            - @rows_updated
            - @rows_deleted
            - @load_strategy
            - @dag_id
            - @execution_date
        """
        return f"""
        UPDATE `{fq_table}` SET
            state = @state,
            ended_at = @ended_at,
            duration_min = @duration_min,
            error_message = @error_message,
            rows_read = @rows_read,
            rows_written = @rows_written,
            rows_updated = @rows_updated,
            rows_deleted = @rows_deleted,
            load_strategy = @load_strategy,
            __dpone__updated_at = CURRENT_TIMESTAMP()
        WHERE dag_id = @dag_id AND execution_date = @execution_date
        """

    @staticmethod
    def get_run_state(fq_table: str) -> str:
        """SELECT запрос для получения состояния выполнения.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @dag_id
            - @execution_date
        """
        return f"""
        SELECT * FROM `{fq_table}`
        WHERE dag_id = @dag_id AND execution_date = @execution_date
        LIMIT 1
        """

    @staticmethod
    def get_run_states_by_dag_with_date(fq_table: str) -> str:
        """SELECT запрос для получения состояний по DAG ID и execution_date.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @dag_id
            - @execution_date
            - @limit
        """
        return f"""
        SELECT * FROM `{fq_table}`
        WHERE dag_id = @dag_id AND execution_date = @execution_date
        ORDER BY started_at DESC
        LIMIT @limit
        """

    @staticmethod
    def get_run_states_by_dag(fq_table: str) -> str:
        """SELECT запрос для получения состояний по DAG ID.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @dag_id
            - @limit
        """
        return f"""
        SELECT * FROM `{fq_table}`
        WHERE dag_id = @dag_id
        ORDER BY started_at DESC
        LIMIT @limit
        """

    @staticmethod
    def delete_old_states(fq_table: str) -> str:
        """DELETE запрос для удаления старых состояний.

        Args:
            fq_table: Fully qualified имя таблицы

        Returns:
            SQL запрос с параметрами:
            - @days
        """
        return f"""
        DELETE FROM `{fq_table}`
        WHERE started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        """
