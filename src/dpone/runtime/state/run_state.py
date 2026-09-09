"""Хранение состояния выполнения ETL процессов."""

from __future__ import annotations

from datetime import datetime

from google.cloud import bigquery

from dpone.runtime.connectors import BigQueryConnector
from dpone.runtime.sql_helpers import RunStateQueries
from dpone.runtime.state.models import RunState, RunStateStatus
from dpone.runtime.state_logging import etl_logger


class RunStateStorage:
    """Хранилище состояния выполнения ETL процессов в BigQuery."""

    def __init__(
        self,
        bigquery_connector: BigQueryConnector,
        dataset: str = "etl_state",
        table: str = "etl_run_state",
    ):
        self.bigquery_connector = bigquery_connector
        self.dataset = dataset
        self.table = table
        self._table_created = False

    @property
    def project_id(self) -> str:
        return self.bigquery_connector.project_id

    @property
    def fq_dataset(self) -> str:
        return f"{self.project_id}.{self.dataset}"

    @property
    def fq_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

    def create_state_table(self) -> None:
        """Создает таблицу для хранения состояния выполнения в BigQuery."""
        # (оптимизация для прокси)
        if self._table_created:
            return

        client = self.bigquery_connector.connection

        # Создаем датасет
        dataset_ref = bigquery.Dataset(f"{self.fq_dataset}")
        try:
            client.create_dataset(dataset_ref, exists_ok=True)
        except Exception:
            pass  # Датасет уже существует

        # Проверяем существование таблицы
        table_ref = bigquery.Table(f"{self.fq_table}")
        if self._table_exists(client, table_ref):
            etl_logger.log_run_state_info(
                "Таблица состояния выполнения уже существует",
                {"Dataset": self.dataset, "Table": self.table, "FullName": self.fq_table},
            )
            self._table_created = True  # Кэшируем результат на время жизни объекта
            return

        # Определяем схему таблицы
        # Главный ключ: (dag_id, execution_date) - уникально идентифицирует выполнение DAG
        table_ref.schema = [
            bigquery.SchemaField("dag_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_schema", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("target_schema", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("load_strategy", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ended_at", "TIMESTAMP"),
            bigquery.SchemaField("duration_min", "FLOAT64"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("rows_read", "INT64"),
            bigquery.SchemaField("rows_written", "INT64"),
            bigquery.SchemaField("rows_updated", "INT64"),
            bigquery.SchemaField("rows_deleted", "INT64"),
            bigquery.SchemaField("__dpone__loaded_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("__dpone__updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        client.create_table(table_ref, exists_ok=True)
        etl_logger.log_run_state_info(
            "Таблица состояния выполнения создана",
            {"Dataset": self.dataset, "Table": self.table, "FullName": self.fq_table},
        )

    def _table_exists(self, client: bigquery.Client, table_ref: bigquery.Table) -> bool:
        """Проверяет существование таблицы - переиспользуемый метод как в XMinStateStorage."""
        try:
            client.get_table(table_ref)
            return True
        except Exception:
            return False

    def _ensure_table_exists(self) -> None:
        """Ленивое создание таблицы - переиспользуемый паттерн как в XMinStateStorage."""
        if self._table_created:
            return
        self.create_state_table()
        self._table_created = True

    def save_run_state(self, run_state: RunState) -> None:
        """Сохраняет или обновляет состояние выполнения."""
        self._ensure_table_exists()

        # UPSERT: главный ключ (dag_id, execution_date)
        upsert_query = RunStateQueries.merge_run_state(self.fq_table)

        params = [
            bigquery.ScalarQueryParameter("dag_id", "STRING", run_state.dag_id),
            bigquery.ScalarQueryParameter("source_schema", "STRING", run_state.source_schema),
            bigquery.ScalarQueryParameter("source_table", "STRING", run_state.source_table),
            bigquery.ScalarQueryParameter("target_schema", "STRING", run_state.target_schema),
            bigquery.ScalarQueryParameter("target_table", "STRING", run_state.target_table),
            bigquery.ScalarQueryParameter("load_strategy", "STRING", run_state.load_strategy),
            bigquery.ScalarQueryParameter("execution_date", "TIMESTAMP", run_state.execution_date),
            bigquery.ScalarQueryParameter("state", "STRING", run_state.state.value),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", run_state.started_at),
            bigquery.ScalarQueryParameter("ended_at", "TIMESTAMP", run_state.ended_at),
            bigquery.ScalarQueryParameter("duration_min", "FLOAT64", run_state.duration_min),
            bigquery.ScalarQueryParameter("error_message", "STRING", run_state.error_message),
            bigquery.ScalarQueryParameter("rows_read", "INT64", run_state.rows_read),
            bigquery.ScalarQueryParameter("rows_written", "INT64", run_state.rows_written),
            bigquery.ScalarQueryParameter("rows_updated", "INT64", run_state.rows_updated),
            bigquery.ScalarQueryParameter("rows_deleted", "INT64", run_state.rows_deleted),
        ]

        self.bigquery_connector.execute_query(upsert_query, params=params)

        etl_logger.log_run_state_info(
            "Состояние выполнения сохранено",
            {
                "DAG ID": run_state.dag_id,
                "Execution Date": run_state.execution_date,
                "State": run_state.state.value,
            },
        )

    def update_run_state(self, run_state: RunState) -> None:
        """Обновляет состояние выполнения."""
        self._ensure_table_exists()

        query = RunStateQueries.update_run_state(self.fq_table)

        params = [
            bigquery.ScalarQueryParameter("state", "STRING", run_state.state.value),
            bigquery.ScalarQueryParameter("ended_at", "TIMESTAMP", run_state.ended_at),
            bigquery.ScalarQueryParameter("duration_min", "FLOAT64", run_state.duration_min),
            bigquery.ScalarQueryParameter("error_message", "STRING", run_state.error_message),
            bigquery.ScalarQueryParameter("rows_read", "INT64", run_state.rows_read),
            bigquery.ScalarQueryParameter("rows_written", "INT64", run_state.rows_written),
            bigquery.ScalarQueryParameter("rows_updated", "INT64", run_state.rows_updated),
            bigquery.ScalarQueryParameter("rows_deleted", "INT64", run_state.rows_deleted),
            bigquery.ScalarQueryParameter("load_strategy", "STRING", run_state.load_strategy),
            bigquery.ScalarQueryParameter("dag_id", "STRING", run_state.dag_id),
            bigquery.ScalarQueryParameter("execution_date", "TIMESTAMP", run_state.execution_date),
        ]

        self.bigquery_connector.execute_query(query, params=params)

        etl_logger.log_run_state_info(
            "Состояние выполнения обновлено",
            {
                "DAG ID": run_state.dag_id,
                "State": run_state.state.value,
            },
        )

    def get_run_state(self, dag_id: str, execution_date: datetime) -> RunState | None:
        """Получает состояние выполнения по dag_id и execution_date."""
        self._ensure_table_exists()

        query = RunStateQueries.get_run_state(self.fq_table)

        params = [
            bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id),
            bigquery.ScalarQueryParameter("execution_date", "TIMESTAMP", execution_date),
        ]

        rows = self.bigquery_connector.get_records(query, params=params, as_dict=True)

        if not rows:
            return None

        row = rows[0]
        return RunState(
            dag_id=row["dag_id"],
            source_schema=row["source_schema"],
            source_table=row["source_table"],
            target_schema=row["target_schema"],
            target_table=row["target_table"],
            load_strategy=row["load_strategy"],
            execution_date=row["execution_date"],
            state=RunStateStatus(row["state"]),
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            duration_min=row.get("duration_min"),
            error_message=row["error_message"],
            rows_read=row.get("rows_read"),
            rows_written=row.get("rows_written"),
            rows_updated=row.get("rows_updated"),
            rows_deleted=row.get("rows_deleted"),
        )

    def get_run_states_by_dag(
        self, dag_id: str, execution_date: datetime | None = None, limit: int = 100
    ) -> list[RunState]:
        """Получает состояния выполнения по DAG ID."""
        self._ensure_table_exists()

        if execution_date:
            query = RunStateQueries.get_run_states_by_dag_with_date(self.fq_table)
            params = [
                bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id),
                bigquery.ScalarQueryParameter("execution_date", "TIMESTAMP", execution_date),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        else:
            query = RunStateQueries.get_run_states_by_dag(self.fq_table)
            params = [
                bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]

        rows = self.bigquery_connector.get_records(query, params=params, as_dict=True)

        return [
            RunState(
                id=row["id"],
                dag_id=row["dag_id"],
                source_schema=row["source_schema"],
                source_table=row["source_table"],
                target_schema=row["target_schema"],
                target_table=row["target_table"],
                load_strategy=row["load_strategy"],
                execution_date=row["execution_date"],
                state=RunStateStatus(row["state"]),
                started_at=row["started_at"],
                ended_at=row["ended_at"],
                duration_min=row.get("duration_min"),
                error_message=row["error_message"],
                rows_read=row.get("rows_read"),
                rows_written=row.get("rows_written"),
                rows_updated=row.get("rows_updated"),
                rows_deleted=row.get("rows_deleted"),
            )
            for row in rows
        ]

    def delete_old_states(self, days_to_keep: int = 30) -> int:
        """Удаляет старые состояния выполнения."""
        self._ensure_table_exists()

        query = RunStateQueries.delete_old_states(self.fq_table)

        params = [bigquery.ScalarQueryParameter("days", "INT64", days_to_keep)]
        deleted_count = self.bigquery_connector.execute_query(query, params=params)

        etl_logger.log_run_state_info(
            "Старые состояния выполнения удалены",
            {
                "Deleted Count": deleted_count,
                "Days to Keep": days_to_keep,
            },
        )

        return deleted_count or 0
