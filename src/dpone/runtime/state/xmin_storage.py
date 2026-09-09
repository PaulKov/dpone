"""Хранение состояния xmin в BigQuery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from dpone.runtime.sql_helpers import XMinStateQueries
from dpone.runtime.state_logging import etl_logger

if TYPE_CHECKING:
    from google.cloud import bigquery

    from dpone.runtime.connectors import BigQueryConnector


def _require_bigquery():
    try:
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in minimal envs
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for xmin state storage. Install the 'gcp' extra."
        ) from exc
    return bigquery


@dataclass
class XMinState:
    xmin_value: int
    timestamp: datetime
    is_initial: bool = False
    wraparound_detected: bool = False
    frozen_xid: int | None = None
    revision: int = 0


class XMinStateStorage:
    def __init__(
        self,
        bigquery_connector: BigQueryConnector,
        dataset: str = "etl_state",
        table: str = "etl_xmin_state",
    ) -> None:
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
        # Если уже проверяли в этой сессии - пропускаем
        if self._table_created:
            return

        bigquery = _require_bigquery()
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
            etl_logger.log_xmin_state_info(
                "Таблица состояния уже существует",
                {"Dataset": self.dataset, "Table": self.table, "FullName": self.fq_table},
            )
            self._table_created = True  # Кэшируем результат на время жизни объекта
            return

        table_ref.schema = [
            bigquery.SchemaField("source_schema", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("xmin_value", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("is_initial", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("wraparound_detected", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("frozen_xid", "INT64"),
            bigquery.SchemaField("__dpone__loaded_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("__dpone__updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        client.create_table(table_ref, exists_ok=True)
        etl_logger.log_xmin_state_info(
            "Таблица состояния создана",
            {"Dataset": self.dataset, "Table": self.table, "FullName": self.fq_table},
        )

    def _table_exists(self, client: bigquery.Client, table_ref: bigquery.Table) -> bool:
        try:
            client.get_table(table_ref)
            return True
        except Exception:
            return False

    def _ensure_table_exists(self) -> None:
        """Ленивое создание таблицы - вызывается только при необходимости."""
        if self._table_created:
            return
        self.create_state_table()
        self._table_created = True

    def save_state(self, source_schema: str, source_table: str, xmin_state: XMinState) -> None:
        self._ensure_table_exists()
        bigquery = _require_bigquery()
        epoch_mod = 2**32
        etl_logger.log_xmin_state_info(
            "Сохраняем состояние",
            {
                "Epoch": xmin_state.xmin_value // epoch_mod,
                "Low32": xmin_state.xmin_value % epoch_mod,
                "Full": xmin_state.xmin_value,
            },
        )

        query = XMinStateQueries.merge_state(self.fq_table)

        params = [
            bigquery.ScalarQueryParameter("source_schema", "STRING", source_schema),
            bigquery.ScalarQueryParameter("source_table", "STRING", source_table),
            bigquery.ScalarQueryParameter("xmin_value", "INT64", xmin_state.xmin_value),
            bigquery.ScalarQueryParameter("is_initial", "BOOL", xmin_state.is_initial),
            bigquery.ScalarQueryParameter("wraparound_detected", "BOOL", xmin_state.wraparound_detected),
            bigquery.ScalarQueryParameter(
                "frozen_xid", "INT64", xmin_state.frozen_xid if xmin_state.frozen_xid is not None else None
            ),
        ]
        self.bigquery_connector.execute_query(query, params=params)

    def load_state(self, source_schema: str, source_table: str) -> XMinState | None:
        self._ensure_table_exists()
        query = XMinStateQueries.load_state(self.fq_table)

        rows = self.bigquery_connector.get_records(
            query,
            params={"source_schema": source_schema, "source_table": source_table},
            as_dict=True,
        )
        if not rows:
            return None

        row = rows[0]
        state = XMinState(
            xmin_value=int(row["xmin_value"]),
            timestamp=row["__dpone__loaded_at"],
            is_initial=bool(row["is_initial"]),
            wraparound_detected=bool(row["wraparound_detected"]),
            frozen_xid=row.get("frozen_xid"),
        )

        epoch_mod = 2**32
        etl_logger.log_xmin_state_info(
            "Загружено состояние",
            {
                "Epoch": state.xmin_value // epoch_mod,
                "Low32": state.xmin_value % epoch_mod,
                "Full": state.xmin_value,
            },
        )

        return state

    def delete_state(self, source_schema: str, source_table: str) -> None:
        query = XMinStateQueries.delete_state(self.fq_table)

        self.bigquery_connector.execute_query(
            query,
            params={"source_schema": source_schema, "source_table": source_table},
        )
        etl_logger.log_xmin_state_info(
            "Состояние xmin удалено",
            {"Source": f"{source_schema}.{source_table}"},
        )
