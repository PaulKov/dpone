from __future__ import annotations

import csv
import logging
import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.connectors.base import AbstractConnector
from dpone.runtime.connectors.bigquery_gcs_mixin import BigQueryGcsMixin
from dpone.runtime.connectors.bigquery_runtime import _require_bigquery
from dpone.runtime.support.bigquery_load_config import (  # noqa: F401
    CSVLoadConfig,
    postgres_headerless_csv_load_config,
)

if TYPE_CHECKING:
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account

    from dpone.runtime.connectors.proxy import GCPProxyManager

logger = logging.getLogger(__name__)


def _load_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in minimal envs
        raise ModuleNotFoundError(
            "pandas is required for DataFrame-based BigQuery loads. Install the 'pandas' extra."
        ) from exc
    return pd


def _is_pandas_dataframe(value: Any) -> bool:
    try:
        pd = _load_pandas()
    except ModuleNotFoundError:
        return False
    return isinstance(value, pd.DataFrame)


def ensure_client_ready(func):
    """Decorator ensuring the BigQuery client is initialised."""

    @wraps(func)
    def wrapper(self: BigQueryConnector, *args, **kwargs):
        self._ensure_client_initialized()
        return func(self, *args, **kwargs)

    return wrapper


@dataclass
class BigQueryConnector(BigQueryGcsMixin, AbstractConnector):
    """
    Коннектор для работы с Google BigQuery.

    Поддерживает:
    - Выполнение запросов с параметризацией
    - Экспорт данных в CSV
    - Загрузку данных через GCS (оптимальный путь для больших объёмов)
    - Работу через корпоративный proxy (через GCPProxyManager)

    Args:
        project_id: GCP project ID
        credentials: Service account credentials
        client: Опциональный pre-configured BigQuery client (если нужен кастомный setup)
        proxy_manager: Опциональный GCPProxyManager для работы через корпоративный proxy
    """

    project_id: str = ""
    credentials: service_account.Credentials | None = None
    client: bigquery.Client | None = None
    proxy_manager: GCPProxyManager | None = None

    _client: bigquery.Client | None = field(default=None, init=False, repr=False)
    _scoped_credentials: service_account.Credentials | None = field(default=None, init=False, repr=False)

    DEFAULT_SCOPES: Sequence[str] = ("https://www.googleapis.com/auth/cloud-platform",)

    @property
    def connection(self) -> bigquery.Client:
        if self.client:
            return self.client
        self._ensure_client_initialized()
        return cast("bigquery.Client", self._client)

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def get_gcs_bucket(self, schema: str) -> str | None:
        """Возвращает GCS bucket для указанной схемы.

        Args:
            schema: Название схемы (например, 'landing', 'tech', 'stg')

        Returns:
            Имя GCS bucket или None если не удалось определить
        """
        try:
            from dpone.config.env import get_env_code
            from dpone.runtime.support.gcs import get_gcs_bucket_name

            env_code = get_env_code()
            return get_gcs_bucket_name(schema, env_code)
        except Exception as e:
            logger.warning(f"Failed to get GCS bucket for schema {schema}: {e}")
            return None

    def _ensure_client_initialized(self) -> None:
        """Инициализирует BigQuery client с optional proxy support."""
        if self._client:
            return

        if not self.credentials:
            raise ValueError("BigQuery credentials must be provided")

        bigquery = _require_bigquery()
        scoped_credentials = self._get_scoped_credentials()

        client_kwargs: dict[str, Any] = {
            "project": self.project_id,
            "credentials": scoped_credentials,
        }

        # Если есть proxy manager — используем его для получения настроенной сессии
        if self.proxy_manager:
            session = self.proxy_manager.get_authorized_session()
            client_kwargs["_http"] = session

            # Логируем инициализацию прокси в одном красивом блоке
            proxy_config = self.proxy_manager.proxy_config
            if proxy_config:
                etl_logger.log_proxy_initialization(
                    vault_path=proxy_config.get("vault_path", "unknown"),
                    proxy_name=proxy_config.get("proxy_name", "unnamed-proxy"),
                    client_type="BigQuery",
                )
        else:
            etl_logger.info("BigQuery client initialized without proxy")

        self._client = bigquery.Client(**client_kwargs)

    def _build_query_config(self, params) -> bigquery.QueryJobConfig | None:
        bigquery = _require_bigquery()
        if not params:
            return None

        query_params = []
        if isinstance(params, dict):
            for name, value in params.items():
                query_params.append(self._make_bq_param(name, value))
        elif isinstance(params, list):
            if all(
                isinstance(
                    param,
                    bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter | bigquery.StructQueryParameter,
                )
                for param in params
            ):
                query_params.extend(params)
            else:
                for idx, value in enumerate(params, start=1):
                    query_params.append(self._make_bq_param(f"p{idx}", value))
        else:
            raise ValueError("Params must be dict or list")

        return bigquery.QueryJobConfig(query_parameters=query_params)

    def _make_bq_param(
        self,
        name: str,
        value: Any,
    ) -> bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter | bigquery.StructQueryParameter:
        bigquery = _require_bigquery()
        if isinstance(value, list):
            if not value:
                raise ValueError(f"Array parameter {name} cannot be empty")
            elem_type = self._get_bq_type(value[0])
            return bigquery.ArrayQueryParameter(name, elem_type, value)
        return bigquery.ScalarQueryParameter(name, self._get_bq_type(value), value)

    def _get_scoped_credentials(self) -> service_account.Credentials:
        if self._scoped_credentials:
            return self._scoped_credentials

        if not self.credentials:
            raise ValueError("BigQuery credentials must be provided")

        credentials = self.credentials
        if getattr(credentials, "requires_scopes", False):
            credentials = credentials.with_scopes(self.DEFAULT_SCOPES)

        self._scoped_credentials = credentials
        return credentials

    @staticmethod
    def _get_bq_type(value: Any) -> str:
        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        return "STRING"

    @ensure_client_ready
    def execute_query(self, query: str, params=None) -> int | None:
        job_config = self._build_query_config(params)
        job = self.connection.query(query, job_config=job_config)
        result = job.result()
        return result.total_rows if hasattr(result, "total_rows") else None

    @ensure_client_ready
    def get_records(self, query: str, params=None, as_dict: bool = False) -> list[dict[str, Any]]:
        job_config = self._build_query_config(params)
        job = self.connection.query(query, job_config=job_config)
        rows = job.result()
        results = [dict(row) for row in rows]
        return results

    def commit_transaction(self):
        pass

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        """
        Строит SELECT запрос для BigQuery.

        Args:
            schema: Имя dataset
            table: Имя таблицы
            columns: Список колонок
            limit: LIMIT clause (optional)
            offset: OFFSET clause (optional)

        Returns:
            SQL query string с backticks для идентификаторов
        """
        # SELECT columns FROM `project.dataset.table`
        # В BigQuery полный путь: project.dataset.table
        columns_str = ", ".join(f"`{col}`" for col in columns)

        # Если у нас уже есть project_id, можем построить полный путь
        if hasattr(self, "project_id") and self.project_id:
            query = f"SELECT {columns_str} FROM `{self.project_id}.{schema}.{table}`"
        else:
            # Fallback без project_id
            query = f"SELECT {columns_str} FROM `{schema}.{table}`"

        if limit is not None:
            query += f" LIMIT {limit}"

        if offset is not None:
            query += f" OFFSET {offset}"

        return query

    def build_max_query(self, schema: str, table: str, column: str) -> str:
        """Строит MAX запрос для BigQuery с backticks и project_id."""
        fq_table = f"`{self.project_id}.{schema}.{table}`"
        return f"SELECT MAX(`{column}`) as max_val FROM {fq_table}"

    @ensure_client_ready
    def export_to_csv(
        self,
        query: str,
        output_dir: str,
        chunk_size: int = 500000,
        file_prefix: str = "export",
        params=None,
    ) -> list[str]:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

        job_config = self._build_query_config(params)
        job = self.connection.query(query, job_config=job_config)
        rows_iter = job.result(page_size=chunk_size)

        file_paths = []
        file_index = 1

        for page in rows_iter.pages:
            file_name = f"{file_prefix}_{file_index}.csv"
            file_path = os.path.join(output_dir, file_name)

            with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([field.name for field in rows_iter.schema])
                for row in page:
                    writer.writerow(list(row.values()))

            file_paths.append(file_path)
            file_index += 1

        return file_paths

    @ensure_client_ready
    def load_data(
        self,
        table_id: str,
        source: str | pd.DataFrame,
        write_disposition: str | Any = "WRITE_APPEND",
        schema: list[Any] | None = None,
    ):
        """
        Загружает данные в BigQuery из CSV файла или pandas DataFrame.

        Args:
            table_id: Полный ID таблицы (project.dataset.table или dataset.table)
            source: Путь к CSV файлу или pandas DataFrame
            write_disposition: Режим записи (enum bigquery.WriteDisposition)
            schema: Опциональная схема таблицы
        """
        bigquery = _require_bigquery()
        if isinstance(write_disposition, str):
            write_disposition = getattr(bigquery.WriteDisposition, write_disposition.upper(), write_disposition)

        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        if isinstance(source, str):
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            if schema:
                job_config.schema = schema

            with open(source, "rb") as f:
                job = self.connection.load_table_from_file(f, table_id, job_config=job_config)
        elif _is_pandas_dataframe(source):
            job = self.connection.load_table_from_dataframe(source, table_id, job_config=job_config)
        else:
            raise ValueError("Unsupported source type. Use file path or pandas.DataFrame.")

        job.result()
