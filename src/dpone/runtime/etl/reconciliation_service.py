"""Reconciliation orchestration extracted from ETLProcessor."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.technical_columns import include_technical_columns
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.gcs_replacement import resolve_gcs_attempt_scope


class ReconciliationService:
    """Encapsulates reconciliation runtime flow and tech-connector lookup."""

    def __init__(
        self,
        *,
        source: Any,
        sink: Any,
        logger: Any,
        run_state_storage: Any | None = None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.logger = logger
        self.run_state_storage = run_state_storage

    def run_if_enabled(self, load_config: LoadConfig, extract_result: Any) -> dict[str, Any] | None:
        """Runs reconciliation if enabled, otherwise returns ``None``."""

        if not load_config.reconciliation:
            return None

        if load_config.load_strategy == LoadStrategy.FULL_REFRESH:
            self.logger.warning(
                "⚠️  FULL_REFRESH не поддерживает reconciliation. "
                "При полной перезагрузке таблицы невозможно отследить soft-delete строк. "
                "Reconciliation будет пропущен."
            )
            self.logger.log_etl_progress(
                "RECONCILIATION_SKIPPED",
                {
                    "Reason": "FULL_REFRESH strategy incompatible with reconciliation",
                    "LoadStrategy": load_config.load_strategy.value,
                    "Recommendation": "Используйте incremental_merge/incremental_append для reconciliation",
                },
            )
            return None

        return self.process(load_config, extract_result)

    def process(self, load_config: LoadConfig, extract_result: Any) -> dict[str, Any]:
        """Executes reconciliation: snapshot + soft delete."""

        if not load_config.unique_key:
            self.logger.warning("⚠️  Reconciliation требует unique_key, но он не указан. Reconciliation будет пропущен.")
            return {"skipped": True, "reason": "unique_key not specified"}

        include_tech = include_technical_columns(load_config.options or {})
        if not include_tech:
            raise RuntimeConfigurationError(
                "Reconciliation включен, но технические колонки отключены ("
                "sink.options.technical_columns=forbidden или sink.options.include_technical_columns=false). "
                "Soft-delete reconciliation требует __dpone__loaded_at/__dpone__deleted_at. "
                "Включите technical_columns=required (или include_technical_columns=true) либо отключите reconciliation."
            )

        tech_connector = self.get_tech_connector()
        target_connector = self.sink.connector
        source_connector = (load_config.options or {}).get("_source_connector")

        if not tech_connector:
            self.logger.warning("⚠️  BigQuery connector для tech таблиц не доступен. Reconciliation будет пропущен.")
            return {"skipped": True, "reason": "tech_connector not available"}

        if not source_connector:
            self.logger.warning("⚠️  Source connector не доступен. Reconciliation будет пропущен.")
            return {"skipped": True, "reason": "source_connector not available"}

        self.logger.log_etl_progress(
            "RECONCILIATION_INIT",
            {
                "TechConnector": tech_connector.__class__.__name__,
                "TargetConnector": target_connector.__class__.__name__,
                "SourceConnector": source_connector.__class__.__name__,
                "TechSchema": load_config.tech_schema,
            },
        )

        from dpone.runtime.reconciliation import ReconciliationManager

        reconciliation_manager = ReconciliationManager(
            tech_connector=tech_connector,
            target_connector=target_connector,
            logger=self.logger,
            tech_schema=load_config.tech_schema,
        )

        reconciliation_manager.ensure_reconciliation_infrastructure(
            target_schema=load_config.target_schema,
            target_table=load_config.target_table,
            target_table_schema=extract_result.schema or [],
            unique_key=load_config.unique_key,
        )

        batch_commit_mode = (load_config.options or {}).get("batch_commit_mode", "separate")
        gcs_config = self._build_gcs_config(tech_connector, load_config, batch_commit_mode)

        return reconciliation_manager.process_reconciliation(
            target_schema=load_config.target_schema,
            target_table=load_config.target_table,
            unique_key=load_config.unique_key,
            source_connector=source_connector,
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            batch_size=load_config.batch_size,
            batch_commit_mode=batch_commit_mode,
            gcs_config=gcs_config,
        )

    def get_tech_connector(self) -> Any:
        """Returns BigQuery connector for tech tables if available."""

        if hasattr(self.sink, "connector") and self.sink.connector.__class__.__name__ == "BigQueryConnector":
            return self.sink.connector

        if self.run_state_storage and hasattr(self.run_state_storage, "bigquery_connector"):
            self.logger.log_etl_progress(
                "TECH_CONNECTOR_REUSE",
                {
                    "Source": "run_state_storage.bigquery_connector",
                    "Info": "Переиспользуем BigQuery connector из state manager",
                    "SinkType": self.sink.connector.__class__.__name__,
                },
            )
            return self.run_state_storage.bigquery_connector

        if hasattr(self.source, "xmin_storage") and hasattr(self.source.xmin_storage, "bigquery_connector"):
            self.logger.log_etl_progress(
                "TECH_CONNECTOR_REUSE",
                {
                    "Source": "source.xmin_storage.bigquery_connector",
                    "Info": "Переиспользуем BigQuery connector из XMin state",
                    "SinkType": self.sink.connector.__class__.__name__,
                },
            )
            return self.source.xmin_storage.bigquery_connector

        self.logger.warning(
            f"⚠️  Tech connector: Sink type is {self.sink.connector.__class__.__name__}, "
            f"но BigQuery connector для tech таблиц не найден. "
            f"Reconciliation требует run_state_storage или xmin_storage с BigQuery connector."
        )
        return None

    def _build_gcs_config(
        self,
        tech_connector: Any,
        load_config: LoadConfig,
        batch_commit_mode: str,
    ) -> dict[str, Any] | None:
        if batch_commit_mode != "separate":
            return None

        has_gcs_methods = hasattr(tech_connector, "upload_file_to_gcs") and hasattr(tech_connector, "load_from_gcs")
        if not has_gcs_methods:
            return None

        gcs_bucket = None
        if hasattr(tech_connector, "gcs_bucket"):
            gcs_bucket = tech_connector.gcs_bucket
            self.logger.info(f"📦 GCS bucket from attribute: {gcs_bucket}")
        elif hasattr(tech_connector, "get_gcs_bucket"):
            try:
                gcs_bucket = tech_connector.get_gcs_bucket(schema=load_config.tech_schema)
                self.logger.info(f"📦 GCS bucket from method: {gcs_bucket}")
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.warning(f"⚠️  Failed to get GCS bucket: {exc}")
                gcs_bucket = None

        if gcs_bucket:
            attempt_scope = None
            storage_client = None
            try:
                attempt_scope = resolve_gcs_attempt_scope(load_config)
            except ValueError as exc:
                self.logger.warning(f"⚠️  GCS attempt scope unavailable for reconciliation: {exc}")
            if hasattr(tech_connector, "connection"):
                storage_client = getattr(tech_connector.connection, "_client", None)
            self.logger.info(f"✅ GCS config created for reconciliation: bucket={gcs_bucket}")
            return {
                "gcs_bucket": gcs_bucket,
                "upload_file_to_gcs": tech_connector.upload_file_to_gcs,
                "load_from_gcs": tech_connector.load_from_gcs,
                "attempt_scope": attempt_scope,
                "storage_client": storage_client,
            }

        self.logger.warning(
            f"⚠️  tech_connector has GCS methods but no bucket. "
            f"Has gcs_bucket attr: {hasattr(tech_connector, 'gcs_bucket')}, "
            f"Has get_gcs_bucket method: {hasattr(tech_connector, 'get_gcs_bucket')}"
        )
        return None
