"""Minimal ClickHouse sink for file/stream based cross-database loads."""

from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module
from typing import TYPE_CHECKING, Any

from dpone.runtime.sinks.clickhouse_bulk_mixin import ClickHouseBulkMixin
from dpone.runtime.sinks.clickhouse_cluster_preflight import ClickHouseClusterPreflightMixin
from dpone.runtime.sinks.clickhouse_lineage_projection import ClickHouseSinkSideLineageProjector
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.clickhouse_physical_types import (
    DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER,
    ClickHousePhysicalColumnTypeResolver,
)
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin, ClickHouseTargetCatalogMixin
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder
from dpone.runtime.sinks.clickhouse_staging_finalizer import ClickHouseStagingFinalizer
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.sink_protocol import AbstractSink

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.clickhouse_connector import ClickHouseConnectorPort


class ClickHouseSink(
    ClickHouseTargetCatalogMixin,
    ClickHouseClusterPreflightMixin,
    ClickHouseBulkMixin,
    ClickHouseSqlMixin,
    AbstractSink,
):
    """ClickHouse sink optimized for MSSQL/Postgres exported files and row streams."""

    supports_staged_validation_receipts = True

    def __init__(
        self,
        connector: ClickHouseConnectorPort,
        state_storage: Any = None,
        logger: Any | None = None,
        *,
        client_runner_cls: Any | None = None,
        http_runner_cls: Any | None = None,
        physical_type_resolver: ClickHousePhysicalColumnTypeResolver | None = None,
    ):
        self.connector = connector
        self.state_storage = state_storage
        self.logger = logger or _default_etl_logger()
        self.acceptance_metric_probe = _default_acceptance_metric_probe(connector)
        self._client_runner_cls = client_runner_cls or _default_clickhouse_client_runner()
        self._http_runner_cls = http_runner_cls or _default_clickhouse_http_runner()
        self._physical_type_resolver = physical_type_resolver or DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER
        self._staging_decoder = ClickHouseStagingDecoder(
            connector=self.connector,
            table_name=self._table,
            create_staging_table=self._create_staging_table,
            map_type=self._map_type_for_config,
            drop_staging_table=lambda config: self._drop_table(self._table(config), config),
            plan_staging_table=lambda config: self._operation_table_config(config, "staging"),
            create_planned_staging_table=lambda config, schema: self._create_table(
                config,
                schema,
                if_not_exists=False,
            ),
        )
        self._staging_finalizer = ClickHouseStagingFinalizer(
            connector=self.connector,
            table_name=self._table,
            count_rows=self._count,
            mutations_sync=self._mutations_sync,
        )
        self._payload_ingestion = ClickHousePayloadIngestionService(self, sink_factory=self._clone_sink)
        self._staged_load = ClickHouseStagedLoadService(
            self,
            plan_staging_table=lambda config: self._operation_table_config(config, "staging"),
            create_planned_staging_table=self._create_planned_payload_staging_table,
        )
        self.lineage_projector = ClickHouseSinkSideLineageProjector(self)

    def load(self, load_config: LoadConfig, payload: Any) -> LoadResult:
        return self._staged_load.load(load_config, payload)

    def stage_payload(self, load_config: LoadConfig, payload: Any) -> Any:
        return self._staged_load.stage(load_config, payload)

    def finalize_staged_load(self, load_config: LoadConfig, handle: Any) -> LoadResult:
        frozen_inputs = getattr(handle, "frozen_inputs", None)
        if callable(frozen_inputs):
            validation_token, validated_config, validated_handle = frozen_inputs(
                sink=self,
                load_config=load_config,
            )
            return self._staged_load.finalize_validated(
                validated_config,
                validated_handle,
                validation_token,
            )
        return self._staged_load.finalize(load_config, handle)

    def validate_staged_load(self, load_config: LoadConfig, handle: Any) -> object:
        _lineage_schema_columns(handle)
        return self._staged_load.validate(load_config, handle)

    def abort_staged_load(self, handle: Any) -> None:
        self._staged_load.abort(handle)

    def cleanup_staged_load(self, handle: Any) -> None:
        self._staged_load.cleanup(handle)

    def _prepare_staged_finalization(self, load_config: LoadConfig, handle: Any) -> None:
        schema_columns = _lineage_schema_columns(handle)
        if not schema_columns or not self._table_exists(load_config):
            return
        for column, dtype in schema_columns:
            quoted = column.replace("`", "``")
            self.connector.execute_query(
                f"ALTER TABLE {self._table(load_config)} ADD COLUMN IF NOT EXISTS `{quoted}` {dtype}"
            )

    def target_table_exists(self, load_config: LoadConfig) -> bool:
        return self._table_exists(load_config)

    def inspect_physical_design(self, load_config: LoadConfig) -> Any:
        module = import_module("dpone.runtime.sinks.clickhouse_physical_reconciliation")
        return module.ClickHousePhysicalIntrospector(self.connector).inspect(load_config)

    def apply_physical_ddl(self, request: Any) -> None:
        self.connector.execute_query(request.sql)

    def apply_schema_plan(self, load_config: LoadConfig, plan: Any) -> None:
        qualified = f"{load_config.target_schema}.{load_config.target_table}"
        for statement in plan.ddl_sql("clickhouse", qualified):
            self.connector.execute_query(statement)

    def _insert_payload(self, load_config: LoadConfig, payload: Any) -> int:
        return self._payload_ingestion.insert_payload(load_config, payload)

    def _create_payload_staging_table(self, load_config: LoadConfig, payload: Any) -> LoadConfig:
        staging_schema = self._staging_decoder.staging_schema(load_config, payload)
        if staging_schema.clickhouse_types:
            return self._create_typed_staging_table(load_config, staging_schema.columns)
        return self._create_staging_table(load_config, staging_schema.columns)

    def _create_planned_payload_staging_table(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        payload: Any,
    ) -> None:
        staging_schema = self._staging_decoder.staging_schema(load_config, payload)
        if (
            load_config.load_strategy.value == "partition_replace"
            and self._table_exists(load_config)
            and not staging_schema.clickhouse_types
        ):
            self._ensure_database(staging_config)
            self.connector.execute_query(
                f"CREATE TABLE {self._table(staging_config)}{self._cluster_ddl_clause(load_config)} "
                f"AS {self._table(load_config)}"
            )
        elif staging_schema.clickhouse_types:
            self._create_table_with_clickhouse_types(staging_config, staging_schema.columns, if_not_exists=False)
        else:
            self._create_table(staging_config, staging_schema.columns, if_not_exists=False)

    def _create_staging_table(
        self,
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
    ) -> LoadConfig:
        staging_config = self._operation_table_config(load_config, "staging")
        self._create_table(staging_config, schema, if_not_exists=False)
        return staging_config

    def _create_typed_staging_table(
        self,
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
    ) -> LoadConfig:
        staging_config = self._operation_table_config(load_config, "staging")
        self._create_table_with_clickhouse_types(staging_config, schema, if_not_exists=False)
        return staging_config

    def _create_shadow_table(self, load_config: LoadConfig) -> LoadConfig:
        shadow_config = self._operation_table_config(load_config, "shadow")
        self._ensure_database(shadow_config)
        self.connector.execute_query(
            f"CREATE TABLE {self._table(shadow_config)}{self._cluster_ddl_clause(load_config)} "
            f"AS {self._table(load_config)}"
        )
        return shadow_config

    def _create_staging_like_target_table(self, load_config: LoadConfig) -> LoadConfig:
        staging_config = self._operation_table_config(load_config, "staging")
        self._ensure_database(staging_config)
        self.connector.execute_query(
            f"CREATE TABLE {self._table(staging_config)}{self._cluster_ddl_clause(load_config)} "
            f"AS {self._table(load_config)}"
        )
        return staging_config

    def _swap_table_into_target(self, load_config: LoadConfig, replacement_config: LoadConfig) -> None:
        target_exists = self._table_exists(load_config)
        backup_config = self._operation_table_config(load_config, "backup")
        cluster_clause = self._cluster_ddl_clause(load_config)

        if target_exists:
            self.connector.execute_query(
                f"RENAME TABLE {self._table(load_config)} TO {self._table(backup_config)}, "
                f"{self._table(replacement_config)} TO {self._table(load_config)}{cluster_clause}"
            )
            self._drop_table(self._table(backup_config), backup_config)
        else:
            self.connector.execute_query(
                f"RENAME TABLE {self._table(replacement_config)} TO {self._table(load_config)}{cluster_clause}"
            )

    def _insert_from_table(self, source_config: LoadConfig, target_config: LoadConfig) -> int:
        settings_clause = ClickHouseNullInsertPolicy.from_load_config(target_config).insert_select_settings_clause()
        self.connector.execute_query(
            f"INSERT INTO {self._table(target_config)}{settings_clause} SELECT * FROM {self._table(source_config)}"
        )
        return self._count(source_config)

    def _clone_sink(self, connector: ClickHouseConnectorPort) -> ClickHouseSink:
        return ClickHouseSink(
            connector,
            state_storage=self.state_storage,
            logger=self.logger,
            client_runner_cls=self._client_runner_cls,
            http_runner_cls=self._http_runner_cls,
        )

    def _execute_insert(self, load_config: LoadConfig, columns: Sequence[str], rows: Sequence[tuple[Any, ...]]) -> int:
        return self._payload_ingestion.execute_insert(load_config, columns, rows)


def _lineage_schema_columns(handle: Any) -> tuple[tuple[str, str], ...]:
    metadata = dict(getattr(handle, "metadata", {}) or {})
    raw_schema = metadata.get("lineage_schema_columns", ())
    if not isinstance(raw_schema, (list, tuple)):
        raise ValueError("clickhouse_lineage_schema_columns_invalid")
    columns: list[tuple[str, str]] = []
    for item in raw_schema:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("clickhouse_lineage_schema_columns_invalid")
        column, dtype = item
        if not isinstance(column, str) or not column or not isinstance(dtype, str) or not dtype:
            raise ValueError("clickhouse_lineage_schema_columns_invalid")
        columns.append((column, dtype))
    return tuple(columns)


def _default_clickhouse_client_runner() -> Any:
    module = import_module("dpone.runtime.support.clickhouse_bulk")
    return module.ClickHouseClientRunner


def _default_clickhouse_http_runner() -> Any:
    module = import_module("dpone.runtime.support.clickhouse_bulk")
    return module.ClickHouseHttpBulkRunner


def _default_etl_logger() -> Any:
    module = import_module("dpone.runtime.sink_logging")
    return module.etl_logger


def _default_acceptance_metric_probe(connector: ClickHouseConnectorPort) -> Any:
    module = import_module("dpone.runtime.governance.clickhouse_acceptance_metrics")
    return module.ClickHouseAcceptanceMetricProbe(connector)
