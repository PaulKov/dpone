"""ClickHouse источник данных."""

from __future__ import annotations

from dataclasses import dataclass, replace
from importlib import import_module
from typing import TYPE_CHECKING

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.clickhouse_incremental_cursor import assert_clickhouse_mssql_cursor_supported
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.runtime.connectors import ClickHouseConnector
from dpone.runtime.mssql_spool_route import MSSQL_CHARACTER_SPOOL_REQUIREMENT
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.sources.strategies import (
    ClickHouseFullExtractStrategy,
    ClickHouseIncrementalExtractStrategy,
    SourceStrategy,
)
from dpone.type_system.source_sink.provenance import (
    SourceColumnProvenance,
    SourceRelationDialect,
    clickhouse_type_nullable,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


@dataclass(frozen=True, slots=True)
class ClickHouseFetchedSchema:
    """Immutable source schema read from ``system.columns``."""

    relation_schema: tuple[tuple[str, str], ...]
    projected_schema: tuple[tuple[str, str, bool], ...]
    relation_metadata: tuple[SourceColumnProvenance, ...]


class ClickHouseSource(AbstractSource):
    """
    Источник данных ClickHouse.

    Поддерживаемые стратегии:
    - FULL_REFRESH: Полная перезагрузка через GCS
    - INCREMENTAL_APPEND: Инкрементальная загрузка по incremental_column
    - REPLACE: Извлечение по custom_predicate + DELETE/INSERT на стороне sink

    Отличия от PostgresSource:
    - Не использует XMinStateStorage (нет xmin в ClickHouse)
    - Incremental стратегия получает max(date_column) из sink БД
    - Поддержка партиционирования по дате (day/month/year)
    - Нативный экспорт в GCS через s3() table function
    """

    target_max_cursor_source_type = "clickhouse"

    def __init__(
        self,
        connector: ClickHouseConnector,
        logger,
        sink_connector=None,
    ):
        self.connector = connector
        self.sink_connector = sink_connector  # Для получения max date из sink
        self.logger = logger
        self.acceptance_metric_probe = _default_acceptance_metric_probe(connector)

        full_extract = ClickHouseFullExtractStrategy(connector=self.connector, logger=logger)

        incremental_extract = ClickHouseIncrementalExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector or self.connector,  # fallback на source
            logger=logger,
        )
        self._incremental_extract = incremental_extract

        self._strategy_map: dict[LoadStrategy, SourceStrategy] = {
            LoadStrategy.FULL_REFRESH: full_extract,
            LoadStrategy.INCREMENTAL_APPEND: incremental_extract,
            LoadStrategy.REPLACE: full_extract,
            LoadStrategy.PARTITION_REPLACE: full_extract,
            # Backfill extraction is a full scan bounded by the chunk predicate
            # (options.source_custom_predicate injected by the orchestrator).
            LoadStrategy.BACKFILL: full_extract,
        }

    def get_incremental_state(self, load_config: LoadConfig) -> dict | None:
        """
        Возвращает текущее состояние инкрементальной загрузки.

        Для ClickHouse это max(date_column) из sink БД.
        """
        strategy = self._resolve_strategy(load_config)
        return strategy.get_state(load_config)

    def extract(self, load_config: LoadConfig, last_state: dict | None) -> ExtractResult:
        """Извлекает данные из ClickHouse согласно стратегии."""
        strategy = self._resolve_strategy(load_config)
        result = strategy.extract(load_config, last_state)
        fetched = self.fetch_schema_projection(load_config)
        observed = tuple((str(name), str(dtype)) for name, dtype in result.schema)
        if observed != fetched.relation_schema:
            raise RuntimeError("clickhouse_source_schema_projection.changed_during_extract")
        return replace(
            result,
            relation_schema=fetched.relation_schema,
            relation_metadata=fetched.relation_metadata,
            relation_dialect=SourceRelationDialect.CLICKHOUSE,
        )

    def mssql_character_spool_preflight_requirement(self, load_config: LoadConfig) -> str | None:
        """Prove the deterministic local row spool without touching ClickHouse."""

        strategy = self._resolve_strategy(load_config)
        options = strategy.get_options(load_config)
        if isinstance(options.get("query"), dict) or bool(options.get("export_to_gcs", False)):
            return None
        return MSSQL_CHARACTER_SPOOL_REQUIREMENT

    def mssql_transaction_checkpoint_mode(
        self,
        load_config: LoadConfig,
    ) -> MssqlTransactionCheckpointMode:
        """Declare stateless scans separately from the lossy legacy cursor."""

        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(
                f"Неизвестная стратегия load_strategy: {load_config.load_strategy}. "
                f"Поддерживаемые: {', '.join(s.value for s in self._strategy_map.keys())}"
            )
        if strategy is self._incremental_extract:
            return MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
        return MssqlTransactionCheckpointMode.STATELESS

    def mssql_transaction_source_physical_identity(
        self,
        load_config: LoadConfig,
    ) -> object:
        """Read one database-issued ClickHouse identity before MSSQL admission."""

        database = str(load_config.source_schema or "").strip()
        if not database:
            raise RuntimeError("clickhouse_source_physical_identity.database_required")
        rows = self.connector.get_records(
            """
            SELECT
                toString(serverUUID()) AS cluster_identifier,
                name AS database,
                currentUser() AS principal,
                hostName() AS server_address,
                tcpPort() AS server_port
            FROM system.databases
            WHERE name = %(database)s
            """,
            params={"database": database},
            as_dict=True,
        )
        if len(rows) != 1:
            raise RuntimeError("clickhouse_source_physical_identity.database_unavailable")
        row = rows[0]
        principal = str(row.get("principal") or "").strip()
        identity_type = import_module("dpone.contracts.source_physical_identity").SourcePhysicalIdentity
        return identity_type(
            dialect="clickhouse",
            cluster_identifier=str(row.get("cluster_identifier") or "").strip(),
            database=str(row.get("database") or "").strip(),
            effective_principal=principal,
            session_principal=principal,
            server_address=str(row.get("server_address") or "").strip() or None,
            server_port=int(row["server_port"]),
            topology_role="standalone",
        )

    def fetch_schema_projection(self, load_config: LoadConfig) -> ClickHouseFetchedSchema:
        """Read exact ClickHouse column order, type spelling, and nullability."""

        strategy = self._resolve_strategy(load_config)
        fetch = getattr(strategy, "get_table_schema", None)
        if not callable(fetch):
            raise RuntimeError("clickhouse_source_schema_projection.unavailable")
        schema = tuple((str(name), str(dtype)) for name, dtype in fetch(load_config))
        if not schema:
            raise RuntimeError("clickhouse_source_schema_projection.empty")
        metadata = tuple(
            SourceColumnProvenance(
                name=name,
                declared_type=dtype,
                nullable=clickhouse_type_nullable(dtype),
            )
            for name, dtype in schema
        )
        return ClickHouseFetchedSchema(
            relation_schema=schema,
            projected_schema=tuple((column.name, column.declared_type, bool(column.nullable)) for column in metadata),
            relation_metadata=metadata,
        )

    def _resolve_strategy(self, load_config: LoadConfig) -> SourceStrategy:
        """Определяет стратегию извлечения на основе LoadStrategy."""
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(
                f"Неизвестная стратегия load_strategy: {load_config.load_strategy}. "
                f"Поддерживаемые: {', '.join(s.value for s in self._strategy_map.keys())}"
            )
        if strategy is self._incremental_extract:
            options = getattr(load_config, "options", {}) or {}
            assert_clickhouse_mssql_cursor_supported(
                configured_sink=options.get("sink_type") or options.get("target_type"),
                sink_connector=self.sink_connector,
            )
        return strategy


def _default_acceptance_metric_probe(connector):
    module = import_module("dpone.runtime.governance.clickhouse_acceptance_metrics")
    return module.ClickHouseAcceptanceMetricProbe(connector)
