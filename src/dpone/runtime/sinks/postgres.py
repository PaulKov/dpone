"""PostgreSQL приёмник данных.

NEW ARCHITECTURE (2025-11): Reconciliation теперь ПОДДЕРЖИВАЕТСЯ для PostgreSQL sink!
- Tech таблицы (__rs, __deleted_log) хранятся в BigQuery
- Soft delete применяется к PostgreSQL target таблице
- Reconciliation выполняется в ETLProcessor (до sink.load)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.readiness.schema_evolution import ColumnDef, SchemaPlan
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.postgres_strategy_factory import PostgresSinkCompositionFactory
from dpone.runtime.sinks.sink_protocol import AbstractSink
from dpone.runtime.sinks.strategies.base import SinkStrategy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.postgres_connector import PostgresSinkConnectorPort
    from dpone.ports.source_state_storage import SourceStateStoragePort


class PostgresSink(AbstractSink):
    """Приёмник данных PostgreSQL.
    - Reconciliation выполняется в ETLProcessor
    - Tech таблицы в BigQuery, soft delete в PostgreSQL target
    """

    def __init__(
        self,
        connector: PostgresSinkConnectorPort,
        state_storage: SourceStateStoragePort | None,
        logger: ETLLogger | None = None,
        composition_factory: PostgresSinkCompositionFactory | None = None,
    ):
        self.connector = connector
        self.state_storage = state_storage
        self.logger = logger or etl_logger
        composition = (composition_factory or PostgresSinkCompositionFactory()).build(connector, self.logger)
        self.staging_manager = composition.staging_manager
        self._strategy_map = composition.strategy_map

    def load(self, load_config: LoadConfig, payload: LoadPayload) -> LoadResult:
        """Apply the selected strategy and return only after commit acknowledgement.

        This is the transaction entry point for full refresh, including exchange.
        PostgreSQL rollback restores transactional DDL; no compensating target DDL
        is needed. The existing append micro-batch mode owns its inner commits.
        A failed commit/rollback acknowledgement never proves a database outcome.
        """
        strategy = self._resolve_strategy(load_config)
        self.connector.begin()
        try:
            result = strategy.load(load_config, payload)
            self.connector.commit_transaction()
            return result
        except BaseException as primary_error:
            try:
                self.connector.rollback()
            except BaseException as rollback_error:
                add_note = getattr(primary_error, "add_note", None)
                if callable(add_note):
                    add_note(f"PostgreSQL rollback failed: {type(rollback_error).__name__}; outcome unverified")
            raise

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        if hasattr(self.connector, "get_table_column_types"):
            return list(
                self.connector.get_table_column_types(load_config.target_schema, load_config.target_table).items()
            )
        rows = self.connector.get_records(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        return [(str(row["column_name"]), str(row["data_type"])) for row in rows]

    def get_target_columns(self, load_config: LoadConfig) -> list[ColumnDef]:
        """Read exact PostgreSQL type, nullability, and collation authority."""

        rows = self.connector.get_records(
            """
            SELECT a.attname AS column_name,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) AS declared_type,
                   NOT a.attnotnull AS is_nullable,
                   CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collname END AS collation_name
            FROM pg_catalog.pg_class AS rel
            INNER JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace
            INNER JOIN pg_catalog.pg_attribute AS a ON a.attrelid = rel.oid
            LEFT JOIN pg_catalog.pg_collation AS coll ON coll.oid = a.attcollation
            WHERE ns.nspname = %s AND rel.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        return [
            ColumnDef(
                str(row["column_name"]),
                str(row["declared_type"]),
                nullable=bool(row["is_nullable"]),
                collation=(str(row["collation_name"]) if row.get("collation_name") else None),
            )
            for row in rows
        ]

    def apply_schema_plan(self, load_config: LoadConfig, plan: SchemaPlan) -> None:
        qualified = f"{load_config.target_schema}.{load_config.target_table}"
        for statement in plan.ddl_sql("postgres", qualified):
            self.connector.execute_query(statement)

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        if self.state_storage is None:
            return
        self.state_storage.save_state(
            load_config.source_schema,
            load_config.source_table,
            state,
        )

    def _resolve_strategy(self, load_config: LoadConfig) -> SinkStrategy:
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(
                f"Неизвестная стратегия load_strategy: {load_config.load_strategy}. "
                f"Поддерживаемые: {', '.join(s.value for s in self._strategy_map.keys())}"
            )
        return strategy
