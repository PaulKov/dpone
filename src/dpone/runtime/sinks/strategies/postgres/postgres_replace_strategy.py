"""Стратегия REPLACE для PostgreSQL."""

from __future__ import annotations

from psycopg import sql

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase


class PostgresReplaceStrategy(PostgresStrategyBase):
    def load(self, load_config, payload: LoadPayload) -> LoadResult:
        if not load_config.custom_predicate:
            raise ValueError("Для REPLACE необходимо определить custom_predicate")

        def handler(staging):
            deleted = self._delete_with_predicate(load_config)
            inserted = self._insert_from_staging(load_config, staging, payload.schema)
            return LoadResult(inserted_rows=inserted, updated_rows=deleted, total_rows=inserted)

        return self._consume_with_staging(load_config, payload, handler)

    def _delete_with_predicate(self, load_config) -> int:
        delete_sql = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
            sql.Identifier(load_config.target_schema),
            sql.Identifier(load_config.target_table),
            sql.SQL(load_config.custom_predicate),
        )
        return self.connector.execute_query(delete_sql)
