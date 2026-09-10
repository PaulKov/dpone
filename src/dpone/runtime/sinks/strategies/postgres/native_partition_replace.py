"""PostgreSQL declarative partition replacement helper."""

from __future__ import annotations

import uuid
from typing import Any

from psycopg import sql

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


class PostgresNativePartitionReplacer:
    """Executes safe DETACH/ATTACH partition replacement when metadata allows it."""

    def __init__(self, connector: Any, logger: Any) -> None:
        self.connector = connector
        self.logger = logger

    def try_replace(self, load_config: Any, staging: Any, payload: LoadPayload, partition: Any) -> LoadResult | None:
        if not partition.native:
            return None
        if not self._target_is_declarative_partitioned(load_config):
            self._warn(load_config, "target table is not declaratively partitioned")
            return None

        values = self._partition_values_from_staging(staging, partition.column)
        if len(values) > partition.max_partitions_per_run:
            raise ValueError(
                "PostgreSQL partition_replace would replace "
                f"{len(values)} partitions, above max_partitions_per_run={partition.max_partitions_per_run}."
            )
        if not values:
            return LoadResult(
                inserted_rows=0, updated_rows=0, total_rows=self._count_target(load_config), replaced_rows=0
            )

        # Keep catalog/row scope stable until DETACH/ATTACH or predicate fallback.
        self.connector.execute_query(
            f"LOCK TABLE {_qualified(load_config.target_schema, load_config.target_table)} IN ACCESS EXCLUSIVE MODE"
        )
        if any(value is None for value in values):
            self._warn(load_config, "NULL partition values require predicate replacement")
            return None
        partition_plan = self._resolve_partition_plan(
            load_config, partition.column, [v for v in values if v is not None]
        )
        if partition_plan is None:
            return None

        return self._apply_partition_plan(load_config, staging, payload, partition.column, partition_plan)

    def _resolve_partition_plan(
        self, load_config: Any, partition_column: str, values: list[str]
    ) -> list[tuple[str, str, str]] | None:
        partition_plan: list[tuple[str, str, str]] = []
        for value in values:
            child_regclass, partition_bound = self._existing_partition_for_value(load_config, partition_column, value)
            if not child_regclass or not partition_bound:
                self._warn(load_config, f"could not resolve existing partition bound for {partition_column}={value!r}")
                return None
            if any(child == child_regclass for _, child, _ in partition_plan):
                self._warn(load_config, "multiple staged values resolve to the same physical partition")
                return None
            outside_scope = self.connector.get_records(
                f"SELECT EXISTS (SELECT 1 FROM {child_regclass} "
                f"WHERE {_quote_ident(partition_column)}::text IS DISTINCT FROM %s)",
                (value,),
            )
            if not outside_scope or outside_scope[0][0]:
                self._warn(load_config, "physical partition contains rows outside the staged value")
                return None
            partition_plan.append((value, str(child_regclass), str(partition_bound)))
        return partition_plan

    def _apply_partition_plan(
        self,
        load_config: Any,
        staging: Any,
        payload: LoadPayload,
        partition_column: str,
        partition_plan: list[tuple[str, str, str]],
    ) -> LoadResult:
        inserted_total = 0
        replaced_partitions = 0
        for value, child_regclass, partition_bound in partition_plan:
            replacement_table = f"dpone_part_{uuid.uuid4().hex}"
            self.connector.execute_query(self._create_replacement_sql(load_config, replacement_table, child_regclass))
            inserted_total += self._insert_partition_value_into_table(
                load_config, staging, payload.schema, replacement_table, partition_column, value
            )
            self.connector.execute_query(self._detach_partition_sql(load_config, child_regclass))
            self.connector.execute_query(self._attach_partition_sql(load_config, replacement_table, partition_bound))
            replaced_partitions += 1
            self.connector.execute_query(f"DROP TABLE IF EXISTS {child_regclass}")

        return LoadResult(
            inserted_rows=inserted_total,
            updated_rows=0,
            total_rows=self._count_target(load_config),
            replaced_rows=replaced_partitions,
        )

    def _target_is_declarative_partitioned(self, load_config: Any) -> bool:
        rows = self.connector.get_records(
            sql.SQL(
                """
                SELECT c.relkind = 'p'
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """
            ),
            (load_config.target_schema, load_config.target_table),
        )
        return bool(rows and rows[0][0])

    def _partition_values_from_staging(self, staging: Any, partition_column: str) -> list[str | None]:
        rows = self.connector.get_records(
            f"SELECT DISTINCT {_quote_ident(partition_column)}::text FROM {_qualified(staging.schema, staging.table)}"
        )
        return [str(row[0]) if row[0] is not None else None for row in rows]

    def _existing_partition_for_value(
        self, load_config: Any, partition_column: str, value: str
    ) -> tuple[str, str | None]:
        rows = self.connector.get_records(
            "SELECT tableoid::regclass::text "
            f"FROM {_qualified(load_config.target_schema, load_config.target_table)} "
            f"WHERE {_quote_ident(partition_column)}::text = %s LIMIT 1",
            (value,),
        )
        if not rows:
            return "", None
        child_regclass = str(rows[0][0])
        bound_rows = self.connector.get_records(
            """
            SELECT pg_get_expr(c.relpartbound, c.oid)
            FROM pg_class c
            WHERE c.oid = %s::regclass
            """,
            (child_regclass,),
        )
        return child_regclass, str(bound_rows[0][0]) if bound_rows else None

    def _insert_partition_value_into_table(
        self,
        load_config: Any,
        staging: Any,
        schema: list[tuple[str, str]],
        destination_table: str,
        partition_column: str,
        partition_value: str,
    ) -> int:
        data_columns = [column for column, _ in schema if column != "__dpone__xmin"]
        columns_sql = ", ".join(_quote_ident(column) for column in data_columns)
        query = f"""
            INSERT INTO {_qualified(load_config.target_schema, destination_table)} ({columns_sql})
            SELECT {columns_sql}
            FROM {_qualified(staging.schema, staging.table)}
            WHERE {_quote_ident(partition_column)}::text = %s
            """
        return self.connector.execute_query(query, (partition_value,))

    def _count_target(self, load_config: Any) -> int:
        rows = self.connector.get_records(
            f"SELECT COUNT(*) FROM {_qualified(load_config.target_schema, load_config.target_table)}"
        )
        return int(rows[0][0]) if rows else 0

    def _warn(self, load_config: Any, reason: str) -> None:
        if hasattr(self.logger, "warning"):
            self.logger.warning(
                "PostgreSQL native partition_replace unavailable for "
                f"{load_config.target_schema}.{load_config.target_table}: {reason}. "
                "Using staging-first partition predicate fallback unless native_mode=required."
            )

    @staticmethod
    def _create_replacement_sql(load_config: Any, replacement_table: str, child_regclass: str) -> str:
        return (
            f"CREATE TABLE {_qualified(load_config.target_schema, replacement_table)} "
            f"(LIKE {child_regclass} INCLUDING ALL)"
        )

    @staticmethod
    def _detach_partition_sql(load_config: Any, child_regclass: str) -> str:
        return f"ALTER TABLE {_qualified(load_config.target_schema, load_config.target_table)} DETACH PARTITION {child_regclass}"

    @staticmethod
    def _attach_partition_sql(load_config: Any, replacement_table: str, partition_bound: str) -> str:
        return (
            f"ALTER TABLE {_qualified(load_config.target_schema, load_config.target_table)} "
            f"ATTACH PARTITION {_qualified(load_config.target_schema, replacement_table)} {partition_bound}"
        )


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'
