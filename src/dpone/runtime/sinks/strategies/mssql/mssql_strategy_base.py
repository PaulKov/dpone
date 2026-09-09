"""Shared staging and target operations for SQL Server load strategies."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from dpone.contracts.mssql_table_swap import mssql_shadow_swap_statements
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.consumed_payload_evidence import canonical_source_provenance_sha256
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import validate_duplicate_policy
from dpone.runtime.sinks.mssql_table_ddl import render_load_strategy_create_table
from dpone.runtime.sinks.mssql_transaction_requirement import MSSQL_TRANSACTION_ADMISSION_OPTION
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.mssql.mssql_counted_dml import execute_counted_dml
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import (
    MssqlGenericTargetContract,
)
from dpone.runtime.sinks.strategies.mssql.mssql_object_naming import MSSQLObjectNamingMixin
from dpone.runtime.sinks.strategies.mssql.mssql_partition_fallback import (
    MssqlPartitionFallbackPlan,
    plan_mssql_partition_fallback,
)
from dpone.runtime.sinks.strategies.mssql.mssql_staging_consumer import MssqlStagingConsumer
from dpone.runtime.sinks.strategies.mssql.mssql_staging_sql_mixin import MSSQLStagingSqlMixin
from dpone.runtime.support.mssql_native_projection import (
    project_mssql_schema_evolution_columns,
    resolve_native_column_types,
)
from dpone.runtime.support.mssql_types import MSSQLTypeMapper


class MSSQLStrategyBase(MSSQLStagingSqlMixin, MSSQLObjectNamingMixin, SinkStrategy):
    """Materialize payloads and expose transaction-scoped target operations."""

    _SERVICE_COLUMNS = {"__dpone__xmin"}

    @staticmethod
    def project_schema_evolution_source_columns(
        load_config: Any,
        columns: Sequence[tuple[str, str, bool, str | None]],
    ) -> tuple[tuple[str, str, bool, str | None], ...]:
        """Expose the value-guarded native shape to schema evolution."""

        return project_mssql_schema_evolution_columns(load_config, columns)

    def __init__(
        self,
        connector: Any,
        logger: Any,
        staging_manager: Any,
        state_storage: Any = None,
        transaction_finalizer_factory: Any | None = None,
        staging_consumer_factory: Any | None = None,
    ) -> None:
        self.connector = connector
        self.logger = logger
        self.staging_manager = staging_manager
        self.state_storage = state_storage
        self.transaction_finalizer_factory = transaction_finalizer_factory
        self.staging_consumer_factory = staging_consumer_factory

    def _consume_with_staging(self, load_config: Any, payload: LoadPayload, handler: Any) -> LoadResult:
        factory = self.staging_consumer_factory or MssqlStagingConsumer
        return factory(self).consume(load_config, payload, handler)

    def _materialize(
        self,
        load_config: Any,
        payload: LoadPayload,
        *,
        staging_schema: Sequence[tuple[str, str]] | None = None,
    ) -> StagingTableArtifact:
        if isinstance(payload.artifact, InternalQueryArtifact):
            raise RuntimeError("mssql_transaction.internal_query_payload_evidence_required")
        options = dict(getattr(load_config, "options", {}) or {})
        options["__dpone_consumed_source_provenance_sha256"] = canonical_source_provenance_sha256(
            relation_dialect=payload.relation_dialect,
            relation_schema=payload.relation_schema,
            relation_metadata=payload.relation_metadata,
            fallback_schema=payload.schema,
        )
        materialization_config = replace(load_config, options=options)
        return payload.artifact.materialize(
            self.staging_manager,
            materialization_config,
            staging_schema or payload.schema,
        )

    def _ensure_target_table(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
        *,
        staging: StagingTableArtifact | None = None,
    ) -> bool:
        if self._table_exists(load_config):
            return False
        if (getattr(load_config, "options", {}) or {}).get(MSSQL_TRANSACTION_ADMISSION_OPTION) is not None:
            raise RuntimeError("mssql_transaction.unplanned_target_creation_forbidden")
        self._ensure_schema(load_config.target_schema, database=getattr(load_config, "target_database", None))
        self._execute_create_table(load_config, schema, staging=staging)
        return True

    def _execute_create_table(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
        *,
        table: str | None = None,
        staging: StagingTableArtifact | None = None,
    ) -> None:
        if staging is None:
            data_schema = self._data_schema(schema)
            projected = resolve_native_column_types(load_config, data_schema)
            nullability: dict[str, bool] = {}
            collations: dict[str, str] = {}
        else:
            # The extraction payload keeps immutable source/wire names.  Native
            # staging is the single resolved target projection authority and
            # may intentionally use different target names after schema
            # evolution.  Fresh target/shadow DDL must therefore derive its
            # ordered shape exclusively from that native artifact.
            data_schema = [
                (column, staging.target_column_types[column]) for column in self._data_columns(staging.columns)
            ]
            data_columns = {column for column, _dtype in data_schema}
            projected = {
                column: dtype for column, dtype in (staging.target_column_types or {}).items() if column in data_columns
            }
            if set(projected) != data_columns:
                raise ValueError("mssql_native_projection.target_shape_incomplete")
            nullability = {
                column: nullable
                for column, nullable in (staging.target_column_nullability or {}).items()
                if column in data_columns
            }
            collations = {
                column: collation
                for column, collation in (staging.target_column_collations or {}).items()
                if column in data_columns
            }
        self.connector.execute_query(
            render_load_strategy_create_table(
                options=getattr(load_config, "options", {}),
                qualified_table=self._target_name(load_config, table=table),
                columns=[(column, projected[column]) for column, _dtype in data_schema],
                quote_identifier=self.connector.quote_identifier,
                to_mssql_type=MSSQLTypeMapper.to_mssql,
                nullability=nullability,
                collations=collations,
            )
        )
        if staging is not None:
            MssqlGenericTargetContract(self).create_unique_authority(
                load_config,
                staging,
                table=table or load_config.target_table,
            )

    def _create_shadow_table(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
        *,
        staging: StagingTableArtifact | None = None,
    ) -> str:
        self._ensure_schema(load_config.target_schema, database=getattr(load_config, "target_database", None))
        shadow_table = f"{load_config.target_table}__dpone_shadow_{uuid.uuid4().hex[:8]}"
        self._execute_create_table(load_config, schema, table=shadow_table, staging=staging)
        return shadow_table

    def _insert_from_staging_to_table(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        destination_table: str,
        *,
        table_lock: bool = False,
    ) -> int:
        data_columns = self._data_columns(staging.columns)
        columns = ", ".join(self.connector.quote_identifier(column) for column in data_columns)
        select_columns = ", ".join(self._staging_select_expression(staging, column, "s") for column in data_columns)
        table_hint = " WITH (TABLOCK)" if table_lock else ""
        return self._execute_counted_dml(
            f"INSERT INTO {self._target_name(load_config, table=destination_table)}{table_hint} ({columns}) "
            f"SELECT {select_columns} FROM {self._staging_name(staging)} AS s"
        )

    def _insert_from_staging(self, load_config: Any, staging: StagingTableArtifact) -> int:
        return self._insert_from_staging_to_table(load_config, staging, load_config.target_table)

    def _swap_shadow_into_target(self, load_config: Any, shadow_table: str) -> None:
        target_exists = self._table_exists(load_config)
        backup_table = f"{load_config.target_table}__dpone_backup_{uuid.uuid4().hex[:8]}"
        target = self._target_object(load_config)
        for statement in mssql_shadow_swap_statements(
            target=target,
            shadow_table=shadow_table,
            backup_table=backup_table,
            target_existed=target_exists,
        ):
            self.connector.execute_query(statement)

    def _copy_target_excluding_staging_keys(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        shadow_table: str,
        unique_key: str | Sequence[str],
    ) -> int:
        columns = ", ".join(self.connector.quote_identifier(column) for column in self._data_columns(staging.columns))
        return self._execute_counted_dml(
            f"INSERT INTO {self._target_name(load_config, table=shadow_table)} ({columns}) "
            f"SELECT {columns} FROM {self._target_name(load_config)} AS t "
            f"WHERE NOT EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )

    def _copy_target_excluding_predicate(self, load_config: Any, shadow_table: str, columns: Sequence[str]) -> int:
        columns_sql = ", ".join(self.connector.quote_identifier(column) for column in self._data_columns(columns))
        return self._execute_counted_dml(
            f"INSERT INTO {self._target_name(load_config, table=shadow_table)} ({columns_sql}) "
            f"SELECT {columns_sql} FROM {self._target_name(load_config)} "
            f"WHERE NOT ({load_config.custom_predicate})"
        )

    def _count_target_matching_staging_keys(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        rows = self.connector.get_records(
            f"SELECT COUNT_BIG(*) FROM {self._target_name(load_config)} AS t "
            f"WHERE EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )
        return int(rows[0][0]) if rows else 0

    def _validate_staging_duplicates(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> None:
        validate_duplicate_policy(load_config)
        key_columns = [unique_key] if isinstance(unique_key, str) else list(unique_key)
        columns = ", ".join(self._key_expression(staging, "s", column) for column in key_columns)
        rows = self.connector.get_records(
            f"SELECT TOP 1 COUNT_BIG(*) AS duplicate_count "
            f"FROM {self._staging_name(staging)} AS s "
            f"GROUP BY {columns} HAVING COUNT_BIG(*) > 1"
        )
        if rows:
            raise ValueError(
                "MSSQL incremental_merge staging contains duplicate unique_key values. "
                "Default duplicate_policy=fail rejected the batch before target mutation."
            )

    def _delete_target_matching_staging_keys(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        return self._execute_counted_dml(
            f"DELETE t FROM {self._target_name(load_config)} AS t "
            f"WHERE EXISTS (SELECT 1 FROM {self._staging_name(staging)} AS s "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )

    def _update_target_from_staging(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
        *,
        where: str | None = None,
    ) -> int:
        """Update non-key columns from staging without SQL Server ``MERGE``."""

        keys = set([unique_key] if isinstance(unique_key, str) else unique_key)
        update_columns = [column for column in self._data_columns(staging.columns) if column not in keys]
        if not update_columns:
            return 0
        from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
            staging_scalar_expression,
        )

        assignments = ", ".join(
            f"t.{self.connector.quote_identifier(column)} = {staging_scalar_expression(self, staging, column, 's')}"
            for column in update_columns
        )
        predicate = f" WHERE {where}" if where else ""
        return self._execute_counted_dml(
            f"UPDATE t SET {assignments} "
            f"FROM {self._target_name(load_config)} AS t "
            f"INNER JOIN {self._staging_name(staging)} AS s "
            f"ON {self._key_condition(staging, 's', 't', unique_key)}{predicate}"
        )

    def _insert_missing_staging_rows(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        unique_key: str | Sequence[str],
    ) -> int:
        """Insert staging keys absent from target as a separate set operation."""

        columns = self._data_columns(staging.columns)
        target_columns = ", ".join(self.connector.quote_identifier(column) for column in columns)
        source_columns = ", ".join(self._staging_select_expression(staging, column, "s") for column in columns)
        return self._execute_counted_dml(
            f"INSERT INTO {self._target_name(load_config)} ({target_columns}) "
            f"SELECT {source_columns} FROM {self._staging_name(staging)} AS s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {self._target_name(load_config)} AS t "
            f"WHERE {self._key_condition(staging, 's', 't', unique_key)})"
        )

    def _delete_target_matching_partition_values(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        partition_column: str,
        *,
        plan: MssqlPartitionFallbackPlan | None = None,
    ) -> int:
        resolved = plan or plan_mssql_partition_fallback(staging, partition_column)
        return self._execute_counted_dml(
            resolved.delete_sql(
                target_name=self._target_name(load_config),
                staging_name=self._staging_name(staging),
                quote_identifier=self.connector.quote_identifier,
            )
        )

    def _count_target_matching_predicate(self, load_config: Any) -> int:
        return self._count_target_matching_sql(load_config, str(load_config.custom_predicate), ())

    def _count_target_matching_sql(
        self,
        load_config: Any,
        predicate_sql: str,
        params: Sequence[Any] = (),
    ) -> int:
        rows = self.connector.get_records(
            f"SELECT COUNT_BIG(*) FROM {self._target_name(load_config)} WHERE {predicate_sql}",
            params,
        )
        return int(rows[0][0]) if rows else 0

    def _count_target(self, load_config: Any) -> int:
        rows = self.connector.get_records(f"SELECT COUNT_BIG(*) FROM {self._target_name(load_config)}")
        return int(rows[0][0]) if rows else 0

    def _execute_counted_dml(
        self,
        statement: str,
        params: Sequence[Any] | None = None,
    ) -> int:
        return execute_counted_dml(self.connector, statement, params)


__all__ = ["MSSQLStrategyBase"]
