"""Native SQL Server partition switch helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_partition_fallback import (
    MssqlPartitionFallbackPlan,
    plan_mssql_partition_fallback,
)


class MSSQLPartitionSwitchMixin:
    if TYPE_CHECKING:
        # The concrete strategy supplies these capabilities through its other
        # mixins. Keep this dependency contract static-only so MRO behavior is
        # unchanged at runtime.
        connector: Any
        logger: Any

        def _staging_table_exists(
            self,
            schema: str,
            table: str,
            *,
            database: str | None = None,
        ) -> bool: ...

        def _target_name(self, load_config: Any, *, table: str | None = None) -> str: ...

        def _qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str: ...

        def _staging_name(self, staging: StagingTableArtifact) -> str: ...

        def _count_target(self, load_config: Any) -> int: ...

    def _try_native_partition_switch(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        partition,
    ) -> LoadResult | None:
        if not partition.native:
            return None

        raw_partition = getattr(load_config, "partition", {}) or {}
        switch_out_template = raw_partition.get("switch_out_table_template")
        if not switch_out_template:
            self._warn_native_partition_fallback(
                load_config,
                "partition.switch_out_table_template is required for safe MSSQL SWITCH replacement",
            )
            return None

        partition_function = raw_partition.get("partition_function") or self._discover_partition_function(
            load_config.target_schema, load_config.target_table
        )
        if not partition_function:
            self._warn_native_partition_fallback(load_config, "target partition function could not be discovered")
            return None
        staging_function = self._discover_partition_function(staging.schema, staging.table)
        if staging_function != partition_function:
            self._warn_native_partition_fallback(
                load_config,
                "staging table is not aligned to the same partition function as target",
            )
            return None
        if not self._has_switch_compatible_index_shape(load_config.target_schema, load_config.target_table):
            self._warn_native_partition_fallback(
                load_config,
                "target has secondary indexes; automatic SWITCH supports heaps/clustered aligned tables only",
            )
            return None

        values = self._validated_partition_values(staging, partition)

        switch_plan: list[tuple[int, str, str]] = []
        switch_out_schema = str(raw_partition.get("switch_out_schema") or load_config.target_schema)
        switch_out_database = None if "." in switch_out_schema else getattr(load_config, "target_database", None)
        for value in values:
            partition_number = self._mssql_partition_number(partition_function, value)
            switch_out_table = str(switch_out_template).format(
                partition=partition_number,
                value=self._safe_template_value(value),
                target=load_config.target_table,
            )
            if not self._staging_table_exists(
                switch_out_schema,
                switch_out_table,
                database=switch_out_database,
            ):
                self._warn_native_partition_fallback(
                    load_config,
                    f"switch-out table {switch_out_schema}.{switch_out_table} does not exist",
                )
                return None
            if self._count_table(switch_out_schema, switch_out_table, database=switch_out_database) != 0:
                raise ValueError(
                    "MSSQL native partition switch requires empty switch-out table "
                    f"{switch_out_schema}.{switch_out_table} for partition {partition_number}."
                )
            switch_plan.append((partition_number, switch_out_schema, switch_out_table))

        for partition_number, switch_out_schema, switch_out_table in switch_plan:
            self.connector.execute_query(
                f"ALTER TABLE {self._target_name(load_config)} "
                f"SWITCH PARTITION {partition_number} TO "
                f"{self._qualified_name(switch_out_schema, switch_out_table, database=switch_out_database)} "
                f"PARTITION {partition_number}"
            )
            self.connector.execute_query(
                f"ALTER TABLE {self._staging_name(staging)} "
                f"SWITCH PARTITION {partition_number} TO "
                f"{self._target_name(load_config)} PARTITION {partition_number}"
            )

        return LoadResult(
            inserted_rows=staging.row_count,
            updated_rows=0,
            total_rows=self._count_target(load_config),
            replaced_rows=len(switch_plan),
        )

    def _discover_partition_function(self, schema: str, table: str) -> str | None:
        rows = self.connector.get_records(
            """
            SELECT TOP 1 pf.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
            JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
            WHERE s.name = ? AND t.name = ?
            """,
            (schema, table),
        )
        return str(rows[0][0]) if rows else None

    def _has_switch_compatible_index_shape(self, schema: str, table: str) -> bool:
        rows = self.connector.get_records(
            """
            SELECT COUNT_BIG(*)
            FROM sys.indexes i
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
              AND i.index_id > 1
              AND i.is_hypothetical = 0
            """,
            (schema, table),
        )
        return not rows or int(rows[0][0]) == 0

    def _partition_values_from_staging(
        self,
        staging: StagingTableArtifact,
        partition_column: str,
        *,
        plan: MssqlPartitionFallbackPlan | None = None,
    ) -> list[Any]:
        resolved = plan or plan_mssql_partition_fallback(staging, partition_column)
        rows = self.connector.get_records(
            resolved.validation_sql(
                staging_name=self._staging_name(staging),
                quote_identifier=self.connector.quote_identifier,
            )
        )
        return [row[0] for row in rows]

    def _validated_partition_values(
        self,
        staging: StagingTableArtifact,
        partition: Any,
        *,
        plan: MssqlPartitionFallbackPlan | None = None,
    ) -> list[Any]:
        """Apply the same cardinality/null guards to native and fallback paths."""

        values = self._partition_values_from_staging(staging, partition.column, plan=plan)
        if any(value is None for value in values):
            raise ValueError("MSSQL partition_replace rejects NULL partition identities before target mutation.")
        if len(values) > partition.max_partitions_per_run:
            raise ValueError(
                "MSSQL partition_replace would replace "
                f"{len(values)} partitions, above max_partitions_per_run={partition.max_partitions_per_run}."
            )
        return values

    def _mssql_partition_number(self, partition_function: str, partition_value: Any) -> int:
        rows = self.connector.get_records(
            f"SELECT $PARTITION.{self.connector.quote_identifier(partition_function)}(?)",
            (partition_value,),
        )
        if not rows:
            raise ValueError(f"MSSQL partition function {partition_function!r} returned no partition number")
        return int(rows[0][0])

    def _count_table(self, schema: str, table: str, *, database: str | None = None) -> int:
        rows = self.connector.get_records(
            f"SELECT COUNT_BIG(*) FROM {self._qualified_name(schema, table, database=database)}"
        )
        return int(rows[0][0]) if rows else 0

    @staticmethod
    def _safe_template_value(value: Any) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in str(value))[:64]

    def _warn_native_partition_fallback(self, load_config: Any, reason: str) -> None:
        if hasattr(self.logger, "warning"):
            self.logger.warning(
                "MSSQL native partition_replace unavailable for "
                f"{load_config.target_schema}.{load_config.target_table}: {reason}. "
                "Using staging-first partition predicate fallback unless native_mode=required."
            )


__all__ = ["MSSQLPartitionSwitchMixin"]
