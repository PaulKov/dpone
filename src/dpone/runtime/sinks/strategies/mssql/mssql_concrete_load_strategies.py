"""Concrete set-based SQL Server load strategies."""

from __future__ import annotations

from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.contracts.portable_scope_resolution import resolve_bound_portable_scope
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import IncrementalSnapshotEnvelope
from dpone.runtime.postgres_mssql_r1_execution import require_r1_execution_method
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_partition_fallback import (
    plan_mssql_partition_fallback,
    publish_mssql_partition_fallback_decision,
)
from dpone.runtime.sinks.strategies.mssql.mssql_partition_switch_mixin import MSSQLPartitionSwitchMixin
from dpone.runtime.sinks.strategies.mssql.mssql_portable_scope import render_mssql_portable_scope
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_base import MSSQLStrategyBase


class MSSQLFullRefreshStrategy(MSSQLStrategyBase):
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        execution = payload.postgres_mssql_r1_execution
        if execution is not None:
            return require_r1_execution_method(execution, "load_batch")(
                strategy=self,
                load_config=load_config,
                payload=payload,
            )
        contract = normalize_mssql_load_strategy(load_config)
        assert contract.full_refresh is not None

        def handler(staging: StagingTableArtifact) -> LoadResult:
            if self._table_exists(load_config):
                self.connector.execute_query(f"TRUNCATE TABLE {self._target_name(load_config)}")
            else:
                self._ensure_target_table(load_config, payload.schema, staging=staging)
            inserted = self._insert_from_staging_to_table(
                load_config,
                staging,
                load_config.target_table,
                table_lock=True,
            )
            return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

        return self._consume_with_staging(load_config, payload, handler)


class MSSQLIncrementAppendStrategy(MSSQLStrategyBase):
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        contract = normalize_mssql_load_strategy(load_config)
        policy = contract.incremental_append
        assert policy is not None

        def handler(staging: StagingTableArtifact) -> LoadResult:
            shadow_authority = require_shadow_append_authority(load_config)
            if shadow_authority is not None:
                if policy.only_new_rows:
                    raise RuntimeError("mssql_backfill_publication.shadow_append_must_insert_all")
                inserted = self._insert_from_staging_to_table(
                    load_config,
                    staging,
                    load_config.target_table,
                )
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)
            if policy.only_new_rows:
                self._validate_staging_duplicates(load_config, staging, policy.unique_key)
            target_exists = self._table_exists(load_config)
            if not target_exists:
                shadow_table = self._create_shadow_table(load_config, payload.schema, staging=staging)
                inserted = self._insert_from_staging_to_table(load_config, staging, shadow_table)
                self._swap_shadow_into_target(load_config, shadow_table)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            if not policy.only_new_rows:
                inserted = self._insert_from_staging(load_config, staging)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))
            condition = self._key_condition(staging, "s", "t", policy.unique_key)
            columns = ", ".join(self.connector.quote_identifier(column) for column in staging.columns)
            select_columns = ", ".join(
                self._staging_select_expression(staging, column, "s") for column in staging.columns
            )
            inserted = self._execute_counted_dml(
                f"INSERT INTO {self._target_name(load_config)} ({columns}) "
                f"SELECT {select_columns} FROM {self._staging_name(staging)} AS s "
                f"WHERE NOT EXISTS (SELECT 1 FROM {self._target_name(load_config)} AS t WHERE {condition})"
            )
            return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

        return self._consume_with_staging(load_config, payload, handler)


class MSSQLIncrementMergeStrategy(MSSQLStrategyBase):
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        execution = payload.postgres_mssql_r1_execution
        if execution is not None:
            return require_r1_execution_method(execution, "load_xmin")(
                strategy=self,
                load_config=load_config,
                payload=payload,
            )
        if isinstance(payload.artifact, IncrementalSnapshotEnvelope):
            from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_finalizer import (
                MssqlSnapshotFinalizer,
            )

            return MssqlSnapshotFinalizer(self, self.state_storage).load(
                load_config,
                payload,
                payload.artifact,
            )
        contract = normalize_mssql_load_strategy(load_config)
        policy = contract.incremental_merge
        assert policy is not None
        unique_key = policy.unique_key
        merge_policy = policy.merge_policy

        def handler(staging: StagingTableArtifact) -> LoadResult:
            self._validate_staging_duplicates(load_config, staging, unique_key)
            target_exists = self._table_exists(load_config)
            if not target_exists:
                shadow_table = self._create_shadow_table(load_config, payload.schema, staging=staging)
                inserted = self._insert_from_staging_to_table(load_config, staging, shadow_table)
                self._swap_shadow_into_target(load_config, shadow_table)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            updated = self._count_target_matching_staging_keys(load_config, staging, unique_key)
            if merge_policy == MergePolicy.UPDATE_INSERT:
                updated = self._update_target_from_staging(load_config, staging, unique_key)
                inserted = self._insert_missing_staging_rows(load_config, staging, unique_key)
                return LoadResult(
                    inserted_rows=inserted,
                    updated_rows=updated,
                    total_rows=self._count_target(load_config),
                )

            if merge_policy == MergePolicy.DELETE_INSERT:
                self._delete_target_matching_staging_keys(load_config, staging, unique_key)
                inserted = self._insert_from_staging(load_config, staging)
                return LoadResult(
                    inserted_rows=max(0, inserted - updated),
                    updated_rows=updated,
                    total_rows=self._count_target(load_config),
                )

            if merge_policy != MergePolicy.SHADOW_SWAP:
                raise ValueError(f"Unsupported MSSQL merge_policy: {merge_policy}")

            shadow_table = self._create_shadow_table(load_config, payload.schema, staging=staging)
            self._copy_target_excluding_staging_keys(load_config, staging, shadow_table, unique_key)
            inserted = self._insert_from_staging_to_table(load_config, staging, shadow_table)
            self._swap_shadow_into_target(load_config, shadow_table)
            return LoadResult(
                inserted_rows=max(0, inserted - updated),
                updated_rows=updated,
                total_rows=self._count_target(load_config),
            )

        return self._consume_with_staging(load_config, payload, handler)


class MSSQLPartitionReplaceStrategy(MSSQLPartitionSwitchMixin, MSSQLStrategyBase):
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        contract = normalize_mssql_load_strategy(load_config)
        partition = contract.partition_replace
        assert partition is not None
        if not partition.values_from_staging:
            raise ValueError("mssql_native.composed_staged_service_required")

        def handler(staging: StagingTableArtifact) -> LoadResult:
            target_exists = self._table_exists(load_config)
            if not target_exists:
                shadow_table = self._create_shadow_table(load_config, payload.schema, staging=staging)
                inserted = self._insert_from_staging_to_table(load_config, staging, shadow_table)
                self._swap_shadow_into_target(load_config, shadow_table)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            native_result = self._try_native_partition_switch(load_config, staging, partition)
            if native_result is not None:
                return native_result
            if partition.require_native:
                raise ValueError(
                    "MSSQL partition_replace native_mode=required could not use ALTER TABLE ... SWITCH PARTITION. "
                    "Ensure target and staging are aligned to the same partition function, secondary indexes are "
                    "compatible, and partition.switch_out_table_template points to empty aligned switch-out tables."
                )

            fallback_plan = plan_mssql_partition_fallback(staging, partition.column)
            self._validated_partition_values(staging, partition, plan=fallback_plan)
            publish_mssql_partition_fallback_decision(fallback_plan)
            replaced = self._delete_target_matching_partition_values(
                load_config,
                staging,
                partition.column,
                plan=fallback_plan,
            )
            inserted = self._insert_from_staging(load_config, staging)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=self._count_target(load_config),
                replaced_rows=replaced,
            )

        return self._consume_with_staging(load_config, payload, handler)


class MSSQLReplaceStrategy(MSSQLStrategyBase):
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        contract = normalize_mssql_load_strategy(load_config)
        policy = contract.replace
        assert policy is not None
        resolved_portable_scope = resolve_bound_portable_scope(load_config)
        portable_predicate = (
            render_mssql_portable_scope(
                resolved_portable_scope.scope,
                resolved_portable_scope.binding,
                quote_identifier=self.connector.quote_identifier,
            )
            if resolved_portable_scope is not None
            else None
        )

        def handler(staging: StagingTableArtifact) -> LoadResult:
            target_exists = self._table_exists(load_config)
            if not target_exists:
                self._ensure_target_table(load_config, payload.schema, staging=staging)
                inserted = self._insert_from_staging(load_config, staging)
                return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=self._count_target(load_config))

            if portable_predicate is None:
                replaced = self._count_target_matching_predicate(load_config)
                self.connector.execute_query(
                    f"DELETE FROM {self._target_name(load_config)} WHERE {load_config.custom_predicate}"
                )
            else:
                replaced = self._count_target_matching_sql(
                    load_config,
                    portable_predicate.sql,
                    portable_predicate.params,
                )
                self.connector.execute_query(
                    f"DELETE FROM {self._target_name(load_config)} WHERE {portable_predicate.sql}",
                    portable_predicate.params,
                )
            inserted = self._insert_from_staging(load_config, staging)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=self._count_target(load_config),
                replaced_rows=replaced,
            )

        return self._consume_with_staging(load_config, payload, handler)


__all__ = [
    "MSSQLFullRefreshStrategy",
    "MSSQLIncrementAppendStrategy",
    "MSSQLIncrementMergeStrategy",
    "MSSQLPartitionReplaceStrategy",
    "MSSQLReplaceStrategy",
]
