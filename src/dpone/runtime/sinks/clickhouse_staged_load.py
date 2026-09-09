"""Staged ClickHouse load lifecycle used by governed and legacy loads."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.governance.ports import (
    StagedLoadHandle,
    StagedLoadPostCommitCleanupError,
    staged_load_commit_unknown_details,
    staged_load_failure_details,
)
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.clickhouse_production_finalize import ClickHouseProductionFinalizer
from dpone.runtime.sinks.clickhouse_staged_evidence import staged_handle_metadata
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
    require_unique_key,
    resolve_merge_policy,
    validate_partition_replace,
)
from dpone.runtime.sinks.strategies.backfill import backfill_inner_strategy


class ClickHouseStagedLoadService:
    """Split ClickHouse bulk loading into stage, finalize, abort and cleanup."""

    def __init__(
        self,
        sink: Any,
        *,
        plan_staging_table: Callable[[Any], Any] | None = None,
        create_planned_staging_table: Callable[[Any, Any, Any], None] | None = None,
    ) -> None:
        if (plan_staging_table is None) != (create_planned_staging_table is None):
            raise ValueError("clickhouse_staged_load_plan_callbacks_incomplete")
        self._sink = sink
        self._plan_staging_table = plan_staging_table
        self._create_planned_staging_table = create_planned_staging_table
        self._production = ClickHouseProductionFinalizer(sink)

    def stage(self, load_config: Any, payload: Any) -> StagedLoadHandle:
        load_config = self._effective_config(load_config)
        staging_config = self._create_staging(load_config, payload)
        finalization_config = None
        decoded_config = None
        try:
            staged_rows = self._sink._insert_payload(staging_config, payload)
            finalization_config, decoded_config = self._sink._staging_decoder.prepare(
                load_config,
                staging_config,
                payload,
            )
        except BaseException as error:
            try:
                self._drop_configs(
                    finalization_config,
                    decoded_config,
                    staging_config,
                )
            except Exception as cleanup_error:
                add_exception_note(error, f"raw staging cleanup failed: {type(cleanup_error).__name__}")
            raise
        return StagedLoadHandle(
            staging_config=staging_config,
            payload_schema=tuple(getattr(payload, "schema", ())),
            staged_rows=staged_rows,
            finalization_config=finalization_config,
            decoded_config=decoded_config,
            metadata=staged_handle_metadata(load_config, staging_config, finalization_config, decoded_config, payload),
        )

    def load(self, load_config: Any, payload: Any) -> LoadResult:
        """Run the direct staged lifecycle while preserving its primary error."""

        handle = self.stage(load_config, payload)
        try:
            validation_token = self.validate(load_config, handle)
        except Exception as error:
            cleanup_status = "succeeded"
            try:
                self.abort(handle)
            except Exception as cleanup_error:
                cleanup_status = "failed"
                add_exception_note(error, f"staged cleanup failed: {type(cleanup_error).__name__}")
            setattr(
                error,
                "details",
                staged_load_failure_details(
                    error,
                    handle,
                    cleanup_status=cleanup_status,
                ),
            )
            raise
        try:
            result = self.finalize_validated(load_config, handle, validation_token)
        except Exception as error:
            setattr(error, "details", staged_load_commit_unknown_details(error, handle))
            raise
        try:
            self.cleanup(handle)
        except Exception as error:
            raise StagedLoadPostCommitCleanupError(error, handle) from error
        return result

    def finalize(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        load_config = self._effective_config(load_config)
        validation_token = self.validate(load_config, handle)
        return self.finalize_validated(load_config, handle, validation_token)

    def finalize_validated(
        self,
        load_config: Any,
        handle: StagedLoadHandle,
        validation_token: object,
    ) -> LoadResult:
        """Finalize a handle whose exact effective table already passed validation."""

        load_config = self._effective_config(load_config)
        self._sink._staging_finalizer.require_strategy_staging_validation(
            validation_token,
            load_config,
            self._finalization_config(handle),
        )
        prepare = getattr(self._sink, "_prepare_staged_finalization", None)
        if callable(prepare):
            prepare(load_config, handle)
        strategy = load_config.load_strategy
        if strategy == LoadStrategy.FULL_REFRESH:
            return self._full_refresh(load_config, handle)
        if strategy == LoadStrategy.INCREMENTAL_APPEND:
            return self._incremental_append(load_config, handle)
        if strategy == LoadStrategy.REPLACE:
            return self._replace(load_config, handle)
        if strategy == LoadStrategy.INCREMENTAL_MERGE:
            return self._incremental_merge(load_config, handle)
        if strategy == LoadStrategy.PARTITION_REPLACE:
            return self._partition_replace(load_config, handle)
        if strategy == LoadStrategy.SNAPSHOT_DIFF:
            return self._production.snapshot_diff(load_config, handle)
        if strategy == LoadStrategy.SCD2:
            return self._production.scd2(load_config, handle)
        raise ValueError(f"Unsupported ClickHouse load strategy: {strategy.value}")

    def validate(self, load_config: Any, handle: StagedLoadHandle) -> object:
        """Validate the exact post-projection table before target finalization."""

        load_config = self._effective_config(load_config)
        return self._sink._staging_finalizer.validate_strategy_staging_key_integrity(
            load_config,
            self._finalization_config(handle),
        )

    def _effective_config(self, load_config: Any) -> Any:
        """Map ``backfill`` to its configured inner strategy for staging/finalize."""

        if load_config.load_strategy != LoadStrategy.BACKFILL:
            return load_config
        return replace(load_config, load_strategy=backfill_inner_strategy(load_config))

    def cleanup(self, handle: StagedLoadHandle) -> None:
        finalizer = getattr(self._sink, "_staging_finalizer", None)
        retire = getattr(finalizer, "retire_strategy_staging_validations", None)
        if callable(retire):
            retire(self._finalization_config(handle))
        self._drop_configs(
            handle.finalization_config,
            handle.decoded_config,
            handle.staging_config,
        )

    def abort(self, handle: StagedLoadHandle) -> None:
        self.cleanup(handle)

    def _create_staging(self, load_config: Any, payload: Any) -> Any:
        if self._plan_staging_table is not None:
            staging_config = self._plan_staging_table(load_config)
            try:
                assert self._create_planned_staging_table is not None
                self._create_planned_staging_table(load_config, staging_config, payload)
            except BaseException as error:
                try:
                    self._drop_configs(staging_config)
                except Exception as cleanup_error:
                    add_exception_note(error, f"planned staging cleanup failed: {type(cleanup_error).__name__}")
                raise
            return staging_config
        if load_config.load_strategy != LoadStrategy.PARTITION_REPLACE:
            return self._sink._create_payload_staging_table(load_config, payload)

        staging_schema = self._sink._staging_decoder.staging_schema(load_config, payload)
        if self._sink._table_exists(load_config) and not staging_schema.clickhouse_types:
            return self._sink._create_staging_like_target_table(load_config)
        if staging_schema.clickhouse_types:
            return self._sink._create_typed_staging_table(load_config, staging_schema.columns)
        return self._sink._create_staging_table(load_config, staging_schema.columns)

    def _full_refresh(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        self._sink._swap_table_into_target(load_config, self._finalization_config(handle))
        return LoadResult(
            inserted_rows=handle.staged_rows,
            updated_rows=0,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
        )

    def _incremental_append(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        if self._sink._table_exists(load_config):
            inserted = self._sink._insert_from_table(self._finalization_config(handle), load_config)
        else:
            inserted = handle.staged_rows
            self._sink._swap_table_into_target(load_config, self._finalization_config(handle))
        return LoadResult(
            inserted_rows=inserted,
            updated_rows=0,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
        )

    def _replace(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        if not load_config.custom_predicate:
            raise ValueError("ClickHouse replace requires sink.strategy.custom_predicate")
        if not self._sink._table_exists(load_config):
            self._sink._swap_table_into_target(load_config, self._finalization_config(handle))
            return LoadResult(
                inserted_rows=handle.staged_rows,
                updated_rows=0,
                total_rows=self._sink._count(load_config),
                staging_rows=handle.staged_rows,
                replaced_rows=0,
            )

        shadow_config = self._sink._create_shadow_table(load_config)
        try:
            replaced = self._sink._count_where(load_config, load_config.custom_predicate)
            self._sink._staging_finalizer.copy_target_to_shadow_excluding_predicate(load_config, shadow_config)
            inserted = self._sink._insert_from_table(self._finalization_config(handle), shadow_config)
            self._sink._swap_table_into_target(load_config, shadow_config)
        except Exception:
            self._sink._drop_table(self._sink._table(shadow_config), shadow_config)
            raise
        return LoadResult(
            inserted_rows=inserted,
            updated_rows=0,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
            replaced_rows=replaced,
        )

    def _incremental_merge(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        unique_key = require_unique_key(load_config)
        finalization_config = self._finalization_config(handle)
        if not self._sink._table_exists(load_config):
            self._sink._swap_table_into_target(load_config, finalization_config)
            return LoadResult(
                inserted_rows=handle.staged_rows,
                updated_rows=0,
                total_rows=self._sink._count(load_config),
                staging_rows=handle.staged_rows,
            )
        return self._merge_existing_target(load_config, handle, unique_key)

    def _merge_existing_target(
        self,
        load_config: Any,
        handle: StagedLoadHandle,
        unique_key: Sequence[str],
    ) -> LoadResult:
        finalization_config = self._finalization_config(handle)
        updated = self._sink._staging_finalizer.count_target_matching_staging_keys(
            load_config, finalization_config, unique_key
        )
        merge_policy = resolve_merge_policy(load_config, "clickhouse")
        if merge_policy == MergePolicy.LIGHTWEIGHT_DELETE_INSERT:
            self._sink._staging_finalizer.lightweight_delete_matching_staging_keys(
                load_config, finalization_config, unique_key
            )
            inserted = self._sink._insert_from_table(finalization_config, load_config)
        elif merge_policy == MergePolicy.MUTATION_DELETE_INSERT:
            self._sink._staging_finalizer.mutation_delete_matching_staging_keys(
                load_config, finalization_config, unique_key
            )
            inserted = self._sink._insert_from_table(finalization_config, load_config)
        elif merge_policy == MergePolicy.SHADOW_SWAP:
            inserted = self._shadow_swap_merge(load_config, finalization_config, unique_key)
        else:
            raise ValueError(f"Unsupported ClickHouse merge_policy: {merge_policy}")
        return LoadResult(
            inserted_rows=max(0, inserted - updated),
            updated_rows=updated,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
        )

    def _shadow_swap_merge(self, load_config: Any, finalization_config: Any, unique_key: Sequence[str]) -> int:
        shadow_config = self._sink._create_shadow_table(load_config)
        try:
            self._sink._staging_finalizer.copy_target_to_shadow_excluding_staging_keys(
                load_config, finalization_config, shadow_config, unique_key
            )
            inserted = self._sink._insert_from_table(finalization_config, shadow_config)
            self._sink._swap_table_into_target(load_config, shadow_config)
            return inserted
        except Exception:
            self._sink._drop_table(self._sink._table(shadow_config), shadow_config)
            raise

    def _partition_replace(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        partition = validate_partition_replace(load_config, "clickhouse")
        finalization_config = self._finalization_config(handle)
        if not self._sink._table_exists(load_config):
            self._sink._swap_table_into_target(load_config, finalization_config)
            return LoadResult(
                inserted_rows=handle.staged_rows,
                updated_rows=0,
                total_rows=self._sink._count(load_config),
                staging_rows=handle.staged_rows,
            )
        values = self._sink._staging_finalizer.partition_values_from_staging(
            finalization_config,
            partition.column,
            partition.value_expression,
        )
        if len(values) > partition.max_partitions_per_run:
            raise ValueError(
                "ClickHouse partition_replace would replace "
                f"{len(values)} partitions, above max_partitions_per_run={partition.max_partitions_per_run}."
            )
        for value in values:
            self._sink.connector.execute_query(
                f"ALTER TABLE {self._sink._table(load_config)}{self._sink._cluster_ddl_clause(load_config)} "
                f"REPLACE PARTITION {self._sink._literal(value)} "
                f"FROM {self._sink._table(finalization_config)}"
            )
        return LoadResult(
            inserted_rows=handle.staged_rows,
            updated_rows=0,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
            replaced_rows=len(values),
        )

    @staticmethod
    def _finalization_config(handle: StagedLoadHandle) -> Any:
        return handle.finalization_config or handle.staging_config

    def _drop_configs(self, *configs: Any | None) -> None:
        seen: set[str] = set()
        first_error: Exception | None = None
        for config in configs:
            if config is None or not getattr(config, "target_table", None):
                continue
            table = self._sink._table(config)
            if table in seen:
                continue
            seen.add(table)
            try:
                self._sink._drop_table(table, config)
            except Exception as error:
                if first_error is None:
                    first_error = error
                else:
                    add_exception_note(
                        first_error,
                        f"additional staging cleanup failed: {type(error).__name__}",
                    )
        if first_error is not None:
            raise first_error


__all__ = ["ClickHouseStagedLoadService"]
