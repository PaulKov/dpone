"""ClickHouse snapshot_diff / scd2 finalizers for staged loads."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
    require_unique_key,
    resolve_merge_policy,
)


class ClickHouseProductionFinalizer:
    """Staging-first production finalizers mirroring Postgres/MSSQL/BQ semantics."""

    def __init__(self, sink: Any) -> None:
        self._sink = sink

    def snapshot_diff(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        unique_key = require_unique_key(load_config)
        delete_policy = _strategy_option(load_config, "diff", "delete_policy", default="hard_delete")
        staging = _finalization_config(handle)
        finalizer = self._sink._staging_finalizer

        if not self._sink._table_exists(load_config):
            self._sink._swap_table_into_target(load_config, staging)
            return LoadResult(
                inserted_rows=handle.staged_rows,
                updated_rows=0,
                total_rows=self._sink._count(load_config),
                staging_rows=handle.staged_rows,
            )

        deleted_missing = self._apply_missing_key_policy(load_config, staging, unique_key, delete_policy)
        updated = finalizer.count_target_matching_staging_keys(load_config, staging, unique_key)
        inserted = self._reload_matching_keys(load_config, staging, unique_key)
        return LoadResult(
            inserted_rows=max(0, inserted - updated),
            updated_rows=updated,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
            replaced_rows=deleted_missing if delete_policy == "hard_delete" else 0,
            soft_deleted_rows=deleted_missing if delete_policy == "soft_delete" else 0,
        )

    def scd2(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        unique_key = require_unique_key(load_config)
        columns = _Scd2Columns.from_config(load_config)
        staging = _finalization_config(handle)

        if not self._sink._table_exists(load_config):
            self._sink._swap_table_into_target(load_config, staging)
            return LoadResult(
                inserted_rows=handle.staged_rows,
                updated_rows=0,
                total_rows=self._sink._count(load_config),
                staging_rows=handle.staged_rows,
            )

        expired_changed = self._expire_changed_current_rows(load_config, staging, unique_key, columns)
        expired_deleted = self._apply_scd2_delete_policy(load_config, staging, unique_key, columns)
        inserted = self._insert_new_current_versions(load_config, staging, unique_key, columns)
        return LoadResult(
            inserted_rows=inserted,
            updated_rows=expired_changed + expired_deleted,
            total_rows=self._sink._count(load_config),
            staging_rows=handle.staged_rows,
            soft_deleted_rows=expired_deleted,
        )

    def _apply_missing_key_policy(
        self,
        load_config: Any,
        staging: Any,
        unique_key: Sequence[str],
        delete_policy: str,
    ) -> int:
        if delete_policy == "ignore":
            return 0
        if delete_policy == "hard_delete":
            return self._delete_missing_keys(load_config, staging, unique_key)
        if delete_policy == "soft_delete":
            return self._soft_delete_missing_keys(load_config, staging, unique_key)
        raise ValueError(
            "Unsupported ClickHouse snapshot_diff delete_policy. Supported values: hard_delete, soft_delete, ignore."
        )

    def _delete_missing_keys(self, load_config: Any, staging: Any, unique_key: Sequence[str]) -> int:
        before = self._sink._count(load_config)
        merge_policy = resolve_merge_policy(load_config, "clickhouse")
        finalizer = self._sink._staging_finalizer
        if merge_policy in {MergePolicy.LIGHTWEIGHT_DELETE_INSERT, MergePolicy.SHADOW_SWAP}:
            finalizer.lightweight_delete_missing_from_staging(load_config, staging, unique_key)
        elif merge_policy == MergePolicy.MUTATION_DELETE_INSERT:
            finalizer.mutation_delete_missing_from_staging(load_config, staging, unique_key)
        else:
            raise ValueError(f"Unsupported ClickHouse merge_policy: {merge_policy}")
        return max(0, before - self._sink._count(load_config))

    def _soft_delete_missing_keys(self, load_config: Any, staging: Any, unique_key: Sequence[str]) -> int:
        deleted_at = TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT)
        where = (
            f"`{deleted_at}` IS NULL AND NOT ("
            f"{self._sink._staging_finalizer.key_in_staging_condition(staging, unique_key)})"
        )
        before = self._sink._count_where(load_config, where)
        self._sink._staging_finalizer.mutation_update(
            load_config,
            assignments=f"`{deleted_at}` = now64(6)",
            where=where,
        )
        return before

    def _reload_matching_keys(self, load_config: Any, staging: Any, unique_key: Sequence[str]) -> int:
        merge_policy = resolve_merge_policy(load_config, "clickhouse")
        finalizer = self._sink._staging_finalizer
        if merge_policy == MergePolicy.LIGHTWEIGHT_DELETE_INSERT:
            finalizer.lightweight_delete_matching_staging_keys(load_config, staging, unique_key)
            return self._sink._insert_from_table(staging, load_config)
        if merge_policy == MergePolicy.MUTATION_DELETE_INSERT:
            finalizer.mutation_delete_matching_staging_keys(load_config, staging, unique_key)
            return self._sink._insert_from_table(staging, load_config)
        if merge_policy == MergePolicy.SHADOW_SWAP:
            return self._shadow_swap_reload(load_config, staging, unique_key)
        raise ValueError(f"Unsupported ClickHouse merge_policy: {merge_policy}")

    def _shadow_swap_reload(self, load_config: Any, staging: Any, unique_key: Sequence[str]) -> int:
        shadow = self._sink._create_shadow_table(load_config)
        try:
            self._sink._staging_finalizer.copy_target_to_shadow_excluding_staging_keys(
                load_config, staging, shadow, unique_key
            )
            inserted = self._sink._insert_from_table(staging, shadow)
            self._sink._swap_table_into_target(load_config, shadow)
            return inserted
        except Exception:
            self._sink._drop_table(self._sink._table(shadow), shadow)
            raise

    def _expire_changed_current_rows(
        self,
        load_config: Any,
        staging: Any,
        unique_key: Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        key_in_staging = self._sink._staging_finalizer.key_in_staging_condition(staging, unique_key)
        key_hash = _key_hash_tuple_sql(unique_key, columns.row_hash)
        staging_table = self._sink._table(staging)
        where = (
            f"`{columns.is_current}` = 1 AND {key_in_staging} AND "
            f"({key_hash}) NOT IN ("
            f"SELECT {key_hash} FROM {staging_table}"
            f")"
        )
        before = self._sink._count_where(load_config, where)
        self._sink._staging_finalizer.mutation_update(
            load_config,
            assignments=f"`{columns.valid_to}` = now64(6), `{columns.is_current}` = 0",
            where=where,
        )
        return before

    def _apply_scd2_delete_policy(
        self,
        load_config: Any,
        staging: Any,
        unique_key: Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        if columns.delete_policy == "ignore":
            return 0
        if columns.delete_policy != "expire":
            raise ValueError("ClickHouse scd2 supports delete_policy values: expire, ignore.")
        key_in_staging = self._sink._staging_finalizer.key_in_staging_condition(staging, unique_key)
        where = f"`{columns.is_current}` = 1 AND NOT ({key_in_staging})"
        before = self._sink._count_where(load_config, where)
        self._sink._staging_finalizer.mutation_update(
            load_config,
            assignments=f"`{columns.valid_to}` = now64(6), `{columns.is_current}` = 0",
            where=where,
        )
        return before

    def _insert_new_current_versions(
        self,
        load_config: Any,
        staging: Any,
        unique_key: Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        target = self._sink._table(load_config)
        staging_table = self._sink._table(staging)
        key_hash = _key_hash_tuple_sql(unique_key, columns.row_hash)
        from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy

        settings = ClickHouseNullInsertPolicy.from_load_config(load_config).insert_select_settings_clause()
        before = self._sink._count(load_config)
        self._sink.connector.execute_query(
            f"INSERT INTO {target}{settings} SELECT * FROM {staging_table} AS s "
            f"WHERE ({key_hash}) NOT IN ("
            f"SELECT {key_hash} FROM {target} "
            f"WHERE `{columns.is_current}` = 1"
            f")"
        )
        return max(0, self._sink._count(load_config) - before)


def _finalization_config(handle: StagedLoadHandle) -> Any:
    return handle.finalization_config or handle.staging_config


def _key_hash_tuple_sql(unique_key: Sequence[str], row_hash: str) -> str:
    key_parts = ", ".join(f"`{column}`" for column in unique_key)
    hash_part = f"ifNull(toString(`{row_hash}`), '')"
    if len(unique_key) == 1:
        return f"{key_parts}, {hash_part}"
    return f"{key_parts}, {hash_part}"


class _Scd2Columns:
    def __init__(
        self,
        *,
        valid_to: str,
        is_current: str,
        row_hash: str,
        delete_policy: str,
    ) -> None:
        self.valid_to = valid_to
        self.is_current = is_current
        self.row_hash = row_hash
        self.delete_policy = delete_policy

    @classmethod
    def from_config(cls, load_config: Any) -> _Scd2Columns:
        options = _strategy_options(load_config, "scd2")
        catalog = TechnicalColumnCatalog()
        return cls(
            valid_to=str(options.get("valid_to_column", catalog.name(TechnicalColumnRole.VALID_TO_AT))),
            is_current=str(options.get("current_flag_column", catalog.name(TechnicalColumnRole.IS_CURRENT))),
            row_hash=str(options.get("row_hash_column", catalog.name(TechnicalColumnRole.ROW_HASH))),
            delete_policy=str(options.get("delete_policy", "expire")),
        )


def _strategy_options(load_config: Any, section: str) -> dict[str, Any]:
    options = getattr(load_config, "options", {}) or {}
    nested = options.get(section)
    return nested if isinstance(nested, dict) else {}


def _strategy_option(load_config: Any, section: str, key: str, *, default: str) -> str:
    return str(_strategy_options(load_config, section).get(key, default))


__all__ = ["ClickHouseProductionFinalizer"]
