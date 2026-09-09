"""Production-grade BigQuery finalizers for diff/history load strategies."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.merge_policy import require_unique_key
from dpone.runtime.sinks.strategies.bigquery.bigquery_increment_merge import BigQueryIncrementMergeStrategy


class BigQuerySnapshotDiffStrategy(BigQueryIncrementMergeStrategy):
    """Apply snapshot diff semantics with BigQuery DML."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)
        delete_policy = _strategy_option(load_config, "diff", "delete_policy", default="hard_delete")
        fq_staging = _fq_staging(self.connector.project_id, load_config)
        self._validate_staging_duplicates(load_config, fq_staging, unique_key)

        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)
        if target_created:
            inserted = self._insert_all_from_staging(load_config, payload, fq_staging)
            return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

        deleted_missing = self._apply_missing_key_policy(load_config, fq_staging, unique_key, delete_policy)
        updated = self._count_target_matching_staging_keys(load_config, fq_staging, unique_key)
        self._delete_existing(load_config, fq_staging, unique_key)
        inserted = self._insert_all_from_staging(load_config, payload, fq_staging)
        return LoadResult(
            inserted_rows=max(0, inserted - updated),
            updated_rows=updated,
            total_rows=inserted,
            replaced_rows=deleted_missing if delete_policy == "hard_delete" else 0,
            soft_deleted_rows=deleted_missing if delete_policy == "soft_delete" else 0,
        )

    def _apply_missing_key_policy(
        self,
        load_config: Any,
        fq_staging: str,
        unique_key: str | Sequence[str],
        delete_policy: str,
    ) -> int:
        if delete_policy == "ignore":
            return 0
        if delete_policy == "hard_delete":
            return self._delete_target_missing_from_staging(load_config, fq_staging, unique_key)
        if delete_policy == "soft_delete":
            return self._soft_delete_target_missing_from_staging(load_config, fq_staging, unique_key)
        raise ValueError(
            "Unsupported BigQuery snapshot_diff delete_policy. Supported values: hard_delete, soft_delete, ignore."
        )

    def _delete_target_missing_from_staging(
        self,
        load_config: Any,
        fq_staging: str,
        unique_key: str | Sequence[str],
    ) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        return self._execute_dml_query(
            f"""
            DELETE FROM {fq_target} AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM {fq_staging} AS s
                WHERE {condition}
            )
            """
        )

    def _soft_delete_target_missing_from_staging(
        self,
        load_config: Any,
        fq_staging: str,
        unique_key: str | Sequence[str],
    ) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        deleted_at = TechnicalColumnCatalog().name(TechnicalColumnRole.DELETED_AT)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        return self._execute_dml_query(
            f"""
            UPDATE {fq_target} AS t
            SET `{deleted_at}` = CURRENT_TIMESTAMP()
            WHERE t.`{deleted_at}` IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {fq_staging} AS s
                  WHERE {condition}
              )
            """
        )

    def _insert_all_from_staging(self, load_config: Any, payload: LoadPayload, fq_staging: str) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=self._include_technical_columns(load_config),
        )
        return self._execute_dml_query(
            f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
        )


class BigQuerySCD2Strategy(BigQueryIncrementMergeStrategy):
    """Maintain BigQuery SCD2 history with staged DML."""

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        unique_key = require_unique_key(load_config)
        columns = _Scd2Columns.from_config(load_config)
        fq_staging = _fq_staging(self.connector.project_id, load_config)
        self._validate_staging_duplicates(load_config, fq_staging, unique_key)

        target_created = self._create_target_table_if_not_exists(load_config, payload.schema)
        if target_created:
            inserted = self._insert_all_from_staging(load_config, payload, fq_staging)
            return LoadResult(inserted_rows=inserted, updated_rows=0, total_rows=inserted)

        expired_changed = self._expire_changed_current_rows(load_config, fq_staging, unique_key, columns)
        expired_deleted = self._apply_delete_policy(load_config, fq_staging, unique_key, columns)
        inserted = self._insert_new_current_versions(load_config, payload, fq_staging, unique_key, columns)
        return LoadResult(
            inserted_rows=inserted,
            updated_rows=expired_changed + expired_deleted,
            total_rows=inserted,
            soft_deleted_rows=expired_deleted,
        )

    def _expire_changed_current_rows(
        self,
        load_config: Any,
        fq_staging: str,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        return self._execute_dml_query(
            f"""
            UPDATE {fq_target} AS t
            SET `{columns.valid_to}` = CURRENT_TIMESTAMP(),
                `{columns.is_current}` = FALSE
            WHERE t.`{columns.is_current}` IS TRUE
              AND EXISTS (
                  SELECT 1
                  FROM {fq_staging} AS s
                  WHERE {condition}
                    AND COALESCE(CAST(s.`{columns.row_hash}` AS STRING), '')
                        != COALESCE(CAST(t.`{columns.row_hash}` AS STRING), '')
              )
            """
        )

    def _apply_delete_policy(
        self,
        load_config: Any,
        fq_staging: str,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        if columns.delete_policy == "ignore":
            return 0
        if columns.delete_policy != "expire":
            raise ValueError("BigQuery scd2 supports delete_policy values: expire, ignore.")
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        condition = self._build_unique_key_condition("s", "t", unique_key)
        return self._execute_dml_query(
            f"""
            UPDATE {fq_target} AS t
            SET `{columns.valid_to}` = CURRENT_TIMESTAMP(),
                `{columns.is_current}` = FALSE
            WHERE t.`{columns.is_current}` IS TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM {fq_staging} AS s
                  WHERE {condition}
              )
            """
        )

    def _insert_new_current_versions(
        self,
        load_config: Any,
        payload: LoadPayload,
        fq_staging: str,
        unique_key: str | Sequence[str],
        columns: _Scd2Columns,
    ) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            staging_table_alias="s",
            include_technical_columns=self._include_technical_columns(load_config),
        )
        condition = self._build_unique_key_condition("s", "t", unique_key)
        return self._execute_dml_query(
            f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging} AS s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {fq_target} AS t
                WHERE {condition}
                  AND t.`{columns.is_current}` IS TRUE
                  AND COALESCE(CAST(t.`{columns.row_hash}` AS STRING), '')
                      = COALESCE(CAST(s.`{columns.row_hash}` AS STRING), '')
            )
            """
        )

    def _insert_all_from_staging(self, load_config: Any, payload: LoadPayload, fq_staging: str) -> int:
        fq_target = self._get_fq_table(load_config.target_schema, load_config.target_table)
        columns_str, select_str = self._build_select_with_json_parse(
            payload.schema,
            include_technical_columns=self._include_technical_columns(load_config),
        )
        return self._execute_dml_query(
            f"""
            INSERT INTO {fq_target} ({columns_str})
            SELECT {select_str}
            FROM {fq_staging}
            """
        )


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


def _fq_staging(project_id: str, load_config: Any) -> str:
    return f"`{project_id}.{load_config.staging_schema}.{load_config.target_table}__tmp`"


def _strategy_options(load_config: Any, section: str) -> dict[str, Any]:
    options = getattr(load_config, "options", {}) or {}
    nested = options.get(section)
    return nested if isinstance(nested, dict) else {}


def _strategy_option(load_config: Any, section: str, key: str, *, default: str) -> str:
    return str(_strategy_options(load_config, section).get(key, default))
