"""ClickHouse staging finalization helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.governance.validation_snapshot import (
    snapshot_staged_validation_values,
)
from dpone.runtime.sinks.merge_policy import (
    require_unique_key,
    validate_duplicate_policy,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

CLICKHOUSE_STAGING_UNIQUE_KEY_NULL = "DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL"
CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE = "DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE"
CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID = "DPONE_CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID"


class _ClickHouseStagedValidationToken:
    """Opaque identity whose authoritative binding stays inside the finalizer."""

    __slots__ = ()

    def __copy__(self) -> _ClickHouseStagedValidationToken:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> _ClickHouseStagedValidationToken:
        del memo
        return self


@dataclass(frozen=True, slots=True)
class _ClickHouseStagedValidationBinding:
    load_config: Any
    staging_config: Any
    staging_identity: tuple[str, str]


class ClickHouseStagingKeyError(ValueError):
    """Stable pre-target failure for an unsafe staged key."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.details: dict[str, Any] = {}


class ClickHouseStagingFinalizer:
    """Finalize staged ClickHouse batches through target-native operations."""

    def __init__(
        self,
        *,
        connector: Any,
        table_name: Callable[[LoadConfig], str],
        count_rows: Callable[[LoadConfig], int],
        mutations_sync: Callable[[LoadConfig], int],
    ) -> None:
        self._connector = connector
        self._table_name = table_name
        self._count_rows = count_rows
        self._mutations_sync = mutations_sync
        self._validation_tokens: dict[
            _ClickHouseStagedValidationToken,
            _ClickHouseStagedValidationBinding,
        ] = {}
        self._validation_tokens_lock = Lock()

    def count_target_matching_staging_keys(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        rows = self._connector.get_records(
            f"SELECT count() FROM {self._table_name(load_config)} "
            f"WHERE {self._key_in_staging_condition(staging_config, unique_key)}"
        )
        return int(rows[0][0]) if rows else 0

    def copy_target_to_shadow_excluding_predicate(
        self,
        load_config: LoadConfig,
        shadow_config: LoadConfig,
    ) -> int:
        self._connector.execute_query(
            f"INSERT INTO {self._table_name(shadow_config)} "
            f"SELECT * FROM {self._table_name(load_config)} WHERE NOT ({load_config.custom_predicate})"
        )
        return self._count_rows(shadow_config)

    def copy_target_to_shadow_excluding_staging_keys(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        shadow_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        self._connector.execute_query(
            f"INSERT INTO {self._table_name(shadow_config)} "
            f"SELECT * FROM {self._table_name(load_config)} "
            f"WHERE NOT ({self._key_in_staging_condition(staging_config, unique_key)})"
        )
        return self._count_rows(shadow_config)

    def lightweight_delete_matching_staging_keys(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        return self._connector.execute_query(
            f"DELETE FROM {self._table_name(load_config)} "
            f"WHERE {self._key_in_staging_condition(staging_config, unique_key)} "
            f"SETTINGS mutations_sync = {self._mutations_sync(load_config)}"
        )

    def lightweight_delete_missing_from_staging(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        return self._connector.execute_query(
            f"DELETE FROM {self._table_name(load_config)} "
            f"WHERE NOT ({self._key_in_staging_condition(staging_config, unique_key)}) "
            f"SETTINGS mutations_sync = {self._mutations_sync(load_config)}"
        )

    def mutation_delete_matching_staging_keys(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        return self._connector.execute_query(
            f"ALTER TABLE {self._table_name(load_config)} DELETE "
            f"WHERE {self._key_in_staging_condition(staging_config, unique_key)} "
            f"SETTINGS mutations_sync = {self._mutations_sync(load_config)}"
        )

    def mutation_delete_missing_from_staging(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        return self._connector.execute_query(
            f"ALTER TABLE {self._table_name(load_config)} DELETE "
            f"WHERE NOT ({self._key_in_staging_condition(staging_config, unique_key)}) "
            f"SETTINGS mutations_sync = {self._mutations_sync(load_config)}"
        )

    def mutation_update(
        self,
        load_config: LoadConfig,
        *,
        assignments: str,
        where: str,
    ) -> int:
        """Run a synchronous ``ALTER TABLE … UPDATE`` mutation."""

        return self._connector.execute_query(
            f"ALTER TABLE {self._table_name(load_config)} UPDATE {assignments} "
            f"WHERE {where} "
            f"SETTINGS mutations_sync = {self._mutations_sync(load_config)}"
        )

    def key_in_staging_condition(self, staging_config: LoadConfig, unique_key: Sequence[str]) -> str:
        return self._key_in_staging_condition(staging_config, unique_key)

    def validate_staging_key_integrity(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> None:
        """Reject NULL and duplicate staged keys before target invocation."""

        validate_duplicate_policy(load_config)
        null_condition = " OR ".join(f"`{column}` IS NULL" for column in unique_key)
        null_rows = self._connector.get_records(
            f"SELECT count() FROM {self._table_name(staging_config)} WHERE {null_condition}"
        )
        if _required_count(null_rows, code=CLICKHOUSE_STAGING_UNIQUE_KEY_NULL):
            raise ClickHouseStagingKeyError(
                CLICKHOUSE_STAGING_UNIQUE_KEY_NULL,
                (
                    "ClickHouse staged load contains NULL unique_key values. "
                    "The batch was rejected before target mutation."
                ),
            )
        self.validate_staging_duplicates(
            load_config,
            staging_config,
            unique_key,
        )

    def validate_strategy_staging_key_integrity(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
    ) -> object:
        """Apply key-integrity gates to every strategy with key semantics."""

        if load_config.load_strategy in {
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.SNAPSHOT_DIFF,
            LoadStrategy.SCD2,
        }:
            unique_key = require_unique_key(
                load_config,
                strategy_name=load_config.load_strategy.value,
            )
            self.validate_staging_key_integrity(
                load_config,
                staging_config,
                unique_key,
            )
        token = _ClickHouseStagedValidationToken()
        validated_load_config, validated_staging_config = snapshot_staged_validation_values(
            load_config,
            staging_config,
        )
        binding = _ClickHouseStagedValidationBinding(
            load_config=validated_load_config,
            staging_config=validated_staging_config,
            staging_identity=_staging_identity(staging_config),
        )
        with self._validation_tokens_lock:
            self._validation_tokens[token] = binding
        return token

    def require_strategy_staging_validation(
        self,
        token: object,
        load_config: LoadConfig,
        staging_config: LoadConfig,
    ) -> None:
        """Reject forged, replayed, or semantically drifted validation tokens."""

        if not isinstance(token, _ClickHouseStagedValidationToken):
            raise ValueError(CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID)
        with self._validation_tokens_lock:
            binding = self._validation_tokens.pop(token, None)
        if binding is None or binding.load_config != load_config or binding.staging_config != staging_config:
            raise ValueError(CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID)

    def retire_strategy_staging_validations(self, staging_config: LoadConfig) -> None:
        """Retire unused tokens when their attempt-local staging is discarded."""

        identity = _staging_identity(staging_config)
        with self._validation_tokens_lock:
            stale = [
                token for token, binding in self._validation_tokens.items() if binding.staging_identity == identity
            ]
            for token in stale:
                self._validation_tokens.pop(token, None)

    def validate_staging_duplicates(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> None:
        """Compatibility helper for duplicate-only callers."""

        validate_duplicate_policy(load_config)
        key_sql = ", ".join(f"`{column}`" for column in unique_key)
        rows = self._connector.get_records(
            f"SELECT count() FROM (SELECT {key_sql} FROM {self._table_name(staging_config)} "
            f"GROUP BY {key_sql} HAVING count() > 1 LIMIT 1)"
        )
        if _required_count(
            rows,
            code=CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE,
        ):
            raise ClickHouseStagingKeyError(
                CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE,
                (
                    "ClickHouse staged load contains duplicate unique_key values. "
                    "Default duplicate_policy=fail rejected the batch before "
                    "target mutation."
                ),
            )

    def partition_values_from_staging(
        self,
        staging_config: LoadConfig,
        partition_column: str,
        value_expression: str | None = None,
    ) -> list[Any]:
        value_sql = value_expression or f"`{partition_column}`"
        rows = self._connector.get_records(
            f"SELECT DISTINCT {value_sql} AS __dpone_partition_value FROM {self._table_name(staging_config)}"
        )
        return [row[0] for row in rows]

    def _key_in_staging_condition(self, staging_config: LoadConfig, unique_key: Sequence[str]) -> str:
        if len(unique_key) == 1:
            column = unique_key[0]
            return f"`{column}` IN (SELECT `{column}` FROM {self._table_name(staging_config)})"
        columns = ", ".join(f"`{column}`" for column in unique_key)
        return f"({columns}) IN (SELECT {columns} FROM {self._table_name(staging_config)})"


def _required_count(rows: object, *, code: str) -> int:
    try:
        if not isinstance(rows, Sequence) or not rows:
            raise ValueError
        first = rows[0]
        if not isinstance(first, Sequence) or isinstance(first, str | bytes):
            raise ValueError
        value = int(first[0])
        if value < 0:
            raise ValueError
        return value
    except (IndexError, TypeError, ValueError) as exc:
        raise ClickHouseStagingKeyError(
            code,
            "ClickHouse could not prove staged unique_key integrity before target mutation.",
        ) from exc


def _staging_identity(staging_config: Any) -> tuple[str, str]:
    return (
        str(getattr(staging_config, "target_schema", "")),
        str(getattr(staging_config, "target_table", "")),
    )


__all__ = [
    "CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE",
    "CLICKHOUSE_STAGING_UNIQUE_KEY_NULL",
    "ClickHouseStagingFinalizer",
    "ClickHouseStagingKeyError",
]
