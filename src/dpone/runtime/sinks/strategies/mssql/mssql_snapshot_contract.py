"""Fail-closed MSSQL contracts for snapshot staging and target objects."""

from __future__ import annotations

import re
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import KEY_HASH_COLUMN
from dpone.runtime.sinks.mssql_table_ddl import MssqlTableDdlRenderer, MssqlTableDesign
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    business_schema,
    resolved_business_nullability,
    resolved_target_type,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotReconciliationError,
    SoftDeletePolicy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import (
    MssqlTargetShapeError,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import (
    validate_business_shape as _validate_resolved_business_shape,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import (
    validate_required_technical_shape as _validate_resolved_technical_shape,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import (
    validate_soft_delete_shape as _validate_resolved_soft_delete_shape,
)
from dpone.runtime.sinks.strategies.mssql.mssql_target_behavior_contract import (
    MssqlTargetBehaviorContract,
)
from dpone.runtime.support.mssql_snapshot_projection import (
    is_text_key_type,
    key_column_definition,
    require_indexable_key_types,
    validate_text_key_metadata,
)


class MssqlSnapshotContract:
    """Validate immutable staging evidence and the published target shape."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy
        self._connector = strategy.connector

    def validate_staging(
        self,
        envelope: Any,
        delta: StagingTableArtifact,
        keys: StagingTableArtifact,
    ) -> None:
        delta_receipt = envelope.delta_receipt
        if not delta_receipt.complete or delta.row_count != delta_receipt.row_count:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.incomplete_delta_snapshot")
        receipt = envelope.key_receipt
        if not receipt.complete or keys.row_count != receipt.row_count:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.incomplete_key_snapshot")
        for staging in (delta, keys):
            name = self._strategy._staging_name(staging)
            columns = ", ".join(self._connector.quote_identifier(key) for key in receipt.key_columns)
            nulls = " OR ".join(f"{self._connector.quote_identifier(key)} IS NULL" for key in receipt.key_columns)
            if self._count(name, nulls):
                raise SnapshotReconciliationError("mssql_snapshot_reconciliation.null_key")
            rows = self._connector.get_records(
                f"SELECT TOP (1) 1 FROM {name} GROUP BY {columns} HAVING COUNT_BIG(*) > 1"
            )
            if rows:
                raise SnapshotReconciliationError("mssql_snapshot_reconciliation.duplicate_key")
            self._reject_padded_text_keys(name, staging, receipt.key_columns)
        key_condition = " AND ".join(
            f"d.{self._connector.quote_identifier(key)} = k.{self._connector.quote_identifier(key)}"
            for key in receipt.key_columns
        )
        extra_delta = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(delta)} AS d "
            f"WHERE NOT EXISTS (SELECT 1 FROM {self._strategy._staging_name(keys)} AS k WHERE {key_condition})"
        )
        if extra_delta:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.delta_key_not_in_snapshot")
        if not self._staged_key_hashes_are_valid(keys):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.key_checksum_mismatch")

    def index_keys(self, keys: StagingTableArtifact, unique_key: tuple[str, ...]) -> None:
        columns = ", ".join(self._connector.quote_identifier(key) for key in unique_key)
        name = _index_name("ux", keys.table, unique_key)
        self._connector.execute_query(
            f"CREATE UNIQUE CLUSTERED INDEX [{name}] ON {self._strategy._staging_name(keys)} "
            f"({columns}) WITH (DATA_COMPRESSION = NONE)"
        )

    def ensure_target(
        self,
        load_config: Any,
        schema: Any,
        unique_key: tuple[str, ...],
        soft_delete: SoftDeletePolicy,
    ) -> None:
        if self._strategy._table_exists(load_config):
            return
        require_indexable_key_types(schema, load_config, unique_key)
        physical = load_config.options.get("physical_design") if isinstance(load_config.options, dict) else None
        if not isinstance(physical, dict) or physical.get("apply_runtime") is not True:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_external_provisioning_required")
        self._require_schema(load_config.target_database, load_config.target_schema)
        nullability = resolved_business_nullability(load_config, schema, unique_key)
        definitions = []
        for column, dtype in schema:
            if str(column).lower().startswith("__dpone__"):
                continue
            nullable_sql = "NULL" if nullability[column] else "NOT NULL"
            target_type = resolved_target_type(load_config, column, dtype)
            if column in unique_key and "(max)" in target_type.lower():
                raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_indexable_type_required")
            physical_type = key_column_definition(
                target_type, is_text_key=column in unique_key and is_text_key_type(target_type)
            )
            definitions.append(f"{self._connector.quote_identifier(column)} {physical_type} {nullable_sql}")
        definitions.extend(
            (
                "[__dpone__run_id] char(26) NOT NULL",
                "[__dpone__load_id] char(26) NOT NULL",
                "[__dpone__row_hash] char(64) NOT NULL",
                "[__dpone__extracted_at] datetime2(7) NOT NULL",
                "[__dpone__loaded_at] datetime2(7) NOT NULL",
            )
        )
        definitions.extend(soft_delete.target_definitions())
        ddl = MssqlTableDdlRenderer().render_create_table(
            table=self._strategy._target_name(load_config),
            column_definitions=definitions,
            design=MssqlTableDesign.from_options(load_config.options),
        )
        self._connector.execute_query(ddl)
        columns = ", ".join(self._connector.quote_identifier(key) for key in unique_key)
        index = _index_name("ux", load_config.target_table, unique_key)
        self._connector.execute_query(
            f"CREATE UNIQUE NONCLUSTERED INDEX [{index}] ON {self._strategy._target_name(load_config)} "
            f"({columns}) WITH (DATA_COMPRESSION = NONE)"
        )

    def validate_target(
        self,
        load_config: Any,
        payload_schema: Any,
        unique_key: tuple[str, ...],
        policy: SoftDeletePolicy,
    ) -> None:
        MssqlTargetBehaviorContract(self._strategy).validate(load_config)
        metadata = self._managed_column_metadata(load_config)
        _validate_managed_columns(policy, metadata)
        business_metadata = self._business_column_metadata(load_config)
        _validate_business_shape(
            load_config,
            payload_schema,
            unique_key,
            business_metadata,
        )
        expected_types = {
            column: resolved_target_type(load_config, column, source_type)
            for column, source_type in business_schema(payload_schema)
        }
        validate_text_key_metadata(unique_key, expected_types, business_metadata)
        _validate_required_technical_shape(metadata)
        _validate_soft_delete_shape(policy, metadata)
        target = self._strategy._target_name(load_config)
        null_keys = " OR ".join(f"t.{self._connector.quote_identifier(key)} IS NULL" for key in unique_key)
        if self._count(target, null_keys, alias="t"):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_null_key")
        if self._count(target, "t.[__dpone__row_hash] IS NULL", alias="t"):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_null_row_hash")
        columns = ", ".join(self._connector.quote_identifier(key) for key in unique_key)
        duplicate = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {target} GROUP BY {columns} HAVING COUNT_BIG(*) > 1"
        )
        if duplicate:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_duplicate_key")
        self._reject_padded_text_keys(target, None, unique_key, expected_types=expected_types)
        normalized_key = tuple(column.lower() for column in unique_key)
        if normalized_key not in self._unique_indexes(load_config):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_unique_index_missing")

    def _managed_column_metadata(self, load_config: Any) -> dict[str, dict[str, Any]]:
        database = getattr(load_config, "target_database", None)
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            "SELECT c.name AS column_name, ty.name AS type_name, c.max_length, c.scale, c.is_nullable, "
            "c.is_computed, cc.is_persisted, cc.definition AS computed_definition, "
            "dc.definition AS default_definition "
            f"FROM {prefix}sys.columns AS c "
            f"INNER JOIN {prefix}sys.types AS ty ON ty.user_type_id = c.user_type_id "
            f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
            f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"LEFT JOIN {prefix}sys.computed_columns AS cc "
            "ON cc.object_id = c.object_id AND cc.column_id = c.column_id "
            f"LEFT JOIN {prefix}sys.default_constraints AS dc ON dc.object_id = c.default_object_id "
            "WHERE s.name = ? AND t.name = ? AND c.name LIKE N'__dpone__%'",
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        return {str(row["column_name"]).lower(): dict(row) for row in rows}

    def _business_column_metadata(self, load_config: Any) -> dict[str, dict[str, Any]]:
        database = getattr(load_config, "target_database", None)
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            "SELECT c.name AS column_name, ty.name AS type_name, c.max_length, c.precision, c.scale, "
            "c.is_nullable, c.is_computed, c.is_identity, c.default_object_id, c.collation_name "
            f"FROM {prefix}sys.columns AS c "
            f"INNER JOIN {prefix}sys.types AS ty ON ty.user_type_id = c.user_type_id "
            f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
            f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = ? AND t.name = ? AND c.name NOT LIKE N'__dpone__%'",
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        return {str(row["column_name"]).lower(): dict(row) for row in rows}

    def _reject_padded_text_keys(
        self,
        table: str,
        staging: StagingTableArtifact | None,
        unique_key: tuple[str, ...],
        *,
        expected_types: dict[str, str] | None = None,
    ) -> None:
        types = expected_types or (getattr(staging, "column_types", {}) if staging is not None else {})
        predicates = [
            f"DATALENGTH({self._connector.quote_identifier(key)}) "
            f"<> DATALENGTH(RTRIM({self._connector.quote_identifier(key)}))"
            for key in unique_key
            if is_text_key_type(str(types.get(key, "")))
        ]
        if predicates and self._connector.get_records(f"SELECT TOP (1) 1 FROM {table} WHERE {' OR '.join(predicates)}"):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.text_key_trailing_space_unsupported")

    def _require_schema(self, database: str | None, schema: str) -> None:
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {prefix}sys.schemas WHERE name = ?) "
            "THEN 1 ELSE 0 END AS schema_exists",
            (schema,),
            as_dict=True,
        )
        if not rows or not int(rows[0]["schema_exists"]):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_schema_missing")

    def _unique_indexes(self, load_config: Any) -> set[tuple[str, ...]]:
        database = getattr(load_config, "target_database", None)
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            "SELECT i.index_id, c.name AS column_name, ic.key_ordinal "
            f"FROM {prefix}sys.indexes AS i "
            f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
            f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"INNER JOIN {prefix}sys.index_columns AS ic "
            "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            f"INNER JOIN {prefix}sys.columns AS c "
            "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE s.name = ? AND t.name = ? AND i.is_unique = 1 "
            "AND i.is_hypothetical = 0 AND i.is_disabled = 0 "
            "AND i.has_filter = 0 AND ic.is_included_column = 0 "
            "ORDER BY i.index_id, ic.key_ordinal",
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        grouped: dict[int, list[tuple[int, str]]] = {}
        for row in rows:
            grouped.setdefault(int(row["index_id"]), []).append((int(row["key_ordinal"]), str(row["column_name"])))
        return {tuple(column.lower() for _ordinal, column in sorted(columns)) for columns in grouped.values()}

    def _staged_key_hashes_are_valid(
        self,
        keys: StagingTableArtifact,
    ) -> bool:
        hash_column = self._connector.quote_identifier(KEY_HASH_COLUMN)
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(keys)} AS k "
            f"WHERE k.{hash_column} IS NULL OR LEN(k.{hash_column}) <> 64 "
            f"OR LOWER(k.{hash_column}) LIKE '%[^0-9a-f]%'"
        )
        return not rows

    def _count(self, table: str, predicate: str | None = None, *, alias: str | None = None) -> int:
        from_sql = f"{table} AS {alias}" if alias else table
        where = f" WHERE {predicate}" if predicate else ""
        rows = self._connector.get_records(
            f"SELECT COUNT_BIG(*) AS row_count FROM {from_sql}{where}",
            as_dict=True,
        )
        if not rows:
            return 0
        row = rows[0]
        return int(row["row_count"] if isinstance(row, dict) else row[0])


def _validate_business_shape(
    load_config: Any,
    schema: Any,
    unique_key: tuple[str, ...],
    metadata: dict[str, dict[str, Any]],
) -> None:
    """Resolve the physical contract before delegating pure shape checks."""

    projected = business_schema(schema)
    nullability = resolved_business_nullability(load_config, projected, unique_key)
    expected = tuple(
        (column, resolved_target_type(load_config, column, source_type), nullability[column])
        for column, source_type in projected
    )
    try:
        _validate_resolved_business_shape(expected, metadata)
    except MssqlTargetShapeError as exc:
        raise SnapshotReconciliationError(exc.code) from exc


def _validate_managed_columns(
    policy: SoftDeletePolicy,
    metadata: dict[str, dict[str, Any]],
) -> None:
    expected = {
        "__dpone__run_id",
        "__dpone__load_id",
        "__dpone__row_hash",
        "__dpone__extracted_at",
        "__dpone__loaded_at",
    }
    expected.add("__dpone__deleted_at" if policy.timestamp_is_source_of_truth else "__dpone__is_deleted")
    if policy.flag_is_computed:
        expected.add("__dpone__is_deleted")
    actual = set(metadata)
    if not expected.issubset(actual):
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_contract_missing")
    if actual != expected:
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_contract_unexpected_technical_columns")


def _validate_required_technical_shape(metadata: dict[str, dict[str, Any]]) -> None:
    try:
        _validate_resolved_technical_shape(metadata)
    except MssqlTargetShapeError as exc:
        raise SnapshotReconciliationError(exc.code) from exc


def _validate_soft_delete_shape(policy: SoftDeletePolicy, metadata: dict[str, dict[str, Any]]) -> None:
    try:
        _validate_resolved_soft_delete_shape(policy, metadata)
    except MssqlTargetShapeError as exc:
        raise SnapshotReconciliationError(exc.code) from exc


def _index_name(prefix: str, table: str, columns: tuple[str, ...]) -> str:
    raw = "_".join((prefix, table, *columns))
    return re.sub(r"[^A-Za-z0-9_]", "_", raw)[:120]


__all__ = ["MssqlSnapshotContract"]
