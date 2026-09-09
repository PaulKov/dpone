"""Pre-mutation target authority for generic SQL Server strategies."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.readiness.mssql_technical_type_compatibility import (
    mssql_fixed_technical_types_compatible,
)
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.schema_evolution_options import accepts_existing_nullable_target
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import metadata_matches_type
from dpone.runtime.sinks.strategies.mssql.mssql_target_behavior_contract import MssqlTargetBehaviorContract
from dpone.runtime.sinks.strategies.mssql.mssql_unique_authority import (
    MssqlUniqueAuthorityContract,
    external_physical_primary_key_authority,
    raise_projection_error,
    render_unique_authority_ddl,
    resolve_unique_authority_contract,
    unique_authorities,
    unique_authority_matches,
    unique_keys,
)
from dpone.runtime.support.mssql_snapshot_projection import is_text_key_type


class MssqlGenericTargetContract:
    """Require one exact physical shape and unique-key authority."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy
        self._connector = strategy.connector

    def validate_existing(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        *,
        target_projection: Any | None = None,
    ) -> None:
        """Validate an existing target before the target transaction begins."""

        if not self._strategy._table_exists(load_config):
            return
        MssqlTargetBehaviorContract(self._strategy).validate(load_config)
        expected_columns = [column for column in staging.columns if column != "__dpone__xmin"]
        retained_columns = tuple(getattr(target_projection, "retained_target_columns", ()) or ())
        metadata = self._metadata(load_config)
        expected_names = {*expected_columns, *(column.name for column in retained_columns)}
        if set(metadata) != expected_names:
            _raise("target_shape_invalid")
        for column in expected_columns:
            actual = metadata[column]
            expected_type = staging.target_column_types[column]
            if not _metadata_type_matches(column, actual, expected_type):
                _raise(f"target_shape_invalid:type:{column}")
            expected_nullable = staging.target_column_nullability.get(column, True)
            accepts_nullable_legacy_target = (
                accepts_existing_nullable_target(load_config)
                and not expected_nullable
                and bool(actual.get("is_nullable"))
            )
            if (
                (bool(actual.get("is_nullable")) is not expected_nullable and not accepts_nullable_legacy_target)
                or bool(actual.get("is_computed"))
                or bool(actual.get("is_identity"))
                or bool(actual.get("default_object_id"))
            ):
                reason = "nullability" if bool(actual.get("is_nullable")) is not expected_nullable else "behavior"
                _raise(f"target_shape_invalid:{reason}:{column}")
            expected_collation = staging.target_column_collations.get(column)
            if (
                expected_collation
                and str(actual.get("collation_name") or "").casefold() != expected_collation.casefold()
            ):
                _raise("target_collation_invalid")
        for retained in retained_columns:
            actual = metadata[retained.name]
            if not metadata_matches_type(actual, retained.target_type):
                _raise("retained_target_shape_invalid")
            if bool(actual.get("is_nullable")) is not retained.nullable:
                _raise("retained_target_shape_invalid")
            actual_collation = str(actual.get("collation_name") or "")
            expected_collation = str(retained.collation or "")
            if actual_collation.casefold() != expected_collation.casefold():
                _raise("retained_target_collation_invalid")
            if (
                bool(actual.get("is_computed"))
                or bool(actual.get("is_identity"))
                or bool(actual.get("default_object_id"))
            ):
                _raise("retained_target_behavior_invalid")
        keys = unique_keys(load_config)
        if not keys:
            return
        if require_shadow_append_authority(load_config) is not None:
            return
        framework_authority = external_physical_primary_key_authority(
            load_config,
            qualified_target=self._strategy._target_name(load_config),
        )
        if framework_authority is None:
            framework_authority = resolve_unique_authority_contract(load_config, staging.target_column_types)
        if framework_authority is not None:
            self._validate_unique_authority(load_config, framework_authority)
        self._reject_target_sql_equivalence_alias(load_config, staging, keys)

    def create_unique_authority(self, load_config: Any, staging: StagingTableArtifact, *, table: str) -> None:
        """Create the strategy-owned unique authority on a fresh table."""

        target = self._strategy._target_name(load_config, table=table)
        statement = render_unique_authority_ddl(load_config, target, staging.target_column_types, table=table)
        if statement is not None:
            self._connector.execute_query(statement)

    def _metadata(self, load_config: Any) -> dict[str, dict[str, Any]]:
        database = getattr(load_config, "target_database", None)
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            "SELECT c.name AS column_name, ty.name AS type_name, c.max_length, c.precision, c.scale, "
            "c.is_nullable, c.is_computed, c.is_identity, c.default_object_id, c.collation_name "
            f"FROM {prefix}sys.columns AS c "
            f"INNER JOIN {prefix}sys.types AS ty ON ty.user_type_id = c.user_type_id "
            f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
            f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = ? AND t.name = ?",
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        output: dict[str, dict[str, Any]] = {}
        casefolded: set[str] = set()
        for row in rows:
            name = str(row["column_name"])
            folded = name.casefold()
            if name in output or folded in casefolded:
                _raise("target_column_casefold_collision")
            output[name] = dict(row)
            casefolded.add(folded)
        return output

    def _validate_unique_authority(self, load_config: Any, expected: MssqlUniqueAuthorityContract) -> None:
        database = getattr(load_config, "target_database", None)
        prefix = f"{self._connector.quote_identifier(database)}." if database else ""
        rows = self._connector.get_records(
            "SELECT i.index_id, i.name AS index_name, i.type_desc, i.is_primary_key, "
            "i.is_unique_constraint, i.is_disabled, i.is_hypothetical, "
            "i.filter_definition, i.ignore_dup_key, ds.type_desc AS data_space_type_desc, "
            "c.name AS column_name, ic.key_ordinal, ic.is_descending_key, ic.is_included_column, "
            "part.partition_count, part.minimum_compression, part.maximum_compression "
            f"FROM {prefix}sys.indexes AS i "
            f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
            f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"INNER JOIN {prefix}sys.index_columns AS ic "
            "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            f"INNER JOIN {prefix}sys.columns AS c "
            "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            f"LEFT JOIN {prefix}sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id "
            "OUTER APPLY (SELECT COUNT_BIG(*) AS partition_count, "
            "MIN(UPPER(p.data_compression_desc)) AS minimum_compression, "
            "MAX(UPPER(p.data_compression_desc)) AS maximum_compression "
            f"FROM {prefix}sys.partitions AS p "
            "WHERE p.object_id = i.object_id AND p.index_id = i.index_id) AS part "
            "WHERE s.name = ? AND t.name = ? AND i.is_unique = 1 "
            "AND ic.key_ordinal > 0 ORDER BY i.index_id, ic.key_ordinal",
            (load_config.target_schema, load_config.target_table),
            as_dict=True,
        )
        if any(unique_authority_matches(index, expected) for index in unique_authorities(rows)):
            return
        _raise("target_unique_authority_missing")

    def _reject_target_sql_equivalence_alias(
        self,
        load_config: Any,
        staging: StagingTableArtifact,
        keys: Sequence[str],
    ) -> None:
        text_keys = [key for key in keys if is_text_key_type(staging.target_column_types.get(key, ""))]
        if not text_keys:
            return
        padded = " OR ".join(
            f"DATALENGTH({self._connector.quote_identifier(key)}) <> "
            f"DATALENGTH(RTRIM({self._connector.quote_identifier(key)}))"
            for key in text_keys
        )
        if self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._target_name(load_config)} WHERE {padded}"
        ):
            _raise("text_key_trailing_space_unsupported")
        sql_equal = " AND ".join(
            f"s.{self._connector.quote_identifier(key)} = t.{self._connector.quote_identifier(key)}" for key in keys
        )
        byte_different = " OR ".join(
            f"CONVERT(varbinary(max), s.{self._connector.quote_identifier(key)}) "
            f"<> CONVERT(varbinary(max), t.{self._connector.quote_identifier(key)})"
            for key in text_keys
        )
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(staging)} AS s "
            f"INNER JOIN {self._strategy._target_name(load_config)} AS t ON {sql_equal} "
            f"WHERE {byte_different}"
        )
        if rows:
            _raise("key_sql_equivalence_collision")


_raise = raise_projection_error


def _metadata_type_matches(column: str, actual: dict[str, Any], expected_type: str) -> bool:
    if metadata_matches_type(actual, expected_type):
        return True
    type_name = str(actual.get("type_name") or "")
    max_length = int(actual.get("max_length") or 0)
    actual_type = f"{type_name}({max_length})" if type_name.casefold() in {"char", "varchar"} else type_name
    return mssql_fixed_technical_types_compatible(column, expected_type, actual_type)


__all__ = [
    "MssqlGenericTargetContract",
    "MssqlUniqueAuthorityContract",
    "render_unique_authority_ddl",
    "resolve_unique_authority_contract",
]
