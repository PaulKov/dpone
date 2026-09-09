"""Exact fail-closed SQL Server object contract for shadow publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    SHADOW_OWNER_PROPERTY,
    MssqlPublicationTarget,
    quote_publication_identifier,
)
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    PUBLICATION_OBJECT_PROPERTIES,
)
from dpone.runtime.sinks.mssql_shadow_swap_object_catalog_queries import (
    FEATURE_COUNTS,
    TABLE_FLAGS,
    index_options_catalog_sql,
    residual_catalog_sql,
)
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlIndexState,
    MssqlSchemaCatalogSnapshot,
    MssqlTableBehaviorState,
)
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot

_ROWS_FILEGROUP = "ROWS_FILEGROUP"
_RESERVED_PROPERTIES = (SHADOW_OWNER_PROPERTY, *PUBLICATION_OBJECT_PROPERTIES)


@dataclass(frozen=True, slots=True)
class MssqlShadowSwapIndexOptions:
    """Physical index options not projected by the shared schema snapshot."""

    name: str
    padded: bool
    allow_row_locks: bool
    allow_page_locks: bool
    compression_delay: int | None
    optimize_for_sequential_key: bool
    columnstore_order_columns: int


@dataclass(frozen=True, slots=True)
class MssqlShadowSwapResidualCatalog:
    """Exact publication-relevant surfaces outside the shared snapshot."""

    base_data_space_name: str | None
    base_data_space_type: str | None
    lob_data_space_name: str | None
    lob_data_space_type: str | None
    unsupported_table_flags: tuple[str, ...]
    unsupported_feature_counts: tuple[tuple[str, int], ...]
    index_options: tuple[MssqlShadowSwapIndexOptions, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "base_data_space_name": self.base_data_space_name,
            "base_data_space_type": self.base_data_space_type,
            "lob_data_space_name": self.lob_data_space_name,
            "lob_data_space_type": self.lob_data_space_type,
            "unsupported_table_flags": list(self.unsupported_table_flags),
            "unsupported_feature_counts": [list(item) for item in self.unsupported_feature_counts],
            "index_options": [
                asdict(item) for item in sorted(self.index_options, key=lambda value: value.name.casefold())
            ],
        }


@dataclass(frozen=True, slots=True)
class MssqlShadowSwapObjectContract:
    """Canonical live-object evidence durably pinned by a campaign."""

    target: MssqlPublicationTarget
    catalog: MssqlSchemaCatalogSnapshot
    residual: MssqlShadowSwapResidualCatalog

    @property
    def sha256(self) -> str:
        payload = {
            "kind": "dpone.mssql_shadow_swap_object_contract.v1",
            "target": self.target.dataset,
            "catalog": _object_catalog_payload(self.catalog),
            "residual": self.residual.to_dict(),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class _CatalogConfig:
    target_database: str
    target_schema: str
    target_table: str


@dataclass(frozen=True, slots=True)
class _CatalogOwner:
    connector: Any


class MssqlShadowSwapObjectContractGuard:
    """Read and certify the exact subset reproducible by ``SELECT INTO``."""

    def __init__(
        self,
        connector: Any,
        *,
        catalog_reader: Callable[[Any, Any], MssqlSchemaCatalogSnapshot] = read_schema_catalog_snapshot,
        residual_reader: Callable[[Any, MssqlPublicationTarget], MssqlShadowSwapResidualCatalog] | None = None,
    ) -> None:
        self._connector = connector
        self._catalog_reader = catalog_reader
        self._residual_reader = residual_reader or read_shadow_swap_residual_catalog

    def require_supported(self, target: MssqlPublicationTarget) -> MssqlShadowSwapObjectContract:
        """Return immutable evidence or reject before shadow/source mutation."""

        database = _database(target)
        snapshot = self._catalog_reader(
            _CatalogOwner(self._connector),
            _CatalogConfig(database, target.schema, target.table),
        )
        residual = self._residual_reader(self._connector, target)
        _require_supported_catalog(snapshot, residual)
        return MssqlShadowSwapObjectContract(target, snapshot, residual)

    def require_loadable_shadow(
        self,
        live: MssqlShadowSwapObjectContract,
        shadow: MssqlPublicationTarget,
    ) -> MssqlShadowSwapObjectContract:
        """Allow only the exact live CCI subset before chunk source I/O."""

        actual = self.require_supported(shadow)
        if _catalog_without_indexes(actual.catalog) != _catalog_without_indexes(live.catalog):
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        live_indexes = {item.name.casefold(): item for item in live.catalog.indexes}
        if any(live_indexes.get(item.name.casefold()) != item for item in actual.catalog.indexes):
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        required = {item.name.casefold() for item in live.catalog.indexes if item.type_desc == "CLUSTERED COLUMNSTORE"}
        if not required.issubset({item.name.casefold() for item in actual.catalog.indexes}):
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        if _residual_without_indexes(actual.residual) != _residual_without_indexes(live.residual):
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        return actual

    def require_matching_shadow(
        self,
        live: MssqlShadowSwapObjectContract,
        shadow: MssqlPublicationTarget,
    ) -> MssqlShadowSwapObjectContract:
        """Require the complete candidate contract under the cutover lock."""

        actual = self.require_supported(shadow)
        if _object_catalog_payload(actual.catalog) != _object_catalog_payload(live.catalog):
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        if actual.residual.to_dict() != live.residual.to_dict():
            raise RuntimeError("mssql_backfill_publication.shadow_object_contract_mismatch")
        return actual


def _require_supported_catalog(
    snapshot: MssqlSchemaCatalogSnapshot,
    residual: MssqlShadowSwapResidualCatalog,
) -> None:
    if not snapshot.exists or not snapshot.columns:
        raise RuntimeError("mssql_backfill_publication.object_contract_table_missing")
    for values, error in (
        (snapshot.checks, "check_constraint_unsupported"),
        (snapshot.foreign_keys, "foreign_key_unsupported"),
        (snapshot.triggers, "trigger_unsupported"),
        (snapshot.permissions, "explicit_permission_unsupported"),
    ):
        if values:
            raise RuntimeError(f"mssql_backfill_publication.object_contract_{error}")
    if snapshot.behavior != MssqlTableBehaviorState.ordinary_disk_table():
        raise RuntimeError("mssql_backfill_publication.object_contract_table_behavior_unsupported")
    for column in snapshot.columns:
        if (
            column.identity
            or column.computed
            or column.sparse
            or column.rowguidcol
            or column.generated_always_type != 0
            or column.default_definition is not None
        ):
            raise RuntimeError("mssql_backfill_publication.object_contract_column_behavior_unsupported")
        system_type = (column.system_type_schema.casefold(), column.system_type_name.casefold())
        user_type = (column.user_type_schema.casefold(), column.user_type_name.casefold())
        if system_type != user_type or system_type[0] != "sys":
            raise RuntimeError("mssql_backfill_publication.object_contract_user_type_unsupported")
    if residual.unsupported_table_flags or any(count for _name, count in residual.unsupported_feature_counts):
        raise RuntimeError("mssql_backfill_publication.object_contract_residual_surface_unsupported")
    if residual.base_data_space_name != snapshot.default_filegroup or residual.base_data_space_type != _ROWS_FILEGROUP:
        raise RuntimeError("mssql_backfill_publication.object_contract_table_placement_unsupported")
    if residual.lob_data_space_name is not None and (
        residual.lob_data_space_name != snapshot.default_filegroup or residual.lob_data_space_type != _ROWS_FILEGROUP
    ):
        raise RuntimeError("mssql_backfill_publication.object_contract_lob_placement_unsupported")
    _require_supported_indexes(snapshot, residual.index_options)


def _require_supported_indexes(
    snapshot: MssqlSchemaCatalogSnapshot,
    options: tuple[MssqlShadowSwapIndexOptions, ...],
) -> None:
    by_name = {item.name.casefold(): item for item in options}
    expected_names = {item.name.casefold() for item in snapshot.indexes}
    if len(by_name) != len(options) or set(by_name) != expected_names:
        raise RuntimeError("mssql_backfill_publication.object_contract_index_catalog_ambiguous")
    column_names = tuple(column.name for column in snapshot.columns)
    columnstore_count = 0
    for index in snapshot.indexes:
        if index.primary_key or index.unique_constraint:
            raise RuntimeError("mssql_backfill_publication.object_contract_key_constraint_unsupported")
        _require_common_index_shape(index, snapshot.default_filegroup)
        if index.type_desc == "CLUSTERED COLUMNSTORE":
            columnstore_count += 1
            indexed_columns = index.key_columns + index.included_columns
            if index.unique or indexed_columns != column_names or index.partition_compression != ("COLUMNSTORE",):
                raise RuntimeError("mssql_backfill_publication.object_contract_columnstore_unsupported")
        elif index.type_desc == "NONCLUSTERED":
            if (
                not index.unique
                or not index.key_columns
                or index.included_columns
                or any(index.descending_keys)
                or index.partition_compression != ("NONE",)
            ):
                raise RuntimeError("mssql_backfill_publication.object_contract_index_shape_unsupported")
        else:
            raise RuntimeError("mssql_backfill_publication.object_contract_index_type_unsupported")
        option = by_name[index.name.casefold()]
        row_locks_supported = index.type_desc == "NONCLUSTERED"
        if (
            option.padded
            or option.allow_row_locks is not row_locks_supported
            or option.allow_page_locks is not row_locks_supported
            or option.compression_delay not in (None, 0)
            or option.optimize_for_sequential_key
            or option.columnstore_order_columns
        ):
            raise RuntimeError("mssql_backfill_publication.object_contract_index_options_unsupported")
    if columnstore_count > 1:
        raise RuntimeError("mssql_backfill_publication.object_contract_multiple_columnstores")


def _require_common_index_shape(index: MssqlIndexState, default_filegroup: str) -> None:
    if (
        not index.name
        or index.disabled
        or index.hypothetical
        or index.ignore_dup_key
        or index.filter_definition is not None
        or index.fill_factor != 0
        or index.data_space_name != default_filegroup
        or index.data_space_type_desc != _ROWS_FILEGROUP
        or len(index.partition_compression) != 1
    ):
        raise RuntimeError("mssql_backfill_publication.object_contract_index_physical_unsupported")


def read_shadow_swap_residual_catalog(
    connector: Any,
    target: MssqlPublicationTarget,
) -> MssqlShadowSwapResidualCatalog:
    """Read database-qualified feature and physical surfaces omitted above."""

    prefix = quote_publication_identifier(_database(target)) + "."
    rows = connector.get_records(
        residual_catalog_sql(prefix),
        (*_RESERVED_PROPERTIES, target.schema, target.table),
        as_dict=True,
    )
    if len(rows or ()) != 1:
        raise RuntimeError("mssql_backfill_publication.object_contract_residual_catalog_unavailable")
    row = rows[0]
    capabilities = connector.get_records(
        f"EXEC {prefix}sys.sp_executesql N'SELECT "
        "TRY_CONVERT(int, SERVERPROPERTY(''ProductMajorVersion'')) AS major, "
        "TRY_CONVERT(int, SERVERPROPERTY(''EngineEdition'')) AS edition, "
        "HAS_PERMS_BY_NAME(DB_NAME(), ''DATABASE'', ''VIEW DEFINITION'') AS view_definition "
        "/* dpone_shadow_swap_index_capabilities_v1 */'",
        as_dict=True,
    )
    if len(capabilities or ()) != 1:
        raise RuntimeError("mssql_backfill_publication.object_contract_server_capability_unavailable")
    major = capabilities[0].get("major")
    edition = capabilities[0].get("edition")
    if not isinstance(major, int) or not isinstance(edition, int):
        raise RuntimeError("mssql_backfill_publication.object_contract_server_capability_unavailable")
    if int(capabilities[0].get("view_definition") or 0) != 1:
        raise RuntimeError("mssql_backfill_publication.object_contract_metadata_visibility_required")
    modern = major >= 15 or edition in {5, 8}
    ordered_columnstore = major >= 16 or edition in {5, 8}
    index_rows = connector.get_records(
        index_options_catalog_sql(prefix, modern=modern, ordered_columnstore=ordered_columnstore),
        (target.schema, target.table),
        as_dict=True,
    )
    return MssqlShadowSwapResidualCatalog(
        base_data_space_name=_text(row.get("base_data_space_name")),
        base_data_space_type=_text(row.get("base_data_space_type")),
        lob_data_space_name=_text(row.get("lob_data_space_name")),
        lob_data_space_type=_text(row.get("lob_data_space_type")),
        unsupported_table_flags=tuple(name for name in TABLE_FLAGS if bool(row.get(name))),
        unsupported_feature_counts=tuple((name, int(row.get(name) or 0)) for name in FEATURE_COUNTS),
        index_options=tuple(_index_options(item) for item in index_rows or ()),
    )


def _index_options(row: Any) -> MssqlShadowSwapIndexOptions:
    name = str(row.get("name") or "").strip()
    if not name:
        raise RuntimeError("mssql_backfill_publication.object_contract_index_name_missing")
    delay = row.get("compression_delay")
    return MssqlShadowSwapIndexOptions(
        name=name,
        padded=bool(row.get("is_padded")),
        allow_row_locks=bool(row.get("allow_row_locks")),
        allow_page_locks=bool(row.get("allow_page_locks")),
        compression_delay=int(delay) if delay is not None else None,
        optimize_for_sequential_key=bool(row.get("optimize_for_sequential_key")),
        columnstore_order_columns=int(row.get("columnstore_order_columns") or 0),
    )


def _object_catalog_payload(snapshot: MssqlSchemaCatalogSnapshot) -> dict[str, object]:
    payload = snapshot.to_dict()
    payload.pop("available_row_filegroups", None)
    payload["indexes"] = [index.to_dict() for index in sorted(snapshot.indexes, key=lambda item: item.name.casefold())]
    return payload


def _catalog_without_indexes(snapshot: MssqlSchemaCatalogSnapshot) -> dict[str, object]:
    payload = _object_catalog_payload(snapshot)
    payload.pop("indexes", None)
    return payload


def _residual_without_indexes(residual: MssqlShadowSwapResidualCatalog) -> dict[str, object]:
    payload = residual.to_dict()
    payload.pop("index_options", None)
    return payload


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _database(target: MssqlPublicationTarget) -> str:
    database = str(target.database or "").strip()
    if not database:
        raise RuntimeError("mssql_backfill_publication.object_contract_database_required")
    return database


__all__ = [
    "MssqlShadowSwapIndexOptions",
    "MssqlShadowSwapObjectContract",
    "MssqlShadowSwapObjectContractGuard",
    "MssqlShadowSwapResidualCatalog",
    "read_shadow_swap_residual_catalog",
]
