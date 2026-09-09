"""SQL Server physical design introspection and safe migration rendering."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState, TableSettingValue
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.sinks.mssql_target_catalog_types import (
    canonical_mssql_catalog_type,
    render_mssql_catalog_type,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


@dataclass(frozen=True, slots=True)
class MssqlPhysicalIntrospector:
    connector: Any

    def inspect(self, load_config: LoadConfig) -> PhysicalTableState:
        table = _qualified_table(load_config)
        if not self._table_exists(load_config):
            return PhysicalTableState(sink_type="mssql", table=table)
        compression = self._compression(load_config)
        clustered_columnstore = self._has_clustered_columnstore(load_config)
        primary_key, primary_key_clustered, fillfactor = self._primary_key(load_config)
        filegroup, textimage_filegroup = self._placement(load_config)
        return PhysicalTableState(
            sink_type="mssql",
            table=table,
            columns=self._columns(load_config),
            primary_key=primary_key,
            table_settings=_storage_settings(
                compression=compression,
                clustered_columnstore=clustered_columnstore,
                filegroup=filegroup,
                textimage_filegroup=textimage_filegroup,
                primary_key_clustered=primary_key_clustered,
                index_fillfactor=fillfactor,
            ),
        )

    def _table_exists(self, load_config: LoadConfig) -> bool:
        database = getattr(load_config, "target_database", None)
        schema = str(load_config.target_schema)
        table = str(load_config.target_table)
        try:
            return bool(self.connector.table_exists(schema, table, database=database))
        except TypeError:
            schema_label = f"{database}.{schema}" if database and "." not in schema else schema
            return bool(self.connector.table_exists(schema_label, table))

    def _compression(self, load_config: LoadConfig) -> str:
        sys_prefix = _sys_catalog_prefix(load_config)
        rows = self._records(
            load_config,
            "SELECT TOP (1) UPPER(p.data_compression_desc) "
            f"FROM {sys_prefix}sys.partitions AS p "
            f"INNER JOIN {sys_prefix}sys.tables AS t ON t.object_id = p.object_id "
            f"INNER JOIN {sys_prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"WHERE s.name = {_sql_literal(_schema_name(load_config))} "
            f"AND t.name = {_sql_literal(load_config.target_table)} "
            "AND p.index_id IN (0, 1) "
            "ORDER BY p.partition_number",
        )
        if not rows:
            return "NONE"
        compression = str(rows[0][0] or "NONE").upper()
        return compression if compression in {"NONE", "ROW", "PAGE"} else "NONE"

    def _has_clustered_columnstore(self, load_config: LoadConfig) -> bool:
        sys_prefix = _sys_catalog_prefix(load_config)
        rows = self._records(
            load_config,
            "SELECT TOP (1) 1 "
            f"FROM {sys_prefix}sys.indexes AS i "
            f"INNER JOIN {sys_prefix}sys.tables AS t ON t.object_id = i.object_id "
            f"INNER JOIN {sys_prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"WHERE s.name = {_sql_literal(_schema_name(load_config))} "
            f"AND t.name = {_sql_literal(load_config.target_table)} "
            "AND i.type = 5",
        )
        return bool(rows)

    def _columns(self, load_config: LoadConfig) -> Mapping[str, PhysicalColumnState]:
        sys_prefix = _sys_catalog_prefix(load_config)
        rows = self._records(
            load_config,
            "SELECT c.name, t.name AS type_name, c.max_length, c.precision, c.scale, "
            "c.is_nullable, c.column_id "
            f"FROM {sys_prefix}sys.columns AS c "
            f"INNER JOIN {sys_prefix}sys.types AS t ON t.user_type_id = c.user_type_id "
            f"INNER JOIN {sys_prefix}sys.tables AS tab ON tab.object_id = c.object_id "
            f"INNER JOIN {sys_prefix}sys.schemas AS s ON s.schema_id = tab.schema_id "
            f"WHERE s.name = {_sql_literal(_schema_name(load_config))} "
            f"AND tab.name = {_sql_literal(load_config.target_table)} "
            "ORDER BY c.column_id",
        )
        return {
            str(row[0]): PhysicalColumnState(
                name=str(row[0]),
                target_type=_catalog_type(row[1], row[2], row[3], row[4]),
                nullable=bool(row[5]),
                position=int(row[6]),
            )
            for row in rows
        }

    def _primary_key(self, load_config: LoadConfig) -> tuple[tuple[str, ...], bool | None, int | None]:
        sys_prefix = _sys_catalog_prefix(load_config)
        rows = self._records(
            load_config,
            "SELECT c.name, i.type, i.fill_factor, ic.key_ordinal "
            f"FROM {sys_prefix}sys.indexes AS i "
            f"INNER JOIN {sys_prefix}sys.tables AS t ON t.object_id = i.object_id "
            f"INNER JOIN {sys_prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"INNER JOIN {sys_prefix}sys.index_columns AS ic "
            "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            f"INNER JOIN {sys_prefix}sys.columns AS c "
            "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            f"WHERE s.name = {_sql_literal(_schema_name(load_config))} "
            f"AND t.name = {_sql_literal(load_config.target_table)} "
            "AND i.is_primary_key = 1 AND i.is_disabled = 0 AND i.is_hypothetical = 0 "
            "AND ic.is_included_column = 0 ORDER BY ic.key_ordinal",
        )
        if not rows:
            return (), None, None
        columns = tuple(str(row[0]) for row in rows)
        fillfactor = int(rows[0][2])
        return columns, int(rows[0][1]) == 1, fillfactor or None

    def _placement(self, load_config: LoadConfig) -> tuple[str | None, str | None]:
        sys_prefix = _sys_catalog_prefix(load_config)
        rows = self._records(
            load_config,
            "SELECT TOP (1) ds.name, lob.name "
            f"FROM {sys_prefix}sys.tables AS t "
            f"INNER JOIN {sys_prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
            f"LEFT JOIN {sys_prefix}sys.indexes AS i ON i.object_id = t.object_id AND i.index_id IN (0, 1) "
            f"LEFT JOIN {sys_prefix}sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id "
            f"LEFT JOIN {sys_prefix}sys.data_spaces AS lob ON lob.data_space_id = t.lob_data_space_id "
            f"WHERE s.name = {_sql_literal(_schema_name(load_config))} "
            f"AND t.name = {_sql_literal(load_config.target_table)} "
            "ORDER BY CASE WHEN i.index_id = 1 THEN 0 ELSE 1 END",
        )
        if not rows:
            return None, None
        return _optional_text(rows[0][0]), _optional_text(rows[0][1])

    def _records(self, load_config: LoadConfig, query: str) -> list[Any]:
        database = getattr(load_config, "target_database", None)
        if database and hasattr(self.connector, "get_records_in_database"):
            return list(self.connector.get_records_in_database(database, query))
        if database and hasattr(self.connector, "execute_query_in_database"):
            return list(self.connector.execute_query_in_database(database, query))
        if hasattr(self.connector, "get_records"):
            return list(self.connector.get_records(query))
        return list(self.connector.execute_query(query))


@dataclass(frozen=True, slots=True)
class MssqlPhysicalMigrationDialect:
    """Render only online-safe SQL Server physical changes."""

    def is_online_safe_table_setting(self, setting: str) -> bool:
        return False

    def is_safe_window_table_setting(self, setting: str) -> bool:
        return setting == "compression"

    def suppress_table_setting_ddl(self, changed_settings: tuple[str, ...]) -> bool:
        """Avoid invalid rowstore rebuild SQL while columnstore layout drifts."""

        return "clustered_columnstore" in changed_settings

    def render_table_setting_update(
        self,
        *,
        table: str,
        setting: str,
        value: TableSettingValue,
    ) -> str:
        if setting != "compression":
            raise RuntimeError(f"SQL Server online setting update is not supported for {setting}")
        compression = str(value).upper()
        if compression not in {"NONE", "ROW", "PAGE"}:
            raise ValueError("MSSQL rowstore compression must be NONE, ROW, or PAGE")
        qualified = table if "[" in table else _quote_qualified_table(table)
        return f"ALTER TABLE {qualified} REBUILD WITH (DATA_COMPRESSION = {compression});"


def expected_mssql_created_physical_state(
    *,
    table: str,
    columns: tuple[ColumnDef, ...],
    contract: MssqlPhysicalDesignContract,
    default_filegroup: str,
) -> PhysicalTableState:
    """Project the exact physical state produced by the target CREATE renderer."""

    effective = contract if contract.active else MssqlPhysicalDesignContract(active=False)
    storage = effective.storage
    filegroup = storage.filegroup or default_filegroup
    has_lob = any(_is_lob_type(column.dtype) for column in columns)
    return PhysicalTableState(
        sink_type="mssql",
        table=table,
        columns={
            column.name: PhysicalColumnState(
                name=column.name,
                target_type=canonical_mssql_catalog_type(column.dtype),
                nullable=column.nullable,
                position=index,
            )
            for index, column in enumerate(columns, start=1)
        },
        primary_key=effective.primary_key,
        table_settings=_storage_settings(
            compression=storage.compression,
            clustered_columnstore=storage.clustered_columnstore,
            filegroup=filegroup,
            textimage_filegroup=(storage.textimage_filegroup or filegroup) if has_lob else None,
            primary_key_clustered=True if effective.primary_key else None,
            index_fillfactor=storage.index_fillfactor,
        ),
    )


def _qualified_table(load_config: LoadConfig) -> str:
    database = getattr(load_config, "target_database", None)
    schema = str(load_config.target_schema)
    if database and "." not in schema:
        return _quote_qualified_table(f"{database}.{schema}.{load_config.target_table}")
    return _quote_qualified_table(f"{schema}.{load_config.target_table}")


def _schema_name(load_config: LoadConfig) -> str:
    schema = str(load_config.target_schema)
    if "." in schema:
        return schema.split(".")[-1]
    return schema


def _storage_settings(
    *,
    compression: str,
    clustered_columnstore: bool,
    filegroup: str | None,
    textimage_filegroup: str | None,
    primary_key_clustered: bool | None,
    index_fillfactor: int | None,
) -> Mapping[str, TableSettingValue]:
    settings: dict[str, TableSettingValue] = {
        "compression": compression,
        "clustered_columnstore": clustered_columnstore,
    }
    for key, value in (
        ("filegroup", filegroup),
        ("textimage_filegroup", textimage_filegroup),
        ("primary_key_clustered", primary_key_clustered),
        ("index_fillfactor", index_fillfactor),
    ):
        if value is not None:
            settings[key] = value
    return settings


def _catalog_type(name: Any, max_length: Any, precision: Any, scale: Any) -> str:
    return render_mssql_catalog_type(
        str(name),
        max_length=int(max_length),
        precision=int(precision),
        scale=int(scale),
    )


def _optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _is_lob_type(dtype: str) -> bool:
    normalized = str(dtype).casefold().replace(" ", "")
    return normalized in {"nvarchar(max)", "varchar(max)", "varbinary(max)", "xml", "text", "ntext", "image"}


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sys_catalog_prefix(load_config: LoadConfig) -> str:
    """Qualify ``sys.*`` catalogs when the target database differs from the session DB.

    ``table_exists`` already uses database-qualified ``INFORMATION_SCHEMA``, but
    compression/index probes historically queried bare ``sys.*`` via
    ``get_records``. Without a connector ``get_records_in_database`` helper that
    falsely reports ``NONE`` and blocks PAGE reconciliation on live PAGE tables.
    """
    database = getattr(load_config, "target_database", None)
    if database and "." not in str(load_config.target_schema):
        return f"{quote_mssql_identifier(str(database))}."
    schema = str(load_config.target_schema)
    if "." in schema:
        return f"{quote_mssql_identifier(schema.split('.', 1)[0])}."
    return ""


def _quote_qualified_table(table: str) -> str:
    parts = tuple(str(part).strip() for part in str(table).split("."))
    if len(parts) not in {2, 3} or any(not part for part in parts):
        raise ValueError(f"Unsafe MSSQL table identity: {table}")
    return ".".join(quote_mssql_identifier(part) for part in parts)


__all__ = [
    "MssqlPhysicalIntrospector",
    "MssqlPhysicalMigrationDialect",
    "expected_mssql_created_physical_state",
]
