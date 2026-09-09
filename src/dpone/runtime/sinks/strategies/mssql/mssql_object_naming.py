"""SQL Server object-name helpers shared by MSSQL load strategies."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_object_name import (
    MSSQLObjectName,
    mssql_ensure_schema_statement,
    mssql_sp_rename_statement,
)
from dpone.runtime.artifact_models import StagingTableArtifact


class MSSQLObjectNamingMixin:
    connector: Any

    def _object_name(self, schema: str, table: str, *, database: str | None = None) -> MSSQLObjectName:
        return MSSQLObjectName.from_parts(schema=schema, table=table, database=database)

    def _target_object(self, load_config: Any, *, table: str | None = None) -> MSSQLObjectName:
        return self._object_name(
            load_config.target_schema,
            table or load_config.target_table,
            database=getattr(load_config, "target_database", None),
        )

    def _target_name(self, load_config: Any, *, table: str | None = None) -> str:
        return self._target_object(load_config, table=table).quoted()

    def _qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str:
        return self._object_name(schema, table, database=database).quoted()

    def _staging_name(self, staging: StagingTableArtifact) -> str:
        return self._object_name(staging.schema, staging.table, database=getattr(staging, "database", None)).quoted()

    def _table_exists(self, load_config: Any) -> bool:
        return self._connector_table_exists(self._target_object(load_config))

    def _ensure_schema(self, schema: str, *, database: str | None = None) -> None:
        name = self._object_name(schema, "__dpone_schema_probe", database=database)
        self.connector.execute_query(*mssql_ensure_schema_statement(name.schema_label))

    def _staging_table_exists(self, schema: str, table: str, *, database: str | None = None) -> bool:
        return self._connector_table_exists(self._object_name(schema, table, database=database))

    def _rename_target_table(self, load_config: Any, old_table: str, new_table: str) -> None:
        self.connector.execute_query(
            mssql_sp_rename_statement(self._target_object(load_config, table=old_table), new_table)
        )

    def _connector_table_exists(self, name: MSSQLObjectName) -> bool:
        try:
            return self.connector.table_exists(name.schema, name.table, database=name.database)
        except TypeError:
            return self.connector.table_exists(name.schema_label, name.table)


__all__ = ["MSSQLObjectNamingMixin"]
