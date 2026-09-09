from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from dpone.ports.partition_clone import PartitionCloneFactory
from dpone.runtime.sinks.clickhouse_operation_tables import ClickHouseOperationTableResolver
from dpone.runtime.sinks.clickhouse_physical_types import (
    DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER,
    ClickHousePhysicalColumnTypeResolver,
)
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
    clickhouse_type_for_mssql,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.clickhouse_connector import ClickHouseConnectorPort


@dataclass(frozen=True, slots=True)
class ClickHouseCatalogColumn:
    """Exact ClickHouse catalog projection consumed by schema governance."""

    name: str
    dtype: str
    nullable: bool
    collation: None = None


class ClickHouseTargetCatalogMixin:
    """Project exact target types/nullability from ``system.columns``."""

    connector: ClickHouseConnectorPort

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        rows = self.connector.get_records(f"EXISTS TABLE {self._table(load_config)}")
        if not (rows and int(rows[0][0])):
            return []
        database = str(load_config.target_schema).replace("'", "\\'")
        table = str(load_config.target_table).replace("'", "\\'")
        rows = self.connector.get_records(
            "SELECT name, type FROM system.columns "
            f"WHERE database = '{database}' AND table = '{table}' ORDER BY position"
        )
        return [(str(row[0]), str(row[1])) for row in rows]

    def get_target_columns(self, load_config: LoadConfig) -> list[ClickHouseCatalogColumn]:
        """Return exact ClickHouse target types and nullability from the catalog."""

        return [
            ClickHouseCatalogColumn(
                name,
                dtype,
                nullable=DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER.is_nullable(dtype),
            )
            for name, dtype in self.get_target_schema(load_config)
        ]

    def _table(self, load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"


class ClickHouseSqlMixin:
    connector: ClickHouseConnectorPort
    logger: Any
    _physical_type_resolver: ClickHousePhysicalColumnTypeResolver

    @staticmethod
    def _mutations_sync(load_config: LoadConfig) -> int:
        options = getattr(load_config, "options", {}) or {}
        raw = getattr(load_config, "mutations_sync", None)
        if raw is None:
            raw = options.get("mutations_sync", 1)
        return int(raw)

    @staticmethod
    def _literal(value: Any) -> str:
        if value is None:
            raise ValueError("ClickHouse partition_replace cannot replace NULL partition values")
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int | float):
            return str(value)
        if isinstance(value, date | datetime):
            return f"'{value.isoformat()}'"
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _count_where(self, load_config: LoadConfig, predicate: str) -> int:
        rows = self.connector.get_records(f"SELECT count() FROM {self._table(load_config)} WHERE {predicate}")
        return int(rows[0][0]) if rows else 0

    def _count_target_matching_staging_keys(
        self,
        load_config: LoadConfig,
        staging_config: LoadConfig,
        unique_key: Sequence[str],
    ) -> int:
        rows = self.connector.get_records(
            f"SELECT count() FROM {self._table(load_config)} "
            f"WHERE {self._key_in_staging_condition(staging_config, unique_key)}"
        )
        return int(rows[0][0]) if rows else 0

    def _key_in_staging_condition(self, staging_config: LoadConfig, unique_key: Sequence[str]) -> str:
        if len(unique_key) == 1:
            column = unique_key[0]
            return f"`{column}` IN (SELECT `{column}` FROM {self._table(staging_config)})"
        columns = ", ".join(f"`{column}`" for column in unique_key)
        return f"({columns}) IN (SELECT {columns} FROM {self._table(staging_config)})"

    def _table_exists(self, load_config: LoadConfig) -> bool:
        rows = self.connector.get_records(f"EXISTS TABLE {self._table(load_config)}")
        return bool(rows and int(rows[0][0]))

    def _cluster_ddl_clause(self, load_config: LoadConfig | None) -> str:
        if load_config is None:
            return ""
        return ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {}).cluster.ddl_clause

    def _drop_table(self, table_name: str, load_config: LoadConfig | None = None) -> None:
        self.connector.execute_query(f"DROP TABLE IF EXISTS {table_name}{self._cluster_ddl_clause(load_config)}")

    def _count(self, load_config: LoadConfig) -> int:
        rows = self.connector.get_records(f"SELECT count() FROM {self._table(load_config)}")
        return int(rows[0][0]) if rows else 0

    def _table(self, load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    def _operation_table_config(self, load_config: LoadConfig, operation: str) -> LoadConfig:
        return _operation_table_resolver(self).operation_config(load_config, operation)

    @staticmethod
    def _map_type(dtype: str) -> str:
        return clickhouse_type_for_mssql(dtype)

    @staticmethod
    def _map_type_for_config(dtype: str, load_config: LoadConfig) -> str:
        return DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER.resolve(
            load_config=load_config,
            type_mapper=MssqlClickHouseTypeMapper(_type_policy(load_config)),
            column="_",
            source_type=dtype,
        )

    @classmethod
    def _coerce_file_row(cls, row: Sequence[str], schema: Sequence[tuple[str, str]]) -> tuple[Any, ...]:
        return tuple(cls._coerce_file_value(value, dtype) for value, (_, dtype) in zip(row, schema, strict=False))

    @classmethod
    def _coerce_file_value(cls, value: str, dtype: str) -> Any:
        # ClickHouse TabSeparated null marker and empty CSV fields are both NULL.
        if value == "" or value == "\\N":
            return None
        normalized = str(dtype).lower().strip()
        if " nullable" in normalized:
            normalized = normalized.replace(" nullable", "").strip()
        if normalized.startswith("nullable(") and normalized.endswith(")"):
            normalized = normalized[len("nullable(") : -1].strip()
        base = normalized.split("(", 1)[0].strip()
        if "bigint" in normalized or normalized.startswith("int") or base in {"smallint", "tinyint", "integer"}:
            return int(value)
        if any(token in normalized for token in ("decimal", "numeric", "money")):
            return Decimal(value)
        if any(token in normalized for token in ("float", "double", "real")):
            return float(value)
        if "bool" in normalized or base == "bit":
            return 1 if value.lower() in {"1", "true", "t", "yes", "y"} else 0
        # Match type bases explicitly: substring "time"/"date" must not match
        # uniqueidentifier, datetime*, or Postgres "time without time zone".
        if base == "date":
            return date.fromisoformat(value[:10])
        if base == "time" or base.startswith("time "):
            return value
        if base.startswith("datetime") or base in {"smalldatetime", "timestamp"} or "timestamp" in base:
            return cls._parse_datetime(value)
        return value

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        normalized = value.replace(" ", "T")
        if "." in normalized:
            head, tail = normalized.split(".", 1)
            fraction = "".join(ch for ch in tail if ch.isdigit())[:6]
            normalized = f"{head}.{fraction}"
        return datetime.fromisoformat(normalized)

    def _ensure_target_table(self, load_config: LoadConfig, schema: Sequence[tuple[str, str]]) -> None:
        self._create_table(load_config, schema, if_not_exists=True)

    def project_schema_evolution_source_columns(
        self,
        load_config: LoadConfig,
        columns: Sequence[tuple[str, str, bool, str | None]],
    ) -> tuple[tuple[str, str, bool, None], ...]:
        """Return the exact ClickHouse shape produced by the physical policy.

        Raw source nullability is intentionally not copied: ClickHouse may
        authoritatively default NULL values into non-nullable physical columns.
        The same resolver is used by target and staging DDL, so schema
        evolution cannot invent a contradictory relaxation.
        """

        type_mapper = MssqlClickHouseTypeMapper(_type_policy(load_config))
        projected: list[tuple[str, str, bool, None]] = []
        for column, dtype, _nullable, _collation in columns:
            target_type = self._column_type(load_config, type_mapper, column, dtype)
            projected.append(
                (
                    column,
                    target_type,
                    self._physical_type_resolver.is_nullable(target_type),
                    None,
                )
            )
        return tuple(projected)

    def _create_table(
        self,
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
        *,
        if_not_exists: bool,
    ) -> None:
        type_mapper = MssqlClickHouseTypeMapper(_type_policy(load_config))
        columns_sql = [
            f"`{column}` {self._column_type(load_config, type_mapper, column, dtype)}" for column, dtype in schema
        ]
        self._create_table_from_columns_sql(load_config, columns_sql, if_not_exists=if_not_exists)

    def _column_type(
        self,
        load_config: LoadConfig,
        type_mapper: MssqlClickHouseTypeMapper,
        column: str,
        dtype: str,
    ) -> str:
        resolver = getattr(self, "_physical_type_resolver", DEFAULT_CLICKHOUSE_PHYSICAL_COLUMN_TYPE_RESOLVER)
        return resolver.resolve(load_config=load_config, type_mapper=type_mapper, column=column, source_type=dtype)

    def _create_table_with_clickhouse_types(
        self,
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
        *,
        if_not_exists: bool,
    ) -> None:
        columns_sql = [f"`{column}` {dtype}" for column, dtype in schema]
        self._create_table_from_columns_sql(load_config, columns_sql, if_not_exists=if_not_exists)

    def _create_table_from_columns_sql(
        self,
        load_config: LoadConfig,
        columns_sql: Sequence[str],
        *,
        if_not_exists: bool,
    ) -> None:
        renderer = ClickHouseTableDdlRenderer()
        design = self._ensure_database(load_config)
        for statement in renderer.render_create_table_statements(
            table=self._table(load_config),
            columns_sql=columns_sql,
            design=design,
            if_not_exists=if_not_exists,
        ):
            self.connector.execute_query(statement)

    def _clone_connector(self, partition_index: int = 0) -> ClickHouseConnectorPort:
        return PartitionCloneFactory().clone(self.connector, partition_index)

    def _ensure_database(self, load_config: LoadConfig) -> ClickHouseTableDesign:
        design = ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {})
        renderer = ClickHouseTableDdlRenderer()
        self.connector.execute_query(renderer.render_create_database(database=load_config.target_schema, design=design))
        return design


def _type_policy(load_config: LoadConfig) -> MssqlClickHouseTypePolicy:
    return MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity"))


def _operation_table_resolver(owner: Any) -> ClickHouseOperationTableResolver:
    resolver = getattr(owner, "_clickhouse_operation_tables", None)
    if resolver is None:
        resolver = ClickHouseOperationTableResolver()
        setattr(owner, "_clickhouse_operation_tables", resolver)
    return resolver
