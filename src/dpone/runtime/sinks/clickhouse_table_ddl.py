"""ClickHouse table DDL rendering used by planning and runtime sinks."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from dpone.runtime.physical_design.table_settings import (
    PhysicalDesignAdvisor,
    PhysicalSettingClassifier,
    PhysicalSettingRegistry,
    ResolvedTableSetting,
    TableSettingsOptions,
    TableSettingValue,
    TargetTableSettingsDialect,
)

ClickHouseDdlScope = Literal["local", "cluster"]


@dataclass(frozen=True, slots=True)
class ClickHouseClusterDesign:
    """ClickHouse cluster clause for replicated DDL."""

    name: str | None = None
    ddl_scope: ClickHouseDdlScope = "local"

    @classmethod
    def from_config(cls, raw: Any) -> ClickHouseClusterDesign:
        if isinstance(raw, str):
            name = raw.strip() or None
            return cls(name=name, ddl_scope="cluster" if name else "local")
        values = raw if isinstance(raw, Mapping) else {}
        name = str(values.get("name") or "").strip() or None
        ddl_scope = _ddl_scope(values)
        if ddl_scope == "cluster" and not name:
            raise ValueError("physical_design.storage.clickhouse.cluster.name is required for cluster DDL")
        return cls(name=name, ddl_scope=ddl_scope)

    @property
    def on_cluster(self) -> bool:
        return self.ddl_scope == "cluster" and bool(self.name)

    @property
    def ddl_clause(self) -> str:
        if not self.on_cluster:
            return ""
        return f" ON CLUSTER {_quote_identifier(self.name or '')}"


@dataclass(frozen=True, slots=True)
class ClickHouseAccessTableDesign:
    """Optional ClickHouse access table facade, for example Distributed."""

    name: str | None = None
    engine: str = "Distributed"
    database: str | None = None
    sharding_key: str = "rand()"

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> ClickHouseAccessTableDesign:
        values = raw if isinstance(raw, Mapping) else {}
        name = str(values.get("name") or "").strip() or None
        engine = str(values.get("engine") or "Distributed").strip()
        if name and engine != "Distributed":
            raise ValueError("physical_design.storage.clickhouse.access_table.engine must be Distributed")
        database = str(values.get("database") or "").strip() or None
        sharding_key = str(values.get("sharding_key") or "rand()").strip()
        return cls(name=name, engine=engine, database=database, sharding_key=sharding_key)

    @property
    def enabled(self) -> bool:
        return bool(self.name)


@dataclass(frozen=True, slots=True)
class ClickHouseTableDesign:
    """Physical table options relevant to ClickHouse CREATE TABLE."""

    engine: str = "MergeTree"
    partition_by: str | None = None
    order_by: tuple[str, ...] = ()
    ttl: str | None = None
    cluster: ClickHouseClusterDesign = field(default_factory=ClickHouseClusterDesign)
    access_table: ClickHouseAccessTableDesign = field(default_factory=ClickHouseAccessTableDesign)
    table_settings: TableSettingsOptions = field(default_factory=TableSettingsOptions)

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> ClickHouseTableDesign:
        values = options if isinstance(options, Mapping) else {}
        physical = values.get("physical_design", {})
        storage: Mapping[str, Any] = {}
        if isinstance(physical, Mapping):
            raw_storage = physical.get("storage", {})
            if isinstance(raw_storage, Mapping) and isinstance(raw_storage.get("clickhouse"), Mapping):
                storage = raw_storage["clickhouse"]
        engine = str(storage.get("engine") or values.get("clickhouse_engine") or "MergeTree")
        partition_by = (
            storage.get("partition_by") or values.get("clickhouse_partition_by") or values.get("partition_by")
        )
        order_by = storage.get("order_by")
        if order_by is None:
            order_by = values.get("clickhouse_order_by", values.get("order_by", ()))
        ttl = storage.get("ttl")
        if ttl is None:
            ttl = storage.get("table_ttl")
        if ttl is None:
            ttl = values.get("clickhouse_ttl") or values.get("ttl") or values.get("table_ttl")
        return cls(
            engine=engine,
            partition_by=str(partition_by) if partition_by else None,
            order_by=tuple(_list_option(order_by)),
            ttl=_normalize_ttl_expression(ttl),
            cluster=ClickHouseClusterDesign.from_config(storage.get("cluster")),
            access_table=ClickHouseAccessTableDesign.from_config(_mapping_or_none(storage.get("access_table"))),
            table_settings=TableSettingsOptions.from_config(
                storage.get("table_settings"),
                field_prefix="physical_design.storage.clickhouse.table_settings",
            ),
        )


@dataclass(frozen=True, slots=True)
class ClickHouseTableSettingsDialect:
    """Render ClickHouse MergeTree table settings from validated options."""

    classifier: PhysicalSettingClassifier = field(
        default_factory=lambda: PhysicalSettingClassifier(PhysicalSettingRegistry.clickhouse())
    )

    def resolve(self, options: TableSettingsOptions) -> tuple[ResolvedTableSetting, ...]:
        return self.classifier.resolve(options)

    def warnings(self, options: TableSettingsOptions) -> tuple[str, ...]:
        return PhysicalDesignAdvisor().warnings(self.resolve(options))

    def render_settings(self, options: TableSettingsOptions) -> str:
        decisions = self.resolve(options)
        if not decisions:
            return ""
        assignments = (f"{decision.key} = {_clickhouse_literal(decision.value)}" for decision in decisions)
        return "SETTINGS " + ", ".join(assignments)


@dataclass(frozen=True, slots=True)
class ClickHouseTableDdlRenderer:
    """Render ClickHouse CREATE TABLE DDL without opening a connection."""

    table_settings_dialect: TargetTableSettingsDialect = field(default_factory=ClickHouseTableSettingsDialect)

    def render_create_database(self, *, database: str, design: ClickHouseTableDesign | None = None) -> str:
        resolved = design or ClickHouseTableDesign()
        return f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(database)}{resolved.cluster.ddl_clause}"

    def render_create_table(
        self,
        *,
        table: str,
        columns_sql: Sequence[str],
        design: ClickHouseTableDesign | None = None,
        if_not_exists: bool = False,
    ) -> str:
        resolved = design or ClickHouseTableDesign()
        clause = "IF NOT EXISTS " if if_not_exists else ""
        lines = [
            f"CREATE TABLE {clause}{table}{resolved.cluster.ddl_clause} "
            f"({', '.join(columns_sql)}) ENGINE = {resolved.engine}"
        ]
        if resolved.partition_by:
            lines.append(f"PARTITION BY {resolved.partition_by}")
        lines.append(_order_by_sql(resolved.order_by))
        if resolved.ttl:
            lines.append(f"TTL {resolved.ttl}")
        settings_clause = self.table_settings_dialect.render_settings(resolved.table_settings)
        if settings_clause:
            lines.append(settings_clause)
        return " ".join(lines)

    def render_create_table_statements(
        self,
        *,
        table: str,
        columns_sql: Sequence[str],
        design: ClickHouseTableDesign | None = None,
        if_not_exists: bool = False,
    ) -> tuple[str, ...]:
        resolved = design or ClickHouseTableDesign()
        statements = [
            self.render_create_table(
                table=table,
                columns_sql=columns_sql,
                design=resolved,
                if_not_exists=if_not_exists,
            )
        ]
        access = self._render_access_table(table=table, design=resolved)
        if access:
            statements.append(access)
        return tuple(statements)

    def _render_access_table(self, *, table: str, design: ClickHouseTableDesign) -> str:
        access = design.access_table
        if not access.enabled:
            return ""
        if not design.cluster.name:
            raise ValueError("physical_design.storage.clickhouse.access_table requires cluster name")
        database, source_table = _split_table_name(table)
        access_database = access.database or database
        access_table = _quote_table_name(access.name or "", default_database=access_database)
        return (
            f"CREATE TABLE IF NOT EXISTS {access_table}{design.cluster.ddl_clause} "
            f"AS {table} "
            "ENGINE = Distributed("
            f"{_clickhouse_literal(design.cluster.name)}, "
            f"{_clickhouse_literal(database)}, "
            f"{_clickhouse_literal(source_table)}, "
            f"{access.sharding_key})"
        )


def normalize_clickhouse_ttl_expression(value: Any) -> str | None:
    """Normalize a table-level TTL expression for CREATE/recon (no TTL keyword)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise ValueError(
            "physical_design.storage.clickhouse.ttl must be a non-empty expression "
            "(example: created_at + toIntervalDay(7))"
        )
    if ";" in text or "\x00" in text:
        raise ValueError("physical_design.storage.clickhouse.ttl must not contain ';' or null bytes")
    if re.match(r"(?i)^TTL\b", text):
        raise ValueError(
            "physical_design.storage.clickhouse.ttl must be an expression without the TTL keyword "
            "(example: created_at + toIntervalDay(7))"
        )
    return " ".join(text.split())


def _normalize_ttl_expression(value: Any) -> str | None:
    return normalize_clickhouse_ttl_expression(value)


def _order_by_sql(order_by: Sequence[str]) -> str:
    if not order_by:
        return "ORDER BY tuple()"
    return "ORDER BY (" + ", ".join(_clickhouse_order_item(item) for item in order_by) + ")"


def _clickhouse_order_item(value: str) -> str:
    item = str(value).strip()
    if item.startswith("`") or "(" in item or ")" in item:
        return item
    return "`" + item.replace("`", "``") + "`"


def _clickhouse_literal(value: TableSettingValue) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def _list_option(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _ddl_scope(values: Mapping[str, Any]) -> ClickHouseDdlScope:
    if "ddl_scope" in values:
        scope = str(values.get("ddl_scope") or "local").strip().lower()
        if scope not in {"local", "cluster"}:
            raise ValueError("physical_design.storage.clickhouse.cluster.ddl_scope must be local or cluster")
        return cast(ClickHouseDdlScope, scope)
    return "cluster" if bool(values.get("on_cluster", False)) else "local"


def _quote_identifier(value: str) -> str:
    return "`" + str(value).strip("`").replace("`", "``") + "`"


def _quote_table_name(value: str, *, default_database: str) -> str:
    if "." in value:
        return ".".join(_quote_identifier(part) for part in value.split("."))
    return f"{_quote_identifier(default_database)}.{_quote_identifier(value)}"


def _split_table_name(table: str) -> tuple[str, str]:
    parts = [part.strip().strip("`") for part in table.split(".")]
    if len(parts) != 2:
        raise ValueError(f"ClickHouse table name must be database-qualified: {table}")
    return parts[0], parts[1]


__all__ = [
    "ClickHouseAccessTableDesign",
    "ClickHouseClusterDesign",
    "ClickHouseTableDesign",
    "ClickHouseTableDdlRenderer",
    "ClickHouseTableSettingsDialect",
    "normalize_clickhouse_ttl_expression",
]
