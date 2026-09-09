"""Physical target state models and target-facing ports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from dpone.readiness.physical_design_models import PhysicalDesignPlan
from dpone.runtime.sinks.clickhouse_table_ddl import normalize_clickhouse_ttl_expression

TableSettingValue = str | int | float | bool


@dataclass(frozen=True, slots=True)
class PhysicalColumnState:
    name: str
    target_type: str
    nullable: bool = True
    position: int | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> PhysicalColumnState:
        return cls(
            name=str(raw.get("name", "")),
            target_type=str(raw.get("target_type") or raw.get("type") or raw.get("dtype") or ""),
            nullable=bool(raw.get("nullable", "Nullable(" in str(raw.get("target_type") or raw.get("type") or ""))),
            position=_optional_int(raw.get("position")),
        )

    def to_dict(self) -> dict[str, object]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True, slots=True)
class PhysicalTableState:
    sink_type: str
    table: str
    columns: Mapping[str, PhysicalColumnState] = field(default_factory=dict)
    engine: str | None = None
    engine_full: str | None = None
    partition_by: str | None = None
    order_by: tuple[str, ...] = ()
    primary_key: tuple[str, ...] = ()
    ttl: str | None = None
    cluster: str | None = None
    on_cluster: bool = False
    access_table: str | None = None
    access_table_engine: str | None = None
    access_table_sharding_key: str | None = None
    table_settings: Mapping[str, TableSettingValue] = field(default_factory=dict)
    table_settings_error: str | None = None

    @classmethod
    def from_physical_plan(cls, plan: PhysicalDesignPlan) -> PhysicalTableState:
        if plan.sink_type == "mssql":
            return _physical_state_from_mssql_plan(plan)
        storage = plan.options.storage.get("clickhouse", {}) if plan.sink_type == "clickhouse" else {}
        storage = storage if isinstance(storage, Mapping) else {}
        cluster_name, on_cluster = _cluster_state(storage.get("cluster"))
        access = storage.get("access_table", {})
        access = access if isinstance(access, Mapping) else {}
        ttl_raw = storage.get("ttl")
        if ttl_raw is None:
            ttl_raw = storage.get("table_ttl")
        return cls(
            sink_type=plan.sink_type,
            table=plan.table,
            columns={
                name: PhysicalColumnState(
                    name=column.name,
                    target_type=column.target_type,
                    nullable=column.nullable,
                    position=index,
                )
                for index, (name, column) in enumerate(plan.columns.items(), start=1)
            },
            engine=str(storage.get("engine") or "MergeTree") if plan.sink_type == "clickhouse" else None,
            partition_by=str(storage["partition_by"]) if storage.get("partition_by") else None,
            order_by=tuple(_list_option(storage.get("order_by"))),
            ttl=normalize_clickhouse_ttl_expression(ttl_raw) if plan.sink_type == "clickhouse" else None,
            cluster=cluster_name,
            on_cluster=on_cluster,
            access_table=_optional_str(access.get("name")),
            access_table_engine=_optional_str(access.get("engine")),
            access_table_sharding_key=_optional_str(access.get("sharding_key")),
            table_settings=dict(_table_settings(storage.get("table_settings"))),
        )

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> PhysicalTableState:
        columns = _columns(raw.get("columns", {}))
        settings, settings_error = _settings(raw.get("table_settings", {}))
        ttl_raw = raw.get("ttl")
        if ttl_raw is None:
            ttl_raw = raw.get("table_ttl")
        return cls(
            sink_type=str(raw.get("sink_type") or raw.get("target") or ""),
            table=str(raw.get("table") or ""),
            columns=columns,
            engine=_optional_str(raw.get("engine")),
            engine_full=_optional_str(raw.get("engine_full")),
            partition_by=_optional_str(raw.get("partition_by") or raw.get("partition_key")),
            order_by=tuple(_list_option(raw.get("order_by") or raw.get("sorting_key"))),
            primary_key=tuple(_list_option(raw.get("primary_key"))),
            ttl=normalize_clickhouse_ttl_expression(ttl_raw),
            cluster=_optional_str(raw.get("cluster")),
            on_cluster=bool(raw.get("on_cluster", False)),
            access_table=_optional_str(raw.get("access_table")),
            access_table_engine=_optional_str(raw.get("access_table_engine")),
            access_table_sharding_key=_optional_str(raw.get("access_table_sharding_key")),
            table_settings=settings,
            table_settings_error=settings_error or _optional_str(raw.get("table_settings_error")),
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "sink_type": self.sink_type,
            "table": self.table,
            "columns": {name: column.to_dict() for name, column in self.columns.items()},
            "order_by": list(self.order_by),
            "primary_key": list(self.primary_key),
            "table_settings": dict(self.table_settings),
        }
        for key in (
            "engine",
            "engine_full",
            "partition_by",
            "ttl",
            "cluster",
            "access_table",
            "access_table_engine",
            "access_table_sharding_key",
            "table_settings_error",
        ):
            value = getattr(self, key)
            if value:
                payload[key] = value
        if self.on_cluster:
            payload["on_cluster"] = self.on_cluster
        return payload


class TargetPhysicalIntrospector(Protocol):
    def inspect(self, load_config: Any) -> PhysicalTableState: ...


class TargetPhysicalMigrationDialect(Protocol):
    def is_online_safe_table_setting(self, setting: str) -> bool:
        """Return whether an existing table setting can be safely changed online."""

    def is_safe_window_table_setting(self, setting: str) -> bool:
        """Return whether a blocking table-setting change may run in safe_window mode."""

    def render_table_setting_update(
        self,
        *,
        table: str,
        setting: str,
        value: TableSettingValue,
    ) -> str: ...


def _physical_state_from_mssql_plan(plan: PhysicalDesignPlan) -> PhysicalTableState:
    resolved = plan.resolved_target_design
    if resolved is None or not hasattr(resolved, "storage") or not hasattr(resolved, "primary_key"):
        raise TypeError("resolved MSSQL physical-design contract is required")
    contract = resolved
    if not contract.active:
        return PhysicalTableState(sink_type=plan.sink_type, table=plan.table)
    storage = contract.storage
    settings: dict[str, TableSettingValue] = {
        "compression": storage.compression,
        "clustered_columnstore": storage.clustered_columnstore,
    }
    if storage.filegroup is not None:
        settings["filegroup"] = storage.filegroup
    if storage.textimage_filegroup is not None:
        settings["textimage_filegroup"] = storage.textimage_filegroup
    if storage.index_fillfactor is not None:
        settings["index_fillfactor"] = storage.index_fillfactor
    if contract.primary_key:
        settings["primary_key_clustered"] = True
    return PhysicalTableState(
        sink_type=plan.sink_type,
        table=plan.table,
        columns={
            name: PhysicalColumnState(
                name=column.name,
                target_type=column.target_type,
                nullable=column.nullable,
                position=index,
            )
            for index, (name, column) in enumerate(plan.columns.items(), start=1)
        },
        primary_key=contract.primary_key,
        table_settings=settings,
    )


def _columns(raw: Any) -> Mapping[str, PhysicalColumnState]:
    if isinstance(raw, Mapping):
        values = {
            str(name): PhysicalColumnState.from_mapping({"name": name, **dict(value)})
            for name, value in raw.items()
            if isinstance(value, Mapping)
        }
        ordered = sorted(values.values(), key=lambda item: (item.position is None, item.position or 0, item.name))
        return {column.name: column for column in ordered}
    if isinstance(raw, Sequence) and not isinstance(raw, str):
        column_states = [PhysicalColumnState.from_mapping(item) for item in raw if isinstance(item, Mapping)]
        return {column.name: column for column in sorted(column_states, key=lambda item: item.position or 0)}
    return {}


def _settings(raw: Any) -> tuple[Mapping[str, TableSettingValue], str | None]:
    if raw is None:
        return {}, None
    if not isinstance(raw, Mapping):
        return {}, "table_settings must be an object"
    return dict(sorted(_table_settings(raw).items())), None


def _table_settings(raw: Any) -> Mapping[str, TableSettingValue]:
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): value for key, value in raw.items() if isinstance(value, bool | int | float | str)}


def _cluster_state(raw: Any) -> tuple[str | None, bool]:
    if isinstance(raw, str):
        name = _optional_str(raw)
        return name, bool(name)
    if not isinstance(raw, Mapping):
        return None, False
    name = _optional_str(raw.get("name"))
    if "ddl_scope" in raw:
        return name, str(raw.get("ddl_scope") or "local").lower() == "cluster" and bool(name)
    return name, bool(raw.get("on_cluster", False)) and bool(name)


def _list_option(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return _split_expression_list(value)
    if isinstance(value, Sequence):
        return [str(item) for item in value]
    return [str(value)]


def _split_expression_list(value: str) -> list[str]:
    normalized = value.strip()
    if not normalized or normalized.lower() == "tuple()":
        return []
    return [item.strip().strip("`") for item in normalized.split(",") if item.strip()]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "PhysicalColumnState",
    "PhysicalTableState",
    "TableSettingValue",
    "TargetPhysicalIntrospector",
    "TargetPhysicalMigrationDialect",
]
