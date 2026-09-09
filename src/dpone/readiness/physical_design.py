"""Physical target DDL planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from dpone.readiness.physical_design_models import (
    LowCardinalityOptions,
    PhysicalDesignOptions,
    PhysicalDesignPlan,
    ResolvedTargetColumn,
)
from dpone.readiness.schema_contracts import SchemaContract
from dpone.readiness.target_type_resolvers import TargetTypeResolver
from dpone.runtime.sinks.clickhouse_table_ddl import (
    ClickHouseTableDdlRenderer,
    ClickHouseTableDesign,
    ClickHouseTableSettingsDialect,
)
from dpone.runtime.sinks.mssql_table_ddl import (
    MssqlPhysicalDesignContract,
    MssqlTableDdlRenderer,
    mssql_column_definitions,
    mssql_qualified_table,
)
from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy
from dpone.type_system import ColumnProfile, TypeInferenceOptions, TypeInferenceService


class PhysicalDesignPlanner:
    """Build target-specific DDL from logical schema and physical hints."""

    def __init__(self, resolver: TargetTypeResolver | None = None) -> None:
        self.resolver = resolver or TargetTypeResolver()

    def plan(
        self,
        *,
        sink_type: str,
        table: str,
        source_schema: Sequence[tuple[str, str]] | None = None,
        schema_contract: SchemaContract | None = None,
        options: PhysicalDesignOptions | None = None,
        profiles: Mapping[str, ColumnProfile] | None = None,
        type_fidelity: Mapping[str, Any] | None = None,
        resolved_columns: Mapping[str, ResolvedTargetColumn] | None = None,
    ) -> PhysicalDesignPlan:
        resolved_options = options or PhysicalDesignOptions()
        contract = schema_contract or SchemaContract()
        resolver = self.resolver
        if type_fidelity is not None:
            resolver = TargetTypeResolver(temporal_policy=TemporalFidelityPolicy.from_config(type_fidelity))
        if resolved_columns is None:
            inference = TypeInferenceService().infer(
                source_schema=source_schema or [],
                schema_contract=contract,
                options=TypeInferenceOptions(),
            )
            columns = {
                name: resolver.resolve(
                    sink_type=sink_type,
                    column=column,
                    options=resolved_options,
                    profile=(profiles or inference.profiles).get(name),
                )
                for name, column in inference.columns.items()
            }
        else:
            columns = dict(resolved_columns)
        target_design = _resolved_target_design(
            sink_type=sink_type,
            columns=columns,
            options=resolved_options,
        )
        columns = _apply_target_design_to_columns(columns=columns, target_design=target_design)
        ddl = _DdlRenderer().render(
            sink_type=sink_type,
            table=table,
            columns=columns,
            options=resolved_options,
            target_design=target_design,
        )
        table_settings = _resolved_table_settings(sink_type=sink_type, options=resolved_options)
        warnings = _table_setting_warnings(sink_type=sink_type, options=resolved_options)
        risks = _risks(options=resolved_options, ddl=ddl)
        return PhysicalDesignPlan(
            sink_type=_normalize_sink(sink_type),
            table=table,
            options=resolved_options,
            columns=columns,
            ddl=ddl,
            risks=risks,
            contract=contract,
            resolved_table_settings=table_settings,
            warnings=warnings,
            resolved_target_design=target_design,
        )


class _DdlRenderer:
    def render(
        self,
        *,
        sink_type: str,
        table: str,
        columns: Mapping[str, ResolvedTargetColumn],
        options: PhysicalDesignOptions,
        target_design: Any = None,
    ) -> list[str]:
        if not options.active:
            return []
        normalized = _normalize_sink(sink_type)
        if normalized == "mssql":
            if not isinstance(target_design, MssqlPhysicalDesignContract):
                raise TypeError("resolved MSSQL physical-design contract is required")
            return self._mssql(table, columns, target_design)
        if normalized == "postgres":
            return self._postgres(table, columns, options)
        if normalized == "clickhouse":
            return self._clickhouse(table, columns, options)
        if normalized == "bigquery":
            return self._bigquery(table, columns, options)
        if normalized == "kafka":
            return ["-- Kafka sink uses Schema Registry compatibility; no table DDL is generated."]
        return []

    def _mssql(
        self,
        table: str,
        columns: Mapping[str, ResolvedTargetColumn],
        contract: MssqlPhysicalDesignContract,
    ) -> list[str]:
        design = contract.storage
        qualified = _mssql_table(table)
        column_definitions = mssql_column_definitions(
            {column.name: column.target_type for column in columns.values()},
            nullable={column.name: column.nullable for column in columns.values()},
        )
        statements = MssqlTableDdlRenderer().render_create_table_statements(
            table=qualified,
            column_definitions=column_definitions,
            design=design,
            primary_key=contract.primary_key,
        )
        return [statement if statement.endswith(";") else f"{statement};" for statement in statements]

    def _postgres(
        self, table: str, columns: Mapping[str, ResolvedTargetColumn], options: PhysicalDesignOptions
    ) -> list[str]:
        storage = _sink_storage(options, "postgres")
        fillfactor = storage.get("fillfactor")
        with_clause = f" WITH (fillfactor={int(fillfactor)})" if fillfactor is not None else ""
        ddl = [
            f"CREATE TABLE {_pg_table(table)} (\n"
            + ",\n".join(
                f'  "{column.name}" {column.target_type} {"NULL" if column.nullable else "NOT NULL"}'
                for column in columns.values()
            )
            + f"\n){with_clause};"
        ]
        primary = _list_option(options.indexes.get("primary_key"))
        if primary:
            name = _safe_name("ix", table, "_".join(primary))
            cols = ", ".join(f'"{item}"' for item in primary)
            ddl.append(f'CREATE INDEX CONCURRENTLY "{name}" ON {_pg_table(table)} ({cols});')
        return ddl

    def _clickhouse(
        self, table: str, columns: Mapping[str, ResolvedTargetColumn], options: PhysicalDesignOptions
    ) -> list[str]:
        statements = ClickHouseTableDdlRenderer().render_create_table_statements(
            table=_ch_table(table),
            columns_sql=[f"`{column.name}` {column.target_type}" for column in columns.values()],
            design=_clickhouse_design(options),
        )
        return [statement + ";" for statement in statements]

    def _bigquery(
        self, table: str, columns: Mapping[str, ResolvedTargetColumn], options: PhysicalDesignOptions
    ) -> list[str]:
        ddl = [
            f"CREATE TABLE `{table}` (\n"
            + ",\n".join(f"  `{column.name}` {column.target_type}" for column in columns.values())
            + "\n)"
        ]
        storage = _sink_storage(options, "bigquery")
        if storage.get("partition_by"):
            ddl.append(f"PARTITION BY DATE({storage['partition_by']})")
        clustering = _list_option(storage.get("clustering"))
        if clustering:
            ddl.append("CLUSTER BY " + ", ".join(clustering))
        return ["\n".join(ddl) + ";"]


def _risks(*, options: PhysicalDesignOptions, ddl: Sequence[str]) -> tuple[str, ...]:
    risks: list[str] = []
    joined = "\n".join(ddl).lower()
    if options.apply in {"online", "plan_only"} and any(token in joined for token in ("rebuild", "columnstore")):
        risks.append("physical_design.blocking_ddl_requires_safe_window_or_manual_approval")
    return tuple(risks)


def _resolved_table_settings(*, sink_type: str, options: PhysicalDesignOptions) -> tuple[Any, ...]:
    if not options.active or _normalize_sink(sink_type) != "clickhouse":
        return ()
    return ClickHouseTableSettingsDialect().resolve(_clickhouse_design(options).table_settings)


def _table_setting_warnings(*, sink_type: str, options: PhysicalDesignOptions) -> tuple[str, ...]:
    if not options.active or _normalize_sink(sink_type) != "clickhouse":
        return ()
    return ClickHouseTableSettingsDialect().warnings(_clickhouse_design(options).table_settings)


def _clickhouse_design(options: PhysicalDesignOptions) -> ClickHouseTableDesign:
    return ClickHouseTableDesign.from_options(
        {"physical_design": {"storage": {"clickhouse": dict(_sink_storage(options, "clickhouse"))}}}
    )


def _sink_storage(options: PhysicalDesignOptions, sink: str) -> Mapping[str, Any]:
    raw = options.storage.get(sink, {})
    return raw if isinstance(raw, Mapping) else {}


def _list_option(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _normalize_sink(sink_type: str) -> str:
    normalized = str(sink_type).strip().lower()
    return "mssql" if normalized in {"sqlserver", "sql_server"} else normalized


def _mssql_table(table: str) -> str:
    return mssql_qualified_table(table)


def _resolved_target_design(
    *,
    sink_type: str,
    columns: Mapping[str, ResolvedTargetColumn],
    options: PhysicalDesignOptions,
) -> MssqlPhysicalDesignContract | None:
    if _normalize_sink(sink_type) != "mssql":
        return None
    contract = MssqlPhysicalDesignContract.from_sections(
        indexes=options.indexes,
        storage=_sink_storage(options, "mssql"),
        partitioning=options.partitioning,
        active=options.active,
    )
    contract.validate_columns({name: column.target_type for name, column in columns.items()})
    return contract


def _apply_target_design_to_columns(
    *,
    columns: Mapping[str, ResolvedTargetColumn],
    target_design: MssqlPhysicalDesignContract | None,
) -> Mapping[str, ResolvedTargetColumn]:
    if target_design is None or not target_design.active or not target_design.primary_key:
        return columns
    primary = {column.casefold() for column in target_design.primary_key}
    return {
        name: (
            replace(
                column,
                nullable=False,
                decision_source="physical_design",
                reason="MSSQL PRIMARY KEY columns are NOT NULL",
                decision_category="explicit_override",
            )
            if name.casefold() in primary
            else column
        )
        for name, column in columns.items()
    }


def _pg_table(table: str) -> str:
    return ".".join(f'"{part}"' for part in table.split("."))


def _ch_table(table: str) -> str:
    return ".".join(f"`{part}`" for part in table.split("."))


def _safe_name(*parts: str) -> str:
    joined = "_".join(parts).replace(".", "_")
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in joined)[:120]


__all__ = [
    "LowCardinalityOptions",
    "PhysicalDesignOptions",
    "PhysicalDesignPlan",
    "PhysicalDesignPlanner",
    "ResolvedTargetColumn",
]
