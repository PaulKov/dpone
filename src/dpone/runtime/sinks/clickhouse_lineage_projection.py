"""ClickHouse sink-side lineage projection for staged bulk/native loads."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.governance.ports import LineageProjectionResult as GovernanceLineageProjectionResult
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.clickhouse_operation_tables import DEFAULT_STAGING_SCHEMA, ClickHouseOperationTableResolver

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_CLICKHOUSE_LINEAGE_TYPES = {
    TechnicalColumnRole.RUN_ID: "String",
    TechnicalColumnRole.LOAD_ID: "String",
    TechnicalColumnRole.ROW_ID: "String",
    TechnicalColumnRole.LOADED_AT: "DateTime64(6, 'UTC')",
    TechnicalColumnRole.EXTRACTED_AT: "DateTime64(6, 'UTC')",
}
_UTC = timezone.utc  # noqa: UP017 - keep mypy-compatible timezone alias.


@dataclass(frozen=True, slots=True)
class LegacyLineageProjectionResult:
    """Compatibility result for the pre-governance ClickHouse projector API."""

    finalization_config: LoadConfig
    cleanup_config: LoadConfig | None = None


class ClickHouseSinkSideLineageProjector:
    """Project dpone lineage columns with ClickHouse SQL expressions."""

    def __init__(
        self,
        sink: Any | None = None,
        *,
        connector: Any | None = None,
        table_name: Callable[[LoadConfig], str] | None = None,
        operation_table_name: Callable[[str, str], str] | None = None,
        create_table_with_clickhouse_types: Callable[..., None] | None = None,
        catalog: TechnicalColumnCatalog | None = None,
    ) -> None:
        self._sink = sink
        self._legacy_connector = connector
        self._legacy_table_name = table_name
        self._legacy_operation_table_name = operation_table_name
        self._legacy_create_table = create_table_with_clickhouse_types
        self._catalog = catalog or TechnicalColumnCatalog()

    def project(
        self,
        load_config: Any | None = None,
        source_config: Any | None = None,
        *,
        handle: StagedLoadHandle | None = None,
        lineage_options: Any | None = None,
        load_record: Any | None = None,
    ) -> GovernanceLineageProjectionResult | LegacyLineageProjectionResult:
        if source_config is not None or self._legacy_connector is not None:
            if load_config is None or source_config is None:
                raise TypeError("legacy ClickHouse lineage projection requires load_config and source_config")
            return self._project_legacy(load_config, source_config)
        if load_config is None or handle is None or lineage_options is None or load_record is None:
            raise TypeError("governance ClickHouse lineage projection requires load_config, handle and load_record")
        if self._sink is None:
            raise TypeError("governance ClickHouse lineage projection requires a sink")
        if not getattr(lineage_options, "enabled", False):
            return GovernanceLineageProjectionResult(handle=handle, projected=False)

        plan = _ProjectionPlan.from_config(load_config, lineage_options, self._catalog, handle.payload_schema)
        if not plan.columns:
            return GovernanceLineageProjectionResult(handle=handle, projected=False)

        projected_config = _operation_config(self._sink, load_config, "projected")
        projected_table = projected_config.target_table
        projected_schema = (
            *_physical_payload_schema(self._sink, load_config, handle.payload_schema),
            *plan.schema_columns,
        )
        self._sink._create_table_with_clickhouse_types(projected_config, projected_schema, if_not_exists=False)
        self._sink.connector.execute_query(
            _insert_projection_sql(
                sink=self._sink,
                source_config=handle.finalization_config or handle.staging_config,
                target_config=projected_config,
                business_columns=tuple(column for column, _ in handle.payload_schema),
                expressions=plan.expressions(load_record),
            )
        )
        projected_handle = StagedLoadHandle(
            staging_config=handle.staging_config,
            payload_schema=projected_schema,
            staged_rows=handle.staged_rows,
            finalization_config=projected_config,
            decoded_config=handle.decoded_config,
            metadata=_projected_metadata(handle, projected_config, plan.schema_columns),
        )
        return GovernanceLineageProjectionResult(
            handle=projected_handle,
            projected=True,
            columns=tuple(column for column, _ in plan.schema_columns),
            row_identity_mode=plan.row_identity_mode,
            warnings=plan.warnings,
            evidence={"projected_table": projected_table},
        )

    def _project_legacy(self, load_config: LoadConfig, source_config: LoadConfig) -> LegacyLineageProjectionResult:
        if not LineageOptions.from_config((load_config.options or {}).get("lineage")).enabled:
            return LegacyLineageProjectionResult(source_config)
        identity = _legacy_lineage_identity(load_config)
        if identity is None:
            return LegacyLineageProjectionResult(source_config)

        source_columns = self._legacy_source_columns(source_config)
        missing = _missing_core_lineage(source_columns)
        if not missing:
            return LegacyLineageProjectionResult(source_config)
        assert self._legacy_operation_table_name is not None
        assert self._legacy_create_table is not None
        assert self._legacy_connector is not None
        assert self._legacy_table_name is not None

        projected_config = replace(
            load_config,
            target_schema=_legacy_operation_schema(load_config),
            target_table=self._legacy_operation_table_name(load_config.target_table, "lineage"),
        )
        self._legacy_create_table(projected_config, (*source_columns, *missing), if_not_exists=False)
        self._legacy_connector.execute_query(
            f"INSERT INTO {self._legacy_table_name(projected_config)} ({_legacy_column_list((*source_columns, *missing))}) "
            f"SELECT {_legacy_select_list(source_columns, missing, identity)} "
            f"FROM {self._legacy_table_name(source_config)}"
        )
        return LegacyLineageProjectionResult(finalization_config=projected_config, cleanup_config=projected_config)

    def _legacy_source_columns(self, source_config: LoadConfig) -> tuple[tuple[str, str], ...]:
        if (
            self._legacy_connector is None
            or self._legacy_table_name is None
            or self._legacy_operation_table_name is None
            or self._legacy_create_table is None
        ):
            raise TypeError("legacy ClickHouse lineage projection dependencies are incomplete")
        rows = self._legacy_connector.get_records(
            "SELECT name, type FROM system.columns "
            f"WHERE database = {_literal(source_config.target_schema)} "
            f"AND table = {_literal(source_config.target_table)} ORDER BY position"
        )
        if not rows:
            raise RuntimeError("clickhouse_lineage_projection_source_columns_missing")
        return tuple((str(row[0]), str(row[1])) for row in rows)


class _ProjectionPlan:
    def __init__(
        self,
        *,
        schema_columns: Sequence[tuple[str, str]],
        unique_key: tuple[str, ...],
        source_identity: str,
        warnings: tuple[str, ...],
    ) -> None:
        self.schema_columns = tuple(schema_columns)
        self.unique_key = unique_key
        self.source_identity = source_identity
        self.warnings = warnings
        self.columns = tuple(column for column, _ in self.schema_columns)
        self.row_identity_mode = "unique_key" if unique_key else "off"

    @classmethod
    def from_config(
        cls,
        load_config: Any,
        lineage_options: Any,
        catalog: TechnicalColumnCatalog,
        payload_schema: Sequence[tuple[str, str]],
    ) -> _ProjectionPlan:
        unique_key = _unique_key(load_config)
        existing = {column.lower() for column, _ in payload_schema}
        raw_lineage = (getattr(load_config, "options", {}) or {}).get("lineage")
        unsupported_policy = _row_identity_unsupported_policy(raw_lineage)
        row_id_name = catalog.name(TechnicalColumnRole.ROW_ID)
        row_id_already_projected = row_id_name.lower() in existing
        wants_row_identity = lineage_options.has_feature("row_identity")
        warnings: list[str] = []
        if wants_row_identity and not unique_key and not row_id_already_projected:
            if unsupported_policy == "fail":
                raise ValueError("lineage row_identity requires unique_key or certified business_hash support")
            warnings.append("row_identity_unique_key_missing")

        roles = [
            TechnicalColumnRole.RUN_ID,
            TechnicalColumnRole.LOAD_ID,
            TechnicalColumnRole.LOADED_AT,
            TechnicalColumnRole.EXTRACTED_AT,
        ]
        if wants_row_identity and unique_key:
            roles.append(TechnicalColumnRole.ROW_ID)
        return cls(
            schema_columns=[
                (catalog.name(role), _CLICKHOUSE_LINEAGE_TYPES[role])
                for role in roles
                if catalog.name(role).lower() not in existing
            ],
            unique_key=unique_key if wants_row_identity else (),
            source_identity=_source_identity(load_config),
            warnings=tuple(warnings),
        )

    def expressions(self, load_record: Any) -> Sequence[tuple[str, str]]:
        values = {
            "__dpone__run_id": _literal(getattr(load_record, "run_id", "")),
            "__dpone__load_id": _literal(getattr(load_record, "load_id", "")),
            "__dpone__loaded_at": "now64(6, 'UTC')",
            "__dpone__extracted_at": "now64(6, 'UTC')",
            "__dpone__row_id": _row_id_expression(self.source_identity, self.unique_key),
        }
        return tuple((column, values[column]) for column in self.columns)


def _insert_projection_sql(
    *,
    sink: Any,
    source_config: Any,
    target_config: Any,
    business_columns: Sequence[str],
    expressions: Sequence[tuple[str, str]],
) -> str:
    target_columns = (*business_columns, *(column for column, _ in expressions))
    select_items = (*(_quote(column) for column in business_columns), *(expr for _, expr in expressions))
    return (
        f"INSERT INTO {sink._table(target_config)} ({', '.join(_quote(column) for column in target_columns)}) "
        f"SELECT {', '.join(select_items)} FROM {sink._table(source_config)}"
    )


def _physical_payload_schema(
    sink: Any,
    load_config: Any,
    payload_schema: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    mapper = getattr(sink, "_map_type_for_config", None)
    if mapper is None:
        return tuple(payload_schema)
    return tuple((column, mapper(dtype, load_config)) for column, dtype in payload_schema)


def _projected_metadata(
    handle: StagedLoadHandle,
    projected_config: LoadConfig,
    schema_columns: Sequence[tuple[str, str]],
) -> dict[str, Any]:
    metadata = dict(handle.metadata)
    operation_tables = dict(metadata.get("operation_tables", {}) or {})
    operation_tables["projected"] = f"{projected_config.target_schema}.{projected_config.target_table}"
    return {
        **metadata,
        "lineage_projected_table": projected_config.target_table,
        "lineage_schema_columns": tuple(schema_columns),
        "operation_tables": operation_tables,
    }


def _unique_key(load_config: Any) -> tuple[str, ...]:
    raw = getattr(load_config, "unique_key", None)
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(item) for item in raw)


def _source_identity(load_config: Any) -> str:
    options = getattr(load_config, "options", {}) or {}
    source_type = str(options.get("source_type") or getattr(load_config, "source_conn_id", "") or "")
    source_schema = str(getattr(load_config, "source_schema", "") or "")
    source_table = str(getattr(load_config, "source_table", "") or "")
    return "|".join((source_type, source_schema, source_table))


def _row_identity_unsupported_policy(raw_lineage: object) -> str:
    if not isinstance(raw_lineage, dict):
        return "warn"
    row_identity = raw_lineage.get("row_identity")
    if not isinstance(row_identity, dict):
        return "warn"
    value = str(row_identity.get("unsupported_policy") or "warn").strip().lower()
    return "fail" if value == "fail" else "warn"


def _row_id_expression(source_identity: str, unique_key: Sequence[str]) -> str:
    if not unique_key:
        return "NULL"
    parts = ", '|', ".join(f"ifNull(toString({_quote(column)}), '<NULL>')" for column in unique_key)
    return f"hex(SHA256(concat({_literal(source_identity)}, '|', {parts})))"


def _literal(value: object) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _quote(column: str) -> str:
    return "`" + str(column).replace("`", "``") + "`"


def _missing_core_lineage(source_columns: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    existing = {column.lower() for column, _ in source_columns}
    return tuple(
        (column, _CLICKHOUSE_LINEAGE_TYPES[role])
        for role in (
            TechnicalColumnRole.RUN_ID,
            TechnicalColumnRole.LOAD_ID,
            TechnicalColumnRole.LOADED_AT,
            TechnicalColumnRole.EXTRACTED_AT,
        )
        if (column := TechnicalColumnCatalog().name(role)).lower() not in existing
    )


def _legacy_operation_schema(load_config: LoadConfig) -> str:
    configured = str(getattr(load_config, "staging_schema", "") or "").strip()
    if not configured or configured == DEFAULT_STAGING_SCHEMA:
        return str(load_config.target_schema)
    return configured


def _operation_config(sink: Any, load_config: LoadConfig, operation: str) -> LoadConfig:
    resolver = getattr(sink, "_operation_table_config", None)
    if callable(resolver):
        return resolver(load_config, operation)
    legacy_name = getattr(sink, "_operation_table_name", None)
    if callable(legacy_name):
        return replace(
            load_config,
            target_schema=_legacy_operation_schema(load_config),
            target_table=legacy_name(load_config.target_table, operation),
        )
    return ClickHouseOperationTableResolver().operation_config(load_config, operation)


def _legacy_lineage_identity(load_config: LoadConfig) -> dict[str, str] | None:
    raw = (load_config.options or {}).get("__dpone_load_identity")
    if not isinstance(raw, dict):
        return None
    run_id = str(raw.get("run_id") or "").strip()
    load_id = str(raw.get("load_id") or "").strip()
    if not run_id or not load_id:
        return None
    loaded_at = str(raw.get("loaded_at") or _now_utc())
    extracted_at = str(raw.get("extracted_at") or loaded_at)
    return {"run_id": run_id, "load_id": load_id, "loaded_at": loaded_at, "extracted_at": extracted_at}


def _legacy_column_list(columns: Sequence[tuple[str, str]]) -> str:
    return ", ".join(_quote(column) for column, _ in columns)


def _legacy_select_list(
    source_columns: Sequence[tuple[str, str]],
    missing: Sequence[tuple[str, str]],
    identity: dict[str, str],
) -> str:
    expressions = [_quote(column) for column, _ in source_columns]
    expressions.extend(f"{_legacy_lineage_expression(column, identity)} AS {_quote(column)}" for column, _ in missing)
    return ", ".join(expressions)


def _legacy_lineage_expression(column: str, identity: dict[str, str]) -> str:
    values = {
        "__dpone__run_id": _literal(identity["run_id"]),
        "__dpone__load_id": _literal(identity["load_id"]),
        "__dpone__loaded_at": f"parseDateTime64BestEffort({_literal(identity['loaded_at'])}, 6, 'UTC')",
        "__dpone__extracted_at": f"parseDateTime64BestEffort({_literal(identity['extracted_at'])}, 6, 'UTC')",
    }
    return values[column]


def _now_utc() -> str:
    return datetime.now(_UTC).isoformat()


__all__ = ["ClickHouseSinkSideLineageProjector", "LegacyLineageProjectionResult"]
