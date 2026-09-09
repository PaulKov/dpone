"""Production factories for runtime route capability orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.columnar_runtime_assembly import ColumnarRuntimeAssembly
from dpone.runtime.route_runtime import RouteCapabilityOrchestrator, RuntimeRouteDecisionPublisher
from dpone.runtime.state.load_step_audit import (
    ClickHouseLoadStepAuditStorage,
    MSSQLLoadStepAuditStorage,
    PostgresLoadStepAuditStorage,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class RouteCapabilityRuntimeFactory:
    """Build runtime route orchestration from manifest/config options."""

    def __init__(
        self,
        *,
        columnar_assembly: Any | None = None,
        audit_storage_factory: Any | None = None,
    ) -> None:
        self._columnar_assembly = columnar_assembly or ColumnarRuntimeAssembly()
        self._audit_storage_factory = audit_storage_factory or LoadStepAuditStorageFactory()

    def build(
        self,
        *,
        load_config: LoadConfig,
        source: Any,
        sink: Any,
        logger: Any | None = None,
    ) -> RouteCapabilityOrchestrator | None:
        if not _route_capabilities_enabled(load_config):
            return None
        assembly = self._columnar_assembly.build(load_config=load_config, source=source, sink=sink)
        if assembly is None:
            return None
        return RouteCapabilityOrchestrator(
            candidate_provider=assembly.candidate_provider,
            probe_runner=assembly.probe_runner,
            publisher=RuntimeRouteDecisionPublisher(
                audit_storage=self._audit_storage_factory.from_sink(sink, load_config),
                logger=logger,
            ),
            executors=assembly.executors,
            decision_details_provider=getattr(assembly, "decision_details_provider", None),
        )


class LoadStepAuditStorageFactory:
    """Select SQL load-step audit storage for the configured sink."""

    def from_sink(self, sink: Any, load_config: LoadConfig) -> Any | None:
        audit = _audit_options(load_config)
        if audit.get("enabled") is False:
            return None
        connector = getattr(sink, "connector", None)
        if connector is None:
            return None
        schema = str(audit.get("state_schema") or "etl_state")
        table = str(audit.get("steps_table") or "__dpone__load_steps")
        names = {_class_name(sink), _class_name(connector)}
        if "ClickHouseSink" in names or "ClickHouseConnector" in names:
            from dpone.runtime.state.clickhouse_state_design import ClickHouseStateTableDesign

            table_design = ClickHouseStateTableDesign.from_load_config(load_config)
            if not table_design.cluster.on_cluster and table_design.engine == "auto":
                return ClickHouseLoadStepAuditStorage(connector, schema=schema, table=table)
            return ClickHouseLoadStepAuditStorage(
                connector,
                schema=schema,
                table=table,
                table_design=table_design,
            )
        if "PostgresSink" in names or "PostgresConnector" in names:
            return PostgresLoadStepAuditStorage(connector, schema=schema, table=table)
        if "MSSQLSink" in names or "MSSQLConnector" in names:
            return MSSQLLoadStepAuditStorage(connector, schema=schema, table=table)
        return None


def _route_capabilities_enabled(load_config: LoadConfig) -> bool:
    runtime = _mapping(load_config.options.get("runtime"))
    if isinstance(runtime.get("capabilities"), dict):
        return True
    return _columnar_fast_path_mode(load_config) not in {"", "off", "benchmark_only"}


def _columnar_fast_path_mode(load_config: LoadConfig) -> str:
    columnar = _columnar_fast_path_options(load_config)
    return str(columnar.get("mode") or "auto").strip().lower() if columnar else ""


def _columnar_fast_path_options(load_config: LoadConfig) -> dict[str, Any]:
    native_transfer = _mapping(load_config.options.get("native_transfer"))
    snapshot = _mapping(native_transfer.get("snapshot"))
    return _mapping(snapshot.get("columnar_fast_path") or load_config.options.get("columnar_fast_path"))


def _audit_options(load_config: LoadConfig) -> dict[str, Any]:
    governance = _mapping(load_config.options.get("load_governance"))
    return _mapping(governance.get("audit"))


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _class_name(value: Any) -> str:
    return value.__class__.__name__


__all__ = ["LoadStepAuditStorageFactory", "RouteCapabilityRuntimeFactory"]
