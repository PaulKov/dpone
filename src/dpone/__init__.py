"""dpone package exports.

Keep imports lazy to avoid importing heavy/optional dependencies on `import dpone`.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from dpone.version import installed_version

__all__ = [
    "__version__",
    "PostgresConnector",
    "ETLProcess",
    "ETLProcessConfig",
    "RunContext",
    "PostgresSink",
    "PostgresSource",
    "LoadConfig",
    "LoadStrategy",
    "run",
    "RunManifestResult",
    "ReconciliationManager",
    "XMinStateQueries",
    "RunStateQueries",
    "ReconciliationQueries",
    "CDCOperation",
    "PostgresLogicalCDCReader",
    "MSSQLCDCReader",
    "MSSQLChangeTrackingReader",
    "KafkaConnector",
    "KafkaSource",
    "KafkaSink",
    "AirflowSelfServiceService",
    "ProjectDiscoveryService",
    "ProjectDiscoverySnapshot",
    "ProjectSelectionLoader",
    "LoadedProjectSelectionGraph",
    "WorkloadIndexChangeImpact",
    "compare_workload_indexes",
    "workload_index_from_snapshot",
    "build_airflow_self_service_service",
]

__version__ = installed_version()

# name -> "module:attribute"
_EXPORTS: dict[str, str] = {
    "PostgresConnector": "dpone.lib.connectors:PostgresConnector",
    "ETLProcess": "dpone.core:ETLProcess",
    "ETLProcessConfig": "dpone.core:ETLProcessConfig",
    "RunContext": "dpone.core:RunContext",
    "PostgresSink": "dpone.sink:PostgresSink",
    "PostgresSource": "dpone.source:PostgresSource",
    "LoadConfig": "dpone.config:LoadConfig",
    "LoadStrategy": "dpone.config:LoadStrategy",
    "run": "dpone.api:run",
    "RunManifestResult": "dpone.api:RunManifestResult",
    "ReconciliationManager": "dpone.reconciliation:ReconciliationManager",
    "XMinStateQueries": "dpone.sql_helpers:XMinStateQueries",
    "RunStateQueries": "dpone.sql_helpers:RunStateQueries",
    "ReconciliationQueries": "dpone.sql_helpers:ReconciliationQueries",
    "CDCOperation": "dpone.runtime.cdc:CDCOperation",
    "PostgresLogicalCDCReader": "dpone.runtime.cdc:PostgresLogicalCDCReader",
    "MSSQLCDCReader": "dpone.runtime.cdc:MSSQLCDCReader",
    "MSSQLChangeTrackingReader": "dpone.runtime.cdc:MSSQLChangeTrackingReader",
    "KafkaConnector": "dpone.runtime.connectors.kafka:KafkaConnector",
    "KafkaSource": "dpone.runtime.sources.kafka:KafkaSource",
    "KafkaSink": "dpone.runtime.sinks.kafka:KafkaSink",
    "AirflowSelfServiceService": ("dpone.readiness.airflow_self_service_composition:AirflowSelfServiceService"),
    "ProjectDiscoveryService": "dpone.manifest.project_discovery:ProjectDiscoveryService",
    "ProjectDiscoverySnapshot": "dpone.manifest.project_discovery:ProjectDiscoverySnapshot",
    "ProjectSelectionLoader": "dpone.readiness.project_selection_loader:ProjectSelectionLoader",
    "LoadedProjectSelectionGraph": "dpone.readiness.project_selection_loader:LoadedProjectSelectionGraph",
    "WorkloadIndexChangeImpact": "dpone.services.workload_index_contract:WorkloadIndexChangeImpact",
    "compare_workload_indexes": "dpone.services.workload_index_contract:compare_workload_indexes",
    "workload_index_from_snapshot": "dpone.services.workload_index_contract:workload_index_from_snapshot",
    "build_airflow_self_service_service": (
        "dpone.readiness.airflow_self_service_composition:build_airflow_self_service_service"
    ),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr = target.split(":")
    mod = import_module(module_name)

    value = getattr(mod, attr)
    globals()[name] = value  # кешируем, чтобы второй раз не импортировать
    return value
