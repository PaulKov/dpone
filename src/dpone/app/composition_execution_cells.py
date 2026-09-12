"""Installed v3 execution cells as callable factories, not capability names."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionDependencies,
    CompositionClickHouseExecutionRoot,
)
from dpone.app.composition_dbt_execution import CompositionDbtExecutionDependencies, CompositionDbtExecutionRoot
from dpone.app.composition_dbt_execution_factory import build_composition_dbt_execution_dependencies
from dpone.app.composition_materialization_seams import build_composition_materialization_seams
from dpone.app.composition_transfer_execution import (
    CompositionTransferExecutionDependencies,
    CompositionTransferExecutionRoot,
)
from dpone.contracts.composition_activation import CompositionAdmissionError

SQLSERVER_DBT_V1 = "sqlserver_dbt_v1"
POSTGRES_MSSQL_FULL_REFRESH_V1 = "postgres_mssql_full_refresh_v1"
MSSQL_CLICKHOUSE_FULL_REFRESH_V1 = "mssql_clickhouse_full_refresh_v1"
COMPLETE_EXECUTION_CELLS = frozenset(
    {SQLSERVER_DBT_V1, POSTGRES_MSSQL_FULL_REFRESH_V1, MSSQL_CLICKHOUSE_FULL_REFRESH_V1}
)


@dataclass(frozen=True, slots=True)
class CompositionExecutionCellFactory:
    """One installed cell: a typed root constructor that stays lazy until called."""

    cell: str
    root_type: type
    builder: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.builder(*args, **kwargs)


class InstalledCompositionExecutionCapabilities:
    """Exact execution cells installed by the selected application composition."""

    def __init__(self, factories: Mapping[str, CompositionExecutionCellFactory]) -> None:
        if frozenset(factories) != COMPLETE_EXECUTION_CELLS:
            raise CompositionAdmissionError("complete_execution_capability_unavailable")
        if any(not callable(factory) or factory.cell != cell for cell, factory in factories.items()):
            raise CompositionAdmissionError("complete_execution_capability_unavailable")
        self._factories = dict(factories)

    @property
    def execution_cells(self) -> frozenset[str]:
        return frozenset(self._factories)

    def factory(self, cell: str) -> CompositionExecutionCellFactory:
        factory = self._factories.get(cell)
        if factory is None:
            raise CompositionAdmissionError("complete_execution_capability_unavailable")
        return factory

    def require_execution(self, plan, context) -> None:
        plan.__post_init__()
        context.__post_init__()
        if plan.sources.release_id != context.release_id:
            raise CompositionAdmissionError("execution_context")
        plan.require_installed_cells(self.execution_cells)

    def materialization_seams(self, **kwargs: Any) -> Mapping[str, Any]:
        return build_composition_materialization_seams(**kwargs)


def build_sqlserver_dbt_execution_root(
    *,
    dependencies: CompositionDbtExecutionDependencies | None = None,
    **kwargs: Any,
) -> CompositionDbtExecutionRoot:
    """Construct the native dbt root from already verified dependencies."""

    if dependencies is None:
        dependencies = build_composition_dbt_execution_dependencies(**kwargs)
    return CompositionDbtExecutionRoot(dependencies)


def build_postgres_mssql_execution_root(
    *,
    dependencies: CompositionTransferExecutionDependencies | None = None,
    **kwargs: Any,
) -> CompositionTransferExecutionRoot:
    """Construct the ordinary PostgreSQL→MSSQL root from injected collaborators."""

    if dependencies is None:
        dependencies = CompositionTransferExecutionDependencies(**kwargs)
    return CompositionTransferExecutionRoot(dependencies)


def build_mssql_clickhouse_execution_root(
    *,
    dependencies: CompositionClickHouseExecutionDependencies | None = None,
    **kwargs: Any,
) -> CompositionClickHouseExecutionRoot:
    """Construct the MSSQL→ClickHouse root from injected collaborators."""

    if dependencies is None:
        dependencies = CompositionClickHouseExecutionDependencies(**kwargs)
    return CompositionClickHouseExecutionRoot(dependencies)


def build_installed_execution_capabilities() -> InstalledCompositionExecutionCapabilities:
    """Return the three production cell factories required by the public root."""

    return InstalledCompositionExecutionCapabilities(
        {
            SQLSERVER_DBT_V1: CompositionExecutionCellFactory(
                SQLSERVER_DBT_V1,
                CompositionDbtExecutionRoot,
                build_sqlserver_dbt_execution_root,
            ),
            POSTGRES_MSSQL_FULL_REFRESH_V1: CompositionExecutionCellFactory(
                POSTGRES_MSSQL_FULL_REFRESH_V1,
                CompositionTransferExecutionRoot,
                build_postgres_mssql_execution_root,
            ),
            MSSQL_CLICKHOUSE_FULL_REFRESH_V1: CompositionExecutionCellFactory(
                MSSQL_CLICKHOUSE_FULL_REFRESH_V1,
                CompositionClickHouseExecutionRoot,
                build_mssql_clickhouse_execution_root,
            ),
        }
    )


__all__ = [
    "COMPLETE_EXECUTION_CELLS",
    "MSSQL_CLICKHOUSE_FULL_REFRESH_V1",
    "POSTGRES_MSSQL_FULL_REFRESH_V1",
    "SQLSERVER_DBT_V1",
    "CompositionExecutionCellFactory",
    "InstalledCompositionExecutionCapabilities",
    "build_installed_execution_capabilities",
    "build_mssql_clickhouse_execution_root",
    "build_postgres_mssql_execution_root",
    "build_sqlserver_dbt_execution_root",
]
