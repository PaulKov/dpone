"""Public composition root wiring and fail-closed capability selection."""

from pathlib import Path

from dpone.app.composition_activation import build_composition_activation_coordinator
from dpone.app.composition_clickhouse_execution import CompositionClickHouseExecutionRoot
from dpone.app.composition_dbt_execution import CompositionDbtExecutionRoot
from dpone.app.composition_transfer_execution import CompositionTransferExecutionRoot
from dpone.services.composition_activation_coordinator import CompositionActivationCoordinator

COMPLETE_CELLS = frozenset(
    {
        "sqlserver_dbt_v1",
        "postgres_mssql_full_refresh_v1",
        "mssql_clickhouse_full_refresh_v1",
    }
)


class ResolverFactory:
    def build(self, **_kwargs):
        raise AssertionError("factory construction must not resolve credentials")


def test_public_factory_constructs_coordinator_without_eager_io(tmp_path: Path) -> None:
    coordinator = build_composition_activation_coordinator(
        cache_root=tmp_path,
        authority_connection_ref="composition_control",
        resolver_factory=ResolverFactory(),
    )

    assert isinstance(coordinator, CompositionActivationCoordinator)


def test_public_factory_advertises_the_complete_installed_cell_set(tmp_path: Path) -> None:
    coordinator = build_composition_activation_coordinator(
        cache_root=tmp_path,
        authority_connection_ref="composition_control",
        resolver_factory=ResolverFactory(),
    )

    assert coordinator.execution_cells == COMPLETE_CELLS


def test_public_factory_owns_callable_factories_for_every_installed_cell(tmp_path: Path) -> None:
    coordinator = build_composition_activation_coordinator(
        cache_root=tmp_path,
        authority_connection_ref="composition_control",
        resolver_factory=ResolverFactory(),
    )

    factories = {
        cell: coordinator.execution_factory(cell)
        for cell in (
            "sqlserver_dbt_v1",
            "postgres_mssql_full_refresh_v1",
            "mssql_clickhouse_full_refresh_v1",
        )
    }

    assert coordinator.execution_cells == frozenset(factories)
    assert all(callable(factory) for factory in factories.values())
    assert factories["sqlserver_dbt_v1"].root_type is CompositionDbtExecutionRoot
    assert factories["postgres_mssql_full_refresh_v1"].root_type is CompositionTransferExecutionRoot
    assert factories["mssql_clickhouse_full_refresh_v1"].root_type is CompositionClickHouseExecutionRoot


def test_public_factory_supplies_materialization_seams_without_opening_sql(tmp_path: Path) -> None:
    coordinator = build_composition_activation_coordinator(
        cache_root=tmp_path,
        authority_connection_ref="composition_control",
        resolver_factory=ResolverFactory(),
    )

    seams = coordinator.materialization_seams()

    assert callable(seams["open_target"])
    assert callable(seams["require_target"])
    assert callable(seams["observe_undispatched_closure"])
