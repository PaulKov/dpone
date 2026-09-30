"""Compose the strict ClickHouse cluster publication collaborators."""

from __future__ import annotations

from functools import partial
from importlib import import_module
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.ports.clickhouse_cluster_publication import ClusterPublicationAuthorityProviderPort


def build_clickhouse_cluster_publication(
    connector: Any, *, authority_provider: ClusterPublicationAuthorityProviderPort | None = None
) -> Any:
    """Build the cluster service without admitting it for runtime use."""

    catalog_type = _runtime_type("clickhouse_cluster_publication_catalog", "ClickHouseClusterPublicationCatalog")
    ddl_type = _runtime_type("clickhouse_cluster_publication_ddl", "ClickHouseClusterPublicationDdl")
    service_type = _runtime_type(
        "clickhouse_cluster_full_refresh_publication", "ClickHouseClusterFullRefreshPublicationService"
    )
    catalog = catalog_type(connector)
    if authority_provider is None:
        authority_type = _runtime_type("clickhouse_cluster_publication_authority", "ClickHouseKeeperMapAuthority")
        bootstrap_type = _runtime_type(
            "clickhouse_cluster_publication_bootstrap", "ClickHouseClusterAuthorityBootstrap"
        )
        authority_factory = partial(authority_type, connector)
        bootstrap = bootstrap_type(connector, catalog)
    else:
        authority_factory = authority_provider.for_database
        bootstrap = authority_provider
    return service_type(
        catalog,
        authority_factory,
        ddl_type(connector, catalog),
        bootstrap,
    )


def _runtime_type(module: str, name: str) -> Any:
    return getattr(import_module(f"dpone.runtime.sinks.{module}"), name)


def build_clickhouse_quality_publication(
    connector: Any,
    *,
    target_acceptance_reader: Any | None = None,
    authority_provider: ClusterPublicationAuthorityProviderPort | None = None,
) -> tuple[Any, Any]:
    """Explicit opt-in to externally provisioned strict authority; no bootstrap DDL."""
    catalog = _runtime_type("clickhouse_cluster_publication_catalog", "ClickHouseClusterPublicationCatalog")(connector)
    authorities: dict[str, Any] = {}

    def authority(database: str) -> Any:
        if authority_provider is not None:
            return authority_provider.for_database(database)
        if database not in authorities:
            authority_type = _runtime_type("clickhouse_quality_authority", "ClickHouseQualityKeeperMapAuthority")
            authorities[database] = authority_type(connector, database)
        return authorities[database]

    reader = target_acceptance_reader if target_acceptance_reader is not None else _target_reader(connector)
    store = _runtime_type("clickhouse_replay_quality", "ClickHouseReplayQualityStore")(
        catalog,
        authority,
        target_acceptance_reader=reader,
        **({"authority_readiness": authority_provider} if authority_provider is not None else {}),
    )
    service = _runtime_type(
        "clickhouse_cluster_full_refresh_publication", "ClickHouseClusterFullRefreshPublicationService"
    )(
        catalog,
        authority,
        _runtime_type("clickhouse_cluster_publication_ddl", "ClickHouseClusterPublicationDdl")(connector, catalog),
        authority_provider
        if authority_provider is not None
        else SimpleNamespace(
            ensure=lambda cluster, database, hosts: authority(database).ensure(cluster, database, hosts)
        ),
        quality_store=store,
    )
    return service, store


def _target_reader(connector: Any) -> Any | None:
    """Compose only the declared native adapter; target admission performs I/O later."""
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    if not isinstance(connector, ClickHouseConnector) or connector.driver != "native":
        return None
    from dpone.adapters.target_acceptance.reader import BoundedClickHouseTargetAcceptanceReader

    return BoundedClickHouseTargetAcceptanceReader.from_connector(connector)
