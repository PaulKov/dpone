"""Compose the strict ClickHouse cluster publication collaborators."""

from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace
from typing import Any


def build_clickhouse_cluster_publication(connector: Any) -> Any:
    """Build the cluster service without admitting it for runtime use."""

    catalog_type = _runtime_type("clickhouse_cluster_publication_catalog", "ClickHouseClusterPublicationCatalog")
    authority_type = _runtime_type("clickhouse_cluster_publication_authority", "ClickHouseKeeperMapAuthority")
    bootstrap_type = _runtime_type("clickhouse_cluster_publication_bootstrap", "ClickHouseClusterAuthorityBootstrap")
    ddl_type = _runtime_type("clickhouse_cluster_publication_ddl", "ClickHouseClusterPublicationDdl")
    service_type = _runtime_type(
        "clickhouse_cluster_full_refresh_publication", "ClickHouseClusterFullRefreshPublicationService"
    )
    catalog = catalog_type(connector)
    return service_type(
        catalog,
        lambda database: authority_type(connector, database),
        ddl_type(connector, catalog),
        bootstrap_type(connector, catalog),
    )


def _runtime_type(module: str, name: str) -> Any:
    return getattr(import_module(f"dpone.runtime.sinks.{module}"), name)


def build_clickhouse_quality_publication(connector: Any) -> tuple[Any, Any]:
    """Explicit opt-in to externally provisioned strict authority; no bootstrap DDL."""
    catalog = _runtime_type("clickhouse_cluster_publication_catalog", "ClickHouseClusterPublicationCatalog")(connector)
    authority_type = _runtime_type("clickhouse_quality_authority", "ClickHouseQualityKeeperMapAuthority")
    authorities: dict[str, Any] = {}

    def authority(database: str) -> Any:
        if database not in authorities:
            authorities[database] = authority_type(connector, database)
        return authorities[database]

    store = _runtime_type("clickhouse_replay_quality", "ClickHouseReplayQualityStore")(catalog, authority)
    service = _runtime_type(
        "clickhouse_cluster_full_refresh_publication", "ClickHouseClusterFullRefreshPublicationService"
    )(
        catalog,
        authority,
        _runtime_type("clickhouse_cluster_publication_ddl", "ClickHouseClusterPublicationDdl")(connector, catalog),
        SimpleNamespace(ensure=lambda cluster, database, hosts: authority(database).ensure(cluster, database, hosts)),
        quality_store=store,
    )
    return service, store
