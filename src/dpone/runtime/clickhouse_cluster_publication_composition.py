"""Compose the strict ClickHouse cluster publication collaborators."""

from __future__ import annotations

from importlib import import_module
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
