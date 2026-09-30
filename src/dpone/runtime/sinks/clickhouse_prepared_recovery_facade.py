"""Infrastructure composition for one strictly admitted prepared recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_prepared_recovery import (
    PreparedRecoveryPlan,
    PreparedRecoveryService,
    plan_prepared_recovery,
)
from dpone.runtime.sinks.clickhouse_quality_authority import ClickHouseQualityKeeperMapAuthority


@dataclass(slots=True)
class PreparedRecoveryBackend:
    """Admitted catalog, strict authority and existing publication lifecycle."""

    catalog: ClickHouseClusterPublicationCatalog
    authority: ClickHouseQualityKeeperMapAuthority
    ddl: ClickHouseClusterPublicationDdl
    service: PreparedRecoveryService

    def plan(self, args: Any) -> PreparedRecoveryPlan:
        return plan_prepared_recovery(
            self.catalog,
            self.authority,
            self.ddl,
            cluster=args.cluster,
            database=args.database,
            target=args.target,
            operation_id=args.operation_id,
            expected_version=args.authority_version,
            operation_started_at=datetime.fromisoformat(args.operation_started_at),
        )

    def execute(self, plan: PreparedRecoveryPlan, *, confirmation_digest: str) -> Any:
        return self.service.execute_prepared_recovery(plan, confirmation_digest=confirmation_digest)


def build_prepared_recovery_backend(connector: Any, *, cluster: str, database: str) -> PreparedRecoveryBackend:
    """Inspect operator-provisioned storage without bootstrapping or migration."""
    catalog = ClickHouseClusterPublicationCatalog(connector)
    inventory = catalog.inventory(cluster)
    catalog.require_atomic_database(cluster, database, inventory.hosts)
    authority = ClickHouseQualityKeeperMapAuthority(connector, database)
    authority.require_ready(cluster, database, inventory.hosts)
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)

    def authority_for(requested_database: str) -> ClickHouseQualityKeeperMapAuthority:
        if requested_database != database:
            raise ValueError("recovery authority database changed")
        return authority

    publication = ClickHouseClusterFullRefreshPublicationService(
        catalog,
        authority_for,
        ddl,
        SimpleNamespace(
            ensure=lambda selected_cluster, selected_database, hosts: authority.require_ready(
                selected_cluster, selected_database, hosts
            )
        ),
    )
    service = PreparedRecoveryService(
        catalog,
        authority,
        ddl,
        lambda current, selected_cluster: publication._reconcile_existing(authority, current, selected_cluster),
        publication.cleanup,
    )
    return PreparedRecoveryBackend(catalog, authority, ddl, service)
