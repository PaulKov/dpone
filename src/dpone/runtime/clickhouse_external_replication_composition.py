"""Production composition for explicit external-replication publication."""

from __future__ import annotations

from typing import Any

from dpone.ports.clickhouse_connector import ClickHouseEndpointClonePort
from dpone.ports.clickhouse_external_replication import ExternalContractError
from dpone.runtime.sinks.clickhouse_cluster_publication_bootstrap import (
    ClickHouseClusterAuthorityBootstrap,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from dpone.runtime.sinks.clickhouse_external_artifact_source import (
    ClickHouseExternalArtifactSource,
    external_content_row_budget,
)
from dpone.runtime.sinks.clickhouse_external_replication_adapter import (
    ClickHouseExternalReplicationServiceAdapter,
)
from dpone.runtime.sinks.clickhouse_external_replication_clickhouse import (
    ClickHouseExternalKeeperMapAuthority,
    ClickHouseExternalReplicaConnectionProvider,
    ClickHouseExternalReplicaStaging,
    ClickHouseExternalTopologyCatalog,
)
from dpone.runtime.sinks.clickhouse_external_replication_ddl import ClickHouseExternalClusterDdl
from dpone.runtime.sinks.clickhouse_external_replication_facade import (
    ClickHouseExternalReplicationFacade,
)
from dpone.runtime.sinks.clickhouse_external_replication_member_driver import (
    ClickHouseExternalReplicationMemberDriver,
)


def build_clickhouse_external_replication(sink: Any) -> ClickHouseExternalReplicationFacade:
    """Wire the V2 authority and direct-member path behind explicit admission."""

    connector = sink.connector

    def artifact_source(load_config: Any, payload: Any) -> ClickHouseExternalArtifactSource:
        return ClickHouseExternalArtifactSource(
            sink=sink,
            load_config=load_config,
            payload=payload,
            maximum_rows=external_content_row_budget(load_config),
        )

    def service(
        cluster: str,
        database: str,
        target: str,
        *,
        load_config: Any | None = None,
        payload: Any | None = None,
        maximum_rows: int | None = None,
    ) -> ClickHouseExternalReplicationServiceAdapter:
        if not callable(getattr(connector, "clone_for_endpoint", None)):
            raise ExternalContractError(
                "INVENTORY_INVALID",
                "direct endpoint clone capability is unavailable",
            )
        catalog = ClickHouseClusterPublicationCatalog(connector)
        topology = ClickHouseExternalTopologyCatalog(
            connector,
            resolve_endpoint=getattr(connector, "external_member_endpoint_resolver", None),
        )
        topology.inventory(cluster)
        catalog.require_atomic_database(cluster, database, topology.bootstrap_hosts)
        ClickHouseClusterAuthorityBootstrap(connector, catalog).ensure(
            cluster,
            database,
            topology.bootstrap_hosts,
        )
        provider = ClickHouseExternalReplicaConnectionProvider(
            connector,
            topology=topology,
            connect=_direct_member_connection,
        )
        provider.require_connections()
        schema = tuple(getattr(payload, "schema", ()) or ())
        budget = external_content_row_budget(load_config) if load_config is not None else maximum_rows
        if budget is None:
            raise ValueError("clickhouse_external_replication.content_row_budget_required")
        driver = ClickHouseExternalReplicationMemberDriver(
            load_config=load_config,
            payload_schema=schema,
            sink_factory=sink._clone_sink,
            member_identity=provider.member_identity,
            max_content_rows=budget,
        )
        adapter = ClickHouseExternalReplicationServiceAdapter(
            topology=topology,
            authority=ClickHouseExternalKeeperMapAuthority(connector, database),
            staging=ClickHouseExternalReplicaStaging(connection_provider=provider, driver=driver),
            ddl=ClickHouseExternalClusterDdl(
                connector,
                catalog,
                member_identity=topology.member_identity,
            ),
            cluster=cluster,
            database=database,
            target=target,
            evidence_scope="runtime",
            evidence_status="UNVERIFIED",
        )
        return adapter

    return ClickHouseExternalReplicationFacade(
        service_factory=service,
        artifact_source_factory=artifact_source,
    )


def _direct_member_connection(
    base: ClickHouseEndpointClonePort,
    host_name: str,
    host_address: str,
    port: int,
) -> Any:
    endpoint = host_name or host_address
    direct = base.clone_for_endpoint(endpoint, port, application_suffix="external-member", driver="native")
    return direct


__all__ = ["build_clickhouse_external_replication"]
