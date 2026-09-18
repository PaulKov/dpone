"""Production composition for explicit external-replication publication."""

from __future__ import annotations

from typing import Any

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
    ClickHouseExternalClusterDdl,
    ClickHouseExternalKeeperMapAuthority,
    ClickHouseExternalReplicaConnectionProvider,
    ClickHouseExternalReplicaStaging,
    ClickHouseExternalTopologyCatalog,
)
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
    ) -> ClickHouseExternalReplicationServiceAdapter:
        catalog = ClickHouseClusterPublicationCatalog(connector)
        topology = ClickHouseExternalTopologyCatalog(connector)
        topology.inventory(cluster)
        ClickHouseClusterAuthorityBootstrap(connector, catalog).ensure(
            cluster,
            database,
            topology.bootstrap_hosts,
        )
        provider = ClickHouseExternalReplicaConnectionProvider(
            connector,
            cluster=cluster,
            connect=_direct_member_connection,
        )
        schema = tuple(getattr(payload, "schema", ()) or ())
        budget = external_content_row_budget(load_config) if load_config is not None else 100_000
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
            evidence_scope="production_live",
        )
        return adapter

    return ClickHouseExternalReplicationFacade(
        service_factory=service,
        artifact_source_factory=artifact_source,
    )


def _direct_member_connection(base: Any, host_name: str, host_address: str, port: int) -> Any:
    endpoint = host_address or host_name
    direct = base.clone_for_endpoint(endpoint, port, application_suffix="external-member")
    return direct


__all__ = ["build_clickhouse_external_replication"]
