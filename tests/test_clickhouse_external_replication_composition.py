from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.contracts.clickhouse_external_replication import ExternalContractError
from dpone.ports.clickhouse_connector import ClickHouseConnectorPort, ClickHouseEndpointClonePort
from dpone.runtime import clickhouse_external_replication_composition as composition


def test_endpoint_cloning_is_an_external_capability_not_a_base_connector_requirement() -> None:
    assert "clone_for_endpoint" not in ClickHouseConnectorPort.__dict__
    assert "clone_for_endpoint" in ClickHouseEndpointClonePort.__dict__


def test_direct_member_connection_uses_advertised_native_transport() -> None:
    calls: list[tuple[str, int, str, str]] = []

    class Connector:
        def clone_for_endpoint(
            self,
            host: str,
            port: int,
            *,
            application_suffix: str,
            driver: str | None = None,
        ) -> object:
            calls.append((host, port, application_suffix, str(driver)))
            return object()

    result = composition._direct_member_connection(Connector(), "node-1", "192.0.2.1", 9000)

    assert result is not None
    assert calls == [("node-1", 9000, "external-member", "native")]


def test_atomic_database_is_required_before_authority_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class Topology:
        bootstrap_hosts = ("member-a", "member-b")

        def __init__(self, connector: Any, **kwargs: Any) -> None:
            del connector, kwargs

        def inventory(self, cluster: str) -> None:
            del cluster
            events.append("inventory")

    class Catalog:
        def __init__(self, connector: Any) -> None:
            del connector

        def require_atomic_database(self, cluster: str, database: str, hosts: tuple[str, ...]) -> None:
            del cluster, database, hosts
            events.append("atomic")
            raise RuntimeError("database is not uniformly Atomic")

    class Bootstrap:
        def __init__(self, connector: Any, catalog: Any) -> None:
            del connector, catalog

        def ensure(self, cluster: str, database: str, hosts: tuple[str, ...]) -> None:
            del cluster, database, hosts
            events.append("bootstrap")

    monkeypatch.setattr(composition, "ClickHouseExternalTopologyCatalog", Topology)
    monkeypatch.setattr(composition, "ClickHouseClusterPublicationCatalog", Catalog)
    monkeypatch.setattr(composition, "ClickHouseClusterAuthorityBootstrap", Bootstrap)
    connector = SimpleNamespace(clone_for_endpoint=lambda *_args, **_kwargs: object())
    sink = SimpleNamespace(connector=connector)
    facade = composition.build_clickhouse_external_replication(sink)

    with pytest.raises(RuntimeError, match="Atomic"):
        facade._service_factory("analytics_cluster", "analytics", "target_table")

    assert events == ["inventory", "atomic"]


def test_missing_endpoint_clone_capability_fails_before_inventory_or_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class Topology:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            events.append("topology")

    monkeypatch.setattr(composition, "ClickHouseExternalTopologyCatalog", Topology)
    facade = composition.build_clickhouse_external_replication(SimpleNamespace(connector=object()))

    with pytest.raises(ExternalContractError, match="INVENTORY_INVALID"):
        facade._service_factory("analytics_cluster", "analytics", "target_table")

    assert events == []


def test_production_composition_emits_runtime_unverified_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class Topology:
        bootstrap_hosts = ("member-a", "member-b")

        def __init__(self, *_args: Any, **_kwargs: Any) -> None: ...

        def inventory(self, _cluster: str) -> None: ...

        def member_identity(self, _host: str) -> str:
            return "member-a"

    class Catalog:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None: ...

        def require_atomic_database(self, *_args: Any, **_kwargs: Any) -> None: ...

    class Bootstrap:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None: ...

        def ensure(self, *_args: Any, **_kwargs: Any) -> None: ...

    class Provider:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None: ...

        def require_connections(self) -> None: ...

        def member_identity(self, _connector: Any) -> str:
            return "member-a"

    class Adapter:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(composition, "ClickHouseExternalTopologyCatalog", Topology)
    monkeypatch.setattr(composition, "ClickHouseClusterPublicationCatalog", Catalog)
    monkeypatch.setattr(composition, "ClickHouseClusterAuthorityBootstrap", Bootstrap)
    monkeypatch.setattr(composition, "ClickHouseExternalReplicaConnectionProvider", Provider)
    monkeypatch.setattr(composition, "ClickHouseExternalReplicationMemberDriver", lambda **_kwargs: object())
    monkeypatch.setattr(composition, "ClickHouseExternalKeeperMapAuthority", lambda *_args: object())
    monkeypatch.setattr(composition, "ClickHouseExternalReplicaStaging", lambda **_kwargs: object())
    monkeypatch.setattr(composition, "ClickHouseExternalClusterDdl", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(composition, "ClickHouseExternalReplicationServiceAdapter", Adapter)
    connector = SimpleNamespace(clone_for_endpoint=lambda *_args, **_kwargs: object())
    sink = SimpleNamespace(connector=connector, _clone_sink=lambda _connector: object())

    service = composition.build_clickhouse_external_replication(sink)._service_factory(
        "analytics_cluster",
        "analytics",
        "target_table",
        maximum_rows=100,
    )

    assert isinstance(service, Adapter)
    assert captured["evidence_scope"] == "runtime"
    assert captured["evidence_status"] == "UNVERIFIED"
