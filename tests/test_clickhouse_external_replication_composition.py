from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.runtime import clickhouse_external_replication_composition as composition


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
    sink = SimpleNamespace(connector=object())
    facade = composition.build_clickhouse_external_replication(sink)

    with pytest.raises(RuntimeError, match="Atomic"):
        facade._service_factory("analytics_cluster", "analytics", "target_table")

    assert events == ["inventory", "atomic"]
