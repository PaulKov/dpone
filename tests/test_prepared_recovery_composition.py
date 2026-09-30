"""Composition must admit strict storage without creating or migrating it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.sinks import clickhouse_prepared_recovery_facade as facade


def test_backend_admits_existing_authority_without_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    hosts = ("replica-a", "replica-b")

    class Catalog:
        def __init__(self, connector: object) -> None:
            assert connector is sentinel

        def inventory(self, cluster: str) -> SimpleNamespace:
            assert cluster == "cluster"
            events.append("inventory")
            return SimpleNamespace(hosts=hosts)

        def require_atomic_database(self, cluster: str, database: str, replicas: tuple[str, ...]) -> None:
            assert (cluster, database, replicas) == ("cluster", "analytics", hosts)
            events.append("atomic")

    class Authority:
        def __init__(self, connector: object, database: str) -> None:
            assert connector is sentinel and database == "analytics"

        def require_ready(self, cluster: str, database: str, replicas: tuple[str, ...]) -> None:
            assert (cluster, database, replicas) == ("cluster", "analytics", hosts)
            events.append("admit")

    class Ddl:
        def __init__(self, connector: object, catalog: Catalog) -> None:
            assert connector is sentinel and isinstance(catalog, Catalog)

    class Publication:
        def __init__(self, catalog: Catalog, authority_for: object, ddl: Ddl, bootstrap: object) -> None:
            assert isinstance(catalog, Catalog) and isinstance(ddl, Ddl)
            assert authority_for("analytics") is not None
            with pytest.raises(ValueError, match="database changed"):
                authority_for("foreign")
            bootstrap.ensure("cluster", "analytics", hosts)

        def _reconcile_existing(self, authority: Authority, current: object, cluster: str) -> object:
            assert isinstance(authority, Authority)
            return current, cluster

        def cleanup(self, receipt: object) -> None:
            raise AssertionError("composition must not clean a publication")

    class Service:
        def __init__(self, catalog: Catalog, authority: Authority, ddl: Ddl, reconcile: object, cleanup: object):
            assert isinstance(catalog, Catalog)
            assert isinstance(authority, Authority)
            assert isinstance(ddl, Ddl)
            assert reconcile("receipt", "cluster") == ("receipt", "cluster")
            events.append("service")

    monkeypatch.setattr(facade, "ClickHouseClusterPublicationCatalog", Catalog)
    monkeypatch.setattr(facade, "ClickHouseQualityKeeperMapAuthority", Authority)
    monkeypatch.setattr(facade, "ClickHouseClusterPublicationDdl", Ddl)
    monkeypatch.setattr(facade, "ClickHouseClusterFullRefreshPublicationService", Publication)
    monkeypatch.setattr(facade, "PreparedRecoveryService", Service)
    sentinel = object()

    backend = facade.build_prepared_recovery_backend(sentinel, cluster="cluster", database="analytics")

    assert isinstance(backend.service, Service)
    assert events == ["inventory", "atomic", "admit", "admit", "service"]
