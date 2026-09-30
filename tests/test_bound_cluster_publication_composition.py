"""Selected authority is shared by normal/replay paths, without legacy bootstrap."""

from types import SimpleNamespace

import pytest

from dpone.runtime import clickhouse_cluster_publication_composition as composition
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink


class Provider:
    def __init__(self):
        self.authority = object()

    def for_database(self, database):
        return self.authority

    def ensure(self, cluster, database, hosts):
        raise ValueError("read-only catalog admission failed")


@pytest.fixture
def forbid_legacy_authority(monkeypatch):
    original = composition._runtime_type

    def strict(module, name):
        assert name not in {
            "ClickHouseKeeperMapAuthority",
            "ClickHouseClusterAuthorityBootstrap",
            "ClickHouseQualityKeeperMapAuthority",
        }, "selected provider must never instantiate another authority"
        return original(module, name)

    monkeypatch.setattr(composition, "_runtime_type", strict)


@pytest.mark.parametrize("quality", [False, True])
def test_selected_provider_composes_service_and_quality_store_without_legacy(forbid_legacy_authority, quality):
    provider = Provider()
    if quality:
        service, store = composition.build_clickhouse_quality_publication(
            object(), authority_provider=provider, target_acceptance_reader=object()
        )
        assert store._authority_factory("Business") is provider.authority
    else:
        service = composition.build_clickhouse_cluster_publication(object(), authority_provider=provider)
    assert service._authority_factory("Business") is provider.authority
    with pytest.raises(ValueError, match="catalog admission failed"):
        service._bootstrap.ensure("replicas", "Business", ("one", "two"))


@pytest.mark.parametrize("quality", [False, True])
def test_sink_and_clone_keep_same_provider_without_intermediate_legacy_router(forbid_legacy_authority, quality):
    provider = Provider()
    connector = SimpleNamespace(driver="http")
    sink = ClickHouseSink(connector, publication_authority_provider=provider, durable_quality_replay=quality)
    clone = sink._clone_sink(connector)
    for selected in (sink, clone):
        assert selected.publication_authority_provider is provider
        assert selected._full_refresh_publication._cluster._authority_factory("Business") is provider.authority


def test_selected_provider_rejects_local_route_before_connector_io(forbid_legacy_authority):
    from tests.test_runtime_connection_composition_root import _load_config

    sink = ClickHouseSink(SimpleNamespace(driver="http"), publication_authority_provider=Provider())
    with pytest.raises(ValueError, match="publication_authority"):
        sink._full_refresh_publication.prepare_admission(_load_config())


def test_quality_readiness_uses_provider_not_an_undocumented_authority_method(forbid_legacy_authority, monkeypatch):
    from tests.test_clickhouse_cluster_full_refresh_publication import _config

    provider = Provider()  # base authority deliberately has no require_ready
    _, store = composition.build_clickhouse_quality_publication(
        object(), authority_provider=provider, target_acceptance_reader=object()
    )
    monkeypatch.setattr(store._catalog, "inventory", lambda cluster: SimpleNamespace(hosts=("one", "two")))
    with pytest.raises(ValueError, match="read-only catalog admission failed"):
        store.require_ready(_config())
