from __future__ import annotations

import pytest

from dpone.contracts.api_sources import get_api_source_defaults, list_api_source_types
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.api_registry import list_registered_api_provider_specs
from dpone.runtime.bootstrap import DefaultRuntimeHydrator


class DummySource:
    def __init__(self, connector, sink_connector, logger):
        self.connector = connector
        self.sink_connector = sink_connector
        self.logger = logger


class DummyConnector:
    DEFAULT_TIMEOUT = 60

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @classmethod
    def from_vault(cls, vault_path, **kwargs):
        return cls(vault_path=vault_path, **kwargs)


@pytest.mark.parametrize(
    "api_type",
    [
        "omnidesk",
        "appsflyer",
        "mindbox",
        "similarweb",
        "google_sheets",
        "google_ads",
        "yandex_webmaster",
        "cbr",
        "rest",
        "amplitude",
        "fasttrack",
        "fastrack",
    ],
)
def test_api_source_defaults_registry_contains_known_types(api_type: str) -> None:
    assert api_type in list_api_source_types()
    defaults = get_api_source_defaults(api_type)
    expected_canonical = "fasttrack" if api_type == "fastrack" else api_type
    assert defaults.api_type == expected_canonical
    assert defaults.connection_id().startswith("api__")
    assert defaults.source_schema().startswith("api__")


def test_load_config_builder_derives_api_connection_id_schema_and_table() -> None:
    cfg = {
        "source": {
            "type": "api",
            "api_type": "mindbox",
            "options": {"resource": "getorders", "batch_size": 321},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "bq-conn",
            "table": {"schema": "landing__mindbox__api", "name": "app__getorders"},
            "strategy": {"mode": "full_refresh"},
        },
    }

    load_cfg = LoadConfigBuilder().build(cfg)
    assert load_cfg.source_conn_id == "api__mindbox"
    assert load_cfg.source_schema == "api__mindbox"
    assert load_cfg.source_table == "getorders"


def test_runtime_bootstrap_uses_api_registry_for_appsflyer(monkeypatch) -> None:
    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.AppsflyerConnector", DummyConnector)
    monkeypatch.setattr("dpone.runtime.sources.api.appsflyer.AppsflyerSource", DummySource)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "appsflyer",
            "options": {
                "app_ids": ["app.one"],
                "rate_limit_delay": 2.0,
                "max_retries": 7,
                "timeout": 15,
            },
        },
        vault_path="api/appsflyer",
        sink_connector=None,
    )
    assert isinstance(source, DummySource)
    assert source.connector.kwargs["vault_path"] == "api/appsflyer"
    assert source.connector.kwargs["default_app_id"] == "app.one"


def test_public_api_provider_can_skip_connection_type_and_build_runtime_source() -> None:
    source = DefaultRuntimeHydrator._build_source(
        {
            "type": "api",
            "api_type": "cbr",
            "options": {"resource": "xml_daily_asp", "timeout": 11},
        },
        xmin_state_storage=None,
    )
    assert source.connector.timeout == 11


def test_vault_api_source_requires_explicit_connection_id_and_mount() -> None:
    from dpone.runtime.errors import RuntimeConfigurationError

    with pytest.raises(RuntimeConfigurationError, match="connection_id"):
        DefaultRuntimeHydrator._build_source(
            {
                "type": "api",
                "api_type": "fasttrack",
                "connection_type": "vault",
                "vault_path": "api/fasttrack",
                "vault_mount_point": "prod",
            },
            xmin_state_storage=None,
        )

    with pytest.raises(RuntimeConfigurationError, match="vault_mount_point"):
        DefaultRuntimeHydrator._build_source(
            {
                "type": "api",
                "api_type": "fasttrack",
                "connection_id": "api__fasttrack",
                "connection_type": "vault",
                "vault_path": "api/fasttrack",
            },
            xmin_state_storage=None,
        )


def test_runtime_registry_lists_registered_specs() -> None:
    specs = list_registered_api_provider_specs()
    names = {spec.api_type for spec in specs}
    assert {
        "omnidesk",
        "appsflyer",
        "mindbox",
        "similarweb",
        "google_sheets",
        "google_ads",
        "yandex_webmaster",
        "cbr",
        "rest",
        "fasttrack",
    }.issubset(names)


def test_fasttrack_alias_uses_canonical_api_defaults() -> None:
    defaults = get_api_source_defaults("fastrack")
    assert defaults.api_type == "fasttrack"
    assert defaults.connection_id() == "api__fasttrack"
    assert defaults.source_schema() == "api__fasttrack"


def test_fasttrack_runtime_provider_builds_via_registry(monkeypatch) -> None:
    monkeypatch.setattr("dpone.runtime.connectors.api.fasttrack.FasttrackConnector", DummyConnector)
    monkeypatch.setattr("dpone.runtime.sources.api.fasttrack.FasttrackSource", DummySource)

    source = DefaultRuntimeHydrator._build_source(
        {
            "type": "api",
            "api_type": "fasttrack",
            "connection_id": "api__fasttrack",
            "options": {"resource": "cascade_transactions", "timeout": 12},
            "connection_type": "vault",
            "vault_mount_point": "prod",
            "vault_path": "api/fasttrack",
        },
        xmin_state_storage=None,
    )
    assert isinstance(source, DummySource)
    assert source.connector.kwargs["vault_path"] == "api/fasttrack"
    assert source.connector.kwargs["timeout"] == 12


def test_runtime_bootstrap_uses_api_registry_for_similarweb(monkeypatch) -> None:
    monkeypatch.setattr("dpone.runtime.connectors.api.similarweb.SimilarwebConnector", DummyConnector)
    monkeypatch.setattr("dpone.runtime.sources.api.similarweb.SimilarwebSource", DummySource)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "similarweb",
            "options": {
                "timeout": 45,
                "max_retries": 5,
                "rate_limit_delay": 1.5,
            },
        },
        vault_path="api/similarweb",
        sink_connector=None,
    )

    assert isinstance(source, DummySource)
    assert source.connector.kwargs["vault_path"] == "api/similarweb"
    assert source.connector.kwargs["timeout"] == 45


def test_runtime_bootstrap_uses_api_registry_for_google_ads(monkeypatch) -> None:
    monkeypatch.setattr("dpone.runtime.connectors.api.google_ads.GoogleAdsConnector", DummyConnector)
    monkeypatch.setattr("dpone.runtime.sources.api.google_ads.GoogleAdsSource", DummySource)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "google_ads",
            "options": {
                "timeout": 48,
                "max_retries": 6,
                "rate_limit_delay": 1.5,
            },
        },
        vault_path="api/google_ads",
        sink_connector=None,
    )

    assert isinstance(source, DummySource)
    assert source.connector.kwargs["vault_path"] == "api/google_ads"
    assert source.connector.kwargs["timeout"] == 48
