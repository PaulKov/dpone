from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.connectors.api.yandex_webmaster_resources import get_yandex_webmaster_resource
from dpone.runtime.sources.api.yandex_webmaster import YandexWebmasterSource


class DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def info(self, *args, **kwargs):
        self.events.append(("info", (args, kwargs)))

    def warning(self, *args, **kwargs):
        self.events.append(("warning", (args, kwargs)))

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))


class DummyConnector:
    DEFAULT_TIMEOUT = 60

    def resolve_user_id(self, user_id=None):
        return int(user_id) if user_id is not None else 42

    def resolve_host_id(self, *, user_id, host_id=None, host_url=None):  # noqa: ARG002
        return host_id or "host-1"

    def get_indexing_history(self, *, user_id, host_id, date_from, date_to):  # noqa: ARG002
        return {
            "indicators": {
                "HTTP_2XX": [{"date": "2026-03-01", "value": 10}],
                "HTTP_4XX": [{"date": "2026-03-02", "value": 1}],
            }
        }

    def get_in_search_history(self, *, user_id, host_id, date_from, date_to):  # noqa: ARG002
        return {
            "history": [
                {"date": "2026-03-01T08:00:00+0300", "value": 7},
                {"date": "2026-03-01T23:00:00+0300", "value": 8},
            ]
        }

    def get_search_events_history(self, *, user_id, host_id, date_from, date_to):  # noqa: ARG002
        return {
            "indicators": {
                "APPEARED_IN_SEARCH": [{"date": "2026-03-01", "value": 2}],
                "REMOVED_FROM_SEARCH": [{"date": "2026-03-02", "value": 1}],
            }
        }

    def iter_search_queries_popular(
        self,
        *,
        user_id,
        host_id,
        date_from,
        date_to,
        order_by="TOTAL_SHOWS",
        limit=500,
        max_queries=None,
        query_indicators=(),
    ):  # noqa: ANN001,ARG002
        yield {"query_id": "q-1", "query_text": "example_travel"}

    def get_search_query_history(
        self,
        *,
        user_id,
        host_id,
        query_id,
        date_from,
        date_to,
        device_type,
        query_indicators=(),
    ):  # noqa: ANN001,ARG002
        return {
            "indicators": {
                "TOTAL_SHOWS": [{"date": "2026-03-01", "value": 12}],
                "TOTAL_CLICKS": [{"date": "2026-03-01", "value": 3}],
                "AVG_SHOW_POSITION": [{"date": "2026-03-01", "value": 4.5}],
                "AVG_CLICK_POSITION": [{"date": "2026-03-01", "value": 2.0}],
            }
        }

    def iter_query_analytics_list(
        self,
        *,
        user_id,
        host_id,
        region_ids,
        limit=500,
        max_queries=None,
        device_type_indicator="ALL",
        search_location="WEB_LOCATION",
        text_indicator="QUERY",
        order_by="TOTAL_SHOWS",
    ):  # noqa: ANN001,ARG002
        yield {
            "text_indicator": {"value": "example_travel"},
            "popular_complementary_indicator": {"value": "https://travel.example.com/"},
            "statistics": [
                {"date": "2026-03-01", "field": "IMPRESSIONS", "value": 10},
                {"date": "2026-03-01", "field": "CLICKS", "value": 2},
                {"date": "2026-03-01", "field": "CTR", "value": 0.2},
            ],
        }

    def health_check(self):
        return True


def _load_config(*, strategy: LoadStrategy, **options) -> LoadConfig:
    resource = str(options.get("resource", "host_metrics_daily"))
    spec = get_yandex_webmaster_resource(resource)
    return LoadConfig(
        source_conn_id="api__yandex_webmaster",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table=resource,
        target_schema="landing__yandex_webmaster__api",
        target_table=f"app__{resource}",
        load_strategy=strategy,
        options={
            "resource": resource,
            "host_url": "https://travel.example.com",
            **options,
        },
        unique_key=list(spec.unique_key),
    )


def test_yandex_webmaster_source_full_extract_merges_daily_metrics() -> None:
    source = YandexWebmasterSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())
    result = source.extract(_load_config(strategy=LoadStrategy.FULL_REFRESH, day="2026-03-01"), None)

    assert result.force_full_refresh is True
    rows = result.artifact._rows
    assert len(rows) == 2
    assert rows[0]["date"] == "2026-03-01"
    assert rows[0]["pages_in_search"] == 8
    assert rows[0]["appeared_in_search"] == 2


def test_yandex_webmaster_source_incremental_merge_sets_state() -> None:
    source = YandexWebmasterSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())
    result = source.extract(
        _load_config(strategy=LoadStrategy.INCREMENTAL_MERGE, initial_start_date="2026-03-01"),
        {"last_value": "2026-03-05"},
    )

    assert result.force_full_refresh is False
    assert result.state is not None
    assert "last_value" in result.state


def test_yandex_webmaster_source_replace_builds_search_queries_rows_and_predicate() -> None:
    source = YandexWebmasterSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())
    load_config = _load_config(
        strategy=LoadStrategy.REPLACE,
        resource="search_queries_history_daily",
        day="2026-03-01",
    )
    result = source.extract(load_config, None)

    assert result.force_full_refresh is False
    assert load_config.custom_predicate == "date = DATE '2026-03-01'"
    assert result.schema == get_yandex_webmaster_resource("search_queries_history_daily").schema
    assert result.artifact._rows == [
        {
            "date": "2026-03-01",
            "query_id": "q-1",
            "query_text": "example_travel",
            "device_type": "DESKTOP",
            "total_shows": 12,
            "total_clicks": 3,
            "avg_show_position": 4.5,
            "avg_click_position": 2.0,
        },
        {
            "date": "2026-03-01",
            "query_id": "q-1",
            "query_text": "example_travel",
            "device_type": "MOBILE",
            "total_shows": 12,
            "total_clicks": 3,
            "avg_show_position": 4.5,
            "avg_click_position": 2.0,
        },
        {
            "date": "2026-03-01",
            "query_id": "q-1",
            "query_text": "example_travel",
            "device_type": "TABLET",
            "total_shows": 12,
            "total_clicks": 3,
            "avg_show_position": 4.5,
            "avg_click_position": 2.0,
        },
    ]


def test_yandex_webmaster_source_replace_builds_query_analytics_rows() -> None:
    source = YandexWebmasterSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())
    result = source.extract(
        _load_config(
            strategy=LoadStrategy.REPLACE,
            resource="query_analytics_by_region_daily",
            day="2026-03-01",
            region_ids="225",
        ),
        None,
    )

    assert result.schema == get_yandex_webmaster_resource("query_analytics_by_region_daily").schema
    assert result.artifact._rows == [
        {
            "date": "2026-03-01",
            "region_id": 225,
            "region_name": "Россия",
            "query": "example_travel",
            "url": "https://travel.example.com/",
            "impressions": 10,
            "clicks": 2,
            "ctr": 0.2,
        }
    ]


def test_runtime_bootstrap_builds_yandex_webmaster_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            del vault_manager
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.yandex_webmaster.YandexWebmasterConnector", FakeConnector)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "yandex_webmaster",
            "options": {"timeout": 66, "max_retries": 4, "rate_limit_delay": 0.7},
        },
        vault_path="api/yandex_webmaster",
        sink_connector=None,
    )

    assert isinstance(source, YandexWebmasterSource)
    assert captured["vault_path"] == "api/yandex_webmaster"
    assert captured["kwargs"]["timeout"] == 66


def test_yandex_webmaster_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_yandex_webmaster_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)

    assert manifest is not None
    assert len(manifest.processes) == 3
    targets = {process.config.load_config.target_table: process.config.load_config for process in manifest.processes}
    assert targets["app__host_metrics_daily"].target_schema == "landing__yandex_webmaster__api"
    assert targets["app__host_metrics_daily"].load_strategy == LoadStrategy.INCREMENTAL_MERGE
    assert targets["app__search_queries_history_daily"].load_strategy == LoadStrategy.REPLACE
    assert targets["app__query_analytics_by_region_daily"].load_strategy == LoadStrategy.REPLACE
