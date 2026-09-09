from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.connectors.api.similarweb_resources import SIMILARWEB_KEYWORDS_SCHEMA
from dpone.runtime.sources.api.similarweb import SimilarwebSource
from dpone.runtime.sources.strategies.api.similarweb import SimilarwebDQValidator


class DummyLogger:
    def __init__(self) -> None:
        self.events = []

    def info(self, *args, **kwargs):
        self.events.append(("info", args, kwargs))

    def warning(self, *args, **kwargs):
        self.events.append(("warning", args, kwargs))

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))


class DummyConnector:
    DEFAULT_TIMEOUT = 60

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.rows_by_domain = {
            "travel.example.com": [
                {
                    "keyword": "example_travel paris",
                    "clicks": 10,
                    "traffic_share": 0.25,
                    "difficulty": 12,
                    "competition": 0.33,
                    "primary_intent": "Informational",
                    "secondary_intent": "Commercial",
                    "volume": 120,
                    "cpc": 1.7,
                    "cpc_low_bid": 1.1,
                    "cpc_high_bid": 2.0,
                    "zero_clicks_share": 0.09,
                    "position": 1,
                    "serp_features": ["images", "news"],
                    "top_url": "https://travel.example.com/paris",
                }
            ]
        }

    def fetch_resource_rows(self, **kwargs):
        self.calls.append(dict(kwargs))
        domain = str(kwargs["url"])
        return list(self.rows_by_domain.get(domain, []))

    def health_check(self):
        return True


class DummySinkConnector:
    def __init__(self, existing: set[tuple[str, str]] | None = None) -> None:
        self.existing = existing or set()
        self.delete_queries: list[str] = []

    def get_records(self, query, as_dict=False):
        del as_dict
        for snapshot_month, domain in self.existing:
            if snapshot_month in query and domain in query:
                return [{"exists": 1}]
        return []

    def execute_query(self, query):
        self.delete_queries.append(query)
        return 1


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__similarweb",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table=options.get("resource", "keywords"),
        target_schema="landing__similarweb__api",
        target_table=f"default__{options.get('resource', 'keywords')}",
        load_strategy=options.pop("load_strategy", LoadStrategy.INCREMENTAL_APPEND),
        options=dict(options),
    )


def test_similarweb_source_extracts_static_schema_and_transformed_rows() -> None:
    connector = DummyConnector()
    source = SimilarwebSource(connector=connector, sink_connector=DummySinkConnector(), logger=DummyLogger())

    result = source.extract(
        _load_config(
            resource="keywords",
            domains=["travel.example.com"],
            snapshot_month="2026-02",
            limit=50,
            page_size=50,
            min_keywords_count=1,
        ),
        None,
    )

    assert result.schema == SIMILARWEB_KEYWORDS_SCHEMA
    assert result.force_full_refresh is False
    rows = result.artifact._rows
    assert len(rows) == 1
    assert rows[0]["date"] == date(2026, 2, 1)
    assert rows[0]["domain"] == "travel.example.com"
    assert rows[0]["serp_features"] == ["images", "news"]
    assert connector.calls[0]["traffic_source"] == "Organic"


def test_similarweb_source_skips_loaded_domain_month_without_force_reload() -> None:
    connector = DummyConnector()
    sink_connector = DummySinkConnector(existing={("2026-02-01", "travel.example.com")})
    source = SimilarwebSource(connector=connector, sink_connector=sink_connector, logger=DummyLogger())

    result = source.extract(
        _load_config(
            resource="keywords",
            domains="travel.example.com",
            snapshot_month="2026-02",
            min_keywords_count=1,
        ),
        None,
    )

    assert result.artifact._rows == []
    assert connector.calls == []
    assert sink_connector.delete_queries == []


def test_similarweb_source_force_reload_deletes_existing_partition_before_reload() -> None:
    connector = DummyConnector()
    sink_connector = DummySinkConnector(existing={("2026-02-01", "travel.example.com")})
    source = SimilarwebSource(connector=connector, sink_connector=sink_connector, logger=DummyLogger())

    result = source.extract(
        _load_config(
            resource="keywords",
            domains=["travel.example.com"],
            snapshot_month="2026-02",
            min_keywords_count=1,
            force_reload=True,
        ),
        None,
    )

    assert len(result.artifact._rows) == 1
    assert len(connector.calls) == 1
    assert len(sink_connector.delete_queries) == 1
    assert "DELETE FROM" in sink_connector.delete_queries[0]


def test_similarweb_validator_rejects_duplicate_business_keys() -> None:
    validator = SimilarwebDQValidator()
    rows = [
        {
            "date": date(2026, 2, 1),
            "domain": "travel.example.com",
            "keyword": "example_travel paris",
            "top_url": "https://travel.example.com/paris",
        },
        {
            "date": date(2026, 2, 1),
            "domain": "travel.example.com",
            "keyword": "example_travel paris",
            "top_url": "https://travel.example.com/paris",
        },
    ]

    with pytest.raises(RuntimeError, match="duplicate rows"):
        validator.validate(rows, snapshot_month="2026-02-01", min_keywords_count=1)


def test_runtime_bootstrap_builds_similarweb_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            del vault_manager
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.similarweb.SimilarwebConnector", FakeConnector)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "similarweb",
            "options": {
                "timeout": 61,
                "max_retries": 4,
                "rate_limit_delay": 1.2,
            },
        },
        vault_path="api/similarweb",
        sink_connector=None,
    )

    assert isinstance(source, SimilarwebSource)
    assert captured["vault_path"] == "api/similarweb"
    assert captured["kwargs"]["timeout"] == 61


def test_similarweb_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_similarweb_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)

    assert manifest is not None
    assert len(manifest.processes) == 1
    process = manifest.processes[0].config.load_config
    assert process.target_schema == "landing__similarweb__api"
    assert process.target_table == "default__keywords"
    assert process.load_strategy == LoadStrategy.INCREMENTAL_APPEND
