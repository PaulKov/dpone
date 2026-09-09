from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.google_ads import GoogleAdsSource


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

    def __init__(self) -> None:
        self.credentials = SimpleNamespace(customer_ids=["1234567890"])
        self.calls: list[tuple[str, str]] = []

    def execute_query(self, *, customer_id, query):
        self.calls.append((customer_id, query))

        def _row(term: str | None):
            keyword = None if term is None else SimpleNamespace(text=term)
            return SimpleNamespace(
                segments=SimpleNamespace(date="2026-03-01"),
                campaign=SimpleNamespace(name="Campaign", id=42),
                ad_group_criterion=SimpleNamespace(keyword=keyword),
                metrics=SimpleNamespace(impressions=10, clicks=2, cost_micros=5_000_000),
            )

        if "keyword_view" in query:
            return [_row("paris")]
        return [_row(None)]

    def health_check(self):
        return True


def _load_config(*, strategy: LoadStrategy, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__google_ads",
        target_conn_id="bigquery-dwh",
        source_schema="api__google_ads",
        source_table="ads_stats",
        target_schema="landing__google_ads__api",
        target_table="app__ads_stats",
        load_strategy=strategy,
        options={"resource": "ads_stats", **options},
    )


def test_google_ads_source_full_extract_returns_rows_and_schema() -> None:
    source = GoogleAdsSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())

    result = source.extract(
        _load_config(strategy=LoadStrategy.FULL_REFRESH, start_date="2026-03-01", end_date="2026-03-02"),
        None,
    )

    assert result.force_full_refresh is True
    assert result.state is None
    assert len(result.artifact._rows) == 7
    assert result.artifact._rows[0]["campaign_id"] == 42
    assert ("report", "STRING") in result.schema


def test_google_ads_source_incremental_merge_sets_state() -> None:
    source = GoogleAdsSource(connector=DummyConnector(), sink_connector=None, logger=DummyLogger())

    result = source.extract(
        _load_config(strategy=LoadStrategy.INCREMENTAL_MERGE, end_date="2026-03-03"),
        {"last_value": "2026-03-02"},
    )

    assert result.force_full_refresh is False
    assert result.state == {"last_value": "2026-03-03"}


def test_runtime_bootstrap_builds_google_ads_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            del vault_manager
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.google_ads.GoogleAdsConnector", FakeConnector)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "google_ads",
            "options": {"timeout": 44, "max_retries": 4, "rate_limit_delay": 2.0},
        },
        vault_path="api/google_ads",
        sink_connector=None,
    )

    assert isinstance(source, GoogleAdsSource)
    assert captured["vault_path"] == "api/google_ads"
    assert captured["kwargs"]["timeout"] == 44


def test_google_ads_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_google_ads_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)

    assert manifest is not None
    assert len(manifest.processes) == 1
    process = manifest.processes[0].config.load_config
    assert process.source_conn_id == "api__google_ads"
    assert process.source_schema == "app"
    assert process.source_table == "ads_stats"
    assert process.target_schema == "landing__google_ads__api"
    assert process.target_table == "app__ads_stats"
    assert process.load_strategy == LoadStrategy.INCREMENTAL_MERGE


def test_google_ads_example_manifest_loads_with_registry_entry(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_google_ads_api.batch.yaml")
    registry = Path("examples/registry/sources.yaml")
    manifests = tmp_path / "examples"
    registry_dir = tmp_path / "registry"
    manifests.mkdir()
    registry_dir.mkdir()

    copied_manifest = manifests / example.name
    copied_manifest.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    copied_registry = registry_dir / registry.name
    copied_registry.write_text(registry.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(
        manifests,
        manifest_loader=ManifestLoaderRouter(registry_paths=(copied_registry,)),
    )
    manifest = loader.get_manifest(copied_manifest, metadata_only=True)

    assert manifest is not None
    assert len(manifest.processes) == 1
