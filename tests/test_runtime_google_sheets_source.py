from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.google_sheets import GoogleSheetsSource


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
        self.calls: list[dict[str, object]] = []

    def get_records(self, **kwargs):
        self.calls.append(dict(kwargs))
        return [
            {
                "order_id": 1,
                "city": "Paris",
                "_meta_spreadsheet_id": "sheet-1",
            },
            {
                "order_id": 2,
                "city": "Berlin",
                "_meta_spreadsheet_id": "sheet-1",
            },
        ]

    def health_check(self):
        return True


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__google_sheets",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table="worksheet_rows",
        target_schema="landing__google_sheets__api",
        target_table="app__worksheet_rows",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "resource": "worksheet_rows",
            "spreadsheet_id": "sheet-1",
            "worksheet_title": "Leads",
            **options,
        },
    )


def test_google_sheets_source_extracts_rows_and_detected_schema() -> None:
    connector = DummyConnector()
    source = GoogleSheetsSource(connector=connector, sink_connector=None, logger=DummyLogger())

    result = source.extract(_load_config(range_name="A1:C3"), None)

    assert result.force_full_refresh is True
    assert result.state is None
    assert len(result.artifact._rows) == 2
    assert result.artifact._rows[0]["city"] == "Paris"
    assert ("order_id", "INT64") in result.schema
    assert connector.calls[0]["range_name"] == "A1:C3"


def test_google_sheets_source_passes_limit_rows_and_limit_columns_to_connector() -> None:
    connector = DummyConnector()
    source = GoogleSheetsSource(connector=connector, sink_connector=None, logger=DummyLogger())

    source.extract(_load_config(limit_rows=10, limit_columns=4), None)

    assert connector.calls[0]["limit_rows"] == 10
    assert connector.calls[0]["limit_columns"] == 4


def test_runtime_bootstrap_builds_google_sheets_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            del vault_manager
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.google_sheets.GoogleSheetsConnector", FakeConnector)

    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "google_sheets",
            "options": {"timeout": 61, "max_retries": 4, "rate_limit_delay": 0.3},
        },
        vault_path="api/google_sheets",
        sink_connector=None,
    )

    assert isinstance(source, GoogleSheetsSource)
    assert captured["vault_path"] == "api/google_sheets"
    assert captured["kwargs"]["timeout"] == 61


def test_google_sheets_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_google_sheets_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)

    assert manifest is not None
    process = manifest.processes[0].config.load_config
    assert process.target_schema == "landing__google_sheets__api"
    assert process.target_table == "app__worksheet_rows"
    assert process.load_strategy == LoadStrategy.FULL_REFRESH
