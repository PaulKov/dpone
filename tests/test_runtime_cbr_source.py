from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.cbr import CbrSource
from dpone.runtime.sources.strategies.api.cbr import (
    CbrFullExtractStrategy,
    CbrIncrementalMergeExtractStrategy,
)


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
    def __init__(self) -> None:
        self.calls = []

    def get_xml_daily(self, day=None):
        self.calls.append(day)
        stamp = day.strftime("%d.%m.%Y") if day else "10.03.2026"
        return f'''<?xml version="1.0" encoding="windows-1251"?>\n<ValCurs Date="{stamp}" name="Foreign Currency Market">\n  <Valute ID="R01235">\n    <NumCode>840</NumCode>\n    <CharCode>USD</CharCode>\n    <Nominal>1</Nominal>\n    <Name>Доллар США</Name>\n    <Value>90,0000</Value>\n  </Valute>\n</ValCurs>'''

    def health_check(self):
        return True


class DummySinkConnector:
    def get_max_column_value(self, schema, table, column):
        return None


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__cbr",
        target_conn_id="bigquery-dwh",
        source_schema="api__cbr",
        source_table=options.get("resource", "xml_daily_asp"),
        target_schema="landing__cbr__api",
        target_table="app__xml_daily_asp",
        load_strategy=options.pop("load_strategy", LoadStrategy.INCREMENTAL_MERGE),
        options=dict(options),
        unique_key=["as_of_date", "valute_id"],
    )


def test_cbr_full_extract_strategy_supports_single_day_and_range() -> None:
    connector = DummyConnector()
    strategy = CbrFullExtractStrategy(connector=connector, sink_connector=DummySinkConnector(), logger=DummyLogger())
    load_config = _load_config(load_strategy=LoadStrategy.FULL_REFRESH, resource="xml_daily_asp", day="2026-03-10")
    result = strategy.extract(load_config, None)
    assert result.force_full_refresh is True
    assert len(result.artifact._rows) == 1
    assert result.artifact._rows[0]["as_of_date"] == "2026-03-10"

    load_config_range = _load_config(
        load_strategy=LoadStrategy.FULL_REFRESH,
        resource="xml_daily_asp",
        start_date="2026-03-09",
        end_date="2026-03-10",
    )
    result_range = strategy.extract(load_config_range, None)
    assert len(result_range.artifact._rows) == 2
    assert connector.calls[-2].isoformat() == "2026-03-09"
    assert connector.calls[-1].isoformat() == "2026-03-10"


def test_cbr_full_extract_deduplicates_same_business_day_from_date_range() -> None:
    class WeekendAwareConnector(DummyConnector):
        def get_xml_daily(self, day=None):
            self.calls.append(day)
            return """<?xml version="1.0" encoding="windows-1251"?>
<ValCurs Date="07.03.2026" name="Foreign Currency Market">
  <Valute ID="R01235">
    <NumCode>840</NumCode>
    <CharCode>USD</CharCode>
    <Nominal>1</Nominal>
    <Name>Доллар США</Name>
    <Value>90,0000</Value>
  </Valute>
</ValCurs>"""

    connector = WeekendAwareConnector()
    strategy = CbrFullExtractStrategy(connector=connector, sink_connector=DummySinkConnector(), logger=DummyLogger())

    result = strategy.extract(
        _load_config(
            load_strategy=LoadStrategy.FULL_REFRESH,
            resource="xml_daily_asp",
            start_date="2026-03-07",
            end_date="2026-03-08",
        ),
        None,
    )

    assert len(result.artifact._rows) == 1
    assert result.artifact._rows[0]["as_of_date"] == "2026-03-07"


def test_cbr_incremental_merge_state_tracks_requested_day() -> None:
    connector = DummyConnector()
    strategy = CbrIncrementalMergeExtractStrategy(
        connector=connector, sink_connector=DummySinkConnector(), logger=DummyLogger()
    )
    load_config = _load_config(
        resource="xml_daily_asp", lookback_days=2, start_date="2026-03-09", end_date="2026-03-10"
    )
    result = strategy.extract(load_config, {"last_value": "2026-03-08"})
    assert result.state == {"last_value": "2026-03-10"}
    assert len(result.artifact._rows) == 2
    assert result.artifact._rows[0]["valute_id"] == "R01235"


def test_cbr_source_maps_health_check_and_extract() -> None:
    source = CbrSource(connector=DummyConnector(), sink_connector=DummySinkConnector(), logger=DummyLogger())
    assert source.health_check() is True
    load_config = _load_config(resource="xml_daily_asp", start_date="2026-03-10", end_date="2026-03-10")
    result = source.extract(load_config, None)
    assert len(result.artifact._rows) == 1


def test_runtime_bootstrap_builds_cbr_source_without_vault() -> None:
    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "cbr",
            "options": {
                "resource": "xml_daily_asp",
                "timeout": 15,
                "max_retries": 4,
                "retry_delay": 0.1,
            },
        },
        vault_path=None,
        sink_connector=None,
    )
    assert isinstance(source, CbrSource)
    assert source.connector.timeout == 15
    assert source.connector.retries == 4


def test_cbr_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_cbr_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 1
    proc = manifest.processes[0].config.load_config
    assert proc.source_conn_id == "api__cbr"
    assert proc.source_schema == "app"
    assert proc.source_table == "xml_daily_asp"
    assert proc.target_table == "app__xml_daily_asp"
