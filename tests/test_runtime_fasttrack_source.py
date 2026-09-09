from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.bootstrap import DefaultRuntimeHydrator
from dpone.runtime.sources.api.fasttrack import FasttrackSource
from dpone.runtime.sources.strategies.api.fasttrack import (
    FasttrackFullExtractStrategy,
    FasttrackIncrementalMergeExtractStrategy,
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
    DEFAULT_TIMEOUT = 60

    def __init__(self) -> None:
        self.calls = []

    def iter_resource_rows(self, *, resource_name: str, extra_params=None):
        self.calls.append((resource_name, dict(extra_params or {})))
        if resource_name == "cascade_transactions":
            yield {
                "transaction_uuid": "tx-1",
                "created_at": "2026-03-12 10:00:00",
                "done_at": "2026-03-12 11:30:00",
                "phone_number": "+79990000000",
            }
            return
        if resource_name == "chat_sessions":
            yield {
                "UUID пользователя": "chat-1",
                "Дата начала": "12-03-2026 10:15:00",
                "Дата окончания": "12-03-2026 11:20:00",
                "Назначен на команду": "12-03-2026 10:16:00",
                "Назначен на оператора": "12-03-2026 10:17:00",
                "Имя пользователя": "Иван",
            }
            return
        if resource_name == "flex_cms_ratings":
            yield {
                "id": 1,
                "created": "2026-03-12T10:00:00.123456+03:00",
                "modified": "2026-03-12T11:00:00.000000+03:00",
                "score": 5,
            }
            return
        raise AssertionError(f"Unexpected resource: {resource_name}")

    def health_check(self):
        return True


class DummySinkConnector:
    def get_max_column_value(self, schema, table, column):
        return None


def _load_config(resource: str, *, load_strategy: LoadStrategy, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__fasttrack",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table=resource,
        target_schema="landing__fasttrack__api",
        target_table=f"default__{resource}",
        load_strategy=load_strategy,
        options={"resource": resource, **options},
    )


def test_fasttrack_incremental_merge_parses_dates_only_for_transactions() -> None:
    connector = DummyConnector()
    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "cascade_transactions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["transaction_uuid"],
        batch_size=10,
    )
    result = strategy.extract(load_config, None)
    rows = list(result.artifact._iterator)
    row = rows[0]
    assert isinstance(row["created_at"], datetime)
    assert isinstance(row["done_at"], datetime)
    assert row["phone_number"] == "+79990000000"
    assert result.force_full_refresh is False


def test_fasttrack_incremental_merge_keeps_chat_sessions_as_sanitized_raw_payload() -> None:
    connector = DummyConnector()
    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "chat_sessions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["uuid_polzovatelya", "data_nachala"],
    )
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert "uuid_polzovatelya" in row
    assert "imya_polzovatelya" in row
    assert isinstance(row["data_nachala"], datetime)
    assert isinstance(row["naznachen_na_komandu"], datetime)
    assert row["imya_polzovatelya"] == "Иван"
    assert row["uuid_polzovatelya"] == "chat-1"
    assert "UUID пользователя" not in row
    assert "chat_uuid" not in row


def test_fasttrack_full_refresh_parses_flex_iso_datetimes() -> None:
    connector = DummyConnector()
    strategy = FasttrackFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config("flex_cms_ratings", load_strategy=LoadStrategy.FULL_REFRESH)
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert isinstance(row["created"], datetime)
    assert row["created"].tzinfo is None
    assert row["score"] == 5
    assert result.force_full_refresh is True


def test_fasttrack_parse_temporal_fields_option_false_keeps_chat_session_temporals_as_strings() -> None:
    connector = DummyConnector()
    strategy = FasttrackFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "chat_sessions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["uuid_polzovatelya", "data_nachala"],
        parse_temporal_fields=False,
    )
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert row["data_nachala"] == "12-03-2026 10:15:00"
    assert row["naznachen_na_komandu"] == "12-03-2026 10:16:00"


def test_fasttrack_parse_temporal_fields_env_default_can_disable_typing(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT", "false")
    connector = DummyConnector()
    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config("cascade_transactions", load_strategy=LoadStrategy.INCREMENTAL_MERGE)
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert row["created_at"] == "2026-03-12 10:00:00"
    assert row["done_at"] == "2026-03-12 11:30:00"


def test_fasttrack_parse_temporal_fields_option_overrides_env_default(monkeypatch) -> None:
    monkeypatch.setenv("DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT", "false")
    connector = DummyConnector()
    strategy = FasttrackFullExtractStrategy(
        connector=connector,
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "flex_cms_ratings",
        load_strategy=LoadStrategy.FULL_REFRESH,
        parse_temporal_fields=True,
    )
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert isinstance(row["created"], datetime)


def test_fasttrack_source_maps_health_check_and_extract() -> None:
    source = FasttrackSource(connector=DummyConnector(), sink_connector=DummySinkConnector(), logger=DummyLogger())
    assert source.health_check() is True
    load_config = _load_config("cascade_transactions", load_strategy=LoadStrategy.INCREMENTAL_MERGE)
    result = source.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert row["transaction_uuid"] == "tx-1"


def test_runtime_bootstrap_builds_fasttrack_source(monkeypatch) -> None:
    captured = {}

    class FakeConnector(DummyConnector):
        @classmethod
        def from_vault(cls, vault_path, vault_manager=None, **kwargs):
            captured["vault_path"] = vault_path
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr("dpone.runtime.connectors.api.fasttrack.FasttrackConnector", FakeConnector)
    source = DefaultRuntimeHydrator._build_api_source(
        source_cfg={
            "api_type": "fasttrack",
            "options": {
                "resource": "cascade_transactions",
                "timeout": 15,
                "max_retries": 7,
                "rate_limit_delay": 0.4,
            },
        },
        vault_path="api/fasttrack",
        sink_connector=None,
    )
    assert isinstance(source, FasttrackSource)
    assert captured["vault_path"] == "api/fasttrack"
    assert captured["kwargs"]["timeout"] == 15


def test_fasttrack_example_manifest_loads(tmp_path: Path) -> None:
    example = Path("examples/batch/landing_fasttrack_api.batch.yaml")
    manifests = tmp_path / "examples"
    manifests.mkdir()
    copied = manifests / example.name
    copied.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    loader = ConfigLoader(manifests, manifest_loader=ManifestLoaderRouter())
    manifest = loader.get_manifest(copied, metadata_only=True)
    assert manifest is not None
    assert len(manifest.processes) == 3
    process_by_table = {proc.config.load_config.target_table: proc.config.load_config for proc in manifest.processes}
    assert process_by_table["default__cascade_transactions"].load_strategy == LoadStrategy.INCREMENTAL_MERGE
    assert process_by_table["default__flex_cms_ratings"].load_strategy == LoadStrategy.FULL_REFRESH
    assert process_by_table["default__chat_sessions"].load_strategy == LoadStrategy.INCREMENTAL_MERGE
    assert process_by_table["default__chat_sessions"].unique_key == ["uuid_polzovatelya", "data_nachala"]


def test_fasttrack_chat_sessions_preserves_all_vendor_columns_and_sanitizes_names() -> None:
    class ExtraConnector(DummyConnector):
        def iter_resource_rows(self, *, resource_name: str, extra_params=None):
            yield {
                "UUID пользователя": "chat-1",
                "Дата начала": "12-03-2026 10:15:00",
                "ИИ Робот: навык": "sales",
                "Неожиданное поле / extra": "value",
                "2xx": 42,
            }

    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=ExtraConnector(),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "chat_sessions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["uuid_polzovatelya", "data_nachala"],
    )
    result = strategy.extract(load_config, None)
    row = next(result.artifact._iterator)
    assert row["ii_robot_navyk"] == "sales"
    assert row["neozhidannoe_pole_extra"] == "value"
    assert row["column_2xx"] == 42


def test_fasttrack_chat_sessions_schema_keeps_temporal_columns_when_first_row_is_open_chat() -> None:
    class OpenChatConnector(DummyConnector):
        def iter_resource_rows(self, *, resource_name: str, extra_params=None):
            del extra_params
            assert resource_name == "chat_sessions"
            yield {
                "UUID пользователя": "chat-1",
                "Дата начала": "12-03-2026 10:15:00",
                "Дата окончания": "",
                "Назначен на команду": "12-03-2026 10:16:00",
                "Назначен на оператора": "12-03-2026 10:17:00",
            }
            yield {
                "UUID пользователя": "chat-2",
                "Дата начала": "12-03-2026 11:15:00",
                "Дата окончания": "12-03-2026 11:45:00",
                "Назначен на команду": "12-03-2026 11:16:00",
                "Назначен на оператора": "12-03-2026 11:17:00",
            }

    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=OpenChatConnector(),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "chat_sessions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["uuid_polzovatelya", "data_nachala"],
    )

    result = strategy.extract(load_config, None)
    schema = dict(result.schema)

    assert schema["data_nachala"] == "TIMESTAMP"
    assert schema["data_okonchaniya"] == "TIMESTAMP"
    assert schema["naznachen_na_komandu"] == "TIMESTAMP"
    assert schema["naznachen_na_operatora"] == "TIMESTAMP"


def test_fasttrack_chat_sessions_schema_keeps_temporals_as_strings_when_parsing_disabled() -> None:
    class OpenChatConnector(DummyConnector):
        def iter_resource_rows(self, *, resource_name: str, extra_params=None):
            del extra_params
            assert resource_name == "chat_sessions"
            yield {
                "UUID пользователя": "chat-1",
                "Дата начала": "12-03-2026 10:15:00",
                "Дата окончания": "",
                "Назначен на команду": "12-03-2026 10:16:00",
                "Назначен на оператора": "12-03-2026 10:17:00",
            }

    strategy = FasttrackIncrementalMergeExtractStrategy(
        connector=OpenChatConnector(),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    load_config = _load_config(
        "chat_sessions",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["uuid_polzovatelya", "data_nachala"],
        parse_temporal_fields=False,
    )

    result = strategy.extract(load_config, None)
    schema = dict(result.schema)

    assert schema["data_nachala"] == "STRING"
    assert schema["data_okonchaniya"] == "STRING"
    assert schema["naznachen_na_komandu"] == "STRING"
    assert schema["naznachen_na_operatora"] == "STRING"
