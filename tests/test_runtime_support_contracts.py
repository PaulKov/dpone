from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from dpone._compat import UTC
from dpone.contracts.errors import ETLConfigurationError
from dpone.runtime import api_registry
from dpone.runtime.api_registry import (
    APIProviderRuntimeSpec,
    build_api_runtime_source,
    get_api_provider_spec,
    list_registered_api_provider_specs,
)
from dpone.runtime.support.cbr_xml_parser import deduplicate_rows, parse_xml_daily, resolve_date_option
from dpone.runtime.support.fasttrack_columns import normalize_fasttrack_record_columns, sanitize_fasttrack_column_name
from dpone.runtime.support.fasttrack_dates import (
    fasttrack_parse_temporal_fields_default,
    parse_fasttrack_datetime,
    parse_fasttrack_record_dates,
    resolve_fasttrack_parse_temporal_fields,
)


def test_fasttrack_temporal_field_option_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT", raising=False)
    assert fasttrack_parse_temporal_fields_default() is True
    assert resolve_fasttrack_parse_temporal_fields(None) is True

    monkeypatch.setenv("DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT", "disabled")
    assert fasttrack_parse_temporal_fields_default() is False
    assert resolve_fasttrack_parse_temporal_fields({}) is False
    assert resolve_fasttrack_parse_temporal_fields({"parse_temporal_fields": "enabled"}) is True
    assert resolve_fasttrack_parse_temporal_fields({"parse_temporal_fields": ""}) is False


def test_fasttrack_datetime_parser_normalizes_to_naive_utc_and_preserves_invalid_values() -> None:
    assert parse_fasttrack_datetime(None) is None
    assert parse_fasttrack_datetime("") is None
    assert parse_fasttrack_datetime("not-a-date") == "not-a-date"
    assert parse_fasttrack_datetime("2024-01-02T03:04:05+03:00") == datetime(2024, 1, 2, 0, 4, 5)
    assert parse_fasttrack_datetime("02/01/2024 03:04", fmt="%d/%m/%Y %H:%M") == datetime(2024, 1, 2, 3, 4)
    assert parse_fasttrack_datetime(datetime(2024, 1, 2, 3, 4, tzinfo=UTC)) == datetime(2024, 1, 2, 3, 4)


def test_fasttrack_record_date_parser_only_updates_declared_columns() -> None:
    parsed = parse_fasttrack_record_dates(
        {"created_at": "2024-01-02 03:04:05", "name": "Alice"},
        {"created_at": None, "missing": None},
    )

    assert parsed == {"created_at": datetime(2024, 1, 2, 3, 4, 5), "name": "Alice"}


def test_fasttrack_column_sanitizer_transliterates_and_deduplicates_columns() -> None:
    assert sanitize_fasttrack_column_name("Дата-Создания") == "data_sozdaniya"
    assert sanitize_fasttrack_column_name("orderID") == "order_id"
    assert sanitize_fasttrack_column_name("123") == "column_123"
    assert sanitize_fasttrack_column_name("!!!") == "column"

    normalized = normalize_fasttrack_record_columns(
        "ignored_resource_name",
        {
            "Дата-Создания": "a",
            "Data Sozdaniya": "b",
            "data_sozdaniya": "c",
        },
    )

    assert normalized == {
        "data_sozdaniya": "a",
        "data_sozdaniya_2": "b",
        "data_sozdaniya_3": "c",
    }


def test_cbr_date_option_resolution_and_validation() -> None:
    assert resolve_date_option(None) is None
    assert resolve_date_option(date(2024, 1, 2)) == date(2024, 1, 2)
    assert resolve_date_option(datetime(2024, 1, 2, 3, 4)) == date(2024, 1, 2)
    assert resolve_date_option("2024-01-02") == date(2024, 1, 2)
    assert resolve_date_option("today()") == datetime.now(UTC).date()

    with pytest.raises(ValueError, match="Unsupported CBR date option"):
        resolve_date_option(123)
    with pytest.raises(ValueError):
        resolve_date_option("02.01.2024")


def test_cbr_xml_parser_normalizes_daily_rows_and_deduplicates_by_date_and_valute() -> None:
    loaded_at = datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
    xml = """
    <ValCurs Date="02.01.2024" name="Foreign Currency Market">
      <Valute ID="R01235">
        <NumCode>840</NumCode>
        <CharCode>USD</CharCode>
        <Nominal>1</Nominal>
        <Name>Доллар США</Name>
        <Value>90,1234</Value>
      </Valute>
      <Valute ID="R01239">
        <NumCode>978</NumCode>
        <CharCode>EUR</CharCode>
        <Nominal>0</Nominal>
        <Name>Евро</Name>
        <Value>bad</Value>
      </Valute>
      <Valute ID="R01235">
        <NumCode>840</NumCode>
        <CharCode>USD</CharCode>
        <Nominal>1</Nominal>
        <Name>Доллар США</Name>
        <Value>91,0000</Value>
      </Valute>
    </ValCurs>
    """

    rows = parse_xml_daily(xml, loaded_at)
    deduped = deduplicate_rows(rows)

    assert rows[0] == {
        "as_of_date": "2024-01-02",
        "valute_id": "R01235",
        "num_code": "840",
        "char_code": "USD",
        "nominal": 1,
        "name": "Доллар США",
        "value": 90.1234,
        "vunit_rate": 90.1234,
        "raw_value": "90,1234",
        "loaded_at": "2024-01-02T03:04:05+00:00",
    }
    assert rows[1]["value"] is None
    assert rows[1]["vunit_rate"] is None
    assert [row["raw_value"] for row in deduped] == ["91,0000", "bad"]


def test_api_registry_reports_empty_unknown_and_unimplemented_api_types() -> None:
    with pytest.raises(ETLConfigurationError, match="необходимо указать api_type"):
        get_api_provider_spec("")
    with pytest.raises(ETLConfigurationError, match="Неизвестный api_type='missing'"):
        get_api_provider_spec("missing")

    amplitude = get_api_provider_spec("amplitude")
    assert amplitude.is_implemented is False
    with pytest.raises(ETLConfigurationError, match="runtime-реализация ещё не добавлена"):
        build_api_runtime_source(source_cfg={"api_type": "amplitude"}, vault_path=None)

    assert {spec.api_type for spec in list_registered_api_provider_specs()} >= {"cbr", "fasttrack", "amplitude"}


def test_api_registry_builds_non_vault_runtime_source(monkeypatch: pytest.MonkeyPatch) -> None:
    created_connector_kwargs: dict[str, object] = {}

    class FakeConnector:
        def __init__(self, **kwargs: object) -> None:
            created_connector_kwargs.update(kwargs)

    class FakeSource:
        def __init__(self, *, connector: FakeConnector, sink_connector: object, logger: object) -> None:
            self.connector = connector
            self.sink_connector = sink_connector
            self.logger = logger

    def fake_load_object(target: str) -> object:
        if target.endswith(":CbrConnector"):
            return FakeConnector
        if target.endswith(":CbrSource"):
            return FakeSource
        raise AssertionError(target)

    monkeypatch.setattr(api_registry, "_load_object", fake_load_object)
    sink_connector = object()
    logger = object()

    source = build_api_runtime_source(
        source_cfg={"api_type": "cbr", "options": {"timeout": "7", "max_retries": "2", "retry_delay": "0.5"}},
        vault_path=None,
        sink_connector=sink_connector,
        logger=logger,
    )

    assert isinstance(source, FakeSource)
    assert isinstance(source.connector, FakeConnector)
    assert source.sink_connector is sink_connector
    assert source.logger is logger
    assert created_connector_kwargs == {"timeout": 7, "retries": 2, "retry_delay": 0.5}


def test_api_registry_enforces_vault_runtime_requirements(monkeypatch: pytest.MonkeyPatch) -> None:
    base = get_api_provider_spec("cbr")
    vault_defaults = replace(base.defaults, credentials_mode="vault")
    fake_spec = APIProviderRuntimeSpec(
        defaults=vault_defaults,
        connector_target="fake:Connector",
        source_target="fake:Source",
        connector_kwargs_factory=lambda options: {"timeout": 3},
    )
    monkeypatch.setitem(api_registry._RUNTIME_SPECS, "fake_vault", fake_spec)

    with pytest.raises(ETLConfigurationError, match="требуется vault_path"):
        build_api_runtime_source(source_cfg={"api_type": "fake_vault"}, vault_path=None)

    class ConnectorWithoutVault:
        pass

    monkeypatch.setattr(
        api_registry,
        "_load_object",
        lambda target: ConnectorWithoutVault if target == "fake:Connector" else SimpleNamespace,
    )
    with pytest.raises(ETLConfigurationError, match="не поддерживает from_vault"):
        build_api_runtime_source(source_cfg={"api_type": "fake_vault"}, vault_path="secret/path")


def test_api_registry_legacy_from_vault_ignores_incompatible_mount_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = get_api_provider_spec("cbr")
    vault_defaults = replace(base.defaults, credentials_mode="vault")
    fake_spec = APIProviderRuntimeSpec(
        defaults=vault_defaults,
        connector_target="fake:Connector",
        source_target="fake:Source",
        connector_kwargs_factory=lambda options: {"timeout": 3},
    )
    monkeypatch.setitem(api_registry._RUNTIME_SPECS, "fake_vault_mount", fake_spec)

    class ConnectorWithNarrowFromVault:
        @classmethod
        def from_vault(cls, vault_path: str):
            assert vault_path == "secret/path"
            return cls()

    class FakeSource:
        def __init__(self, connector, sink_connector=None, logger=None):
            self.connector = connector

    monkeypatch.setattr(
        api_registry,
        "_load_object",
        lambda target: ConnectorWithNarrowFromVault if target.endswith(":Connector") else FakeSource,
    )
    source = build_api_runtime_source(
        source_cfg={"api_type": "fake_vault_mount"},
        vault_path="secret/path",
        vault_mount_point="prod",
    )
    assert isinstance(source.connector, ConnectorWithNarrowFromVault)
