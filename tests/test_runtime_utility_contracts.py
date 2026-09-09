from __future__ import annotations

from datetime import date, datetime

import pytest

from dpone._compat import UTC
from dpone.contracts.technical_columns import (
    TechnicalColumnsMode,
    include_technical_columns,
    parse_mode,
    resolve_technical_columns,
)
from dpone.lib.utils.security import mask_sensitive_params, mask_sensitive_value
from dpone.runtime.support.timezone import TimezoneConverter, format_timestamp_for_sql, to_unix_timestamp


def test_mask_sensitive_value_hides_empty_and_short_values() -> None:
    assert mask_sensitive_value("") == "****"
    assert mask_sensitive_value("abcd") == "****"
    assert mask_sensitive_value("abcdef", visible_chars=2) == "ab****"


def test_mask_sensitive_params_recurses_without_mutating_original_payload() -> None:
    payload = {
        "api_key": "secret-token",
        "nested": {"password": "long-password", "safe": "visible"},
        "args": ("token", "abcdef", {"custom_secret": "value"}),
    }

    masked = mask_sensitive_params(payload, sensitive_keys={"custom_secret"}, visible_chars=3)

    assert masked == {
        "api_key": "sec****",
        "nested": {"password": "lon****", "safe": "visible"},
        "args": ("token", "abc****", {"custom_secret": "val****"}),
    }
    assert payload["api_key"] == "secret-token"


def test_timezone_converter_parses_common_inputs_and_formats_clickhouse_values() -> None:
    converter = TimezoneConverter(target_tz="Europe/Moscow")

    from_string = converter.parse_and_convert("2024-01-01 00:00:00")
    from_date = converter.parse_and_convert(date(2024, 1, 1))
    formatted = converter.format_for_clickhouse(from_string, include_milliseconds=False)

    assert from_string.hour == 3
    assert from_date.hour == 3
    assert formatted == "2024-01-01 03:00:00"


def test_timezone_converter_returns_none_for_unparseable_convert_and_format_values() -> None:
    converter = TimezoneConverter(target_tz="UTC")

    assert converter.parse_timestamp("not-a-date") is None
    assert converter.convert_and_format("not-a-date") is None
    with pytest.raises(ValueError, match="Не удалось распарсить значение"):
        converter.parse_and_convert(object())


def test_timezone_module_formats_sql_and_unix_timestamps() -> None:
    dt = datetime(1970, 1, 2, 0, 0, tzinfo=UTC)

    assert format_timestamp_for_sql("2024-01-01T00:00:00Z") == "2024-01-01 00:00:00.000000+00:00"
    assert format_timestamp_for_sql("not-a-date") == "not-a-date"
    assert to_unix_timestamp(dt) == 86400
    assert to_unix_timestamp(date(1970, 1, 2)) == 86400
    assert to_unix_timestamp("1970-01-02") == 86400
    with pytest.raises(ValueError, match="Не удалось конвертировать"):
        to_unix_timestamp(object())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("required", TechnicalColumnsMode.REQUIRED),
        ("req", TechnicalColumnsMode.REQUIRED),
        ("optional", TechnicalColumnsMode.OPTIONAL),
        ("off", TechnicalColumnsMode.FORBIDDEN),
        (True, TechnicalColumnsMode.REQUIRED),
        (0, TechnicalColumnsMode.FORBIDDEN),
    ],
)
def test_parse_technical_columns_modes(raw: object, expected: TechnicalColumnsMode) -> None:
    assert parse_mode(raw) is expected


def test_parse_technical_columns_mode_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="Invalid technical_columns mode"):
        parse_mode("surprising")


def test_resolve_technical_columns_honors_mode_precedence_and_warnings() -> None:
    required = resolve_technical_columns(
        {
            "technical_columns": "required",
            "include_technical_columns": "false",
        }
    )
    forbidden = resolve_technical_columns(
        {
            "technical_columns_mode": "forbidden",
            "include_technical_columns": "true",
        }
    )
    legacy = resolve_technical_columns({"include_technical_columns": ""})

    assert required.enabled is True
    assert required.warning == "technical_columns=required conflicts with include_technical_columns=false (mode wins)"
    assert forbidden.enabled is False
    assert forbidden.warning == "technical_columns=forbidden conflicts with include_technical_columns=true (mode wins)"
    assert legacy.enabled is True
    assert legacy.warning == "empty string treated as true"
    assert include_technical_columns({}) is True
