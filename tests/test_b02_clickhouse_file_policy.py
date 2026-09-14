"""Finite admission and explicit resource policy for validated staging."""

from pathlib import Path

import pytest

from dpone.runtime.sinks.clickhouse_validated_file_models import (
    ClickHouseValidatedFilePolicy,
    FileConsumptionError,
    require_transport_profile,
)


@pytest.mark.parametrize("value", [None, True, False, 0, -1, 1.2, "1024"])
def test_spool_budget_is_required_positive_integer(value):
    with pytest.raises(ValueError):
        ClickHouseValidatedFilePolicy(work_directory=Path("work"), max_spool_bytes=value)


def test_policy_defaults_are_finite_and_explicit():
    policy = ClickHouseValidatedFilePolicy(work_directory=Path("work"), max_spool_bytes=4096)
    assert policy.max_source_bytes == 4_294_967_296
    assert policy.max_record_bytes == 16_777_216
    assert policy.preparation_timeout_seconds == 3600
    assert policy.verification_timeout_seconds == 3600


@pytest.mark.parametrize("mode", [None, "auto", "python", "driver", "native_driver", "native_tcp", "native-tcp"])
def test_unsupported_modes_denied(mode):
    with pytest.raises(FileConsumptionError):
        require_transport_profile({"clickhouse_bulk": {"mode": mode}})


@pytest.mark.parametrize(
    "mode,expected",
    [("client", "client"), ("clickhouse-client", "client"), ("native_client", "client"), ("http", "http")],
)
def test_explicit_modes_normalize_once(mode, expected):
    assert require_transport_profile({"clickhouse_bulk": {"mode": mode}}) == (expected, 3600)


@pytest.mark.parametrize("value", [True, 0, -1, None, 1.5])
def test_authored_invalid_timeout_never_becomes_default(value):
    with pytest.raises(FileConsumptionError):
        require_transport_profile({"clickhouse_bulk": {"mode": "http", "http": {"timeout_seconds": value}}})


@pytest.mark.parametrize(
    "options",
    [
        {"insert_settings": {"async_insert": 1}},
        {"query_id": "external"},
        {"insert_deduplication_token": "external"},
        {"insert_settings": {"unknown": 1}},
        {"input_format": "CSV"},
    ],
)
def test_unsafe_settings_are_not_silently_rewritten(options):
    with pytest.raises(FileConsumptionError):
        require_transport_profile({"clickhouse_bulk": {"mode": "http", **options}})
