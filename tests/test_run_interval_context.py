from __future__ import annotations

from datetime import datetime

from dpone.config.load_config import LoadConfig
from dpone.contracts.run_interval import (
    RUN_INTERVAL_ENV_NAMES,
    RunInterval,
    parse_interval_datetime,
    run_interval_from_env,
)
from dpone.services.interval_context import IntervalContextService


def _config(**kwargs) -> LoadConfig:
    base = dict(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
    )
    base.update(kwargs)
    return LoadConfig(**base)


def test_run_interval_env_contract_names_are_stable() -> None:
    assert RUN_INTERVAL_ENV_NAMES == (
        "DPONE_DAG_ID",
        "DPONE_DAG_RUN_ID",
        "DPONE_TRY_NUMBER",
        "DPONE_LOGICAL_DATE",
        "DPONE_INTERVAL_START",
        "DPONE_INTERVAL_END",
        "DPONE_PARTITION_KEY",
        "DPONE_PARTITION_DIMENSION",
        "DPONE_PARTITION_MODE",
    )


def test_run_interval_from_env_ignores_empty_and_none_renders() -> None:
    interval = run_interval_from_env(
        {
            "DPONE_INTERVAL_START": "2025-01-01T00:00:00+00:00",
            "DPONE_INTERVAL_END": "",
            "DPONE_LOGICAL_DATE": "None",
            "DPONE_DAG_ID": "orders_daily",
            "DPONE_PARTITION_KEY": "2025-01-01",
            "DPONE_PARTITION_DIMENSION": "business_date",
            "DPONE_PARTITION_MODE": "native",
        }
    )

    assert interval.interval_start == "2025-01-01T00:00:00+00:00"
    assert interval.interval_end is None
    assert interval.logical_date is None
    assert interval.dag_id == "orders_daily"
    assert interval.partition_key == "2025-01-01"
    assert interval.partition_dimension == "business_date"
    assert interval.partition_mode == "native"
    assert not interval.is_empty


def test_execution_datetime_prefers_logical_date_and_supports_zulu() -> None:
    interval = RunInterval(logical_date="2025-06-01T00:00:00Z", interval_start="2025-05-31T00:00:00Z")

    assert interval.execution_datetime() == parse_interval_datetime("2025-06-01T00:00:00+00:00")
    assert RunInterval(interval_start="2025-05-31T00:00:00Z").execution_datetime() == datetime.fromisoformat(
        "2025-05-31T00:00:00+00:00"
    )
    assert RunInterval().execution_datetime() is None


def test_interval_tokens_include_airflow_style_ds() -> None:
    tokens = RunInterval(
        interval_start="2025-01-01T00:00:00+00:00",
        interval_end="2025-01-02T00:00:00+00:00",
        logical_date="2025-01-01T00:00:00+00:00",
        dag_run_id="scheduled__2025-01-01",
        partition_key="2025-01-01",
    ).substitution_tokens()

    assert tokens["ds"] == "2025-01-01"
    assert tokens["data_interval_start"] == "2025-01-01T00:00:00+00:00"
    assert tokens["dag_run_id"] == "scheduled__2025-01-01"
    assert tokens["partition_key"] == "2025-01-01"


def test_interval_context_resolves_tokens_in_options_and_predicates() -> None:
    interval = RunInterval(
        interval_start="2025-01-01T00:00:00+00:00",
        interval_end="2025-01-02T00:00:00+00:00",
        logical_date="2025-01-01T00:00:00+00:00",
    )
    config = _config(
        custom_predicate="business_date >= '{{ data_interval_start }}'",
        options={
            "backfill": {
                "chunk": {"column": "d", "from": "{{ ds }}", "to": "{{ ds }}", "step": "1d"},
            },
            "nested": [{"date_from": "{{ data_interval_start }}"}],
        },
    )

    result = IntervalContextService(interval).apply(config)

    assert result.custom_predicate == "business_date >= '2025-01-01T00:00:00+00:00'"
    assert result.options["backfill"]["chunk"]["from"] == "2025-01-01"
    assert result.options["nested"][0]["date_from"] == "2025-01-01T00:00:00+00:00"
    assert result.options["interval"]["interval_end"] == "2025-01-02T00:00:00+00:00"


def test_interval_context_keeps_unknown_tokens_and_is_noop_when_empty() -> None:
    config = _config(options={"note": "{{ unknown_token }}"})

    untouched = IntervalContextService(RunInterval()).apply(config)
    assert "interval" not in untouched.options

    resolved = IntervalContextService(RunInterval(logical_date="2025-01-01T00:00:00")).apply(config)
    assert resolved.options["note"] == "{{ unknown_token }}"
