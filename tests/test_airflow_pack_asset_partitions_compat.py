"""Exact Airflow matrix contract for native and degraded asset partitions."""

from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow", reason="Apache Airflow is required for compatibility tests")
try:
    from airflow import DAG
except ImportError:
    DAG = None  # type: ignore[assignment,misc]

pytestmark = pytest.mark.skipif(DAG is None, reason="A complete Apache Airflow installation is required")


def _airflow_version() -> tuple[int, int]:
    major, minor, *_ = (int(part) for part in airflow.__version__.split(".") if part.isdigit())
    return major, minor


def _partition_plan(mode: str) -> dict[str, str]:
    return {
        "mode": mode,
        "dimension": "business_date",
        "type": "temporal",
        "granularity": "day",
        "timezone": "UTC",
        "key_format": "%Y-%m-%d",
        "source": "dag_schedule",
        "mapper": "identity",
    }


def test_installed_airflow_materializes_or_explicitly_degrades_cron_partition() -> None:
    from dpone_airflow_pack.asset_partitions import detect_partition_capabilities
    from dpone_airflow_pack.dag_schedule import resolve_schedule_value

    capabilities = detect_partition_capabilities()
    schedule = resolve_schedule_value(
        "0 2 * * *",
        timezone_name="UTC",
        asset_schedule_class=None,
        dag_class=DAG,
        partition_plan=_partition_plan("cron_producer"),
    )

    native_expected = _airflow_version() >= (3, 2)
    assert capabilities.native is native_expected
    assert schedule.__class__.__name__ == ("CronPartitionTimetable" if native_expected else "CronTriggerTimetable")


def test_installed_airflow_materializes_or_explicitly_degrades_asset_partition() -> None:
    from dpone_airflow_pack.asset_partitions import detect_partition_capabilities
    from dpone_airflow_pack.dag_schedule import resolve_schedule_value

    capabilities = detect_partition_capabilities()
    schedule = resolve_schedule_value(
        {"assets": [{"uri": "dpone://clickhouse/analytics/orders"}]},
        timezone_name="UTC",
        asset_schedule_class=None,
        dag_class=DAG,
        partition_plan=_partition_plan("asset_consumer"),
    )

    if _airflow_version() >= (3, 2):
        assert capabilities.native is True
        assert schedule.__class__.__name__ == "PartitionedAssetTimetable"
    else:
        assert capabilities.native is False
        assert isinstance(schedule, list)
        assert len(schedule) == 1
