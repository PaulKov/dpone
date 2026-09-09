from __future__ import annotations

import sys
import types
from datetime import datetime as std_datetime

import pytest
from dpone_airflow_pack.dag_schedule import (
    dag_accepts_parameter,
    dag_schedule_parameter_name,
    is_cron_trigger_timetable,
    legacy_dag_timezone_value,
    materialize_dag_timing,
    resolve_schedule_value,
)


class _FakeDAG:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class _LegacyDAG:
    def __init__(
        self,
        *,
        dag_id: str,
        schedule_interval: object = None,
        timezone: object = None,
        start_date: object = None,
        **kwargs: object,
    ) -> None:
        self.kwargs = {
            "dag_id": dag_id,
            "schedule_interval": schedule_interval,
            "timezone": timezone,
            "start_date": start_date,
            **kwargs,
        }


class _ModernDAG:
    def __init__(
        self,
        *,
        dag_id: str,
        schedule: object = None,
        start_date: object = None,
        **kwargs: object,
    ) -> None:
        self.kwargs = {
            "dag_id": dag_id,
            "schedule": schedule,
            "start_date": start_date,
            **kwargs,
        }


class _CronTriggerTimetable:
    def __init__(self, cron: str, *, timezone: object) -> None:
        self.cron = cron
        self.timezone = timezone


def _install_fake_timetable(monkeypatch: pytest.MonkeyPatch) -> None:
    timetables = types.ModuleType("airflow.timetables")
    trigger = types.ModuleType("airflow.timetables.trigger")
    trigger.CronTriggerTimetable = _CronTriggerTimetable
    monkeypatch.setitem(sys.modules, "airflow.timetables", timetables)
    monkeypatch.setitem(sys.modules, "airflow.timetables.trigger", trigger)


def _install_fake_pendulum(monkeypatch: pytest.MonkeyPatch) -> None:
    pendulum = types.ModuleType("pendulum")

    class _Timezone:
        def __init__(self, name: str) -> None:
            self.name = name

    def timezone(name: str) -> _Timezone:
        return _Timezone(name)

    def datetime(year: int, month: int, day: int, *, tz: _Timezone) -> std_datetime:
        return std_datetime(year, month, day, tzinfo=tz)

    pendulum.timezone = timezone
    pendulum.datetime = datetime
    monkeypatch.setitem(sys.modules, "pendulum", pendulum)
    monkeypatch.setattr(
        "dpone_airflow_pack.dag_schedule.resolve_timezone_object",
        lambda name: timezone(str(name)),
    )


def test_materialize_dag_timing_uses_cron_timetable_on_airflow_3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_timetable(monkeypatch)

    timing = materialize_dag_timing(
        {
            "schedule": "0 7 * * *",
            "start_date": "2026-06-25",
            "timezone": "Europe/Moscow",
        },
        asset_schedule_class=None,
        dag_class=_ModernDAG,
    )

    assert "schedule" in timing
    assert "timezone" not in timing
    assert is_cron_trigger_timetable(timing["schedule"])
    assert timing["schedule"].cron == "0 7 * * *"
    assert getattr(timing["schedule"].timezone, "name", timing["schedule"].timezone) == "Europe/Moscow"
    assert timing["start_date"].tzinfo is not None


def test_materialize_dag_timing_uses_legacy_dag_timezone_on_airflow_2_0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_pendulum(monkeypatch)
    monkeypatch.setattr(
        "dpone_airflow_pack.dag_schedule.build_cron_trigger_timetable",
        lambda cron, timezone_name: None,
    )

    timing = materialize_dag_timing(
        {
            "schedule": "0 7 * * *",
            "start_date": "2026-06-25",
            "timezone": "Europe/Moscow",
        },
        asset_schedule_class=None,
        dag_class=_LegacyDAG,
    )

    assert timing["schedule_interval"] == "0 7 * * *"
    assert getattr(timing["timezone"], "name", timing["timezone"]) == "Europe/Moscow"


def test_materialize_dag_timing_keeps_manual_dags_without_timezone() -> None:
    timing = materialize_dag_timing(
        {"schedule": None, "start_date": "2026-07-07", "timezone": None},
        asset_schedule_class=None,
        dag_class=_ModernDAG,
    )

    assert timing["schedule"] is None
    assert timing["start_date"] == std_datetime(2026, 7, 7)
    assert "timezone" not in timing


def test_resolve_schedule_value_fails_closed_on_airflow_3_without_timetable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dpone_airflow_pack.dag_schedule.build_cron_trigger_timetable",
        lambda cron, timezone_name: None,
    )
    with pytest.raises(ValueError, match="Cannot apply timezone"):
        resolve_schedule_value(
            "0 7 * * *",
            timezone_name="Europe/Moscow",
            asset_schedule_class=None,
            dag_class=_ModernDAG,
        )


def test_dag_schedule_parameter_name_prefers_schedule_when_available() -> None:
    assert dag_schedule_parameter_name(_ModernDAG) == "schedule"
    assert dag_schedule_parameter_name(_LegacyDAG) == "schedule_interval"


def test_legacy_dag_timezone_value_is_only_used_for_plain_cron() -> None:
    timetable = _CronTriggerTimetable("0 7 * * *", timezone="Europe/Moscow")

    assert (
        legacy_dag_timezone_value(
            schedule="0 7 * * *",
            timezone_name="Europe/Moscow",
            schedule_value=timetable,
            dag_class=_LegacyDAG,
        )
        is None
    )
    assert dag_accepts_parameter(_LegacyDAG, "timezone") is True
    assert dag_accepts_parameter(_ModernDAG, "timezone") is False
