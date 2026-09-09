"""Airflow 2+ schedule and timezone materialization for declarative dag-specs.

Declarative catalogs carry ``timezone`` separately from cron strings. Airflow 3
rejects ``timezone`` on ``DAG()``, while Airflow 2.0–2.1 without timetables
still expects it there. This module centralizes the version-aware mapping:

* cron + timezone → ``CronTriggerTimetable`` when available (Airflow 2.2+ and 3.x)
* cron + timezone → ``DAG(timezone=...)`` only on legacy Airflow 2.x images
* ``start_date`` is always timezone-aware when a catalog timezone is present
* ``schedule`` vs ``schedule_interval`` is chosen from the installed ``DAG`` signature
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from datetime import datetime
from typing import Any


def materialize_dag_timing(
    spec: Mapping[str, Any],
    *,
    asset_schedule_class: Any | None,
    dag_class: Any,
) -> dict[str, Any]:
    """Build schedule/start_date/timezone kwargs for ``DAG()`` from a dag-spec."""

    timezone_name = _optional_timezone_name(spec.get("timezone"))
    schedule = spec.get("schedule")
    schedule_key = dag_schedule_parameter_name(dag_class)
    schedule_value = resolve_schedule_value(
        schedule,
        timezone_name=timezone_name,
        asset_schedule_class=asset_schedule_class,
        dag_class=dag_class,
        partition_plan=spec.get("partition_plan"),
    )
    timing: dict[str, Any] = {
        schedule_key: schedule_value,
        "start_date": parse_spec_start_date(spec.get("start_date"), timezone_name),
    }
    legacy_timezone = legacy_dag_timezone_value(
        schedule=schedule,
        timezone_name=timezone_name,
        schedule_value=schedule_value,
        dag_class=dag_class,
    )
    if legacy_timezone is not None:
        timing["timezone"] = legacy_timezone
    return timing


def resolve_schedule_value(
    schedule: Any,
    *,
    timezone_name: str | None,
    asset_schedule_class: Any | None,
    dag_class: Any,
    partition_plan: object = None,
) -> Any:
    """Resolve the DAG schedule argument from a dag-spec schedule block."""

    from dpone_airflow_pack.asset_partitions import materialize_partition_schedule

    assets = _asset_objects(schedule) if isinstance(schedule, Mapping) else []
    partitioned = materialize_partition_schedule(
        schedule=schedule,
        partition_plan=partition_plan,
        assets=assets,
    )
    if partitioned.handled:
        return partitioned.value
    if schedule is None:
        return None
    if isinstance(schedule, str):
        if timezone_name:
            timetable = build_cron_trigger_timetable(schedule, timezone_name)
            if timetable is not None:
                return timetable
            if dag_accepts_parameter(dag_class, "timezone"):
                return schedule
            msg = (
                f"Cannot apply timezone {timezone_name!r} to cron schedule on this Airflow "
                "installation; upgrade to Airflow 2.2+ (CronTriggerTimetable) or use Airflow 2.x "
                "with DAG timezone support."
            )
            raise ValueError(msg)
        return schedule
    if isinstance(schedule, Mapping) and isinstance(schedule.get("assets"), list):
        return _resolve_asset_schedule(schedule, asset_schedule_class)
    return schedule


def legacy_dag_timezone_value(
    *,
    schedule: Any,
    timezone_name: str | None,
    schedule_value: Any,
    dag_class: Any,
) -> Any | None:
    """Return a legacy ``DAG(timezone=...)`` value only when timetables are unavailable."""

    if not timezone_name or not isinstance(schedule, str):
        return None
    if is_cron_trigger_timetable(schedule_value):
        return None
    if not dag_accepts_parameter(dag_class, "timezone"):
        return None
    return resolve_timezone_object(timezone_name)


def parse_spec_start_date(raw: Any, timezone_name: str | None = None) -> datetime:
    """Parse dag-spec ``start_date`` and attach the catalog timezone when present."""

    text = str(raw or "2026-01-01")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime(2026, 1, 1)
    if not timezone_name:
        return parsed
    try:
        tz = resolve_timezone_object(timezone_name)
        import pendulum

        return pendulum.datetime(
            parsed.year,
            parsed.month,
            parsed.day,
            tz=tz,
        )
    except Exception:  # noqa: BLE001 - pendulum is optional outside Airflow images.
        try:
            return parsed.replace(tzinfo=resolve_timezone_object(timezone_name))
        except Exception:
            return parsed


def dag_schedule_parameter_name(dag_class: Any) -> str:
    """Return ``schedule`` or legacy ``schedule_interval`` based on the installed DAG API."""

    if dag_accepts_parameter(dag_class, "schedule"):
        return "schedule"
    if dag_accepts_parameter(dag_class, "schedule_interval"):
        return "schedule_interval"
    # Airflow 2.4+ / 3.x images expose ``schedule``; default when wrappers hide params.
    return "schedule"


def dag_accepts_parameter(dag_class: Any, parameter_name: str) -> bool:
    try:
        return parameter_name in inspect.signature(dag_class.__init__).parameters
    except (TypeError, ValueError):
        return False


def build_cron_trigger_timetable(cron: str, timezone_name: str) -> Any | None:
    try:
        from airflow.timetables.trigger import CronTriggerTimetable
    except ImportError:
        return None

    timezone_value = resolve_timezone_object(timezone_name)
    try:
        return CronTriggerTimetable(cron, timezone=timezone_value)
    except Exception:  # noqa: BLE001 - fall back to legacy DAG timezone path when possible.
        try:
            return CronTriggerTimetable(cron, timezone=str(timezone_name))
        except Exception:
            return None


def resolve_timezone_object(timezone_name: str) -> Any:
    import pendulum

    return pendulum.timezone(str(timezone_name))


def is_cron_trigger_timetable(value: Any) -> bool:
    if value is None:
        return False
    class_name = value.__class__.__name__
    module_name = getattr(value.__class__, "__module__", "")
    return class_name.endswith("CronTriggerTimetable") or module_name.endswith(".timetables.trigger")


def _resolve_asset_schedule(schedule: Mapping[str, Any], asset_schedule_class: Any | None) -> Any:
    assets = _asset_objects(schedule)
    if asset_schedule_class is not None and assets:
        return asset_schedule_class(assets)
    return assets or None


def _asset_objects(schedule: Mapping[str, Any]) -> list[Any]:
    from dpone_airflow_pack.asset_outlets import build_asset_outlets

    raw_assets = schedule.get("assets")
    if not isinstance(raw_assets, list):
        return []
    assets: list[Any] = []
    for item in raw_assets:
        if isinstance(item, str):
            assets.extend(build_asset_outlets([item]))
        elif isinstance(item, Mapping) and item.get("uri"):
            assets.extend(build_asset_outlets([str(item["uri"])]))
    return assets


def _optional_timezone_name(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


__all__ = [
    "build_cron_trigger_timetable",
    "dag_accepts_parameter",
    "dag_schedule_parameter_name",
    "is_cron_trigger_timetable",
    "legacy_dag_timezone_value",
    "materialize_dag_timing",
    "parse_spec_start_date",
    "resolve_schedule_value",
    "resolve_timezone_object",
]
