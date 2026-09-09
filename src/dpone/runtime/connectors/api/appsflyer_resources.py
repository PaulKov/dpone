from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppsflyerResourceSpec:
    """Описание ресурса AppsFlyer Pull API."""

    name: str
    endpoint_path: str
    default_lookback_days: int
    freshness: str
    partition_column: str = "date"
    recommended_strategy: str = "incremental_append"
    endpoint_base_path: str = "/api/raw-data/export/app"
    supports_maximum_rows: bool = True
    supports_timezone: bool = True
    uses_date_only_window: bool = False


_APPSFLYER_RESOURCES: dict[str, AppsflyerResourceSpec] = {
    "installs_report": AppsflyerResourceSpec(
        name="installs_report",
        endpoint_path="installs_report/v5",
        default_lookback_days=10,
        freshness="real_time",
    ),
    "in_app_events_report": AppsflyerResourceSpec(
        name="in_app_events_report",
        endpoint_path="in_app_events_report/v5",
        default_lookback_days=10,
        freshness="real_time",
    ),
    "uninstall_events_report": AppsflyerResourceSpec(
        name="uninstall_events_report",
        endpoint_path="uninstall_events_report/v5",
        default_lookback_days=14,
        freshness="daily",
    ),
    "installs_retarget": AppsflyerResourceSpec(
        name="installs_retarget",
        endpoint_path="installs-retarget/v5",
        default_lookback_days=10,
        freshness="real_time",
    ),
    "in_app_events_retarget": AppsflyerResourceSpec(
        name="in_app_events_retarget",
        endpoint_path="in-app-events-retarget/v5",
        default_lookback_days=10,
        freshness="real_time",
    ),
    "organic_installs_report": AppsflyerResourceSpec(
        name="organic_installs_report",
        endpoint_path="organic_installs_report/v5",
        default_lookback_days=10,
        freshness="continuous",
    ),
    "organic_in_app_events_report": AppsflyerResourceSpec(
        name="organic_in_app_events_report",
        endpoint_path="organic_in_app_events_report/v5",
        default_lookback_days=10,
        freshness="continuous",
    ),
    "organic_uninstall_events_report": AppsflyerResourceSpec(
        name="organic_uninstall_events_report",
        endpoint_path="organic_uninstall_events_report/v5",
        default_lookback_days=14,
        freshness="daily",
    ),
    "daily_report": AppsflyerResourceSpec(
        name="daily_report",
        endpoint_path="daily_report/v5",
        default_lookback_days=14,
        freshness="daily",
        endpoint_base_path="/api/agg-data/export/app",
        supports_maximum_rows=False,
        supports_timezone=False,
        uses_date_only_window=True,
    ),
}


def list_appsflyer_resources() -> list[AppsflyerResourceSpec]:
    """Возвращает ресурсы AppsFlyer в стабильном порядке."""

    return list(_APPSFLYER_RESOURCES.values())


def get_appsflyer_resource(name: str) -> AppsflyerResourceSpec:
    """Возвращает описание ресурса по логическому имени."""

    try:
        return _APPSFLYER_RESOURCES[name]
    except KeyError as exc:
        supported = ", ".join(sorted(_APPSFLYER_RESOURCES))
        raise KeyError(f"Неизвестный ресурс AppsFlyer '{name}'. Поддерживаемые: {supported}") from exc


def appsflyer_table_name(resource_name: str, prefix: str = "app") -> str:
    """Возвращает целевое имя landing-таблицы для ресурса."""

    return f"{prefix}__{resource_name}"


__all__ = [
    "AppsflyerResourceSpec",
    "list_appsflyer_resources",
    "get_appsflyer_resource",
    "appsflyer_table_name",
]
