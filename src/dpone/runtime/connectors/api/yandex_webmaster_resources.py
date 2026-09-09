from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

YandexWebmasterStrategyName = Literal["full_refresh", "incremental_merge", "replace"]
YandexWebmasterResourceFamily = Literal["host_metrics", "search_queries_history", "query_analytics_by_region"]


@dataclass(frozen=True)
class YandexWebmasterResourceSpec:
    """Typed description of a supported Yandex Webmaster logical resource."""

    name: str
    family: YandexWebmasterResourceFamily
    schema: tuple[tuple[str, str], ...]
    table_prefix: str = "app"
    partition_column: str = "date"
    default_load_strategy: YandexWebmasterStrategyName = "incremental_merge"
    unique_key: tuple[str, ...] = ("date",)
    description: str = ""
    default_lookback_days: int | None = None

    @property
    def table_name(self) -> str:
        return yandex_webmaster_table_name(self.name, prefix=self.table_prefix)


YANDEX_WEBMASTER_HOST_METRICS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("date", "DATE"),
    ("http_2xx", "INT64"),
    ("http_3xx", "INT64"),
    ("http_4xx", "INT64"),
    ("pages_in_search", "INT64"),
    ("appeared_in_search", "INT64"),
    ("removed_from_search", "INT64"),
)

YANDEX_WEBMASTER_SEARCH_QUERIES_HISTORY_SCHEMA: tuple[tuple[str, str], ...] = (
    ("date", "DATE"),
    ("query_id", "STRING"),
    ("query_text", "STRING"),
    ("device_type", "STRING"),
    ("total_shows", "INT64"),
    ("total_clicks", "INT64"),
    ("avg_show_position", "FLOAT64"),
    ("avg_click_position", "FLOAT64"),
)

YANDEX_WEBMASTER_QUERY_ANALYTICS_BY_REGION_SCHEMA: tuple[tuple[str, str], ...] = (
    ("date", "DATE"),
    ("region_id", "INT64"),
    ("region_name", "STRING"),
    ("query", "STRING"),
    ("url", "STRING"),
    ("impressions", "INT64"),
    ("clicks", "INT64"),
    ("ctr", "FLOAT64"),
)


_YANDEX_WEBMASTER_RESOURCES: dict[str, YandexWebmasterResourceSpec] = {
    "host_metrics_daily": YandexWebmasterResourceSpec(
        name="host_metrics_daily",
        family="host_metrics",
        schema=YANDEX_WEBMASTER_HOST_METRICS_SCHEMA,
        description="Merged daily host metrics built from indexing and search history endpoints.",
    ),
    "search_queries_history_daily": YandexWebmasterResourceSpec(
        name="search_queries_history_daily",
        family="search_queries_history",
        schema=YANDEX_WEBMASTER_SEARCH_QUERIES_HISTORY_SCHEMA,
        default_load_strategy="replace",
        unique_key=("date", "query_id", "device_type"),
        description="Daily search query statistics from popular queries + per-query history endpoints.",
        default_lookback_days=14,
    ),
    "query_analytics_by_region_daily": YandexWebmasterResourceSpec(
        name="query_analytics_by_region_daily",
        family="query_analytics_by_region",
        schema=YANDEX_WEBMASTER_QUERY_ANALYTICS_BY_REGION_SCHEMA,
        default_load_strategy="replace",
        unique_key=("date", "region_id", "query", "url"),
        description="Daily search query analytics in fixed Yandex regions.",
        default_lookback_days=14,
    ),
}


def list_yandex_webmaster_resources() -> list[YandexWebmasterResourceSpec]:
    return list(_YANDEX_WEBMASTER_RESOURCES.values())


def get_yandex_webmaster_resource(name: str) -> YandexWebmasterResourceSpec:
    key = str(name).strip().lower()
    try:
        return _YANDEX_WEBMASTER_RESOURCES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_YANDEX_WEBMASTER_RESOURCES))
        raise KeyError(f"Unknown Yandex Webmaster resource '{name}'. Supported: {supported}") from exc


def yandex_webmaster_table_name(resource_name: str, *, prefix: str = "app") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "YANDEX_WEBMASTER_HOST_METRICS_SCHEMA",
    "YANDEX_WEBMASTER_QUERY_ANALYTICS_BY_REGION_SCHEMA",
    "YANDEX_WEBMASTER_SEARCH_QUERIES_HISTORY_SCHEMA",
    "YandexWebmasterResourceFamily",
    "YandexWebmasterResourceSpec",
    "YandexWebmasterStrategyName",
    "get_yandex_webmaster_resource",
    "list_yandex_webmaster_resources",
    "yandex_webmaster_table_name",
]
