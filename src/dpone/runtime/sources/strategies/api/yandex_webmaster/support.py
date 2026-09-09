from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS_BY_MODULE: dict[str, tuple[str, ...]] = {
    "dpone.runtime.connectors.api.yandex_webmaster_resources": ("YANDEX_WEBMASTER_HOST_METRICS_SCHEMA",),
    "dpone.runtime.sources.strategies.api.yandex_webmaster.metrics": (
        "aggregate_in_search_history",
        "aggregate_indexing_history",
        "aggregate_search_events",
        "ensure_host_metrics_daily_resource",
        "merge_daily_metrics",
        "resolve_yandex_webmaster_resource",
        "validate_unique_dates",
        "validate_unique_keys",
    ),
    "dpone.runtime.sources.strategies.api.yandex_webmaster.dates": (
        "MOSCOW_TZ",
        "parse_yandex_datetime",
        "to_msk_date",
        "yesterday_msk",
    ),
    "dpone.runtime.sources.strategies.api.yandex_webmaster.options": (
        "_coerce_float",
        "_coerce_int",
        "_within_window",
        "build_date_replace_predicate",
        "parse_csv_list",
        "parse_date_option",
        "parse_device_types",
        "parse_int_list_option",
        "resolve_regions",
        "resolve_window",
    ),
    "dpone.runtime.sources.strategies.api.yandex_webmaster.regions": (
        "YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES",
        "YANDEX_WEBMASTER_DEFAULT_REGIONS",
        "YANDEX_WEBMASTER_REGION_NAME_BY_ID",
        "YandexWebmasterRegion",
    ),
    "dpone.runtime.sources.strategies.api.yandex_webmaster.row_builders": (
        "build_query_analytics_by_region_rows",
        "build_search_queries_history_rows",
        "build_yandex_webmaster_host_metrics_rows",
        "build_yandex_webmaster_rows",
        "build_ywm_rows",
    ),
}

_ALIASES = {
    "YANDEX_WEBMASTER_SCHEMA": (
        "dpone.runtime.connectors.api.yandex_webmaster_resources",
        "YANDEX_WEBMASTER_HOST_METRICS_SCHEMA",
    ),
    "YWM_SCHEMA": ("dpone.runtime.connectors.api.yandex_webmaster_resources", "YANDEX_WEBMASTER_HOST_METRICS_SCHEMA"),
}

_PUBLIC_EXPORTS = (
    "MOSCOW_TZ",
    "YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES",
    "YANDEX_WEBMASTER_DEFAULT_REGIONS",
    "YANDEX_WEBMASTER_REGION_NAME_BY_ID",
    "YANDEX_WEBMASTER_SCHEMA",
    "YWM_SCHEMA",
    "YandexWebmasterRegion",
    "parse_date_option",
    "yesterday_msk",
    "parse_csv_list",
    "parse_device_types",
    "parse_int_list_option",
    "parse_yandex_datetime",
    "to_msk_date",
    "aggregate_indexing_history",
    "aggregate_in_search_history",
    "aggregate_search_events",
    "validate_unique_dates",
    "validate_unique_keys",
    "merge_daily_metrics",
    "ensure_host_metrics_daily_resource",
    "resolve_yandex_webmaster_resource",
    "resolve_window",
    "build_date_replace_predicate",
    "resolve_regions",
    "_within_window",
    "_coerce_float",
    "_coerce_int",
    "build_yandex_webmaster_host_metrics_rows",
    "build_search_queries_history_rows",
    "build_query_analytics_by_region_rows",
    "build_yandex_webmaster_rows",
    "build_ywm_rows",
)

_MODULE_BY_EXPORT = {
    export_name: module_name for module_name, export_names in _EXPORTS_BY_MODULE.items() for export_name in export_names
}

__all__ = _PUBLIC_EXPORTS


def __getattr__(name: str) -> Any:
    alias = _ALIASES.get(name)
    if alias is not None:
        module_name, export_name = alias
        value = getattr(import_module(module_name), export_name)
        globals()[name] = value
        return value
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(f"{__name__!s} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
