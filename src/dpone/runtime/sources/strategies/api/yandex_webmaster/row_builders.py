from __future__ import annotations

from datetime import date
from typing import Any

from dpone.runtime.sources.strategies.api.yandex_webmaster.metrics import (
    aggregate_in_search_history,
    aggregate_indexing_history,
    aggregate_search_events,
    merge_daily_metrics,
    resolve_yandex_webmaster_resource,
    validate_unique_keys,
)
from dpone.runtime.sources.strategies.api.yandex_webmaster.options import (
    YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES,
    _coerce_float,
    _coerce_int,
    _within_window,
    parse_date_option,
    resolve_regions,
    to_msk_date,
)


def build_yandex_webmaster_host_metrics_rows(
    *,
    connector,
    user_id: int | None,
    host_id: str | None,
    host_url: str | None,
    date_from: date,
    date_to: date,
    pages_in_search_daily_agg: str = "last",
) -> list[dict[str, Any]]:
    resolved_user_id = connector.resolve_user_id(user_id=user_id)
    resolved_host_id = connector.resolve_host_id(
        user_id=resolved_user_id,
        host_id=host_id,
        host_url=host_url,
    )
    date_from_str = date_from.isoformat()
    date_to_str = date_to.isoformat()

    indexing_raw = connector.get_indexing_history(
        user_id=resolved_user_id,
        host_id=resolved_host_id,
        date_from=date_from_str,
        date_to=date_to_str,
    )
    in_search_raw = connector.get_in_search_history(
        user_id=resolved_user_id,
        host_id=resolved_host_id,
        date_from=date_from_str,
        date_to=date_to_str,
    )
    events_raw = connector.get_search_events_history(
        user_id=resolved_user_id,
        host_id=resolved_host_id,
        date_from=date_from_str,
        date_to=date_to_str,
    )

    indexing = aggregate_indexing_history(indexing_raw)
    in_search = aggregate_in_search_history(
        in_search_raw,
        daily_agg=pages_in_search_daily_agg,
    )
    events = aggregate_search_events(events_raw)

    return merge_daily_metrics(indexing=indexing, in_search=in_search, events=events)


def build_search_queries_history_rows(
    *,
    connector,
    user_id: int | None,
    host_id: str | None,
    host_url: str | None,
    date_from: date,
    date_to: date,
    device_types: tuple[str, ...] = YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES,
    popular_limit: int = 500,
    popular_max_queries: int | None = None,
    order_by: str = "TOTAL_SHOWS",
) -> list[dict[str, Any]]:
    resolved_user_id = connector.resolve_user_id(user_id=user_id)
    resolved_host_id = connector.resolve_host_id(user_id=resolved_user_id, host_id=host_id, host_url=host_url)
    date_from_str = date_from.isoformat()
    date_to_str = date_to.isoformat()
    rows: list[dict[str, Any]] = []

    for query in connector.iter_search_queries_popular(
        user_id=resolved_user_id,
        host_id=resolved_host_id,
        date_from=date_from_str,
        date_to=date_to_str,
        order_by=order_by,
        limit=popular_limit,
        max_queries=popular_max_queries,
    ):
        query_id = str(query.get("query_id") or "")
        query_text = str(query.get("query_text") or "")
        if not query_id:
            continue

        for device_type in device_types:
            payload = connector.get_search_query_history(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                query_id=query_id,
                date_from=date_from_str,
                date_to=date_to_str,
                device_type=device_type,
            )
            per_day: dict[date, dict[str, Any]] = {}
            indicators = payload.get("indicators", {}) or {}
            metric_map = {
                "TOTAL_SHOWS": ("total_shows", _coerce_int),
                "TOTAL_CLICKS": ("total_clicks", _coerce_int),
                "AVG_SHOW_POSITION": ("avg_show_position", _coerce_float),
                "AVG_CLICK_POSITION": ("avg_click_position", _coerce_float),
            }

            for source_metric, (target_field, caster) in metric_map.items():
                for item in indicators.get(source_metric, []) or []:
                    day = to_msk_date(str(item["date"]))
                    if not _within_window(day, date_from=date_from, date_to=date_to):
                        continue
                    bucket = per_day.setdefault(
                        day,
                        {
                            "date": day.isoformat(),
                            "query_id": query_id,
                            "query_text": query_text,
                            "device_type": device_type,
                            "total_shows": 0,
                            "total_clicks": 0,
                            "avg_show_position": None,
                            "avg_click_position": None,
                        },
                    )
                    bucket[target_field] = caster(item.get("value"))

            rows.extend(per_day[day] for day in sorted(per_day))

    rows.sort(key=lambda item: (item["date"], item["query_id"], item["device_type"]))
    validate_unique_keys(rows, key_columns=("date", "query_id", "device_type"))
    return rows


def build_query_analytics_by_region_rows(
    *,
    connector,
    user_id: int | None,
    host_id: str | None,
    host_url: str | None,
    date_from: date,
    date_to: date,
    region_ids: Any = None,
    limit: int = 500,
    max_queries: int | None = None,
    device_type_indicator: str = "ALL",
    search_location: str = "WEB_LOCATION",
    text_indicator: str = "QUERY",
    order_by: str = "TOTAL_SHOWS",
) -> list[dict[str, Any]]:
    resolved_user_id = connector.resolve_user_id(user_id=user_id)
    resolved_host_id = connector.resolve_host_id(user_id=resolved_user_id, host_id=host_id, host_url=host_url)
    regions = resolve_regions(region_ids)
    rows: list[dict[str, Any]] = []

    for region in regions:
        for item in connector.iter_query_analytics_list(
            user_id=resolved_user_id,
            host_id=resolved_host_id,
            region_ids=(region.region_id,),
            limit=limit,
            max_queries=max_queries,
            device_type_indicator=device_type_indicator,
            search_location=search_location,
            text_indicator=text_indicator,
            order_by=order_by,
        ):
            query = str((item.get("text_indicator") or {}).get("value") or "")
            url = str((item.get("popular_complementary_indicator") or {}).get("value") or "")
            per_day: dict[date, dict[str, Any]] = {}
            for stat in item.get("statistics", []) or []:
                day = parse_date_option(stat.get("date"))
                if day is None or not _within_window(day, date_from=date_from, date_to=date_to):
                    continue
                bucket = per_day.setdefault(
                    day,
                    {
                        "date": day.isoformat(),
                        "region_id": region.region_id,
                        "region_name": region.region_name,
                        "query": query,
                        "url": url,
                        "impressions": 0,
                        "clicks": 0,
                        "ctr": 0.0,
                    },
                )
                field = str(stat.get("field") or "")
                value = stat.get("value")
                if field == "IMPRESSIONS":
                    bucket["impressions"] = _coerce_int(value) or 0
                elif field == "CLICKS":
                    bucket["clicks"] = _coerce_int(value) or 0
                elif field == "CTR":
                    bucket["ctr"] = _coerce_float(value) or 0.0

            rows.extend(
                bucket
                for day, bucket in sorted(per_day.items())
                if any((bucket["impressions"], bucket["clicks"], bucket["ctr"]))
            )

    rows.sort(key=lambda item: (item["date"], item["region_id"], item["query"], item["url"]))
    validate_unique_keys(rows, key_columns=("date", "region_id", "query", "url"))
    return rows


def build_yandex_webmaster_rows(
    *,
    connector,
    resource: str,
    user_id: int | None,
    host_id: str | None,
    host_url: str | None,
    date_from: date,
    date_to: date,
    pages_in_search_daily_agg: str = "last",
    device_types: tuple[str, ...] = YANDEX_WEBMASTER_DEFAULT_DEVICE_TYPES,
    region_ids: Any = None,
    popular_limit: int = 500,
    popular_max_queries: int | None = None,
    query_analytics_limit: int = 500,
    query_analytics_max_queries: int | None = None,
    query_analytics_device_type_indicator: str = "ALL",
    query_analytics_search_location: str = "WEB_LOCATION",
    query_analytics_text_indicator: str = "QUERY",
) -> list[dict[str, Any]]:
    spec = resolve_yandex_webmaster_resource(resource)
    if spec.family == "host_metrics":
        return build_yandex_webmaster_host_metrics_rows(
            connector=connector,
            user_id=user_id,
            host_id=host_id,
            host_url=host_url,
            date_from=date_from,
            date_to=date_to,
            pages_in_search_daily_agg=pages_in_search_daily_agg,
        )
    if spec.family == "search_queries_history":
        return build_search_queries_history_rows(
            connector=connector,
            user_id=user_id,
            host_id=host_id,
            host_url=host_url,
            date_from=date_from,
            date_to=date_to,
            device_types=device_types,
            popular_limit=popular_limit,
            popular_max_queries=popular_max_queries,
        )
    if spec.family == "query_analytics_by_region":
        return build_query_analytics_by_region_rows(
            connector=connector,
            user_id=user_id,
            host_id=host_id,
            host_url=host_url,
            date_from=date_from,
            date_to=date_to,
            region_ids=region_ids,
            limit=query_analytics_limit,
            max_queries=query_analytics_max_queries,
            device_type_indicator=query_analytics_device_type_indicator,
            search_location=query_analytics_search_location,
            text_indicator=query_analytics_text_indicator,
        )
    raise ValueError(f"Unsupported Yandex Webmaster resource family: {spec.family}")


# Backward-compatible aliases for older imports.
build_ywm_rows = build_yandex_webmaster_host_metrics_rows
