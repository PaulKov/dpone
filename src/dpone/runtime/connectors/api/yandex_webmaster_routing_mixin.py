"""Resource routing for Yandex Webmaster connector."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


class YandexWebmasterRoutingMixin:
    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        params = dict(filters or {})
        params.update(kwargs)

        if resource_type == "user":
            yield self.get("/user")
            return

        resolved_user_id = self.resolve_user_id(user_id=_optional_int(params.get("user_id")))

        if resource_type == "hosts":
            yield from self.list_hosts(user_id=resolved_user_id)
            return

        resolved_host_id = self.resolve_host_id(
            user_id=resolved_user_id,
            host_id=_optional_str(params.get("host_id")),
            host_url=_optional_str(params.get("host_url")),
        )
        date_from = str(params.get("date_from"))
        date_to = str(params.get("date_to"))

        if resource_type == "indexing_history":
            yield self.get_indexing_history(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                date_from=date_from,
                date_to=date_to,
            )
            return
        if resource_type == "in_search_history":
            yield self.get_in_search_history(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                date_from=date_from,
                date_to=date_to,
            )
            return
        if resource_type == "search_events_history":
            yield self.get_search_events_history(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                date_from=date_from,
                date_to=date_to,
            )
            return
        if resource_type == "search_queries_popular":
            yield self.get_search_queries_popular(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                date_from=date_from,
                date_to=date_to,
                order_by=str(params.get("order_by") or "TOTAL_SHOWS"),
                limit=int(params.get("limit", 500)),
                offset=int(params.get("offset", 0)),
            )
            return
        if resource_type == "search_query_history":
            query_id = _optional_str(params.get("query_id"))
            if not query_id:
                raise ValueError("Yandex Webmaster raw resource 'search_query_history' requires query_id")
            yield self.get_search_query_history(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                query_id=query_id,
                date_from=date_from,
                date_to=date_to,
                device_type=str(params.get("device_type") or params.get("device_type_indicator") or "ALL"),
            )
            return
        if resource_type == "query_analytics_list":
            region_ids_raw = params.get("region_ids")
            if region_ids_raw is None:
                raise ValueError("Yandex Webmaster raw resource 'query_analytics_list' requires region_ids")
            if isinstance(region_ids_raw, str):
                region_ids = [int(item.strip()) for item in region_ids_raw.split(",") if item.strip()]
            else:
                region_ids = [int(item) for item in region_ids_raw]
            yield self.post_query_analytics_list(
                user_id=resolved_user_id,
                host_id=resolved_host_id,
                region_ids=region_ids,
                limit=int(params.get("limit", 500)),
                offset=int(params.get("offset", 0)),
                device_type_indicator=str(params.get("device_type_indicator") or params.get("device_type") or "ALL"),
                search_location=str(params.get("search_location") or "WEB_LOCATION"),
                text_indicator=str(params.get("text_indicator") or "QUERY"),
                order_by=str(params.get("order_by") or "TOTAL_SHOWS"),
            )
            return

        raise ValueError(
            f"Unsupported Yandex Webmaster resource_type='{resource_type}'. "
            "Supported: user, hosts, indexing_history, in_search_history, search_events_history, "
            "search_queries_popular, search_query_history, query_analytics_list"
        )


__all__ = ["YandexWebmasterRoutingMixin"]
