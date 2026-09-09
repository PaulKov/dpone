from __future__ import annotations

from typing import Any

from dpone.runtime.connectors.api.yandex_webmaster_resources import get_yandex_webmaster_resource
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.sources.strategies.api.yandex_webmaster.common import (
    build_yandex_webmaster_rows,
    parse_device_types,
    resolve_window,
)


class YandexWebmasterFullExtractStrategy(APIBaseStrategy):
    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        del last_state
        options = self._get_options(load_config)
        resource = str(options.get("resource") or load_config.source_table or "host_metrics_daily")
        spec = get_yandex_webmaster_resource(resource)
        default_days = spec.default_lookback_days or 1
        from_day, to_day = resolve_window(options=options, default_days=default_days)

        self.logger.log_etl_progress(
            "API_FULL_EXTRACT",
            {
                "API": "yandex_webmaster",
                "Resource": resource,
                "From_Day": from_day.isoformat(),
                "To_Day": to_day.isoformat(),
            },
        )

        rows = build_yandex_webmaster_rows(
            connector=self.connector,
            resource=resource,
            user_id=options.get("user_id"),
            host_id=options.get("host_id"),
            host_url=options.get("host_url"),
            date_from=from_day,
            date_to=to_day,
            pages_in_search_daily_agg=str(options.get("pages_in_search_daily_agg", "last")),
            device_types=parse_device_types(options.get("device_types")),
            region_ids=options.get("region_ids"),
            popular_limit=int(options.get("popular_limit", 500)),
            popular_max_queries=(
                int(options["popular_max_queries"]) if options.get("popular_max_queries") not in (None, "") else None
            ),
            query_analytics_limit=int(options.get("query_analytics_limit", 500)),
            query_analytics_max_queries=(
                int(options["query_analytics_max_queries"])
                if options.get("query_analytics_max_queries") not in (None, "")
                else None
            ),
            query_analytics_device_type_indicator=str(
                options.get("query_analytics_device_type_indicator", options.get("device_type_indicator", "ALL"))
            ),
            query_analytics_search_location=str(options.get("query_analytics_search_location", "WEB_LOCATION")),
            query_analytics_text_indicator=str(options.get("query_analytics_text_indicator", "QUERY")),
        )

        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=spec.schema,
            state=None,
            force_full_refresh=True,
        )
