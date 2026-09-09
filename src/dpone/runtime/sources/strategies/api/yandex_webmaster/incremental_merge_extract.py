from __future__ import annotations

from datetime import timedelta
from typing import Any

from dpone.runtime.connectors.api.yandex_webmaster_resources import get_yandex_webmaster_resource
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.sources.strategies.api.yandex_webmaster.common import (
    build_yandex_webmaster_rows,
    parse_date_option,
    yesterday_msk,
)


class YandexWebmasterIncrementalMergeExtractStrategy(APIBaseStrategy):
    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        options = self._get_options(load_config)
        resource = str(options.get("resource") or load_config.source_table or "host_metrics_daily")
        spec = get_yandex_webmaster_resource(resource)
        if spec.family != "host_metrics":
            raise ValueError(
                f"Yandex Webmaster incremental_merge supports only host_metrics resources; got '{resource}'. "
                "Use strategy.mode=replace for query resources."
            )

        lookback_days = int(options.get("lookback_days", 3) or 3)
        explicit_start = parse_date_option(options.get("start_date"))
        explicit_end = parse_date_option(options.get("end_date"))
        initial_start_date = parse_date_option(options.get("initial_start_date", "2025-01-01"))

        to_day = yesterday_msk()
        if explicit_start is not None:
            from_day = explicit_start
            to_day = explicit_end or explicit_start
        else:
            if not last_state or not last_state.get("last_value"):
                from_day = initial_start_date or to_day
            else:
                last_loaded_day = parse_date_option(last_state["last_value"])
                from_day = (
                    (last_loaded_day - timedelta(days=lookback_days))
                    if last_loaded_day
                    else (initial_start_date or to_day)
                )

        if from_day > to_day:
            from_day = to_day

        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE",
            {
                "API": "yandex_webmaster",
                "Resource": resource,
                "From_Day": from_day.isoformat(),
                "To_Day": to_day.isoformat(),
                "Lookback_Days": lookback_days,
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
        )
        new_state = {"last_value": to_day.isoformat()}
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=spec.schema,
            state=new_state,
            force_full_refresh=False,
        )
