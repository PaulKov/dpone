from __future__ import annotations

from datetime import timedelta
from typing import Any

from dpone.runtime.connectors.api.mindbox_resources import get_mindbox_resource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.mindbox.base import MindboxBaseStrategy


class MindboxIncrementalMergeExtractStrategy(MindboxBaseStrategy):
    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        parsed, options = self._parse_common_options(load_config)
        resource = parsed["resource"]
        if not resource:
            raise ValueError("options.resource обязателен для MindboxIncrementalMergeExtractStrategy")
        spec = get_mindbox_resource(resource)
        raw_lookback = options.get("lookback_days", spec.default_lookback_days)
        since = parsed["since_datetime_utc"]
        till = parsed["till_datetime_utc"]
        if since is None and till is None and raw_lookback is not None and spec.window_mode != "none":
            lookback_days = int(raw_lookback)
            today = self._mindbox_today(parsed["utc_boundary_time"])
            since = today - timedelta(days=lookback_days + 1)
            till = today - timedelta(days=1)
        fields = {
            "Resource": resource,
            "Lookback_Days": raw_lookback if raw_lookback is not None else "N/A",
            **self._log_period(since, till, parsed["utc_boundary_time"], window_mode=spec.window_mode),
        }
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress("MINDBOX_INCREMENTAL_MERGE_EXTRACT", fields)
        else:
            self.logger.info("Mindbox incremental merge extract: %s", fields)
        filters = self._build_filters(
            parsed["poll_interval"],
            parsed["export_timeout"],
            parsed["utc_boundary_time"],
            since=since,
            till=till,
            csv_delimiter=parsed["csv_delimiter"],
        )
        return self._fetch_and_build_result(resource, filters, parsed["batch_size"])
