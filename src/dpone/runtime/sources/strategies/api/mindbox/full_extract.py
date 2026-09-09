from __future__ import annotations

from datetime import timedelta
from typing import Any

from dpone.runtime.connectors.api.mindbox_resources import get_mindbox_resource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.mindbox.base import MindboxBaseStrategy


class MindboxFullExtractStrategy(MindboxBaseStrategy):
    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        parsed, options = self._parse_common_options(load_config)
        resource = parsed["resource"] or "getmessagingreport"
        spec = get_mindbox_resource(resource)
        since = parsed["since_datetime_utc"]
        till = parsed["till_datetime_utc"]
        full_refresh_days = options.get("full_refresh_days")
        if since is None and till is None and full_refresh_days:
            today = self._mindbox_today(parsed["utc_boundary_time"])
            till = today - timedelta(days=1)
            since = today - timedelta(days=int(full_refresh_days) + 1)
        fields = {
            "Resource": resource,
            **self._log_period(since, till, parsed["utc_boundary_time"], window_mode=spec.window_mode),
        }
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress("MINDBOX_FULL_EXTRACT", fields)
        else:
            self.logger.info("Mindbox full extract: %s", fields)
        filters = self._build_filters(
            parsed["poll_interval"],
            parsed["export_timeout"],
            parsed["utc_boundary_time"],
            since=since,
            till=till,
            csv_delimiter=parsed["csv_delimiter"],
        )
        return self._fetch_and_build_result(resource, filters, parsed["batch_size"], force_full_refresh=True)
