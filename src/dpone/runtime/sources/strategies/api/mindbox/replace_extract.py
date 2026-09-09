from __future__ import annotations

from datetime import timedelta
from typing import Any

from dpone.runtime.connectors.api.mindbox_resources import get_mindbox_resource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.mindbox.base import MindboxBaseStrategy


class MindboxReplaceExtractStrategy(MindboxBaseStrategy):
    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        parsed, options = self._parse_common_options(load_config)
        resource = parsed["resource"]
        if not resource:
            raise ValueError("options.resource обязателен для MindboxReplaceExtractStrategy")
        spec = get_mindbox_resource(resource)
        lookback_days = int(options.get("lookback_days", spec.default_lookback_days or 2))
        replace_column = options.get("replace_column", spec.default_replace_column)
        utc_boundary_time = parsed["utc_boundary_time"]
        since = parsed["since_datetime_utc"]
        till = parsed["till_datetime_utc"]
        if since is None and till is None:
            today = self._mindbox_today(utc_boundary_time)
            since = today - timedelta(days=lookback_days + 1)
            till = today - timedelta(days=1)
        delete_from_utc = self._format_sql_window_value(since, utc_boundary_time) if since is not None else None
        delete_to_utc = self._format_sql_window_value(till, utc_boundary_time) if till is not None else None
        if replace_column:
            predicates: list[str] = []
            if delete_from_utc is not None:
                predicates.append(f"{replace_column} >= TIMESTAMP '{delete_from_utc}'")
            if delete_to_utc is not None:
                predicates.append(f"{replace_column} < TIMESTAMP '{delete_to_utc}'")
            load_config.custom_predicate = " AND ".join(predicates)
        fields = {
            "Resource": resource,
            "Lookback_Days": lookback_days,
            **self._log_period(since, till, utc_boundary_time, window_mode=spec.window_mode),
            "Replace_Column": replace_column or "N/A",
            "Delete_From": delete_from_utc or "N/A",
            "Delete_To": delete_to_utc or "N/A",
        }
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress("MINDBOX_REPLACE_EXTRACT", fields)
        else:
            self.logger.info("Mindbox replace extract: %s", fields)
        filters = self._build_filters(
            parsed["poll_interval"],
            parsed["export_timeout"],
            utc_boundary_time,
            since=since,
            till=till,
            csv_delimiter=parsed["csv_delimiter"],
        )
        return self._fetch_and_build_result(resource, filters, parsed["batch_size"])
