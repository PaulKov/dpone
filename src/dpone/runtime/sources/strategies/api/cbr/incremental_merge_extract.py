from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.support.cbr_xml_parser import deduplicate_rows, parse_xml_daily, resolve_date_option


class CbrIncrementalMergeExtractStrategy(APIBaseStrategy):
    """INCREMENTAL_MERGE extraction for CBR.

    State is tracked by the requested day, not by ``as_of_date`` returned in XML,
    because on weekends/holidays the API may return the previous business day.
    """

    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        options = self._get_options(load_config)
        resource = options.get("resource", "xml_daily_asp")
        if resource != "xml_daily_asp":
            raise ValueError(
                f"CBR incremental_merge does not support resource={resource!r}. Supported resource: 'xml_daily_asp'"
            )

        lookback_days = int(options.get("lookback_days", 3) or 3)
        full_refresh_days = int(options.get("full_refresh_days", 30) or 30)
        explicit_start = resolve_date_option(options.get("start_date"))
        explicit_end = resolve_date_option(options.get("end_date"))

        today = datetime.now(UTC).date()
        if explicit_start is not None:
            from_day = explicit_start
            to_day = explicit_end or today
            if from_day > to_day:
                raise ValueError(f"CBR incremental_merge: start_date ({from_day}) > end_date ({to_day})")
            self.logger.info("CBR explicit date range %s .. %s", from_day.isoformat(), to_day.isoformat())
        else:
            to_day = today
            if not last_state or not last_state.get("last_value"):
                from_day = today - timedelta(days=full_refresh_days)
                self.logger.info("CBR initial load: last %d days", full_refresh_days)
            else:
                try:
                    last_day = datetime.strptime(str(last_state["last_value"]), "%Y-%m-%d").date()
                except ValueError:
                    last_day = today - timedelta(days=full_refresh_days)
                from_day = last_day - timedelta(days=lookback_days)
            if from_day > to_day:
                from_day = to_day

        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE",
            {
                "API": "cbr",
                "Resource": resource,
                "From_Day": str(from_day),
                "To_Day": str(to_day),
                "Lookback_Days": lookback_days,
            },
        )

        rows: list[dict[str, Any]] = []
        new_state = last_state
        current_day = from_day
        while current_day <= to_day:
            rows.extend(parse_xml_daily(self.connector.get_xml_daily(day=current_day), loaded_at_utc=datetime.now(UTC)))
            new_state = {"last_value": current_day.isoformat()}
            current_day += timedelta(days=1)

        rows = deduplicate_rows(rows)
        if not rows:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=last_state,
                force_full_refresh=False,
            )

        schema = self._detect_schema_from_records(rows[:10])
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=new_state,
            force_full_refresh=False,
        )
