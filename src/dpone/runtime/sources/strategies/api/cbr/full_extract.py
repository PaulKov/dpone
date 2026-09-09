from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.support.cbr_xml_parser import deduplicate_rows, parse_xml_daily, resolve_date_option


class CbrFullExtractStrategy(APIBaseStrategy):
    """FULL_REFRESH extraction for CBR daily FX rates."""

    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        options = self._get_options(load_config)
        resource = options.get("resource", "xml_daily_asp")
        if resource != "xml_daily_asp":
            raise ValueError(
                f"CBR full_extract does not support resource={resource!r}. Supported resource: 'xml_daily_asp'"
            )

        explicit_start = resolve_date_option(options.get("start_date"))
        explicit_end = resolve_date_option(options.get("end_date"))
        single_day = resolve_date_option(options.get("day"))

        rows: list[dict[str, Any]] = []
        loaded_at_utc = datetime.now(UTC)

        if explicit_start is not None:
            from_day = explicit_start
            to_day = explicit_end or loaded_at_utc.date()
            if from_day > to_day:
                raise ValueError(f"CBR full_extract: start_date ({from_day}) > end_date ({to_day})")
            self.logger.log_etl_progress(
                "API_FULL_EXTRACT",
                {
                    "API": "cbr",
                    "Resource": resource,
                    "Mode": "date_range",
                    "From_Day": str(from_day),
                    "To_Day": str(to_day),
                },
            )
            current_day = from_day
            while current_day <= to_day:
                rows.extend(parse_xml_daily(self.connector.get_xml_daily(day=current_day), loaded_at_utc=loaded_at_utc))
                current_day += timedelta(days=1)
            # Weekend and holiday requests can map to the same business-date XML snapshot.
            rows = deduplicate_rows(rows)
        else:
            self.logger.log_etl_progress(
                "API_FULL_EXTRACT",
                {
                    "API": "cbr",
                    "Resource": resource,
                    "Mode": "single_day",
                    "Day": str(single_day) if single_day else "current",
                },
            )
            rows = parse_xml_daily(self.connector.get_xml_daily(day=single_day), loaded_at_utc=loaded_at_utc)

        if not rows:
            return ExtractResult(artifact=InMemoryRowsArtifact([]), schema=[], state=None, force_full_refresh=True)

        schema = self._detect_schema_from_records(rows[:10])
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=None,
            force_full_refresh=True,
        )
