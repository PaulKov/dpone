from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dpone._compat import UTC
from dpone.runtime.connectors.api.openexchangerates import (
    DEFAULT_SYMBOLS,
    normalize_openexchangerates_symbols,
    resolve_openexchangerates_date_option,
)
from dpone.runtime.connectors.api.openexchangerates_resources import OPENEXCHANGERATES_HISTORICAL_RATES_SCHEMA
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


class OpenExchangeRatesIncrementalMergeExtractStrategy(APIBaseStrategy):
    """INCREMENTAL_MERGE extraction for OpenExchangeRates."""

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        max_value = self._get_max_from_sink(load_config, "as_of_date")
        if max_value is None:
            return None
        resolved = resolve_openexchangerates_date_option(max_value)
        if resolved is None:
            return None
        return {"last_value": resolved.isoformat()}

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        options = self._get_options(load_config)
        resource = str(options.get("resource", getattr(load_config, "source_table", "historical_rates_daily"))).strip()
        if resource != "historical_rates_daily":
            raise ValueError("OpenExchangeRates incremental_merge supports only resource='historical_rates_daily'")

        explicit_start = resolve_openexchangerates_date_option(options.get("start_date"))
        explicit_end = resolve_openexchangerates_date_option(options.get("end_date"))
        single_day = resolve_openexchangerates_date_option(options.get("day"))
        lookback_days = int(options.get("lookback_days", 3) or 3)
        symbols = normalize_openexchangerates_symbols(options.get("symbols", DEFAULT_SYMBOLS))
        base_currency = str(options.get("base_currency", "")).strip().upper() or None
        yesterday = datetime.now(UTC).date() - timedelta(days=1)

        if explicit_start is not None:
            from_day = explicit_start
            to_day = explicit_end or yesterday
        elif single_day is not None:
            from_day = single_day
            to_day = single_day
        elif last_state and last_state.get("last_value"):
            last_loaded = resolve_openexchangerates_date_option(last_state["last_value"]) or yesterday
            from_day = last_loaded - timedelta(days=lookback_days)
            to_day = yesterday
        else:
            from_day = yesterday - timedelta(days=max(lookback_days - 1, 0))
            to_day = yesterday

        if from_day > to_day:
            from_day = to_day

        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE",
            {
                "API": "openexchangerates",
                "Resource": resource,
                "From_Day": str(from_day),
                "To_Day": str(to_day),
                "Lookback_Days": lookback_days,
                "Symbols": ",".join(symbols),
                "Base_Currency": base_currency or "",
            },
        )

        rows = list(
            self.connector.get_resources(
                resource,
                filters={
                    "start_date": from_day,
                    "end_date": to_day,
                    "symbols": symbols,
                    "base_currency": base_currency,
                },
            )
        )
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=OPENEXCHANGERATES_HISTORICAL_RATES_SCHEMA,
            state={"last_value": to_day.isoformat()},
            force_full_refresh=False,
        )
