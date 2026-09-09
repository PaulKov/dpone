from __future__ import annotations

from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.appsflyer.common import AppsflyerBaseExtractStrategy


class AppsflyerIncrementalAppendExtractStrategy(AppsflyerBaseExtractStrategy):
    """Daily lookback refresh for AppsFlyer landing tables."""

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return super().get_state(self._get_state_load_config(load_config))

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        window = self._build_window(load_config, last_state, full_refresh=False)
        rows_iter = self._iter_window_rows(window)
        state = {"last_value": window.end_date.isoformat(), "column": "date"}
        return self._build_extract_result(
            load_config=load_config,
            rows_iter=rows_iter,
            window=window,
            state=state,
            include_partitions=True,
        )
