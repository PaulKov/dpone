from __future__ import annotations

from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.appsflyer.common import AppsflyerBaseExtractStrategy


class AppsflyerFullExtractStrategy(AppsflyerBaseExtractStrategy):
    """FULL_REFRESH extraction for AppsFlyer raw pull resources."""

    def get_state(self, load_config: Any) -> None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        window = self._build_window(load_config, last_state, full_refresh=True)
        rows_iter = self._iter_window_rows(window)
        return self._build_extract_result(
            load_config=load_config,
            rows_iter=rows_iter,
            window=window,
            state=None,
            include_partitions=False,
        )
