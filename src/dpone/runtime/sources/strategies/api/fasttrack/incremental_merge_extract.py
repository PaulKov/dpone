from __future__ import annotations

from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.fasttrack.base import FasttrackBaseExtractStrategy


class FasttrackIncrementalMergeExtractStrategy(FasttrackBaseExtractStrategy):
    """INCREMENTAL_MERGE extraction for Fasttrack.

    FT-2 preserves legacy semantics: the API data is fetched provider-shaped and
    merged in the sink by unique keys. No additional business normalization is
    applied in landing beyond datetime parsing.
    """

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        spec = self._resolve_resource_spec(load_config)
        params = self._resolve_extra_params(load_config, spec)
        self._log_extract_plan(event="FASTTRACK_INCREMENTAL_MERGE", load_config=load_config, spec=spec, params=params)
        rows_iter = self._iter_normalized_rows(load_config, spec)
        return self._build_extract_result(
            load_config=load_config,
            spec=spec,
            rows_iter=rows_iter,
            force_full_refresh=False,
            state=last_state,
        )
