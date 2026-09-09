"""Generic backfill wrapper strategy.

The wrapper delegates one backfill payload (whole run or one orchestrated
chunk) to the configured inner load strategy. Chunk planning, resume and
parallelism live in :mod:`dpone.runtime.etl.backfill_orchestrator`; this
strategy stays a thin, sink-agnostic delegation point.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dpone.backfill.execution_policy import execution_policy_from_load_config
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.portable_scope_binding import PORTABLE_SCOPE_BINDING_OPTION
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.base import SinkStrategy


def backfill_inner_strategy(load_config: Any) -> LoadStrategy:
    """Resolve the validated inner load strategy for a backfill config."""

    return LoadStrategy(execution_policy_from_load_config(load_config).inner_mode)


class BackfillStrategy(SinkStrategy):
    """Delegates a backfill chunk to the configured inner load strategy."""

    def __init__(self, strategy_map: Mapping[LoadStrategy, SinkStrategy]) -> None:
        self.strategy_map = strategy_map

    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        execution_policy = execution_policy_from_load_config(load_config)
        raw_options = getattr(load_config, "options", {}) or {}
        raw_backfill = raw_options.get("backfill") or {}
        if execution_policy.chunk is not None and "chunk_context" not in raw_backfill:
            raise ValueError(
                "backfill.chunk can be delegated only with a runtime-issued chunk_context and operation scope"
            )
        inner_mode = LoadStrategy(execution_policy.inner_mode)
        strategy = self.strategy_map.get(inner_mode)
        if strategy is None:
            raise ValueError(f"Backfill inner strategy is not supported by this sink: {inner_mode.value}")
        options = dict(getattr(load_config, "options", {}) or {})
        options.pop("backfill", None)
        portable_scope = getattr(load_config, "portable_scope", None)
        if inner_mode is not LoadStrategy.REPLACE:
            portable_scope = None
            options.pop(PORTABLE_SCOPE_BINDING_OPTION, None)
        return strategy.load(
            replace(
                load_config,
                load_strategy=inner_mode,
                portable_scope=portable_scope,
                options=options,
            ),
            payload,
        )


__all__ = ["BackfillStrategy", "backfill_inner_strategy"]
