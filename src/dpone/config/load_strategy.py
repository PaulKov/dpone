"""Load strategy contract for dpone runtime and manifest compilation."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from enum import Enum

SOURCE_BYTE_BUDGET_OPTION = "__dpone_source_byte_budget_v1"
MAX_SOURCE_BYTE_BUDGET = 9_223_372_036_854_775_807


class LoadStrategy(Enum):
    """Supported data loading strategies."""

    FULL_REFRESH = "full_refresh"
    INCREMENTAL_MERGE = "incremental_merge"
    INCREMENTAL_APPEND = "incremental_append"
    REPLACE = "replace"
    PARTITION_REPLACE = "partition_replace"
    SNAPSHOT_DIFF = "snapshot_diff"
    SCD2 = "scd2"
    CDC_APPLY = "cdc_apply"
    BACKFILL = "backfill"


def normalized_source_byte_budget(
    strategy: Mapping[str, object],
    *,
    sink_type: str,
    source_options: Mapping[str, object],
    sink_options: Mapping[str, object],
) -> int | None:
    """Return the authoritative strategy budget or fail on ambiguous authoring."""

    for endpoint, options in (("source", source_options), ("sink", sink_options)):
        if "max_source_bytes" in options or SOURCE_BYTE_BUDGET_OPTION in options:
            raise ValueError(f"{endpoint}.options.max_source_bytes is misplaced; use sink.strategy.max_source_bytes")
    if "max_source_bytes" not in strategy:
        return None
    if sink_type != "clickhouse":
        raise ValueError(
            "sink.strategy.max_source_bytes is unsupported for "
            f"sink.type={sink_type}; the enforcing runtime is available only for clickhouse"
        )
    value = strategy["max_source_bytes"]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > MAX_SOURCE_BYTE_BUDGET:
        raise ValueError(f"sink.strategy.max_source_bytes must be an integer between 1 and {MAX_SOURCE_BYTE_BUDGET}")
    mode = str(strategy.get("mode", "full_refresh"))
    if mode != "full_refresh":
        raise ValueError("sink.strategy.max_source_bytes is supported only for full_refresh")
    return value


def validate_endpoint_option_ownership(
    source_options: Mapping[str, object], sink_options: Mapping[str, object]
) -> None:
    """Reject process-level contracts misplaced inside endpoint options."""

    for endpoint, options in (("source", source_options), ("sink", sink_options)):
        if "quality" in options:
            raise ValueError(f"{endpoint}.options.quality is misplaced; author quality at the process or manifest root")


def inject_source_budget(
    options: MutableMapping[str, object],
    strategy: Mapping[str, object],
    sink_type: str,
    source_options: Mapping[str, object],
    sink_options: Mapping[str, object],
) -> None:
    """Validate and inject the platform-owned source-byte budget."""

    budget = normalized_source_byte_budget(
        strategy,
        sink_type=sink_type,
        source_options=source_options,
        sink_options=sink_options,
    )
    if budget is not None:
        options[SOURCE_BYTE_BUDGET_OPTION] = budget


__all__ = [
    "MAX_SOURCE_BYTE_BUDGET",
    "SOURCE_BYTE_BUDGET_OPTION",
    "LoadStrategy",
    "inject_source_budget",
    "normalized_source_byte_budget",
    "validate_endpoint_option_ownership",
]
