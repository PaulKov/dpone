"""ClickHouse publication projection for dry-run plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION
from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)


def clickhouse_publication_plan(
    load_config: Any,
    sink_type: str,
    strategy_mode: str,
) -> dict[str, object]:
    """Project the shared admission result without performing target I/O."""

    options = load_config.options if isinstance(load_config.options, Mapping) else {}
    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=sink_type,
            strategy_mode=strategy_mode,
            max_source_bytes=options.get(SOURCE_BYTE_BUDGET_OPTION),
            physical_design=(
                options.get("physical_design") if isinstance(options.get("physical_design"), Mapping) else {}
            ),
            target_database=str(load_config.target_schema or ""),
            staging_database=str(load_config.staging_schema or ""),
        )
    )
    return decision.to_dict()
