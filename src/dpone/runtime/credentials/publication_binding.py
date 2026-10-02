"""Pure runtime selection of an independent publication connection capability."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION
from dpone.config.publication_authority import (
    normalized_publication_selection,
    select_publication_authority,
)
from dpone.ports.clickhouse_cluster_publication import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.ports.mssql_publication import PublicationAuthorityBinding


def select_publication_binding(
    config: Mapping[str, Any], *, load_config: Any, environment: str | None
) -> PublicationAuthorityBinding | None:
    """Fail closed before credential/connector I/O for an unsupported route."""
    binding = select_publication_authority(config)
    if binding != normalized_publication_selection(load_config.options):
        raise ValueError("publication_authority: compiled manifest and load config differ")
    if binding is None:
        return None
    sink = config["sink"]
    if environment is None or environment != binding.environment:
        raise ValueError("publication_authority: verified environment mismatch or unavailable")
    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=str(sink.get("type", "")),
            strategy_mode=load_config.load_strategy.value,
            max_source_bytes=load_config.options.get(SOURCE_BYTE_BUDGET_OPTION),
            physical_design=load_config.options.get("physical_design"),
            target_database=load_config.target_schema,
            staging_database=load_config.staging_schema,
        )
    )
    if not decision.selected or decision.mode != "cluster":
        raise ValueError("publication_authority: bounded internal replicated full refresh required")
    return binding


def require_publication_provider(sink: Mapping[str, Any], *, selected_provider: Any = None) -> None:
    """Direct endpoint builders must not ignore a selected authority option."""
    options = sink.get("options", {})
    selected = isinstance(options, Mapping) and "publication_authority" in options
    if selected != (selected_provider is not None):
        raise ValueError("publication_authority: verified resolved provider required for explicit selection")
