"""Static ClickHouse cluster-publication admission for ``dpone check``."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION
from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.manifest.validation_models import Severity, ValidationIssue


def validate_clickhouse_cluster_publication(
    spec: Any,
    *,
    manifest_path: Path,
) -> Iterable[ValidationIssue]:
    """Project pure admission blockers into manifest diagnostics."""

    raw = spec.raw_config if isinstance(getattr(spec, "raw_config", None), Mapping) else {}
    sink = _mapping(raw.get("sink"))
    sink_type = canonical_endpoint_type(str(sink.get("type") or ""))
    strategy = _mapping(sink.get("strategy"))
    options = _mapping(sink.get("options"))
    load_config = getattr(getattr(spec, "config", None), "load_config", None)
    if load_config is None:
        return ()
    resolved_options = getattr(load_config, "options", {}) or {}
    physical = _mapping(resolved_options.get("physical_design")) or _mapping(options.get("physical_design"))
    target_database = str(getattr(load_config, "target_schema", "") or "")
    max_source_bytes = strategy.get("max_source_bytes", resolved_options.get(SOURCE_BYTE_BUDGET_OPTION))
    mode = str(strategy.get("mode") or getattr(getattr(load_config, "load_strategy", None), "value", "") or "")
    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=sink_type,
            strategy_mode=mode,
            max_source_bytes=max_source_bytes,
            physical_design=physical,
            target_database=target_database,
            staging_database=str(getattr(load_config, "staging_schema", "") or ""),
        )
    )
    return tuple(
        ValidationIssue(
            severity=Severity.ERROR,
            code=blocker.upper().replace(".", "_").replace("-", "_"),
            message=(
                f"{blocker}; cluster full_refresh has no local fallback. "
                "Fix the declared strategy and ClickHouse physical design before execution."
            ),
            manifest_path=manifest_path,
            selector=spec.selector or spec.name,
        )
        for blocker in decision.blockers
    )


def _mapping(raw: object) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = ["validate_clickhouse_cluster_publication"]
