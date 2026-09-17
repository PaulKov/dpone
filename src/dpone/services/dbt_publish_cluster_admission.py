"""Cluster-publication admission diagnostics for compiled dbt models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.contracts.dbt_publish_models import (
    DbtModelArtifact,
    DbtPublishIssue,
    DbtPublishProfile,
)


def cluster_admission_issues(
    *,
    model: DbtModelArtifact,
    profile: DbtPublishProfile,
    strategy: Mapping[str, Any],
    physical_design: Mapping[str, Any],
    target_schema: str,
) -> tuple[DbtPublishIssue, ...]:
    """Return compile-time issues from the shared cluster admission decision."""

    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=profile.sink_type,
            strategy_mode=str(strategy.get("mode") or ""),
            max_source_bytes=strategy.get("max_source_bytes"),
            physical_design=physical_design,
            target_database=target_schema,
            staging_database=profile.staging_schema,
        )
    )
    return tuple(
        DbtPublishIssue(
            code=blocker.upper().replace(".", "_").replace("-", "_"),
            message=f"{blocker}; cluster full_refresh has no local publication fallback",
            path=f"{model.original_file_path}#physical_design.storage.clickhouse",
            severity="error",
            remediation="Use a bounded Replicated*MergeTree cluster design in the target database.",
        )
        for blocker in decision.blockers
    )
