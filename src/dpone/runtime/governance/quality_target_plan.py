"""Freeze and reconstruct the exact target metric selection without source I/O."""

from __future__ import annotations

from typing import Any

from dpone.runtime.governance.acceptance_snapshot import business_columns, selected_columns
from dpone.runtime.governance.quality_replay_identity import schema_digest
from dpone.runtime.quality_replay_contracts import TargetAcceptanceRequest, contracts
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import cluster_name


def prepare_target_plan(config: Any, policy: Any, schema: Any) -> dict[str, Any]:
    """Reject unknown requested columns instead of silently reducing a plan."""
    ordered = tuple(tuple(pair) for pair in schema)
    digest = schema_digest(ordered)
    columns = business_columns(ordered)
    selections = []
    for value in (policy.null_counts, policy.distinct_counts):
        if isinstance(value, tuple) and set(value) - set(columns):
            raise contracts.ReplayQualityEvidenceError("UNSUPPORTED")
        selections.append(selected_columns(value, columns))
    nulls, distinct = selections
    if len(set(nulls) | set(distinct)) > 256:
        raise contracts.ReplayQualityEvidenceError("UNSUPPORTED")
    selected = {"row_count": policy.row_count, "null_columns": list(nulls), "distinct_columns": list(distinct)}
    return {
        "mode": policy.mode,
        "cluster": cluster_name(config),
        "database": str(config.target_schema),
        "table": str(config.target_table),
        "dataset": f"{config.target_schema}.{config.target_table}",
        "columns": [list(pair) for pair in ordered],
        "schema_digest": digest,
        "selection_digest": contracts.quality_digest(selected),
        **selected,
    }


def target_request(core: dict[str, Any], core_digest: str, token: str) -> TargetAcceptanceRequest:
    """Reconstruct a request exclusively from sealed original inputs."""
    plan = dict(core["target_plan"])
    plan.pop("replicas", None)
    plan.pop("mode")
    plan["columns"] = tuple(tuple(pair) for pair in plan["columns"])
    plan["null_columns"] = tuple(plan["null_columns"])
    plan["distinct_columns"] = tuple(plan["distinct_columns"])
    request = TargetAcceptanceRequest(**plan, binding=core["binding"], core_digest=core_digest, reader_token=token)
    request.validate()
    return request


def validate_target_plan(config: Any, policy: Any, core: dict[str, Any]) -> None:
    """Require original schema digest, configuration and exact metric selection."""
    plan = core["target_plan"]
    try:
        expected = prepare_target_plan(config, policy, plan["columns"])
        replicas = plan["replicas"]
        if (
            {key: value for key, value in plan.items() if key != "replicas"} != expected
            or expected["schema_digest"] != core["effective_plan"]["target_schema_digest"]
            or not isinstance(replicas, list)
            or not replicas
            or any(not isinstance(host, str) or not host or len(host) > 4096 for host in replicas)
            or replicas != sorted(set(replicas))
        ):
            raise contracts.ReplayQualityEvidenceError("MISMATCH")
    except (KeyError, TypeError, ValueError):
        raise contracts.ReplayQualityEvidenceError("INVALID") from None
