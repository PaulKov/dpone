"""MSSQL queryout source materialization integration."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializationPlanner,
    SourceMaterializationPolicy,
    SourceMaterializedSnapshot,
)
from dpone.runtime.source_materialization_location import effective_source_materialization_policy
from dpone.runtime.source_materialization_preparation import guard_source_materialization_preparation
from dpone.runtime.sources.strategies.mssql.mssql_source_materialization import (
    MssqlWorkTableMaterializationProvider,
)
from dpone.runtime.sources.strategies.mssql.mssql_source_shape import MSSQLSourceShapeInspector


@dataclass(frozen=True, slots=True)
class MaterializedQuery:
    """Rewritten query plus source snapshot lifecycle metadata."""

    query: str
    snapshot: SourceMaterializedSnapshot
    decision: SourceMaterializationDecision
    policy: SourceMaterializationPolicy


def maybe_materialize_query(
    *,
    connector: Any,
    load_config: Any,
    query: str,
    schema: Sequence[tuple[str, str]],
) -> MaterializedQuery | None:
    """Create a source work-table snapshot when configured and selected."""

    if not _has_materialization_policy(load_config.options):
        return None
    policy = effective_source_materialization_policy(
        SourceMaterializationPolicy.from_source_options(load_config.options),
        load_config=load_config,
    )
    provider = MssqlWorkTableMaterializationProvider(connector)
    effective_policy = replace(policy, provider=provider.provider_id) if policy.provider == "auto" else policy
    source_shape = MSSQLSourceShapeInspector(connector).inspect(
        load_config, boundary_column=_boundary_column(load_config)
    )
    decision = SourceMaterializationPlanner().plan(
        effective_policy,
        source_shape=source_shape,
        provider_available=policy.provider in {"auto", provider.provider_id},
        permissions_ok=provider.permissions_ok(load_config, effective_policy),
    )
    publish_runtime_decision(
        decision,
        decision_id="source_materialization",
        phase="extract",
        component="mssql_source",
        category="source_preparation",
        fallback_allowed=effective_policy.mode == "auto" and not decision.blockers,
        provider=effective_policy.provider,
        details={
            "source_shape": {
                "table_kind": source_shape.table_kind,
                "has_seekable_boundary": source_shape.has_seekable_boundary,
                "stats_confidence": source_shape.stats_confidence,
            },
            "work_connection_ref": effective_policy.work_connection_ref,
            "work_database": effective_policy.work_database,
            "work_schema": effective_policy.work_schema,
            "cleanup_policy": effective_policy.cleanup_policy,
        },
    )
    if decision.blockers:
        raise ValueError(", ".join(decision.blockers))
    if not decision.selected:
        return None
    snapshot = provider.prepare(load_config, query=query, schema=schema, policy=effective_policy)
    with guard_source_materialization_preparation(
        cleanup=snapshot.cleanup,
        provider=effective_policy.provider,
        cleanup_policy=effective_policy.cleanup_policy,
    ):
        return MaterializedQuery(
            query=snapshot.select_query([column for column, _ in schema]),
            snapshot=snapshot,
            decision=decision,
            policy=effective_policy,
        )


def wrap_prepared_source_artifact(artifact: Any, materialized: MaterializedQuery | None) -> Any:
    """Attach source snapshot cleanup to an artifact when materialization ran."""

    if materialized is None:
        return artifact
    return PreparedSourceArtifact(
        artifact,
        snapshot=materialized.snapshot,
        decision=materialized.decision,
        cleanup_policy=materialized.policy.cleanup_policy,
    )


def _has_materialization_policy(options: dict[str, Any]) -> bool:
    native = options.get("native_transfer")
    snapshot = native.get("snapshot") if isinstance(native, dict) else None
    return isinstance(snapshot, dict) and "materialization" in snapshot


def _boundary_column(load_config: Any) -> str | None:
    partitioning = getattr(load_config, "options", {}).get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("column"):
        return str(partitioning["column"])
    return None


__all__ = ["MaterializedQuery", "maybe_materialize_query", "wrap_prepared_source_artifact"]
