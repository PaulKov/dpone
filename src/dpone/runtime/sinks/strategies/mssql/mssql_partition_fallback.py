"""Certified SQL shapes for SQL Server partition-replace fallback."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.support.mssql_native_canonical import (
    canonical_identity_expression,
    normalized_mssql_type,
)

TYPED_SARGABLE_DISTINCT_JOIN = "typed_sargable_distinct_join"
CANONICAL_IDENTITY_FALLBACK = "canonical_identity_fallback"
_CERTIFIED_TYPED_PARTITION_TYPES = frozenset({"date"})


@dataclass(frozen=True, slots=True)
class MssqlPartitionFallbackPlan:
    """One type-gated, deterministic fallback finalization plan."""

    column: str
    mssql_type: str
    mode: str

    def validation_sql(
        self,
        *,
        staging_name: str,
        quote_identifier: Callable[[str], str],
    ) -> str:
        """Select distinct typed identities before any target mutation."""

        column = quote_identifier(self.column)
        if self.mode == TYPED_SARGABLE_DISTINCT_JOIN:
            return f"SELECT DISTINCT p.{column} FROM {staging_name} AS p"
        identity = canonical_identity_expression(f"p.{column}", self.mssql_type)
        return f"SELECT DISTINCT p.{column}, {identity} AS [__dpone__partition_identity] FROM {staging_name} AS p"

    def delete_sql(
        self,
        *,
        target_name: str,
        staging_name: str,
        quote_identifier: Callable[[str], str],
    ) -> str:
        """Render the bounded target delete for the selected identity semantics."""

        column = quote_identifier(self.column)
        if self.mode == TYPED_SARGABLE_DISTINCT_JOIN:
            return (
                f"DELETE t FROM {target_name} AS t "
                f"INNER JOIN (SELECT DISTINCT s.{column} AS {column} FROM {staging_name} AS s) AS p "
                f"ON p.{column} = t.{column}"
            )
        left = canonical_identity_expression(f"s.{column}", self.mssql_type)
        right = canonical_identity_expression(f"t.{column}", self.mssql_type)
        return (
            f"DELETE t FROM {target_name} AS t WHERE EXISTS (SELECT 1 FROM {staging_name} AS s WHERE {left} = {right})"
        )


def plan_mssql_partition_fallback(
    staging: StagingTableArtifact,
    partition_column: str,
) -> MssqlPartitionFallbackPlan:
    """Use direct equality only for route-certified exact native types."""

    dtype = (staging.target_column_types or {}).get(partition_column)
    if not dtype:
        raise ValueError(f"MSSQL partition column {partition_column!r} has no resolved native type")
    normalized_type = normalized_mssql_type(str(dtype))
    mode = (
        TYPED_SARGABLE_DISTINCT_JOIN
        if normalized_type in _CERTIFIED_TYPED_PARTITION_TYPES
        else CANONICAL_IDENTITY_FALLBACK
    )
    return MssqlPartitionFallbackPlan(
        column=partition_column,
        mssql_type=normalized_type,
        mode=mode,
    )


def publish_mssql_partition_fallback_decision(plan: MssqlPartitionFallbackPlan) -> None:
    """Publish the actual redaction-aware fallback finalizer selection."""

    publish_runtime_decision(
        {
            "requested_backend": "auto",
            "selected_backend": plan.mode,
            "release_gate": "green",
            "warnings": [],
            "blockers": [],
        },
        decision_id="mssql.partition_replace.finalizer",
        phase="load",
        component="mssql_partition_replace",
        category="finalizer_selection",
        fallback_allowed=True,
        details={"partition_type": plan.mssql_type},
    )


__all__ = [
    "CANONICAL_IDENTITY_FALLBACK",
    "MssqlPartitionFallbackPlan",
    "TYPED_SARGABLE_DISTINCT_JOIN",
    "plan_mssql_partition_fallback",
    "publish_mssql_partition_fallback_decision",
]
