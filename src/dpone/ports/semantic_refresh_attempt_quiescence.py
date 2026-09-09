"""Protected ClickHouse quiescence proof for same-DagRun attempt continuation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.semantic_refresh_document import (
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_evidence_common import require_utc_timestamp

CLICKHOUSE_ATTEMPT_QUIESCENCE_SCHEMA = "dpone.semantic-refresh-clickhouse-attempt-quiescence.v1"


@dataclass(frozen=True, slots=True)
class ClickHouseAttemptQuiescenceProof:
    """Prove that no query for one exact protected operation remains active."""

    workflow_execution_binding_sha256: str
    operation_id: str
    original_attempt_binding_sha256: str
    clickhouse_cluster_authority_id: str
    query_id_prefix: str
    observed_at: str
    quiescence_observation_sha256: str
    active_query_ids: tuple[str, ...] = ()
    status: str = "VERIFIED"
    schema: str = CLICKHOUSE_ATTEMPT_QUIESCENCE_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "original_attempt_binding_sha256",
        ):
            require_digest(getattr(self, field), field)
        for field in ("clickhouse_cluster_authority_id", "query_id_prefix"):
            require_text(getattr(self, field), field)
        require_utc_timestamp(self.observed_at, "observed_at")
        if self.schema != CLICKHOUSE_ATTEMPT_QUIESCENCE_SCHEMA or self.status != "VERIFIED":
            raise ValueError("ClickHouse attempt quiescence status is invalid")
        if self.active_query_ids:
            raise ValueError("ClickHouse attempt quiescence cannot contain active query IDs")
        if require_digest(
            self.quiescence_observation_sha256,
            "quiescence_observation_sha256",
        ) != semantic_refresh_sha256(self._unsigned()):
            raise ValueError("ClickHouse attempt quiescence digest differs")

    @classmethod
    def build(
        cls,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        original_attempt_binding_sha256: str,
        clickhouse_cluster_authority_id: str,
        query_id_prefix: str,
        observed_at: str,
    ) -> ClickHouseAttemptQuiescenceProof:
        """Build the canonical zero-active-query observation."""

        values = {
            "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
            "operation_id": operation_id,
            "original_attempt_binding_sha256": original_attempt_binding_sha256,
            "clickhouse_cluster_authority_id": clickhouse_cluster_authority_id,
            "query_id_prefix": query_id_prefix,
            "observed_at": observed_at,
            "active_query_ids": (),
            "status": "VERIFIED",
            "schema": CLICKHOUSE_ATTEMPT_QUIESCENCE_SCHEMA,
        }
        unsigned = {
            **values,
            "active_query_ids": [],
        }
        return cls(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
            original_attempt_binding_sha256=original_attempt_binding_sha256,
            clickhouse_cluster_authority_id=clickhouse_cluster_authority_id,
            query_id_prefix=query_id_prefix,
            observed_at=observed_at,
            quiescence_observation_sha256=semantic_refresh_sha256(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return {
            "active_query_ids": list(self.active_query_ids),
            "clickhouse_cluster_authority_id": self.clickhouse_cluster_authority_id,
            "observed_at": self.observed_at,
            "operation_id": self.operation_id,
            "original_attempt_binding_sha256": self.original_attempt_binding_sha256,
            "query_id_prefix": self.query_id_prefix,
            "schema": self.schema,
            "status": self.status,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }


class SemanticRefreshClickHouseAttemptQuiescencePort(Protocol):
    """Observe the exact operation query-id namespace through a protected client."""

    def prove_quiescent(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
        original_attempt_binding_sha256: str,
        clickhouse_cluster_authority_id: str,
        observed_at: str,
    ) -> ClickHouseAttemptQuiescenceProof:
        """Return a typed zero-active-query proof or fail closed."""


__all__ = [
    "CLICKHOUSE_ATTEMPT_QUIESCENCE_SCHEMA",
    "ClickHouseAttemptQuiescenceProof",
    "SemanticRefreshClickHouseAttemptQuiescencePort",
]
