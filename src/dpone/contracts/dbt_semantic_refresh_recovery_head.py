"""Canonical protected ClickHouse target-head identity for dbt recovery plans."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
)

RECOVERY_TARGET_HEAD_SCHEMA = "dpone.dbt-semantic-refresh-recovery-target-head.v1"


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshRecoveryTargetHead:
    """Protected current ClickHouse target head for one replay model."""

    model_unique_id: str
    clickhouse_target_authority_id: str
    target_generation: int
    target_generation_id: str
    target_uuid: str
    owner_operation_id: str
    terminal_receipt_sha256: str
    head_authority_receipt_sha256: str

    @classmethod
    def build(
        cls,
        *,
        model_unique_id: str,
        clickhouse_target_authority_id: str,
        target_generation: int,
        target_generation_id: str,
        target_uuid: str,
        owner_operation_id: str,
        terminal_receipt_sha256: str,
    ) -> SemanticRefreshRecoveryTargetHead:
        """Build the exact receipt over one protected durable target head."""

        receipt = semantic_refresh_recovery_target_head_sha256(
            model_unique_id=model_unique_id,
            clickhouse_target_authority_id=clickhouse_target_authority_id,
            target_generation=target_generation,
            target_generation_id=target_generation_id,
            target_uuid=target_uuid,
            owner_operation_id=owner_operation_id,
            terminal_receipt_sha256=terminal_receipt_sha256,
        )
        return cls(
            model_unique_id=model_unique_id,
            clickhouse_target_authority_id=clickhouse_target_authority_id,
            target_generation=target_generation,
            target_generation_id=target_generation_id,
            target_uuid=target_uuid,
            owner_operation_id=owner_operation_id,
            terminal_receipt_sha256=terminal_receipt_sha256,
            head_authority_receipt_sha256=receipt,
        )

    def __post_init__(self) -> None:
        require_digest(self.head_authority_receipt_sha256, "recovery target head authority receipt")
        expected = semantic_refresh_recovery_target_head_sha256(
            model_unique_id=self.model_unique_id,
            clickhouse_target_authority_id=self.clickhouse_target_authority_id,
            target_generation=self.target_generation,
            target_generation_id=self.target_generation_id,
            target_uuid=self.target_uuid,
            owner_operation_id=self.owner_operation_id,
            terminal_receipt_sha256=self.terminal_receipt_sha256,
        )
        if self.head_authority_receipt_sha256 != expected:
            raise SemanticRefreshContractError("recovery target head authority receipt differs")


def semantic_refresh_recovery_target_head_sha256(
    *,
    model_unique_id: str,
    clickhouse_target_authority_id: str,
    target_generation: int,
    target_generation_id: str,
    target_uuid: str,
    owner_operation_id: str,
    terminal_receipt_sha256: str,
) -> str:
    """Digest the closed physical target-head authority preimage."""

    require_text(model_unique_id, "recovery target model_unique_id")
    require_text(clickhouse_target_authority_id, "recovery clickhouse target authority")
    require_positive_int(target_generation, "recovery target generation")
    require_digest(target_generation_id, "recovery target generation_id")
    require_digest(owner_operation_id, "recovery target owner operation_id")
    require_digest(terminal_receipt_sha256, "recovery target terminal receipt")
    try:
        if str(UUID(target_uuid)) != target_uuid:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as exc:
        raise SemanticRefreshContractError("recovery target UUID must be canonical") from exc
    return semantic_refresh_sha256(
        {
            "clickhouse_target_authority_id": clickhouse_target_authority_id,
            "model_unique_id": model_unique_id,
            "owner_operation_id": owner_operation_id,
            "schema": RECOVERY_TARGET_HEAD_SCHEMA,
            "target_generation": target_generation,
            "target_generation_id": target_generation_id,
            "target_uuid": target_uuid,
            "terminal_receipt_sha256": terminal_receipt_sha256,
        }
    )


__all__ = [
    "RECOVERY_TARGET_HEAD_SCHEMA",
    "SemanticRefreshRecoveryTargetHead",
    "semantic_refresh_recovery_target_head_sha256",
]
