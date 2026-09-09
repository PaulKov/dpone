"""Immutable ClickHouse publication receipts and head transitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.semantic_refresh_clickhouse_identity import (
    require_digest,
    require_uuid,
    semantic_refresh_fingerprint,
)


@dataclass(frozen=True)
class ClickHousePreparedReceipt:
    """Closed PREPARED receipt authorized for one exact UUID exchange."""

    operation_id: str
    attempt_binding_sha256: str
    prepare_plan_sha256: str
    status: str
    target_uuid: str
    staging_uuid: str | None
    shadow_uuid: str | None
    staging_rows: int
    shadow_rows: int
    desired_rows: int
    forward_difference_groups: int
    reverse_difference_groups: int
    shadow_equation: dict[str, str]
    conformance_mode: str
    receipt_sha256: str
    publication_mode: str = "EXCHANGE"


@dataclass(frozen=True)
class ClickHouseHeadPublicationPlan:
    """All heads and checkpoint that must commit with terminal journal state."""

    scope_id: str
    expected_target_generation: int
    target_generation: int
    target_generation_id: str
    expected_scope_revision: int
    scope_revision: int
    expected_checkpoint_sha256: str | None
    checkpoint_sha256: str
    target_mutation_outcome: str = "TARGET_COMMITTED"
    value_conversion_outcome: str = "CONFORMANT"

    def __post_init__(self) -> None:
        if not self.scope_id.strip():
            raise ValueError("ClickHouse publication scope_id must be non-empty")
        for field_name in (
            "expected_target_generation",
            "target_generation",
            "expected_scope_revision",
            "scope_revision",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("ClickHouse publication heads must be non-negative integers")
        if self.target_mutation_outcome == "TARGET_COMMITTED":
            if self.target_generation != self.expected_target_generation + 1:
                raise ValueError("ClickHouse target generation successor must be predecessor + 1")
            if self.value_conversion_outcome != "CONFORMANT":
                raise ValueError("ClickHouse exchanged publication must be conformant")
        elif self.target_mutation_outcome == "NOT_REQUIRED_EMPTY_SCOPE":
            if self.target_generation != self.expected_target_generation:
                raise ValueError("empty scope must preserve target generation")
            if self.value_conversion_outcome != "NOT_APPLICABLE_NO_DATA":
                raise ValueError("empty scope conversion must be not applicable")
        else:
            raise ValueError("ClickHouse target mutation outcome is unsupported")
        if self.scope_revision != self.expected_scope_revision + 1:
            raise ValueError("ClickHouse scope revision successor must be predecessor + 1")
        if self.expected_checkpoint_sha256 is not None:
            require_digest(self.expected_checkpoint_sha256, "expected_checkpoint_sha256")
        require_digest(self.target_generation_id, "target_generation_id")
        require_digest(self.checkpoint_sha256, "checkpoint_sha256")


@dataclass(frozen=True)
class ClickHouseUuidMap:
    """Actual UUID ownership of target and shadow names."""

    target_uuid: str
    shadow_uuid: str | None

    @classmethod
    def from_mapping(cls, raw: Any) -> ClickHouseUuidMap:
        if not isinstance(raw, dict) or set(raw) != {"target_uuid", "shadow_uuid"}:
            raise ValueError("ClickHouse UUID map must contain target_uuid and shadow_uuid")
        value = cls(**raw)
        require_uuid(value.target_uuid, "target_uuid")
        if value.shadow_uuid is not None:
            require_uuid(value.shadow_uuid, "shadow_uuid")
        return value


@dataclass(frozen=True)
class ClickHouseExchangeReceipt:
    """UUID-reconciled proof that the desired shadow owns the target name."""

    operation_id: str
    status: str
    old_target_uuid: str
    new_target_uuid: str
    exchange_receipt_sha256: str


@dataclass(frozen=True)
class ClickHousePublicationReceipt:
    """Terminal receipt emitted only after atomic state acknowledgement."""

    operation_id: str
    status: str
    exchange_outcome: str
    target_generation: int
    target_generation_id: str
    scope_revision: int
    terminal_receipt_sha256: str
    target_mutation_outcome: str = "TARGET_COMMITTED"
    value_conversion_outcome: str = "CONFORMANT"
    cleanup_status: str = "NOT_REQUIRED"
    retained_generation_status: str = "NOT_APPLICABLE"


def terminal_receipt_sha256(
    plan: Any,
    exchange_receipt: ClickHouseExchangeReceipt | None,
    heads: ClickHouseHeadPublicationPlan,
) -> str:
    """Fingerprint one exact terminal publication receipt."""

    return semantic_refresh_fingerprint(
        {
            "operation_id": plan.operation_id,
            "status": "COMPLETE",
            "exchange_outcome": "TARGET_COMMITTED",
            "scope_id": heads.scope_id,
            "target_generation": heads.target_generation,
            "target_generation_id": heads.target_generation_id,
            "scope_revision": heads.scope_revision,
            "checkpoint_sha256": heads.checkpoint_sha256,
            "target_mutation_outcome": heads.target_mutation_outcome,
            "value_conversion_outcome": heads.value_conversion_outcome,
            "exchange_receipt_sha256": (None if exchange_receipt is None else exchange_receipt.exchange_receipt_sha256),
        }
    )
