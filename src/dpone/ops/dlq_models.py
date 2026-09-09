"""Serializable application models for DLQ replay and retention."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class DlqReplayPolicy:
    max_records: int = 100_000
    max_bytes: int = 67_108_864

    def validate(self) -> None:
        if not 1 <= self.max_records <= 1_000_000:
            raise ValueError("DLQ replay max_records must be between 1 and 1000000")
        if not 1_048_576 <= self.max_bytes <= 536_870_912:
            raise ValueError("DLQ replay max_bytes must be between 1048576 and 536870912")

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DlqReplayItem:
    record_id: str
    record_sha256: str
    record_ref: str
    idempotency_key: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DlqReplayPlan:
    plan_id: str
    run_id: str
    resolver_identity: str
    target_identity: str
    policy: DlqReplayPolicy
    items: tuple[DlqReplayItem, ...]
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dlq-replay-plan.v1",
            "plan_id": self.plan_id,
            "run_id": self.run_id,
            "resolver_identity": self.resolver_identity,
            "target_identity": self.target_identity,
            "policy": self.policy.to_dict(),
            "items": [item.to_dict() for item in self.items],
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class DlqReplayResult:
    plan_id: str
    replayed_rows: int
    already_applied_rows: int
    applied: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DlqRetentionPlan:
    plan_id: str
    snapshot_fingerprint: str
    delete_record_ids: tuple[str, ...]
    protected_record_ids: tuple[str, ...]
    planned_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dlq-retention-plan.v1",
            **asdict(self),
            "delete_record_ids": list(self.delete_record_ids),
            "protected_record_ids": list(self.protected_record_ids),
        }


@dataclass(frozen=True, slots=True)
class DlqRetentionResult:
    plan_id: str
    deleted_record_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


__all__ = [
    "DlqReplayItem",
    "DlqReplayPlan",
    "DlqReplayPolicy",
    "DlqReplayResult",
    "DlqRetentionPlan",
    "DlqRetentionResult",
]
