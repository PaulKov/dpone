"""Redacted receipt for externally replicated ClickHouse publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.ports.clickhouse_external_replication import (
    EXTERNAL_RECEIPT_SCHEMA_VERSION,
    ExternalAuthorityRecord,
)


@dataclass(frozen=True, slots=True)
class ExternalReplicationReceipt:
    """Serializable proof that excludes names, endpoints, paths, and row data."""

    target_key: str
    operation_id: str
    generation_id: str
    inventory_digest: str
    plan_digest: str
    artifact_sha256: str
    member_ids: tuple[str, ...]
    authority_version: int
    phase: str = "COMPLETED"
    replication_mode: str = "external"
    evidence_status: str = "PASS"
    evidence_scope: str = "local_synthetic"
    schema_version: str = EXTERNAL_RECEIPT_SCHEMA_VERSION

    @classmethod
    def from_authority(cls, record: ExternalAuthorityRecord, *, version: int) -> ExternalReplicationReceipt:
        """Project only immutable, opaque authority identity into public evidence."""

        if record.artifact is None or record.generation_id is None:
            raise ValueError("external publication authority is incomplete")
        return cls(
            target_key=record.target_key,
            operation_id=record.operation_id,
            generation_id=record.generation_id,
            inventory_digest=record.inventory_digest,
            plan_digest=record.plan_digest,
            artifact_sha256=record.artifact.sha256,
            member_ids=tuple(sorted(member.member_id for member in record.members)),
            authority_version=version,
            phase=record.phase.value,
        )

    @classmethod
    def from_state(
        cls, state: dict[str, Any], *, evidence_scope: str = "local_synthetic"
    ) -> ExternalReplicationReceipt:
        """Create a receipt from the high-level durable runtime state."""

        return cls(
            target_key=str(state["target_key"]),
            operation_id=str(state["operation_id"]),
            generation_id=str(state["generation_id"]),
            inventory_digest=str(state["inventory_digest"]),
            plan_digest=str(state["plan_digest"]),
            artifact_sha256=str(state["artifact_sha256"]),
            member_ids=tuple(sorted(str(value) for value in state["member_ids"])),
            authority_version=int(state["version"]),
            phase=str(state["phase"]),
            evidence_scope=evidence_scope,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable redacted evidence representation."""

        return {
            "schema_version": self.schema_version,
            "replication_mode": self.replication_mode,
            "phase": self.phase,
            "target_key": self.target_key,
            "operation_id": self.operation_id,
            "generation_id": self.generation_id,
            "inventory_digest": self.inventory_digest,
            "plan_digest": self.plan_digest,
            "artifact_sha256": self.artifact_sha256,
            "member_ids": list(self.member_ids),
            "authority_version": self.authority_version,
            "evidence_status": self.evidence_status,
            "evidence_scope": self.evidence_scope,
        }


__all__ = ["ExternalReplicationReceipt"]
