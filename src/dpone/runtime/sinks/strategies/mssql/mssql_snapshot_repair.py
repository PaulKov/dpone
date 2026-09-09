"""One-shot repair admission around MSSQL snapshot reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.contracts.repair_authority import RepairAuthority, RepairAuthorityError
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError


@dataclass(frozen=True, slots=True)
class SnapshotRepairAdmission:
    """Exceptional mutation admitted for this exact snapshot and load."""

    authority: RepairAuthority | None
    used_full_baseline: bool
    missing_rows: int
    missing_ratio: float

    def evidence(self) -> dict[str, Any]:
        if self.authority is None:
            return {}
        return {
            "repair_authority_id": self.authority.authority_id,
            "repair_authority_digest": self.authority.authority_digest,
            "repair_full_baseline": self.used_full_baseline,
            "repair_target_authority_transfer": self.authority.transfer_from is not None,
        }


def admit_snapshot_repair(
    *,
    state: Any,
    executor: Any,
    load_config: Any,
    envelope: Any,
    policy: Any,
    target_rows_before: int,
    active_rows_before: int,
    missing_rows: int,
) -> SnapshotRepairAdmission:
    """Fail closed before business DML unless exact exceptional work is approved."""

    authority_ref = getattr(load_config, "repair_authority_ref", None)
    if envelope.previous_checkpoint is None and target_rows_before > 0 and not envelope.baseline:
        raise SnapshotReconciliationError("repair_authority.nonbaseline_missing_state")
    empty_key_snapshot = int(envelope.key_receipt.row_count) == 0
    if empty_key_snapshot and active_rows_before > 0 and missing_rows != active_rows_before:
        raise SnapshotReconciliationError("repair_authority.empty_snapshot_observation_invalid")
    full_baseline = bool(getattr(envelope, "baseline", False)) and bool(authority_ref or target_rows_before > 0)
    ratio = (missing_rows / active_rows_before) if active_rows_before else 0.0
    guard_exceeded = missing_rows > policy.max_delete_rows or ratio > policy.max_delete_ratio
    if not empty_key_snapshot and not full_baseline and not guard_exceeded and not authority_ref:
        return SnapshotRepairAdmission(None, False, missing_rows, ratio)
    admit = getattr(state, "admit_repair_authority", None)
    if not callable(admit):
        raise SnapshotReconciliationError("repair_authority.state_service_required")
    try:
        authority = admit(
            executor=executor,
            authority_ref=authority_ref,
            key=envelope.state_key,
            expected_checkpoint=envelope.previous_checkpoint,
            scope_hash=envelope.scope_hash,
            require_full_baseline=full_baseline,
            require_empty_snapshot_override=empty_key_snapshot,
            missing_rows=missing_rows,
            missing_ratio=ratio,
            configured_max_delete_rows=policy.max_delete_rows,
            configured_max_delete_ratio=policy.max_delete_ratio,
        )
    except RepairAuthorityError as exc:
        raise SnapshotReconciliationError(exc.code) from exc
    return SnapshotRepairAdmission(authority, full_baseline, missing_rows, ratio)


def consume_snapshot_repair(
    *,
    state: Any,
    executor: Any,
    admission: SnapshotRepairAdmission,
    envelope: Any,
    load_id: str,
    receipt_id: str,
) -> None:
    """Persist authority consumption only after the matching CAS receipt exists."""

    if admission.authority is None:
        return
    state.consume_repair_authority(
        executor=executor,
        authority=admission.authority,
        key=envelope.state_key,
        load_id=load_id,
        receipt_id=receipt_id,
        used_full_baseline=admission.used_full_baseline,
        observed_delete_rows=admission.missing_rows,
        observed_delete_ratio=admission.missing_ratio,
    )


__all__ = ["SnapshotRepairAdmission", "admit_snapshot_repair", "consume_snapshot_repair"]
