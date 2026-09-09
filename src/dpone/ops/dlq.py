"""Application services for safe DLQ writes, replay, and retention."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dpone.contracts.dlq import (
    DLQ_DIAGNOSTIC_FIELDS,
    DlqPolicy,
    DlqReason,
    DlqRecord,
    DlqWriteReceipt,
    canonical_fingerprint,
    project_payload,
)
from dpone.ops.dlq_models import (
    DlqReplayItem,
    DlqReplayPlan,
    DlqReplayPolicy,
    DlqReplayResult,
    DlqRetentionPlan,
    DlqRetentionResult,
)
from dpone.ops.dlq_safety import safe_diagnostics
from dpone.ops.ids import new_ulid, utc_now_iso
from dpone.ports.dlq import DlqRecordResolver, DlqRejection, DlqReplaySink, DlqStore


class DlqService:
    """Create durable records using a validated policy and injected store."""

    def __init__(self, store: DlqStore, *, policy: DlqPolicy) -> None:
        policy.validate()
        self.store = store
        self.policy = policy
        self._run_counts: dict[str, int] = {}

    def put(
        self,
        *,
        run_id: str,
        load_id: str,
        row: Mapping[str, object],
        reason: DlqReason,
        diagnostics: Mapping[str, object] | None = None,
        row_index: int = 0,
        record_ref: str | None = None,
    ) -> DlqRecord:
        if row_index < 0:
            raise ValueError("dlq row_index cannot be negative")
        count = self._run_counts.get(run_id)
        if count is None:
            count = len(self.store.records(run_id))
            self._run_counts[run_id] = count
        if count >= self.policy.max_records_per_run:
            raise ValueError("DPONE_DLQ_RUN_RECORD_LIMIT_EXCEEDED: run reached its configured DLQ limit")
        projected = safe_diagnostics(diagnostics or {}, allowed_fields=DLQ_DIAGNOSTIC_FIELDS)
        encoded = json.dumps(projected, ensure_ascii=False, sort_keys=True).encode("utf-8")
        if len(encoded) > self.policy.max_diagnostic_bytes:
            raise ValueError("DPONE_DLQ_DIAGNOSTICS_TOO_LARGE: redacted diagnostics exceed configured limit")
        record = DlqRecord.create(
            record_id=new_ulid(),
            run_id=run_id,
            load_id=load_id,
            record_ref=record_ref or f"dpone://runs/{run_id}/loads/{load_id}/rows/{row_index}",
            reason=reason,
            diagnostics=projected,
            payload=project_payload(row, self.policy.pii_policy),
            retention_days=self.policy.retention_days,
        )
        stored = self.store.append(record)
        self._run_counts[run_id] = count + 1
        return stored

    def records(self, run_id: str) -> tuple[DlqRecord, ...]:
        return self.store.records(run_id)

    def write_rejected(
        self,
        *,
        run_id: str,
        load_id: str,
        row: Mapping[str, object],
        row_index: int,
        rejection: DlqRejection,
    ) -> DlqWriteReceipt:
        record = self.put(
            run_id=run_id,
            load_id=load_id,
            row=row,
            row_index=row_index,
            reason=DlqReason.registered(
                rejection.reason_code,
                stage=rejection.stage,
                column=rejection.column,
            ),
            diagnostics=rejection.diagnostics,
        )
        return DlqWriteReceipt(record_id=record.record_id, reason_code=record.reason.code)

    def finalize_run(self, run_id: str) -> Mapping[str, object]:
        return self.summary(run_id)

    def summary(self, run_id: str) -> dict[str, Any]:
        index_ref = self.store.refresh_index(run_id)
        records = self.records(run_id)
        reasons = Counter(record.reason.code for record in records)
        return {
            "schema": "dpone.dlq-index.v1",
            "record_count": len(records),
            "record_ids": [record.record_id for record in records],
            "reasons": dict(sorted(reasons.items())),
            "index_ref": index_ref,
        }


class DlqReplayService:
    """Build immutable replay intent, then acknowledge only after target apply."""

    def __init__(self, store: DlqStore) -> None:
        self._store = store

    def plan(
        self,
        *,
        run_id: str,
        resolver_identity: str,
        target_identity: str,
        policy: DlqReplayPolicy | None = None,
    ) -> DlqReplayPlan:
        active_policy = policy or DlqReplayPolicy()
        active_policy.validate()
        _validate_replay_identity(resolver_identity, "resolver")
        _validate_replay_identity(target_identity, "target")
        records = self._store.replay_candidates(run_id)
        if len(records) > active_policy.max_records:
            raise RuntimeError("DPONE_DLQ_REPLAY_PLAN_LIMIT_EXCEEDED: record count exceeds replay policy")
        item_payloads = [
            {
                "record_id": record.record_id,
                "record_sha256": record.sha256,
                "record_ref": record.record_ref,
            }
            for record in records
        ]
        plan_id = canonical_fingerprint(
            _replay_plan_payload(
                run_id=run_id,
                resolver_identity=resolver_identity,
                target_identity=target_identity,
                policy=active_policy,
                items=item_payloads,
            )
        )
        items = tuple(
            DlqReplayItem(
                **payload,
                idempotency_key=canonical_fingerprint(
                    {"schema": "dpone.dlq-replay-idempotency.v1", "plan_id": plan_id, **payload}
                ),
            )
            for payload in item_payloads
        )
        plan = DlqReplayPlan(
            plan_id=plan_id,
            run_id=run_id,
            resolver_identity=resolver_identity,
            target_identity=target_identity,
            policy=active_policy,
            items=items,
            created_at=utc_now_iso(),
        )
        plan_bytes = len(json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8"))
        if plan_bytes > active_policy.max_bytes:
            raise RuntimeError("DPONE_DLQ_REPLAY_PLAN_LIMIT_EXCEEDED: serialized plan exceeds replay policy")
        return plan

    def execute(
        self,
        plan: DlqReplayPlan,
        *,
        resolver: DlqRecordResolver,
        sink: DlqReplaySink,
    ) -> DlqReplayResult:
        item_payloads = [
            {
                "record_id": item.record_id,
                "record_sha256": item.record_sha256,
                "record_ref": item.record_ref,
            }
            for item in plan.items
        ]
        expected_plan_id = canonical_fingerprint(
            _replay_plan_payload(
                run_id=plan.run_id,
                resolver_identity=plan.resolver_identity,
                target_identity=plan.target_identity,
                policy=plan.policy,
                items=item_payloads,
            )
        )
        if plan.plan_id != expected_plan_id:
            raise RuntimeError("DPONE_DLQ_REPLAY_PLAN_CHECKSUM_MISMATCH: replay plan content changed")
        if resolver.identity != plan.resolver_identity or sink.identity != plan.target_identity:
            raise RuntimeError("DPONE_DLQ_REPLAY_IDENTITY_MISMATCH: executor identity differs from replay plan")
        replayed = already_applied = 0
        for item in plan.items:
            expected_key = canonical_fingerprint(
                {
                    "schema": "dpone.dlq-replay-idempotency.v1",
                    "plan_id": plan.plan_id,
                    "record_id": item.record_id,
                    "record_sha256": item.record_sha256,
                    "record_ref": item.record_ref,
                }
            )
            if item.idempotency_key != expected_key:
                raise RuntimeError("DPONE_DLQ_REPLAY_PLAN_CHECKSUM_MISMATCH: idempotency key changed")
            record = self._store.record_for_run(plan.run_id, item.record_id)
            if record.sha256 != item.record_sha256 or record.record_ref != item.record_ref:
                raise RuntimeError("DPONE_DLQ_REPLAY_SNAPSHOT_DRIFT: record changed after planning")
            if not record.replay.replayable:
                raise RuntimeError("DPONE_DLQ_RECORD_NOT_REPLAYABLE: record policy forbids replay")
            if self._store.is_acknowledged(record):
                already_applied += 1
                continue
            row = resolver.resolve(record.record_ref)
            sink.apply(row, idempotency_key=item.idempotency_key)
            self._store.acknowledge(record, plan_id=plan.plan_id, idempotency_key=item.idempotency_key)
            replayed += 1
        return DlqReplayResult(
            plan_id=plan.plan_id,
            replayed_rows=replayed,
            already_applied_rows=already_applied,
            applied=True,
        )


class DlqRetentionService:
    """Delete only expired acknowledged records from an unchanged snapshot."""

    def __init__(self, store: DlqStore) -> None:
        self._store = store

    def plan(
        self,
        *,
        now: datetime | None = None,
        active_record_ids: set[str] | None = None,
        pinned_record_ids: set[str] | None = None,
    ) -> DlqRetentionPlan:
        instant = now or datetime.now(UTC)
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("DLQ retention time must be offset-aware")
        active = active_record_ids or set()
        pinned = pinned_record_ids or set()
        records = self._store.all_records()
        snapshot = _retention_snapshot(self._store, records)
        snapshot_fingerprint = canonical_fingerprint({"schema": "dpone.dlq-retention-snapshot.v1", "records": snapshot})
        delete: list[str] = []
        protected: list[str] = []
        for record, state in zip(records, snapshot, strict=True):
            expired = datetime.fromisoformat(record.expires_at.replace("Z", "+00:00")) <= instant
            is_protected = (
                record.record_id in active
                or record.record_id in pinned
                or not bool(state["acknowledged"])
                or not expired
            )
            (protected if is_protected else delete).append(record.record_id)
        payload = {
            "schema": "dpone.dlq-retention-plan.v1",
            "snapshot_fingerprint": snapshot_fingerprint,
            "delete_record_ids": sorted(delete),
            "protected_record_ids": sorted(protected),
        }
        return DlqRetentionPlan(
            plan_id=canonical_fingerprint(payload),
            snapshot_fingerprint=snapshot_fingerprint,
            delete_record_ids=tuple(payload["delete_record_ids"]),
            protected_record_ids=tuple(payload["protected_record_ids"]),
            planned_at=instant.isoformat(),
        )

    def apply(
        self,
        plan: DlqRetentionPlan,
        *,
        active_record_ids: set[str] | None = None,
        pinned_record_ids: set[str] | None = None,
    ) -> DlqRetentionResult:
        newly_protected = (active_record_ids or set()) | (pinned_record_ids or set())
        if newly_protected.intersection(plan.delete_record_ids):
            raise RuntimeError("DPONE_DLQ_RETENTION_RECORD_PROTECTED: protection changed after planning")
        records = self._store.all_records()
        snapshot = _retention_snapshot(self._store, records)
        current = canonical_fingerprint({"schema": "dpone.dlq-retention-snapshot.v1", "records": snapshot})
        if current != plan.snapshot_fingerprint:
            raise RuntimeError("DPONE_DLQ_RETENTION_SNAPSHOT_DRIFT: store changed after planning")
        by_id = {record.record_id: record for record in records}
        for record_id in plan.delete_record_ids:
            record = by_id.get(record_id)
            if record is None or not self._store.is_acknowledged(record):
                raise RuntimeError("DPONE_DLQ_RETENTION_RECORD_PROTECTED: pending record cannot be deleted")
        self._store.delete_many(plan.delete_record_ids)
        return DlqRetentionResult(plan_id=plan.plan_id, deleted_record_ids=plan.delete_record_ids)


def _retention_snapshot(store: DlqStore, records: tuple[DlqRecord, ...]) -> list[dict[str, object]]:
    return [
        {
            "id": record.record_id,
            "sha256": record.sha256,
            "acknowledged": store.is_acknowledged(record),
        }
        for record in records
    ]


def _replay_plan_payload(
    *,
    run_id: str,
    resolver_identity: str,
    target_identity: str,
    policy: DlqReplayPolicy,
    items: list[dict[str, str]],
) -> dict[str, object]:
    return {
        "schema": "dpone.dlq-replay-plan.v1",
        "run_id": run_id,
        "resolver_identity": resolver_identity,
        "target_identity": target_identity,
        "policy": policy.to_dict(),
        "items": items,
    }


def _validate_replay_identity(value: str, kind: str) -> None:
    if not value or len(value) > 512 or any(ord(char) < 32 for char in value):
        raise ValueError(f"DLQ replay {kind}_identity is invalid")


__all__ = [
    "DlqReplayItem",
    "DlqReplayPlan",
    "DlqReplayPolicy",
    "DlqReplayResult",
    "DlqReplayService",
    "DlqRetentionPlan",
    "DlqRetentionResult",
    "DlqRetentionService",
    "DlqService",
]
