"""Acquire one complete, canonical PR Gate shadow reconciliation observation."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone

from dpone.contracts.ci_shadow_reconciliation import AcquiredObservation, ObservationInterval, ReconciliationPolicy
from dpone.ports.ci_shadow_reconciliation import CiShadowReconciliationProvider
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_observation import ObservationError, acquire_workflow_runs
from dpone.services.ci.shadow_reconciliation_receipts import AuditReceipt, acquire_audit_receipts
from dpone.services.ci.shadow_reconciliation_tree import acquire_run_tree


def acquire_complete_observation(
    provider: CiShadowReconciliationProvider,
    *,
    archive_fetcher: Callable[[int], bytes],
    policy: ReconciliationPolicy,
    interval: ObservationInterval,
    budget: RequestBudget,
    utc_clock: Callable[[], datetime],
) -> AcquiredObservation:
    """Read every declared provider input once and reject cross-window ambiguity."""

    started = interval.observation_started_at
    producer_runs = acquire_workflow_runs(
        provider,
        workflow_id=policy.producer_workflow_id,
        event="pull_request",
        interval=interval,
        budget=budget,
        auditor=False,
    )
    auditor_runs = acquire_workflow_runs(
        provider,
        workflow_id=policy.auditor_workflow_id,
        event="workflow_run",
        interval=interval,
        upper_bound=interval.observation_started_at,
        budget=budget,
        auditor=True,
    )
    producer_tree = acquire_run_tree(provider, runs=producer_runs, budget=budget, auditor=False)
    auditor_tree = acquire_run_tree(provider, runs=auditor_runs, budget=budget, auditor=True)
    receipts = acquire_audit_receipts(
        auditor_tree.artifact_inventories,
        archive_fetcher=archive_fetcher,
        provider=provider,
        policy=policy,
        budget=budget,
    )
    _require_receipt_cross_window_coverage(receipts, producer_tree.attempt_identities, interval)
    observed_through = _whole_second(utc_clock())
    canonical = json.dumps(
        {
            "interval": {
                "scan_from": _timestamp(interval.scan_from),
                "safe_scan_through": _timestamp(interval.safe_scan_through),
                "observation_started_at": _timestamp(interval.observation_started_at),
            },
            "producer_tree": base64.b64encode(producer_tree.canonical_bytes).decode("ascii"),
            "auditor_tree": base64.b64encode(auditor_tree.canonical_bytes).decode("ascii"),
            "receipts": [
                {
                    "auditor_run_id": receipt.auditor_run_id,
                    "auditor_attempt": receipt.auditor_attempt,
                    "producer_repository_id": receipt.producer_repository_id,
                    "producer_run_id": receipt.producer_run_id,
                    "producer_attempt": receipt.producer_attempt,
                    "payload": base64.b64encode(receipt.canonical_payload).decode("ascii"),
                    "exact_producer": base64.b64encode(receipt.exact_producer_payload).decode("ascii"),
                }
                for receipt in receipts
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return AcquiredObservation(
        canonical_bytes=canonical,
        complete=True,
        observation_started_at=started,
        evidence_observed_through=observed_through,
    )


def _require_receipt_cross_window_coverage(
    receipts: tuple[AuditReceipt, ...], producer_attempts: tuple[tuple[int, int], ...], interval: ObservationInterval
) -> None:
    known = set(producer_attempts)
    for receipt in receipts:
        record = _object(receipt.exact_producer_payload, "exact producer run")
        created = _timestamp_value(record.get("created_at"))
        key = (receipt.producer_run_id, receipt.producer_attempt)
        if created < interval.scan_from:
            continue
        if created <= interval.safe_scan_through:
            if key not in known:
                raise ObservationError("producer query omitted an in-window audited attempt")
            continue
        if created <= interval.observation_started_at:
            continue
        raise ObservationError("audited producer run was created after the observation started")


def _object(payload: bytes, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ObservationError(f"{label} serialization is invalid") from exc
    if not isinstance(value, Mapping):
        raise ObservationError(f"{label} serialization is malformed")
    return value


def _timestamp_value(value: object) -> datetime:
    if not isinstance(value, str):
        raise ObservationError("producer creation time is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ObservationError("producer creation time is malformed") from exc
    return _whole_second(parsed)


def _whole_second(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ObservationError("observation clock must be whole-second UTC")
    return value.astimezone(timezone.utc).replace(microsecond=0)  # noqa: UP017


def _timestamp(value: datetime) -> str:
    return _whole_second(value).isoformat().replace("+00:00", "Z")


__all__ = ["acquire_complete_observation"]
