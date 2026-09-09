"""Canonical bounded closed-second workflow-run acquisition for PR4C."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from dpone.contracts.ci_shadow_reconciliation import ObservationInterval
from dpone.ports.ci_shadow_reconciliation import CiShadowReconciliationProvider
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget, RequestClass

_PAGE_SIZE = 100
_SLICE_SPLIT_TRIGGER = 1_000
_MAX_PARTITION_DEPTH = 32


class ObservationError(ValueError):
    """Provider pagination, records, or closed-second partitioning is uncertain."""


@dataclass(frozen=True)
class WorkflowRunSlice:
    """One fully acquired workflow-run collection with deterministic identity bytes."""

    records: tuple[dict[str, object], ...]
    canonical_bytes: bytes


def acquire_workflow_runs(
    provider: CiShadowReconciliationProvider,
    *,
    workflow_id: int,
    event: str,
    interval: ObservationInterval,
    budget: RequestBudget,
    auditor: bool,
    upper_bound: datetime | None = None,
) -> WorkflowRunSlice:
    """Acquire one complete provider-observable closed interval without hidden gaps.

    A slice at the provider 1,000-result boundary is split recursively. Adjacent
    child ranges overlap at their midpoint by design; duplicate stable records
    must have byte-identical normalized content or acquisition is unverified.
    """

    if workflow_id < 1 or not event:
        raise ObservationError("workflow identity is invalid")
    _require_interval(interval)
    upper = upper_bound or interval.safe_scan_through
    if (
        upper.tzinfo is None
        or upper.utcoffset() != timedelta(0)
        or upper.microsecond
        or upper > interval.observation_started_at
    ):
        raise ObservationError("workflow-run upper bound is invalid")
    records = _acquire_slice(
        provider,
        workflow_id=workflow_id,
        event=event,
        lower=interval.scan_from,
        upper=upper,
        budget=budget,
        auditor=auditor,
        depth=0,
    )
    canonical = _canonical_records(records)
    return WorkflowRunSlice(records=tuple(records), canonical_bytes=canonical)


def _acquire_slice(
    provider: CiShadowReconciliationProvider,
    *,
    workflow_id: int,
    event: str,
    lower: datetime,
    upper: datetime,
    budget: RequestBudget,
    auditor: bool,
    depth: int,
) -> list[dict[str, object]]:
    if depth > _MAX_PARTITION_DEPTH:
        raise ObservationError("workflow-run partition depth exceeded")
    first = _page(provider, workflow_id, event, lower, upper, 1, budget, auditor)
    if first.total_count >= _SLICE_SPLIT_TRIGGER:
        if lower == upper:
            raise ObservationError("same-second workflow-run slice is irreducibly capped")
        left_upper = _midpoint(lower, upper)
        if left_upper == upper:
            raise ObservationError("workflow-run slice cannot be safely split")
        left = _acquire_slice(
            provider,
            workflow_id=workflow_id,
            event=event,
            lower=lower,
            upper=left_upper,
            budget=budget,
            auditor=auditor,
            depth=depth + 1,
        )
        right = _acquire_slice(
            provider,
            workflow_id=workflow_id,
            event=event,
            lower=left_upper,
            upper=upper,
            budget=budget,
            auditor=auditor,
            depth=depth + 1,
        )
        return _deduplicate_records(left + right)
    pages = [first]
    expected_pages = _pages_for(first.total_count)
    for page_number in range(2, expected_pages + 1):
        page = _page(provider, workflow_id, event, lower, upper, page_number, budget, auditor)
        if page.total_count != first.total_count:
            raise ObservationError("workflow-run page total count changed during acquisition")
        pages.append(page)
    records = [record for page in pages for record in page.records]
    if len(records) != first.total_count:
        raise ObservationError("workflow-run pagination is incomplete")
    return _deduplicate_records(records)


def _page(
    provider: CiShadowReconciliationProvider,
    workflow_id: int,
    event: str,
    lower: datetime,
    upper: datetime,
    page: int,
    budget: RequestBudget,
    auditor: bool,
) -> _WorkflowRunPage:
    request_class: RequestClass = "auditor_list_page_requests" if auditor else "producer_list_page_requests"
    response = budget.dispatch(
        request_class,
        lambda timeout: provider.list_workflow_runs(
            workflow_id=workflow_id,
            event=event,
            created_from=lower,
            created_to=upper,
            page=page,
            timeout_seconds=timeout,
            max_response_bytes=budget.remaining_response_bytes,
        ),
    )
    return _decode_page(response)


@dataclass(frozen=True)
class _WorkflowRunPage:
    records: Sequence[Mapping[str, object]]
    total_count: int


def _decode_page(payload: bytes) -> _WorkflowRunPage:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ObservationError("workflow-run page serialization is invalid") from exc
    if not isinstance(value, dict) or set(value) != {"total_count", "workflow_runs"}:
        raise ObservationError("workflow-run page serialization is malformed")
    records = value["workflow_runs"]
    total_count = value["total_count"]
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise ObservationError("workflow-run records are malformed")
    if not isinstance(total_count, int) or total_count < len(records):
        raise ObservationError("workflow-run total count is malformed")
    return _WorkflowRunPage(records=records, total_count=total_count)


def _deduplicate_records(records: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    result: dict[tuple[int, int], dict[str, object]] = {}
    for raw in records:
        record = dict(raw)
        key = _stable_key(record)
        previous = result.get(key)
        if previous is not None and _canonical_record(previous) != _canonical_record(record):
            raise ObservationError("overlapping workflow-run record changed")
        result[key] = record
    return [result[key] for key in sorted(result)]


def _stable_key(record: Mapping[str, object]) -> tuple[int, int]:
    run_id = record.get("id")
    attempt = record.get("run_attempt")
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 1:
        raise ObservationError("workflow-run id is invalid")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise ObservationError("workflow-run attempt is invalid")
    return run_id, attempt


def _canonical_records(records: Sequence[Mapping[str, object]]) -> bytes:
    return json.dumps([dict(record) for record in records], sort_keys=True, separators=(",", ":")).encode("utf-8")


def _canonical_record(record: Mapping[str, object]) -> bytes:
    return json.dumps(dict(record), sort_keys=True, separators=(",", ":")).encode("utf-8")


def _pages_for(total_count: int) -> int:
    return (total_count + _PAGE_SIZE - 1) // _PAGE_SIZE


def _midpoint(lower: datetime, upper: datetime) -> datetime:
    seconds = int((upper - lower).total_seconds())
    return lower + timedelta(seconds=seconds // 2)


def _require_interval(interval: ObservationInterval) -> None:
    values = (interval.scan_from, interval.safe_scan_through, interval.observation_started_at)
    if any(value.tzinfo is None or value.utcoffset() != timedelta(0) or value.microsecond for value in values):
        raise ObservationError("observation interval must use whole UTC seconds")
    if interval.safe_scan_through < interval.scan_from:
        raise ObservationError("observation interval is reversed")
    if interval.observation_started_at < interval.safe_scan_through:
        raise ObservationError("observation interval is not current")


__all__ = ["ObservationError", "WorkflowRunSlice", "acquire_workflow_runs"]
