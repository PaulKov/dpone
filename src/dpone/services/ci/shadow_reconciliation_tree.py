"""Acquire complete immutable attempt, job, and artifact trees for one run slice."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from dpone.ports.ci_shadow_reconciliation import CiShadowReconciliationProvider
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget, RequestClass
from dpone.services.ci.shadow_reconciliation_observation import ObservationError, WorkflowRunSlice

_PAGE_SIZE = 100


@dataclass(frozen=True)
class WorkflowRunTree:
    """Canonical, complete provider inputs rooted at an acquired run slice."""

    canonical_bytes: bytes
    run_count: int
    attempt_count: int
    artifact_inventories: tuple[AttemptArtifactInventory, ...]
    attempt_identities: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class AttemptArtifactInventory:
    """Complete artifact metadata for one immutable workflow-run attempt."""

    run_id: int
    attempt: int
    artifacts: tuple[dict[str, object], ...]


def acquire_run_tree(
    provider: CiShadowReconciliationProvider,
    *,
    runs: WorkflowRunSlice,
    budget: RequestBudget,
    auditor: bool,
) -> WorkflowRunTree:
    """Expand each listed run into exact attempts plus all jobs and artifacts.

    A list response is mutable and may omit historical reruns.  The expansion
    therefore independently reads attempts ``1..run_attempt`` and rejects a
    non-terminal result rather than treating a partial tree as complete.
    """

    entries: list[dict[str, object]] = []
    inventories: list[AttemptArtifactInventory] = []
    identities: list[tuple[int, int]] = []
    attempt_count = 0
    for run in runs.records:
        run_id, highest_attempt = _run_identity(run)
        attempts = [_attempt(provider, run_id, number, budget) for number in range(1, highest_attempt + 1)]
        artifacts = _all_pages(provider, run_id, 1, budget, jobs=False)
        attempt_count += len(attempts)
        attempt_entries: list[dict[str, object]] = []
        for number, attempt in enumerate(attempts, start=1):
            identities.append((run_id, number))
            inventories.append(AttemptArtifactInventory(run_id, number, tuple(artifacts)))
            attempt_entries.append({"record": attempt, "jobs": _all_pages(provider, run_id, number, budget, jobs=True)})
        entries.append({"run": run, "attempts": attempt_entries, "artifacts": artifacts})
    budget.record_observed_runs(
        producer=0 if auditor else len(runs.records),
        auditor=len(runs.records) if auditor else 0,
        attempts=attempt_count,
    )
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return WorkflowRunTree(
        canonical_bytes=canonical,
        run_count=len(runs.records),
        attempt_count=attempt_count,
        artifact_inventories=tuple(inventories),
        attempt_identities=tuple(identities),
    )


def _attempt(
    provider: CiShadowReconciliationProvider, run_id: int, attempt: int, budget: RequestBudget
) -> dict[str, object]:
    payload = budget.dispatch(
        "attempt_run_requests",
        lambda timeout: provider.get_workflow_run_attempt(
            run_id=run_id,
            attempt=attempt,
            timeout_seconds=timeout,
            max_response_bytes=budget.remaining_response_bytes,
        ),
    )
    record = _object(payload, "workflow-run attempt")
    if record.get("id") != run_id or record.get("run_attempt") != attempt or record.get("status") != "completed":
        raise ObservationError("workflow-run attempt is not terminal or has a changed identity")
    return record


def _all_pages(
    provider: CiShadowReconciliationProvider, run_id: int, attempt: int, budget: RequestBudget, *, jobs: bool
) -> list[dict[str, object]]:
    name = "jobs" if jobs else "artifacts"
    request_class = cast(RequestClass, "jobs_page_requests" if jobs else "artifact_metadata_requests")
    expected_total: int | None = None
    pages: list[dict[str, object]] = []
    for page in range(1, 10_001):
        payload = budget.dispatch(
            request_class,
            lambda timeout: _provider_page(
                provider, run_id, attempt, page, timeout, budget.remaining_response_bytes, jobs
            ),
        )
        decoded = _object(payload, name)
        if set(decoded) != {"total_count", name}:
            raise ObservationError(f"{name} page serialization is malformed")
        total = decoded["total_count"]
        records = decoded[name]
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ObservationError(f"{name} total count is malformed")
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise ObservationError(f"{name} records are malformed")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise ObservationError(f"{name} page total count changed during acquisition")
        pages.extend(records)
        if len(pages) >= total:
            if len(pages) != total:
                raise ObservationError(f"{name} pagination is incomplete")
            return pages
        if len(records) != _PAGE_SIZE:
            raise ObservationError(f"{name} pagination is incomplete")
    raise ObservationError(f"{name} pagination exceeds the approved bound")


def _provider_page(
    provider: CiShadowReconciliationProvider,
    run_id: int,
    attempt: int,
    page: int,
    timeout: float,
    maximum: int,
    jobs: bool,
) -> bytes:
    if jobs:
        return provider.list_attempt_jobs(
            run_id=run_id,
            attempt=attempt,
            page=page,
            timeout_seconds=timeout,
            max_response_bytes=maximum,
        )
    return provider.list_run_artifacts(
        run_id=run_id,
        page=page,
        timeout_seconds=timeout,
        max_response_bytes=maximum,
    )


def _object(payload: bytes, name: str) -> dict[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ObservationError(f"{name} serialization is invalid") from exc
    if not isinstance(value, dict):
        raise ObservationError(f"{name} serialization is malformed")
    return value


def _run_identity(record: Mapping[str, object]) -> tuple[int, int]:
    values = (record.get("id"), record.get("run_attempt"))
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 1 for value in values):
        raise ObservationError("workflow-run identity is invalid")
    return values[0], values[1]  # type: ignore[return-value]


__all__ = ["AttemptArtifactInventory", "WorkflowRunTree", "acquire_run_tree"]
