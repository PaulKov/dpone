from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_observation import ObservationError, acquire_workflow_runs


class _Provider:
    def __init__(self, pages: dict[tuple[datetime, datetime, int], bytes]) -> None:
        self.pages = pages

    def list_workflow_runs(
        self,
        *,
        workflow_id: int,
        event: str,
        created_from: datetime,
        created_to: datetime,
        page: int,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> bytes:
        assert workflow_id == 11
        assert event == "pull_request"
        assert timeout_seconds > 0
        assert max_response_bytes > 0
        return self.pages[(created_from, created_to, page)]


def _record(run_id: int, attempt: int = 1) -> dict[str, object]:
    return {"id": run_id, "run_attempt": attempt, "status": "completed"}


def _page(records: list[dict[str, object]], total_count: int) -> bytes:
    return json.dumps({"total_count": total_count, "workflow_runs": records}, separators=(",", ":")).encode("utf-8")


def _interval():
    return ReconciliationPolicyV1.fixed().interval_for(datetime(2026, 8, 27, 3, 15, tzinfo=UTC))


def _budget() -> RequestBudget:
    return RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)


def test_acquisition_pages_a_complete_slice_and_canonicalizes_stable_records() -> None:
    interval = _interval()
    first_page = [_record(number) for number in range(1, 101)]
    second_page = [_record(number) for number in range(101, 151)]
    provider = _Provider(
        {
            (interval.scan_from, interval.safe_scan_through, 1): _page(first_page, 150),
            (interval.scan_from, interval.safe_scan_through, 2): _page(second_page, 150),
        }
    )

    result = acquire_workflow_runs(
        provider, workflow_id=11, event="pull_request", interval=interval, budget=_budget(), auditor=False
    )

    assert len(result.records) == 150
    assert result.records[0]["id"] == 1
    assert result.records[-1]["id"] == 150


def test_acquisition_rejects_changed_or_incomplete_pagination() -> None:
    interval = _interval()
    provider = _Provider(
        {
            (interval.scan_from, interval.safe_scan_through, 1): _page(
                [_record(number) for number in range(1, 101)], 101
            ),
            (interval.scan_from, interval.safe_scan_through, 2): _page([_record(101)], 102),
        }
    )

    with pytest.raises(ObservationError, match="total count changed"):
        acquire_workflow_runs(
            provider, workflow_id=11, event="pull_request", interval=interval, budget=_budget(), auditor=False
        )


def test_acquisition_splits_at_the_provider_boundary_and_deduplicates_overlap() -> None:
    interval = _interval()
    midpoint = interval.scan_from + (interval.safe_scan_through - interval.scan_from) / 2
    provider = _Provider(
        {
            (interval.scan_from, interval.safe_scan_through, 1): _page([], 1000),
            (interval.scan_from, midpoint, 1): _page([_record(1)], 1),
            (midpoint, interval.safe_scan_through, 1): _page([_record(1), _record(2)], 2),
        }
    )

    budget = _budget()
    result = acquire_workflow_runs(
        provider, workflow_id=11, event="pull_request", interval=interval, budget=budget, auditor=True
    )

    assert [record["id"] for record in result.records] == [1, 2]
    assert budget.counters()["auditor_list_page_requests"] == 3


def test_acquisition_rejects_irreducible_same_second_cap() -> None:
    instant = datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
    policy = ReconciliationPolicyV1.fixed()
    interval = policy.interval_for(instant)
    interval = type(interval)(instant, instant, instant)
    provider = _Provider({(instant, instant, 1): _page([], 1000)})

    with pytest.raises(ObservationError, match="irreducibly capped"):
        acquire_workflow_runs(
            provider, workflow_id=11, event="pull_request", interval=interval, budget=_budget(), auditor=False
        )
