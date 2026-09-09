from __future__ import annotations

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1, ReconciliationPolicyV2
from dpone.services.ci.shadow_reconciliation_budget import (
    RequestBudget,
    ResourceLimitExceeded,
)


class _Clock:
    def __init__(self, values: list[float]) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


def test_budget_counts_before_dispatch_and_never_sends_3001st_request() -> None:
    sent = 0
    budget = RequestBudget(policy=ReconciliationPolicyV2.fixed(), monotonic_clock=_Clock([0.0] * 3_004))

    def dispatch(timeout: float) -> bytes:
        nonlocal sent
        sent += 1
        assert timeout == 900
        return b"x"

    for _ in range(3_000):
        budget.dispatch("producer_list_page_requests", dispatch)
    with pytest.raises(ResourceLimitExceeded, match="total_http_requests"):
        budget.dispatch("producer_list_page_requests", dispatch)

    assert sent == 3_000
    assert budget.counters()["total_http_requests"] == 3_000
    assert budget.limits_crossed == ("total_http_requests",)


def test_budget_counts_response_bytes_and_saturates_after_bounded_overflow() -> None:
    policy = ReconciliationPolicyV1.fixed()
    budget = RequestBudget(policy=policy, monotonic_clock=_Clock([0.0, 0.0]))

    with pytest.raises(ResourceLimitExceeded, match="total_response_body_bytes"):
        budget.record_response_bytes("artifact_download_body_bytes", policy.hard_max_response_bytes + 1)

    counters = budget.counters()
    assert counters["artifact_download_body_bytes"] == policy.hard_max_response_bytes
    assert counters["total_response_body_bytes"] == policy.hard_max_response_bytes
    assert budget.limits_crossed == ("total_response_body_bytes",)


def test_budget_rejects_dispatch_after_wall_time_expires_without_calling_transport() -> None:
    called = False
    budget = RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=_Clock([0.0, 900.0, 900.0]))

    def dispatch(_: float) -> bytes:
        nonlocal called
        called = True
        return b"x"

    with pytest.raises(ResourceLimitExceeded, match="execution_wall_seconds"):
        budget.dispatch("jobs_page_requests", dispatch)

    assert called is False
    assert budget.counters()["execution_wall_seconds"] == 900
    assert budget.limits_crossed == ("execution_wall_seconds",)


def test_retry_is_a_subset_not_an_additional_request_class() -> None:
    budget = RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=_Clock([0.0, 0.0, 0.0]))

    def invoked(_: float) -> bytes:
        return b"x"

    budget.dispatch("git_object_requests", invoked, retry=True)

    counters = budget.counters()
    assert counters["total_http_requests"] == 1
    assert counters["git_object_requests"] == 1
    assert counters["retry_requests"] == 1


def test_non_json_response_dispatch_retains_metadata_and_attributes_artifact_bytes() -> None:
    budget = RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=_Clock([0.0, 0.0, 0.0]))

    response = budget.dispatch_response(
        "artifact_download_requests",
        lambda _: ({"status": 302}, b"redirect"),
        response_class="artifact_download_body_bytes",
    )

    assert response == {"status": 302}
    counters = budget.counters()
    assert counters["artifact_download_requests"] == 1
    assert counters["artifact_download_body_bytes"] == len(b"redirect")
