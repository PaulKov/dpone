from __future__ import annotations

import json

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_observation import ObservationError, WorkflowRunSlice
from dpone.services.ci.shadow_reconciliation_tree import acquire_run_tree


class _Provider:
    def get_workflow_run_attempt(self, **kwargs: object) -> bytes:
        return _json({"id": kwargs["run_id"], "run_attempt": kwargs["attempt"], "status": "completed"})

    def list_attempt_jobs(self, **kwargs: object) -> bytes:
        return _json({"total_count": 1, "jobs": [{"id": 11}]})

    def list_run_artifacts(self, **kwargs: object) -> bytes:
        return _json({"total_count": 1, "artifacts": [{"id": 12}]})


def _json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _budget() -> RequestBudget:
    return RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)


def test_tree_expands_all_attempts_jobs_and_artifacts_under_distinct_meters() -> None:
    runs = WorkflowRunSlice(records=({"id": 7, "run_attempt": 2},), canonical_bytes=b"runs")
    budget = _budget()

    tree = acquire_run_tree(_Provider(), runs=runs, budget=budget, auditor=False)

    assert tree.run_count == 1
    assert tree.attempt_count == 2
    assert b'"artifacts":[{"id":12}]' in tree.canonical_bytes
    counters = budget.counters()
    assert counters["producer_runs"] == 1
    assert counters["producer_attempts"] == 2
    assert counters["attempt_run_requests"] == 2
    assert counters["jobs_page_requests"] == 2
    assert counters["artifact_metadata_requests"] == 1


def test_tree_rejects_nonterminal_attempt_before_treating_tree_as_complete() -> None:
    class _Pending(_Provider):
        def get_workflow_run_attempt(self, **kwargs: object) -> bytes:
            return _json({"id": 7, "run_attempt": 1, "status": "in_progress"})

    with pytest.raises(ObservationError, match="not terminal"):
        acquire_run_tree(
            _Pending(),
            runs=WorkflowRunSlice(records=({"id": 7, "run_attempt": 1},), canonical_bytes=b"runs"),
            budget=_budget(),
            auditor=True,
        )
