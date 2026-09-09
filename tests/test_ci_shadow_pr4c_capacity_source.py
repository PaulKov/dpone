from __future__ import annotations

import json

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_capacity_source import (
    CapacitySourceAuthenticationError,
    authenticate_capacity_source,
)
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget


class _Provider:
    def __init__(self, run: dict[str, object]) -> None:
        self.run = run

    def get_workflow_run_attempt(self, **_: object) -> bytes:
        return json.dumps(self.run, separators=(",", ":")).encode("utf-8")


def _budget() -> RequestBudget:
    return RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)


def _event() -> dict[str, object]:
    return {"repository": {"id": 7, "full_name": "PaulKov/dpone"}}


def _environment() -> dict[str, str]:
    return {
        "GITHUB_REPOSITORY": "PaulKov/dpone",
        "GITHUB_RUN_ID": "11",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_WORKFLOW_REF": "PaulKov/dpone/.github/workflows/pr-gate-shadow-capacity.yml@refs/heads/master",
    }


def _run() -> dict[str, object]:
    return {
        "id": 11,
        "run_attempt": 2,
        "repository": {"id": 7},
        "event": "workflow_dispatch",
        "head_branch": "master",
        "head_sha": "a" * 40,
        "path": ".github/workflows/pr-gate-shadow-capacity.yml",
        "workflow_id": 13,
    }


def test_source_authentication_binds_runner_and_provider_identity() -> None:
    budget = _budget()

    source = authenticate_capacity_source(_event(), _environment(), provider=_Provider(_run()), budget=budget)

    assert source.repository_id == 7
    assert source.workflow_id == 13
    assert budget.counters()["attempt_run_requests"] == 1


def test_source_authentication_rejects_a_provider_source_mismatch() -> None:
    run = _run()
    run["head_sha"] = "b" * 40

    with pytest.raises(CapacitySourceAuthenticationError, match="does not match"):
        authenticate_capacity_source(_event(), _environment(), provider=_Provider(run), budget=_budget())
