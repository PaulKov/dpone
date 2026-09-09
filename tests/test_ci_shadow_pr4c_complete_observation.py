from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_capacity import calibrate_capacity
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_complete_observation import acquire_complete_observation


class _Provider:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, datetime, datetime]] = []

    def list_workflow_runs(self, **kwargs: object) -> bytes:
        self.calls.append(
            (kwargs["workflow_id"], kwargs["event"], kwargs["created_from"], kwargs["created_to"])  # type: ignore[arg-type]
        )
        return json.dumps({"total_count": 0, "workflow_runs": []}, separators=(",", ":")).encode("utf-8")


def test_complete_observation_uses_distinct_producer_and_auditor_scan_boundaries() -> None:
    observed_at = datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
    policy = ReconciliationPolicyV1.fixed()
    budget = RequestBudget(policy=policy, monotonic_clock=lambda: 0.0)
    provider = _Provider()

    observation = acquire_complete_observation(
        provider,
        archive_fetcher=lambda _: b"",
        policy=policy,
        interval=policy.interval_for(observed_at),
        budget=budget,
        utc_clock=lambda: observed_at,
    )

    assert observation.complete is True
    assert provider.calls == [
        (
            policy.producer_workflow_id,
            "pull_request",
            datetime(2026, 8, 13, 2, 45, tzinfo=UTC),
            datetime(2026, 8, 27, 2, 45, tzinfo=UTC),
        ),
        (policy.auditor_workflow_id, "workflow_run", datetime(2026, 8, 13, 2, 45, tzinfo=UTC), observed_at),
    ]
    assert budget.counters()["producer_list_page_requests"] == 1
    assert budget.counters()["auditor_list_page_requests"] == 1


def test_double_observation_binds_auditor_receipt_to_the_same_exact_producer_attempt() -> None:
    now = datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
    policy = ReconciliationPolicyV1.fixed()
    payload = json.dumps(
        {
            "schema_version": "dpone.pr-gate-shadow-audit.v1",
            "observed_event": {"repository_id": 1, "producer_run_id": 10, "producer_run_attempt": 1},
            "auditor_identity": {"run_id": 30, "run_attempt": 1},
        },
        separators=(",", ":"),
    ).encode("utf-8")
    archive = _zip(payload)

    class _CompleteProvider:
        def list_workflow_runs(self, **kwargs: object) -> bytes:
            if kwargs["workflow_id"] == policy.producer_workflow_id:
                records = [{"id": 10, "run_attempt": 1}]
            else:
                records = [{"id": 30, "run_attempt": 1}]
            return _json({"total_count": 1, "workflow_runs": records})

        def get_workflow_run_attempt(self, **kwargs: object) -> bytes:
            run_id = kwargs["run_id"]
            if run_id == 10:
                return _json(
                    {
                        "id": 10,
                        "run_attempt": 1,
                        "status": "completed",
                        "workflow_id": policy.producer_workflow_id,
                        "repository": {"id": 1},
                        "created_at": "2026-08-20T00:00:00Z",
                    }
                )
            return _json({"id": 30, "run_attempt": 1, "status": "completed", "workflow_id": policy.auditor_workflow_id})

        def list_attempt_jobs(self, **_: object) -> bytes:
            return _json({"total_count": 0, "jobs": []})

        def list_run_artifacts(self, **kwargs: object) -> bytes:
            if kwargs["run_id"] == 10:
                return _json({"total_count": 0, "artifacts": []})
            return _json(
                {
                    "total_count": 1,
                    "artifacts": [
                        {
                            "id": 40,
                            "name": "pr-gate-shadow-audit-10-1-30-1",
                            "expired": False,
                            "size_in_bytes": len(archive),
                            "digest": "sha256:" + hashlib.sha256(archive).hexdigest(),
                        }
                    ],
                }
            )

    budget = RequestBudget(policy=policy, monotonic_clock=lambda: 0.0)
    provider = _CompleteProvider()
    interval = policy.interval_for(now)
    calibration = calibrate_capacity(
        lambda: acquire_complete_observation(
            provider,
            archive_fetcher=lambda _: archive,
            policy=policy,
            interval=interval,
            budget=budget,
            utc_clock=lambda: now,
        ),
        budget=budget,
        policy=policy,
        utc_clock=lambda: now,
    )

    assert calibration.complete is True
    assert calibration.observations_match is True
    assert budget.counters()["exact_producer_run_requests"] == 2


def _json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _zip(payload: bytes) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("audit-receipt.json", payload)
    return stream.getvalue()
