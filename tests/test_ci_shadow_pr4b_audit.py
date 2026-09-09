from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from http.client import IncompleteRead
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from dpone.adapters.ci_shadow_audit_github import GitHubAuditApi
from dpone.ports.ci_shadow_audit import CiShadowAuditProvider, CiShadowAuditProviderError
from dpone.services.ci.shadow import canonical_bytes
from dpone.services.ci.shadow_audit import audit_event
from dpone.services.ci.shadow_bundle import load_bundle_digest

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40
ROOT = Path(__file__).resolve().parents[1]
BUNDLE_DIGEST = load_bundle_digest(ROOT / ".agents/policy/ci-shadow-bundle-v1.yml")
TEST_BUNDLE = (
    {
        "path": ".github/workflows/pr-gate-shadow.yml",
        "role": "EXECUTION_WORKFLOW",
        "sha256": hashlib.sha256(b"workflow").hexdigest(),
    },
)


class Provider:
    def __init__(self) -> None:
        self.records = [_record()]

    def list_open_pull_requests(self) -> list[Mapping[str, object]]:
        return list(self.records)

    def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
        assert run_id == 10
        return {
            "repository_id": 1,
            "run_id": 10,
            "run_attempt": 1,
            "head_repository_id": 2,
            "head_branch": "feature/audit",
            "head_sha": SHA_B,
            "status": "completed",
            "conclusion": "success",
            "event": "pull_request",
            "path": ".github/workflows/pr-gate-shadow.yml",
            "workflow_id": 10,
        }

    def list_attempt_jobs(self, run_id: int, attempt: int) -> list[Mapping[str, object]]:
        assert (run_id, attempt) == (10, 1)
        return [{"name": name, "status": "completed", "conclusion": "success"} for name in _JOB_NAMES]

    def get_git_tree(self, commit_sha: str) -> Mapping[str, Mapping[str, object]]:
        assert commit_sha in {SHA_A, SHA_B, SHA_C}
        return {
            ".github/workflows/pr-gate-shadow.yml": {
                "sha": "d" * 40,
                "mode": "100644",
                "type": "blob",
            }
        }

    def get_git_blob(self, blob_sha: str) -> bytes:
        assert blob_sha == "d" * 40
        return b"workflow"

    def get_pull_request(self, number: int) -> Mapping[str, object]:
        assert number == 7
        return self.records[0]

    def resolve_pull_merge_ref(self, number: int) -> str:
        assert number == 7
        return SHA_C

    def get_commit_parents(self, sha: str) -> list[str]:
        assert sha == SHA_C
        return [SHA_A, SHA_B]


def _record() -> dict[str, object]:
    return {
        "number": 7,
        "repository_id": 1,
        "base_repository_id": 1,
        "head_repository_id": 2,
        "base_ref": "master",
        "head_ref": "feature/audit",
        "base_sha": SHA_A,
        "head_sha": SHA_B,
        "mergeable": True,
        "merge_commit_sha": SHA_C,
        "state": "open",
    }


def _event(*, conclusion: str = "success", claims_merge_sha: str = SHA_C) -> dict[str, object]:
    return {
        "workflow_run": {
            "repository_id": 1,
            "run_id": 10,
            "run_attempt": 1,
            "head_repository_id": 2,
            "head_branch": "feature/audit",
            "head_sha": SHA_B,
            "status": "completed",
            "conclusion": conclusion,
        },
        "claims": {
            "repository_id": 1,
            "pr_number": 7,
            "run_id": 10,
            "run_attempt": 1,
            "base_sha": SHA_A,
            "head_sha": SHA_B,
            "merge_sha": claims_merge_sha,
            "implementation_bundle_digest": BUNDLE_DIGEST,
            "jobs": {name: {"selection": "RUN", "outcome": "PASS"} for name in _JOB_PRODUCTS},
            "status": "PASS",
        },
        "auditor": {"workflow_id": 20, "run_id": 30, "run_attempt": 1, "revision": SHA_A},
    }


_JOB_PRODUCTS = (
    "static",
    "contracts",
    "docs",
    "python-3.11",
    "python-3.12",
    "packaging",
    "postgresql",
    "airflow",
    "runtime-wheel-smoke",
)
_JOB_NAMES = (
    "PR Gate shadow static",
    "PR Gate shadow contracts",
    "PR Gate shadow docs",
    "PR Gate shadow Python 3.11",
    "PR Gate shadow Python 3.12",
    "PR Gate shadow packaging",
    "PR Gate shadow PostgreSQL XMin",
    "PR Gate shadow Airflow 2.10.5 / py3.11",
    "PR Gate shadow Airflow 2.10.5 / py3.12",
    "PR Gate shadow Airflow 2.11.0 / py3.11",
    "PR Gate shadow Airflow 2.11.0 / py3.12",
    "PR Gate shadow Airflow 3.2.0 / py3.11",
    "PR Gate shadow Airflow 3.2.0 / py3.12",
    "PR Gate shadow Airflow 3.3.0 / py3.11",
    "PR Gate shadow Airflow 3.3.0 / py3.12",
    "PR Gate shadow runtime wheel smoke",
)


def test_stable_current_merge_tuple_produces_claims_bound_pass_receipt() -> None:
    class CountingProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.exact_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, object]:
            self.exact_reads += 1
            return super().get_pull_request(number)

    provider = CountingProvider()
    result = _audit(_event(), provider)

    assert result is not None
    assert result.decision == "PASS"
    assert result.receipt["audit_stage"] == "CLAIMS_BOUND"
    assert result.receipt["blockers"] == []
    assert provider.exact_reads == 4


def test_final_linearization_requires_a_second_stable_observation() -> None:
    class DriftingFinalProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.exact_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, object]:
            self.exact_reads += 1
            record = dict(super().get_pull_request(number))
            if self.exact_reads >= 4:
                record["head_ref"] = "feature/moved"
            return record

    result = _audit(_event(), DriftingFinalProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["audit_stage"] == "JOBS_BOUND"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]


def test_read_only_claim_and_job_mappings_preserve_legacy_compatibility() -> None:
    event = _event()
    event_claims = event["claims"]
    assert isinstance(event_claims, Mapping)
    claims = dict(event_claims)
    jobs = claims["jobs"]
    assert isinstance(jobs, Mapping)
    read_only_jobs: dict[object, Mapping[object, object]] = {}
    for name, entry in jobs.items():
        assert isinstance(entry, Mapping)
        read_only_jobs[name] = MappingProxyType(dict(entry))
    claims["jobs"] = MappingProxyType(read_only_jobs)
    event["claims"] = MappingProxyType(claims)

    result = _audit(event, Provider())

    assert result is not None
    assert result.decision == "PASS"
    assert result.receipt["audit_stage"] == "CLAIMS_BOUND"


def test_moved_or_forged_merge_identity_is_unverified() -> None:
    result = _audit(_event(claims_merge_sha=SHA_A), Provider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]


def test_nullable_list_mergeability_uses_exact_pull_record() -> None:
    class NullableListProvider(Provider):
        def list_open_pull_requests(self) -> list[Mapping[str, object]]:
            record = dict(super().list_open_pull_requests()[0])
            record["mergeable"] = None
            record["merge_commit_sha"] = None
            return [record]

    result = _audit(_event(), NullableListProvider())

    assert result is not None
    assert result.decision == "PASS"
    assert result.receipt["audit_stage"] == "CLAIMS_BOUND"
    expected_digest = "sha256:" + hashlib.sha256(canonical_bytes([_record()])).hexdigest()
    assert result.receipt["producer_identity"]["eligible_set_digest"] == expected_digest


def test_exact_pull_must_match_enumerated_candidate_identity() -> None:
    class DriftedExactProvider(Provider):
        def get_pull_request(self, number: int) -> Mapping[str, object]:
            record = dict(super().get_pull_request(number))
            record["head_ref"] = "feature/moved"
            return record

    result = _audit(_event(), DriftedExactProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]


def test_provider_api_failure_is_persistable_unverified_evidence() -> None:
    class UnavailableMergeRefProvider(Provider):
        def resolve_pull_merge_ref(self, number: int) -> str:
            raise CiShadowAuditProviderError("provider unavailable")

    result = _audit(_event(), UnavailableMergeRefProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]
    assert result.receipt["audit_stage"] == "EVENT_RECEIVED"
    assert result.receipt["producer_identity"] is None


def test_pending_exact_mergeability_is_bounded_and_retried() -> None:
    class InitiallyPendingProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.exact_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, object]:
            self.exact_reads += 1
            record = dict(super().get_pull_request(number))
            if self.exact_reads == 1:
                record["mergeable"] = None
                record["merge_commit_sha"] = None
            return record

    provider = InitiallyPendingProvider()
    delays: list[float] = []
    result = _audit(_event(), provider, wait=delays.append)

    assert result is not None
    assert result.decision == "PASS"
    assert provider.exact_reads == 5
    assert delays == [1.0]


def test_pending_observation_breaks_consecutive_stability() -> None:
    class InterruptedStabilityProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.exact_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, object]:
            self.exact_reads += 1
            record = dict(super().get_pull_request(number))
            if self.exact_reads == 2:
                record["mergeable"] = None
                record["merge_commit_sha"] = None
            return record

    provider = InterruptedStabilityProvider()
    result = _audit(_event(), provider)

    assert result is not None
    assert result.decision == "PASS"
    assert provider.exact_reads == 6


def test_persistently_pending_mergeability_stops_at_observation_bound() -> None:
    class PendingProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.exact_reads = 0

        def get_pull_request(self, number: int) -> Mapping[str, object]:
            self.exact_reads += 1
            record = dict(super().get_pull_request(number))
            record["mergeable"] = None
            record["merge_commit_sha"] = None
            return record

    provider = PendingProvider()
    delays: list[float] = []
    result = _audit(_event(), provider, wait=delays.append)

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]
    assert provider.exact_reads == 5
    assert delays == [1.0, 1.0, 1.0, 1.0]


@pytest.mark.parametrize("finished_at", [30.0, 30.000001])
def test_observation_finishing_at_or_after_deadline_is_unverified(finished_at: float) -> None:
    class Clock:
        now = 0.0

        def __call__(self) -> float:
            return self.now

    clock = Clock()

    class LateProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.list_reads = 0

        def list_open_pull_requests(self) -> list[Mapping[str, object]]:
            self.list_reads += 1
            if self.list_reads == 2:
                clock.now = finished_at
            return super().list_open_pull_requests()

    result = _audit(_event(), LateProvider(), clock=clock)

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]


def test_pending_wait_is_capped_by_remaining_deadline() -> None:
    class Clock:
        now = 0.0

        def __call__(self) -> float:
            return self.now

        def wait(self, delay: float) -> None:
            delays.append(delay)
            self.now += delay

    clock = Clock()
    delays: list[float] = []

    class NearDeadlineProvider(Provider):
        def get_pull_request(self, number: int) -> Mapping[str, object]:
            record = dict(super().get_pull_request(number))
            clock.now = 29.5
            record["mergeable"] = None
            record["merge_commit_sha"] = None
            return record

    result = _audit(_event(), NearDeadlineProvider(), clock=clock, wait=clock.wait)

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert delays == [0.5]


def test_run_api_failure_uses_run_blocker() -> None:
    class UnavailableRunProvider(Provider):
        def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
            raise CiShadowAuditProviderError("provider unavailable")

    result = _audit(_event(), UnavailableRunProvider())

    assert result is not None
    assert result.receipt["blockers"] == ["AUDITOR_RUN_UNVERIFIED"]
    assert result.receipt["audit_stage"] == "EVENT_RECEIVED"
    assert result.receipt["producer_identity"] is None


def test_truncated_github_response_is_persistable_unverified_evidence() -> None:
    class TruncatedResponse:
        def __enter__(self) -> TruncatedResponse:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self, _: int) -> bytes:
            raise IncompleteRead(b"{", 10)

    def opener(request: Any, *, timeout: float) -> TruncatedResponse:
        assert request.full_url.endswith("/actions/runs/10")
        assert timeout == 10.0
        return TruncatedResponse()

    provider = GitHubAuditApi(repository="owner/repo", token="token", opener=opener)
    result = _audit(_event(), provider)

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_RUN_UNVERIFIED"]


def test_bundle_api_failure_uses_bundle_blocker() -> None:
    class UnavailableBundleProvider(Provider):
        def get_git_tree(self, commit_sha: str) -> Mapping[str, Mapping[str, object]]:
            raise CiShadowAuditProviderError("provider unavailable")

    result = _audit(_event(), UnavailableBundleProvider())

    assert result is not None
    assert result.receipt["blockers"] == ["AUDITOR_BUNDLE_UNAPPROVED"]
    assert result.receipt["audit_stage"] == "RUN_AUTHENTICATED"
    assert result.receipt["producer_identity"] is not None


def test_jobs_api_failure_uses_jobs_blocker() -> None:
    class UnavailableJobsProvider(Provider):
        def list_attempt_jobs(self, run_id: int, attempt: int) -> list[Mapping[str, object]]:
            raise CiShadowAuditProviderError("provider unavailable")

    result = _audit(_event(), UnavailableJobsProvider())

    assert result is not None
    assert result.receipt["blockers"] == ["AUDITOR_JOBS_UNVERIFIED"]
    assert result.receipt["audit_stage"] == "RUN_AUTHENTICATED"
    assert result.receipt["producer_identity"] is not None


def test_incomplete_jobs_evidence_is_jobs_bound_unverified_with_blocker() -> None:
    class IncompleteJobsProvider(Provider):
        def list_attempt_jobs(self, run_id: int, attempt: int) -> list[Mapping[str, object]]:
            return [job for job in super().list_attempt_jobs(run_id, attempt) if job["name"] != _JOB_NAMES[0]]

    event = _event()
    event["claims"]["jobs"]["static"]["outcome"] = "UNVERIFIED"  # type: ignore[index]
    event["claims"]["status"] = "UNVERIFIED"  # type: ignore[index]

    result = _audit(event, IncompleteJobsProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["audit_stage"] == "JOBS_BOUND"
    assert result.receipt["blockers"] == ["AUDITOR_JOBS_UNVERIFIED"]


def test_linearization_api_failure_preserves_jobs_bound_identity() -> None:
    class UnavailableLinearizationProvider(Provider):
        def __init__(self) -> None:
            super().__init__()
            self.list_reads = 0

        def list_open_pull_requests(self) -> list[Mapping[str, object]]:
            self.list_reads += 1
            if self.list_reads == 3:
                raise CiShadowAuditProviderError("provider unavailable")
            return super().list_open_pull_requests()

    result = _audit(_event(), UnavailableLinearizationProvider())

    assert result is not None
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]
    assert result.receipt["audit_stage"] == "JOBS_BOUND"
    assert result.receipt["producer_identity"] is not None


def test_provider_run_identity_mismatch_is_unverified() -> None:
    class MismatchedRunProvider(Provider):
        def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
            record = dict(super().get_workflow_run(run_id))
            record["head_sha"] = SHA_A
            return record

    result = _audit(_event(), MismatchedRunProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_RUN_UNVERIFIED"]


@pytest.mark.parametrize(
    ("field", "value"),
    (("head_repository_id", 999), ("head_branch", "feature/moved")),
)
def test_authenticated_run_must_match_current_pull_request_source(field: str, value: object) -> None:
    class ForeignSourceRunProvider(Provider):
        def get_workflow_run(self, run_id: int) -> Mapping[str, object]:
            record = dict(super().get_workflow_run(run_id))
            record[field] = value
            return record

    event = _event()
    event["workflow_run"][field] = value  # type: ignore[index]

    result = _audit(event, ForeignSourceRunProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_MERGE_REF_UNVERIFIED"]


def test_forged_claimed_job_outcome_is_unverified() -> None:
    event = _event()
    event["claims"]["jobs"]["static"]["outcome"] = "FAIL"  # type: ignore[index]

    result = _audit(event, Provider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_JOBS_UNVERIFIED"]


def test_changed_subject_control_plane_blob_is_unverified() -> None:
    class ChangedBundleProvider(Provider):
        def get_git_blob(self, blob_sha: str) -> bytes:
            assert blob_sha == "d" * 40
            return b"changed workflow"

    result = _audit(_event(), ChangedBundleProvider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_BUNDLE_UNAPPROVED"]


def test_missing_claims_are_persistable_unverified_evidence() -> None:
    event = _event()
    event["claims"] = {}

    result = _audit(event, Provider())

    assert result is not None
    assert result.decision == "UNVERIFIED"
    assert result.receipt["blockers"] == ["AUDITOR_CLAIMS_UNVERIFIED"]


def test_action_required_is_receipt_free_telemetry() -> None:
    assert _audit(_event(conclusion="action_required"), Provider()) is None


def test_ambiguous_open_pull_request_set_is_unverified() -> None:
    provider = Provider()
    twin = _record()
    twin["number"] = 8
    provider.records.append(twin)

    result = _audit(_event(), provider)

    assert result is not None
    assert result.decision == "UNVERIFIED"


def test_audit_receipts_conform_to_the_closed_v1_schema() -> None:
    result = _audit(_event(), Provider())
    assert result is not None
    schema = json.loads((ROOT / "docs/schemas/cicd/pr-gate-shadow-audit-v1.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(result.receipt)


def _audit(
    event: Mapping[str, object],
    provider: CiShadowAuditProvider,
    *,
    wait: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
):
    wait_fn = wait if callable(wait) else lambda _: None
    clock_fn = clock if callable(clock) else lambda: 0.0
    return audit_event(
        event,
        provider=provider,
        expected_bundle_digest=BUNDLE_DIGEST,
        trusted_bundle=TEST_BUNDLE,
        clock=clock_fn,
        wait=wait_fn,
    )
