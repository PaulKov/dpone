from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from dpone.adapters.airflow_rest_dbt_evidence import (
    AirflowRestDbtEvidenceAdapter,
)
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.services.dbt_dev_evidence_campaign import (
    DbtDevEvidenceCampaignError,
    DbtDevEvidenceCampaignService,
)
from dpone.services.dbt_dev_evidence_campaign_receipt import (
    DbtDevEvidenceCampaignReceipt,
)
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
)

EVIDENCE_SET_ID = "sha256:" + "a" * 64
RELEASE_ID = "sha256:" + "b" * 64
DEPLOYMENT_ID = "sha256:" + "c" * 64


def _request() -> DbtDevEvidenceRequest:
    workflows = (
        ("daily", "DAG__daily"),
        ("hourly", "DAG__hourly"),
    )
    identity = {
        "schema": "dpone.dbt-dev-evidence-request.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "producer_repository": "PaulKov/dpone",
        "producer_workflow": "workflow",
        "source_commit": "d" * 40,
        "orchestration_run_id": "123",
        "orchestration_run_attempt": 1,
        "workflows": [{"workflow_id": workflow_id, "dag_id": dag_id} for workflow_id, dag_id in workflows],
    }
    evidence_set_id = canonical_fingerprint(identity)
    return DbtDevEvidenceRequest.from_mapping(
        {
            **identity,
            "evidence_set_id": evidence_set_id,
            "workflows": [
                {
                    "workflow_id": workflow_id,
                    "dag_id": dag_id,
                    "dag_run_id": _run_id(
                        evidence_set_id,
                        workflow_id,
                    ),
                }
                for workflow_id, dag_id in workflows
            ],
        }
    )


def _run_id(evidence_set_id: str, workflow_id: str) -> str:
    suffix = hashlib.sha256(workflow_id.encode("utf-8")).hexdigest()[:12]
    return f"dpone_evidence__{evidence_set_id.removeprefix('sha256:')[:20]}__{suffix}"


class _Journal:
    def __init__(self) -> None:
        self.requests: list[DbtDevEvidenceRequest] = []
        self.receipts: list[DbtDevEvidenceCampaignReceipt] = []

    def open(self, request: DbtDevEvidenceRequest) -> None:
        self.requests.append(request)

    def close(self, receipt: DbtDevEvidenceCampaignReceipt) -> None:
        self.receipts.append(receipt)


class _Airflow:
    def __init__(self, states: Mapping[str, list[str]]) -> None:
        self.states = {key: list(value) for key, value in states.items()}
        self.triggered: list[tuple[str, str, dict[str, object]]] = []

    def trigger(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        conf: Mapping[str, object],
        timeout_seconds: float | None = None,
    ) -> None:
        assert timeout_seconds is None or timeout_seconds > 0
        self.triggered.append((dag_id, dag_run_id, dict(conf)))

    def state(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: float | None = None,
    ) -> str:
        del dag_id
        assert timeout_seconds is None or timeout_seconds > 0
        values = self.states[dag_run_id]
        return values.pop(0) if len(values) > 1 else values[0]


def test_campaign_triggers_every_workflow_and_waits_for_success() -> None:
    request = _request()
    run_ids = {item.workflow_id: item.dag_run_id for item in request.workflows}
    airflow = _Airflow(
        {
            run_ids["daily"]: ["running", "success"],
            run_ids["hourly"]: ["queued", "success"],
        }
    )
    journal = _Journal()
    now = iter(float(value) for value in range(9))
    service = DbtDevEvidenceCampaignService(
        airflow=airflow,
        journal=journal,
        monotonic=lambda: next(now),
        sleep=lambda _seconds: None,
    )

    report = service.run(
        request,
        timeout_seconds=30,
        poll_interval_seconds=1,
    )

    assert report.passed
    assert report.workflow_states == (
        ("daily", "success"),
        ("hourly", "success"),
    )
    assert [item[:2] for item in airflow.triggered] == [
        ("DAG__daily", run_ids["daily"]),
        ("DAG__hourly", run_ids["hourly"]),
    ]
    assert {item[2]["dpone_evidence_authority"]["evidence_set_id"] for item in airflow.triggered} == {
        request.evidence_set_id
    }
    assert journal.requests == [request]
    assert len(journal.receipts) == 1
    assert journal.receipts[0].passed


def test_campaign_fails_immediately_on_terminal_failure() -> None:
    request = _request()
    run_ids = {item.workflow_id: item.dag_run_id for item in request.workflows}
    airflow = _Airflow(
        {
            run_ids["daily"]: ["failed"],
            run_ids["hourly"]: ["running"],
        }
    )
    journal = _Journal()
    service = DbtDevEvidenceCampaignService(
        airflow=airflow,
        journal=journal,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
    )

    with pytest.raises(
        DbtDevEvidenceCampaignError,
        match="failed",
    ):
        service.run(request, timeout_seconds=30)
    assert len(journal.receipts) == 1
    assert journal.receipts[0].status == "failed"
    assert not journal.receipts[0].passed


def test_campaign_deadline_includes_trigger_phase() -> None:
    request = _request()
    airflow = _Airflow({})
    journal = _Journal()
    now = iter((0.0, 1.0, 31.0))
    service = DbtDevEvidenceCampaignService(
        airflow=airflow,
        journal=journal,
        monotonic=lambda: next(now),
        sleep=lambda _seconds: None,
    )

    with pytest.raises(
        DbtDevEvidenceCampaignError,
        match="timed out",
    ):
        service.run(
            request,
            timeout_seconds=30,
            poll_interval_seconds=1,
        )

    assert len(airflow.triggered) == 1
    assert len(journal.receipts) == 1
    assert journal.receipts[0].status == "abandoned"


class _Response:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = json.dumps(dict(payload)).encode("utf-8")

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _size: int) -> bytes:
        return self._payload


def test_rest_adapter_uses_bearer_auth_and_url_encodes_run_identity() -> None:
    observed: list[object] = []

    def opener(request: object, *, timeout: int) -> _Response:
        observed.append((request, timeout))
        return _Response({"dag_run_id": "run/one", "state": "success"})

    adapter = AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v2",
        bearer_token="opaque-token",
        opener=opener,
    )

    adapter.trigger(
        dag_id="DAG/daily",
        dag_run_id="run/one",
        conf={"dpone_evidence_set_id": EVIDENCE_SET_ID},
    )
    state = adapter.state(
        dag_id="DAG/daily",
        dag_run_id="run/one",
    )

    first_request = observed[0][0]
    assert state == "success"
    assert first_request.full_url.endswith("/api/v2/dags/DAG%2Fdaily/dagRuns")
    assert first_request.get_header("Authorization") == "Bearer opaque-token"
    assert b"opaque-token" not in first_request.data


def test_rest_adapter_rejects_conflicting_existing_run() -> None:
    calls = 0

    def opener(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        del timeout
        calls += 1
        if calls == 1:
            raise HTTPError(
                request.full_url,
                409,
                "Conflict",
                {},
                None,
            )
        return _Response(
            {
                "dag_run_id": "run",
                "conf": {"dpone_evidence_set_id": "sha256:" + "f" * 64},
            }
        )

    adapter = AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v1",
        bearer_token="opaque-token",
        opener=opener,
    )

    with pytest.raises(
        DbtDevEvidenceCampaignError,
        match="differs",
    ):
        adapter.trigger(
            dag_id="DAG",
            dag_run_id="run",
            conf={"dpone_evidence_set_id": EVIDENCE_SET_ID},
        )


def test_rest_adapter_replays_identical_conflict_as_no_op() -> None:
    calls = 0
    conf = {"dpone_evidence_set_id": EVIDENCE_SET_ID}
    observed_timeouts: list[float] = []
    monotonic_values = iter((100.0, 101.0, 102.0))

    def opener(request: object, *, timeout: float) -> _Response:
        nonlocal calls
        observed_timeouts.append(timeout)
        calls += 1
        if calls == 1:
            raise HTTPError(
                request.full_url,
                409,
                "Conflict",
                {},
                None,
            )
        return _Response(
            {
                "dag_run_id": "run",
                "conf": conf,
            }
        )

    AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v1",
        bearer_token="opaque-token",
        opener=opener,
        monotonic=lambda: next(monotonic_values),
    ).trigger(
        dag_id="DAG",
        dag_run_id="run",
        conf=conf,
        timeout_seconds=5,
    )

    assert calls == 2
    assert observed_timeouts == [4.0, 3.0]


def test_rest_adapter_reconciles_timeout_after_accepted_trigger() -> None:
    calls = 0
    conf = {"dpone_evidence_set_id": EVIDENCE_SET_ID}

    def opener(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        del timeout
        calls += 1
        if calls == 1:
            raise TimeoutError
        return _Response(
            {
                "dag_run_id": "run",
                "conf": conf,
            }
        )

    AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v2",
        bearer_token="opaque-token",
        opener=opener,
    ).trigger(
        dag_id="DAG",
        dag_run_id="run",
        conf=conf,
    )

    assert calls == 2


@pytest.mark.parametrize("status_code", [502, 503, 504])
def test_rest_adapter_reconciles_ambiguous_server_failure_after_accepted_trigger(
    status_code: int,
) -> None:
    calls = 0
    conf = {"dpone_evidence_set_id": EVIDENCE_SET_ID}

    def opener(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        del timeout
        calls += 1
        if calls == 1:
            raise HTTPError(
                request.full_url,
                status_code,
                "ambiguous upstream failure",
                {},
                None,
            )
        return _Response(
            {
                "dag_run_id": "run",
                "conf": conf,
            }
        )

    AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v2",
        bearer_token="opaque-token",
        opener=opener,
    ).trigger(
        dag_id="DAG",
        dag_run_id="run",
        conf=conf,
    )

    assert calls == 2


@pytest.mark.parametrize(
    "existing",
    [
        None,
        {
            "dag_run_id": "run",
            "conf": {"dpone_evidence_set_id": "sha256:" + "f" * 64},
        },
    ],
)
def test_rest_adapter_fails_closed_when_ambiguous_trigger_cannot_be_reconciled(
    existing: Mapping[str, object] | None,
) -> None:
    calls = 0
    conf = {"dpone_evidence_set_id": EVIDENCE_SET_ID}

    def opener(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        del timeout
        calls += 1
        if calls == 1:
            raise HTTPError(request.full_url, 503, "ambiguous", {}, None)
        if existing is None:
            raise HTTPError(request.full_url, 404, "missing", {}, None)
        return _Response(existing)

    adapter = AirflowRestDbtEvidenceAdapter(
        base_url="https://airflow.internal",
        api_version="v2",
        bearer_token="opaque-token",
        opener=opener,
    )
    with pytest.raises(DbtDevEvidenceCampaignError):
        adapter.trigger(
            dag_id="DAG",
            dag_run_id="run",
            conf=conf,
        )

    assert calls == 2


@pytest.mark.parametrize(
    "url",
    [
        "http://airflow.internal",
        "https://user:password@airflow.internal",
        "https://airflow.internal?token=secret",
    ],
)
def test_rest_adapter_rejects_unsafe_base_urls(url: str) -> None:
    with pytest.raises(DbtDevEvidenceCampaignError):
        AirflowRestDbtEvidenceAdapter(
            base_url=url,
            api_version="v2",
            bearer_token="opaque-token",
            opener=lambda *_args, **_kwargs: SimpleNamespace(),
        )
