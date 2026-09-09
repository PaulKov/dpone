from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.services import dbt_dev_evidence_request as request_module
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
    DbtDevEvidenceRequestError,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64


def _request(monkeypatch: pytest.MonkeyPatch) -> DbtDevEvidenceRequest:
    monkeypatch.setattr(
        request_module,
        "load_expected_dbt_release",
        lambda *_args: SimpleNamespace(
            required_workloads={
                "dbt__hourly": "sha256:" + "d" * 64,
                "dbt__daily": "sha256:" + "e" * 64,
            },
            dbt_workflows={
                "hourly": SimpleNamespace(
                    workflow_id="hourly",
                    dag_id="DAG__hourly",
                ),
                "daily": SimpleNamespace(
                    workflow_id="daily",
                    dag_id="DAG__daily",
                ),
            },
        ),
    )
    return DbtDevEvidenceRequest.build(
        compiled_root="compiled",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/dpone",
        producer_workflow="PaulKov/dpone/.github/workflows/dev-evidence.yml@refs/heads/main",
        source_commit="c" * 40,
        orchestration_run_id="123456",
        orchestration_run_attempt=2,
    )


def test_request_derives_stable_set_and_workflow_run_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _request(monkeypatch)
    second = _request(monkeypatch)

    assert first == second
    assert first.evidence_set_id.startswith("sha256:")
    assert [item.workflow_id for item in first.workflows] == ["daily", "hourly"]
    assert all(
        item.dag_run_id.startswith("dpone_evidence__" + first.evidence_set_id.removeprefix("sha256:")[:20])
        for item in first.workflows
    )
    assert DbtDevEvidenceRequest.from_mapping(first.to_dict()) == first


def test_request_attempt_changes_campaign_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _request(monkeypatch)
    changed = DbtDevEvidenceRequest.build(
        compiled_root="compiled",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/dpone",
        producer_workflow="PaulKov/dpone/.github/workflows/dev-evidence.yml@refs/heads/main",
        source_commit="c" * 40,
        orchestration_run_id="123456",
        orchestration_run_attempt=3,
    )

    assert changed.evidence_set_id != first.evidence_set_id
    assert changed.workflows != first.workflows


def test_prepare_request_cli_emits_closed_json_contract(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    expected = _request(monkeypatch)
    # CLI now receives its reader at the composition boundary; keep this test
    # focused on serialization while full-tree CLI coverage uses real artifacts.
    from dpone.commands import dbt_dev_evidence_cmd

    monkeypatch.setattr(
        dbt_dev_evidence_cmd, "build_dbt_expected_release_loader", lambda: request_module.load_expected_dbt_release
    )

    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "prepare-dev-evidence-request",
                "--compiled-root",
                "compiled",
                "--expected-release-id",
                RELEASE_ID,
                "--expected-deployment-id",
                DEPLOYMENT_ID,
                "--producer-repository",
                "PaulKov/dpone",
                "--producer-workflow",
                "PaulKov/dpone/.github/workflows/dev-evidence.yml@refs/heads/main",
                "--source-commit",
                "c" * 40,
                "--orchestration-run-id",
                "123456",
                "--orchestration-run-attempt",
                "2",
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert DbtDevEvidenceRequest.from_mapping(json.loads(captured.out)) == expected


def test_prepare_request_help_does_not_advertise_ignored_evidence_set(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit, match="0"):
        cli_main.main(
            [
                "dbt",
                "prepare-dev-evidence-request",
                "--help",
            ]
        )

    captured = capsys.readouterr()
    assert "--expected-evidence-set-id" not in captured.out


def test_request_rejects_tampered_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _request(monkeypatch).to_dict()
    payload["deployment_id"] = "sha256:" + "d" * 64

    with pytest.raises(
        DbtDevEvidenceRequestError,
        match="fingerprint",
    ):
        DbtDevEvidenceRequest.from_mapping(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("producer_repository", "not-a-repository"),
        ("source_commit", "short"),
        ("orchestration_run_attempt", True),
        ("orchestration_run_attempt", 0),
    ],
)
def test_request_rejects_unsafe_authority_fields(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    payload = _request(monkeypatch).to_dict()
    payload[field] = value

    with pytest.raises(DbtDevEvidenceRequestError):
        DbtDevEvidenceRequest.from_mapping(payload)


def test_request_rejects_more_than_bounded_workflow_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _request(monkeypatch).to_dict()
    workflows = payload["workflows"]
    assert isinstance(workflows, list)
    template = workflows[0]
    assert isinstance(template, dict)
    payload["workflows"] = [
        {
            **template,
            "workflow_id": f"workflow_{index}",
            "dag_id": f"DAG__workflow_{index}",
            "dag_run_id": f"run_{index}",
        }
        for index in range(201)
    ]

    with pytest.raises(DbtDevEvidenceRequestError):
        DbtDevEvidenceRequest.from_mapping(payload)


@pytest.mark.parametrize(
    ("workflow_count", "accepted"),
    [
        (200, True),
        (201, False),
    ],
)
def test_request_builder_enforces_workflow_inventory_boundary(
    monkeypatch: pytest.MonkeyPatch,
    workflow_count: int,
    accepted: bool,
) -> None:
    monkeypatch.setattr(
        request_module,
        "load_expected_dbt_release",
        lambda *_args: SimpleNamespace(
            required_workloads={f"dbt__workflow_{index}": "sha256:" + "d" * 64 for index in range(workflow_count)},
            dbt_workflows={
                f"workflow_{index}": SimpleNamespace(
                    workflow_id=f"workflow_{index}",
                    dag_id=f"DAG__workflow_{index}",
                )
                for index in range(workflow_count)
            },
        ),
    )
    arguments = {
        "compiled_root": "compiled",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "producer_repository": "PaulKov/dpone",
        "producer_workflow": "PaulKov/dpone/.github/workflows/dev-evidence.yml@refs/heads/main",
        "source_commit": "c" * 40,
        "orchestration_run_id": "123456",
        "orchestration_run_attempt": 2,
    }

    if accepted:
        request = DbtDevEvidenceRequest.build(**arguments)
        assert len(request.workflows) == workflow_count
        return
    with pytest.raises(DbtDevEvidenceRequestError):
        DbtDevEvidenceRequest.build(**arguments)


@pytest.mark.parametrize(
    ("workload_count", "accepted"),
    [
        (500, True),
        (501, False),
    ],
)
def test_request_builder_enforces_airflow_evidence_inventory_boundary(
    monkeypatch: pytest.MonkeyPatch,
    workload_count: int,
    accepted: bool,
) -> None:
    monkeypatch.setattr(
        request_module,
        "load_expected_dbt_release",
        lambda *_args: SimpleNamespace(
            required_workloads={f"workload_{index}": "sha256:" + "d" * 64 for index in range(workload_count)},
            dbt_workflows={
                "daily": SimpleNamespace(
                    workflow_id="daily",
                    dag_id="DAG__daily",
                )
            },
        ),
    )
    arguments = {
        "compiled_root": "compiled",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "producer_repository": "PaulKov/dpone",
        "producer_workflow": "PaulKov/dpone/.github/workflows/dev-evidence.yml@refs/heads/main",
        "source_commit": "c" * 40,
        "orchestration_run_id": "123456",
        "orchestration_run_attempt": 2,
    }

    if accepted:
        DbtDevEvidenceRequest.build(**arguments)
        return
    with pytest.raises(DbtDevEvidenceRequestError):
        DbtDevEvidenceRequest.build(**arguments)
