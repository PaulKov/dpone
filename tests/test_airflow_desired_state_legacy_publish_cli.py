from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.adapters.airflow_desired_state_publish_intent import (
    commit_desired_state_publish_output,
)
from dpone.contracts.airflow_desired_state_publish import (
    DesiredStatePublishCandidate,
    DesiredStatePublishIntent,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.airflow_desired_state import DesiredStateReadResult
from dpone.readiness.airflow_desired_state_store import load_promotion_input
from dpone.services.airflow_desired_state import DesiredStatePublishError
from tests.test_airflow_desired_state_cli import (
    _authority,
    _promotion,
    _run,
    _Store,
)


def test_legacy_publish_keeps_v1_success_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    _promotion(promotion)
    store = _Store(DesiredStateReadResult.absent())
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.uuid4",
        lambda: "123e4567-e89b-42d3-a456-426614174000",
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: type(
            "_AllowSource",
            (),
            {"authorize": lambda self, **kwargs: None},
        )(),
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")

    code = _run(_legacy_publish_args(tmp_path, promotion, output))

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["schema"] == "dpone.airflow-desired-state-publish.v1"
    assert payload["outcome"] == "created"
    assert payload["passed"] is True
    assert "preparation_job_id" not in payload
    assert "publisher_job_id" not in payload
    assert store.written is not None
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-desired-state-publish.v1",
        )
        == ()
    )


def test_publish_failure_uses_separate_redacted_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    payload = json.loads(promotion.read_text(encoding="utf-8"))
    payload["blockers"] = ["secret_backend_failure"]
    payload["status"] = "blocked"
    promotion.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed"),
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")

    code = _run([*_legacy_publish_args(tmp_path, promotion, output), "--status-output", str(status)])

    rendered = status.read_text(encoding="utf-8")
    assert code == 2
    assert not output.exists()
    assert "secret_backend_failure" not in rendered
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in rendered


def test_publish_uncertain_mutation_uses_separate_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: _Store(DesiredStateReadResult.absent()),
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: object(),
    )

    def fail(*_: object, **__: object) -> object:
        raise DesiredStatePublishError(
            "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN",
            "backend detail",
            state_may_have_changed=True,
        )

    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.AirflowDesiredStatePublisher.publish",
        fail,
    )

    code = _run([*_legacy_publish_args(tmp_path, promotion, output), "--status-output", str(status)])

    rendered = status.read_text(encoding="utf-8")
    payload = json.loads(rendered)
    assert code == 4
    assert not output.exists()
    assert payload["state_may_have_changed"] is True
    assert "backend detail" not in rendered


@pytest.mark.parametrize("failure_type", [OSError, ValueError])
def test_publish_success_evidence_failure_reports_remote_state_as_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_type: type[Exception],
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    store = _Store(DesiredStateReadResult.absent())
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: type("_AllowSource", (), {"authorize": lambda self, **kwargs: None})(),
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")

    def fail_success_output(path: Path, body: bytes) -> None:
        if path == output:
            raise failure_type("local disk unavailable")
        commit_desired_state_publish_output(path, body)

    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.commit_desired_state_publish_output",
        fail_success_output,
    )

    code = _run([*_legacy_publish_args(tmp_path, promotion, output), "--status-output", str(status)])

    payload = json.loads(status.read_text(encoding="utf-8"))
    assert code == 5
    assert store.written is not None
    assert not output.exists()
    assert payload["state_may_have_changed"] is True
    assert payload["code"] == "DPONE_INTERNAL_AIRFLOW_DESIRED_STATE_PUBLISH_EVIDENCE_FAILED"
    assert "local disk unavailable" not in status.read_text(encoding="utf-8")
    assert GitOpsSchemaValidator().validate(payload, expected_kind="dpone.error.v1") == ()


def test_publish_failure_evidence_write_failure_never_masks_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    store = _Store(DesiredStateReadResult.absent())
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: type("_AllowSource", (), {"authorize": lambda self, **kwargs: None})(),
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.commit_desired_state_publish_output",
        lambda *_: (_ for _ in ()).throw(ValueError("local writer failed")),
    )

    code = _run([*_legacy_publish_args(tmp_path, promotion, output), "--status-output", str(status)])

    assert code == 5
    assert store.written is not None
    assert not output.exists()
    assert not status.exists()


def test_legacy_publish_failure_replaces_stale_success_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    _promotion(promotion)
    output.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-desired-state-publish.v1",
                "passed": True,
                "status": "published",
            }
        ),
        encoding="utf-8",
    )
    payload = json.loads(promotion.read_text(encoding="utf-8"))
    payload["blockers"] = ["do_not_publish"]
    payload["status"] = "blocked"
    promotion.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: pytest.fail("store must not be constructed"),
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")

    code = _run(_legacy_publish_args(tmp_path, promotion, output))

    rendered = json.loads(output.read_text(encoding="utf-8"))
    assert code == 2
    assert rendered["schema"] == "dpone.error.v1"
    assert rendered["passed"] is False
    assert rendered["errors"] == [
        {
            "code": "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID",
            "message": "Airflow desired-state operation failed.",
        }
    ]
    assert rendered["code"] == "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID"
    assert rendered["severity"] == "error"
    assert GitOpsSchemaValidator().validate(rendered, expected_kind="dpone.error.v1") == ()


def test_legacy_candidate_hash_matches_v07324_and_reuses_retained_intent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    output = tmp_path / "publish.json"
    intent_path = tmp_path / "publish-intent.json"
    _promotion(promotion)
    promotion_input = load_promotion_input(promotion)
    candidate = DesiredStatePublishCandidate(
        environment="dev",
        project="group/repository",
        source_ref="master",
        pipeline_id="1",
        job_id="2",
        git_sha=promotion_input.source_git_sha,
        registry_scope_id=promotion_input.registry_scope_id,
        release_id=promotion_input.release_id,
        deployment_id=promotion_input.deployment_id,
        airflow_index_sha256=promotion_input.airflow_index_sha256,
        runtime_image_digest=promotion_input.runtime_image_digest,
        expected_dag_ids=promotion_input.expected_dag_ids,
        publication_evidence_sha256=promotion_input.evidence_sha256,
        expected_revision=None,
    )
    assert candidate.sha256 == "sha256:adddc66261d355440ac6d18684e3ea510343670a95948da152ad1ec876ca42b5"
    intent_path.write_bytes(
        DesiredStatePublishIntent(
            candidate_sha256=candidate.sha256,
            occurrence_id="123e4567-e89b-42d3-a456-426614174000",
            promoted_at="2026-07-28T10:00:00Z",
        ).to_json_bytes()
    )
    store = _Store(DesiredStateReadResult.absent())
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: type("_AllowSource", (), {"authorize": lambda self, **kwargs: None})(),
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "1")
    monkeypatch.setenv("CI_JOB_ID", "2")

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--expected-revision",
            "absent",
            "--intent",
            str(intent_path),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8"))["schema"] == ("dpone.airflow-desired-state-publish.v1")


def _legacy_publish_args(
    tmp_path: Path,
    promotion: Path,
    output: Path,
) -> list[str]:
    return [
        "airflow",
        "desired-state",
        "publish",
        "--identity-mode",
        "workload_identity",
        "--promotion-evidence",
        str(promotion),
        "--expected-revision",
        "absent",
        "--intent",
        str(tmp_path / "publish-intent.json"),
        "--output",
        str(output),
    ]
