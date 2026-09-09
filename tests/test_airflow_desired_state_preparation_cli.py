from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.airflow_desired_state import (
    AirflowDesiredDeployment,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_publish import (
    AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA,
    DesiredStatePublishPreparation,
)
from dpone.ports.airflow_desired_state import (
    DesiredStateReadResult,
    DesiredStateWriteResult,
)
from dpone.readiness.airflow_desired_state_authority import (
    AirflowDesiredStateAuthority,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")


class _Store:
    def __init__(self) -> None:
        self.written: bytes | None = None

    def read(self, **_: object) -> DesiredStateReadResult:
        return DesiredStateReadResult.absent()

    def create_if_absent(self, body: bytes) -> DesiredStateWriteResult:
        self.written = body
        return DesiredStateWriteResult(DesiredStateRevision('"created"'))

    def replace_if_revision(
        self,
        expected_revision: DesiredStateRevision,
        body: bytes,
    ) -> DesiredStateWriteResult:
        del expected_revision
        self.written = body
        return DesiredStateWriteResult(DesiredStateRevision('"replaced"'))


def _authority() -> AirflowDesiredStateAuthority:
    return AirflowDesiredStateAuthority(
        environment="dev",
        desired_state_uri="s3://bucket/control/dev/desired.json",
        certified_s3_endpoint_url="https://storage.yandexcloud.net",
        artifact_registry_uri="s3://bucket/dpone-artifacts/dev/repository",
        artifact_registry_ref="dpone-artifacts-dev",
        watcher_identity="airflow-pack-watcher",
        source_project="group/repository",
        source_ref="master",
    )


def _promotion(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "dwh.airflow_ci.dpone_deployment_promotion.v2",
                "release_id": _DIGESTS[0],
                "deployment_id": _DIGESTS[1],
                "environment": "dev",
                "registry_scope_id": _authority().registry_scope_id,
                "source_git_sha": "1" * 40,
                "runtime_image_digest": _DIGESTS[3],
                "airflow_index_sha256": _DIGESTS[4],
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "expected_dag_count": 1,
                "status": "passed",
                "warnings": [],
                "blockers": [],
                "created_at": "2026-07-28T10:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _run(args: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        cli_main.main(args)
    return int(exited.value.code or 0)


def _patch_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.load_airflow_desired_state_authority",
        _authority,
    )


def test_preparation_is_canonical_and_reused_by_a_different_publish_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation_path = tmp_path / "preparation.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    _patch_authority(monkeypatch)
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.uuid4",
        lambda: "123e4567-e89b-42d3-a456-426614174000",
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")

    prepare_code = _run(
        [
            "airflow",
            "desired-state",
            "prepare",
            "--promotion-evidence",
            str(promotion),
            "--expected-revision",
            "absent",
            "--output",
            str(preparation_path),
        ]
    )

    preparation = DesiredStatePublishPreparation.from_json_bytes(preparation_path.read_bytes())
    assert prepare_code == 0
    assert preparation.schema == AIRFLOW_DESIRED_STATE_PUBLISH_PREPARATION_SCHEMA
    assert preparation.candidate.pipeline_id == "10"
    assert preparation.candidate.job_id == "11"

    store = _Store()
    monkeypatch.setenv("CI_JOB_ID", "22")
    monkeypatch.setenv("CI_API_V4_URL", "https://gitlab.example.test/api/v4")
    monkeypatch.setenv("CI_JOB_TOKEN", "redacted")
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_publish_cmd._source_authorizer",
        lambda _: type("_AllowSource", (), {"authorize": lambda self, **kwargs: None})(),
    )

    publish_code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--preparation",
            str(preparation_path),
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert publish_code == 0
    assert store.written is not None
    desired = AirflowDesiredDeployment.from_json(store.written)
    assert desired.source.pipeline_id == "10"
    assert desired.source.job_id == "11"
    assert desired.source.occurrence_id == preparation.intent.occurrence_id
    published = json.loads(output.read_text(encoding="utf-8"))
    assert published["preparation_job_id"] == "11"
    assert published["publisher_job_id"] == "22"


def test_publish_rejects_preparation_from_another_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation_path = tmp_path / "preparation.json"
    output = tmp_path / "publish.json"
    status = tmp_path / "publish-status.json"
    _promotion(promotion)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    assert (
        _run(
            [
                "airflow",
                "desired-state",
                "prepare",
                "--promotion-evidence",
                str(promotion),
                "--expected-revision",
                "absent",
                "--output",
                str(preparation_path),
            ]
        )
        == 0
    )
    monkeypatch.setenv("CI_PIPELINE_ID", "99")

    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--preparation",
            str(preparation_path),
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )

    assert code == 2
    assert not output.exists()
    assert "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID" in status.read_text(encoding="utf-8")
    assert DesiredStatePublishPreparation.from_json_bytes(preparation_path.read_bytes())


def test_preparation_parser_rejects_duplicate_and_non_string_candidate_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation_path = tmp_path / "preparation.json"
    _promotion(promotion)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    assert (
        _run(
            [
                "airflow",
                "desired-state",
                "prepare",
                "--promotion-evidence",
                str(promotion),
                "--expected-revision",
                "absent",
                "--output",
                str(preparation_path),
            ]
        )
        == 0
    )
    valid = preparation_path.read_text(encoding="utf-8")
    duplicate = valid.replace(
        '"schema":"dpone.airflow-desired-state-publish-preparation.v1"',
        '"schema":"dpone.airflow-desired-state-publish-preparation.v1",'
        '"schema":"dpone.airflow-desired-state-publish-preparation.v1"',
    )
    with pytest.raises(ValueError, match="duplicate"):
        DesiredStatePublishPreparation.from_json_bytes(duplicate.encode())

    payload = json.loads(valid)
    payload["candidate"]["pipeline_id"] = 10
    malformed = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    with pytest.raises(ValueError, match="pipeline_id"):
        DesiredStatePublishPreparation.from_json_bytes(malformed)


def test_publish_option_contract_blocks_ambiguous_or_incomplete_legacy_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    preparation_path = tmp_path / "preparation.json"
    _promotion(promotion)
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")
    assert (
        _run(
            [
                "airflow",
                "desired-state",
                "prepare",
                "--promotion-evidence",
                str(promotion),
                "--expected-revision",
                "absent",
                "--output",
                str(preparation_path),
            ]
        )
        == 0
    )

    with pytest.raises(SystemExit) as ambiguous:
        cli_main.main(
            [
                "airflow",
                "desired-state",
                "publish",
                "--identity-mode",
                "workload_identity",
                "--promotion-evidence",
                str(promotion),
                "--preparation",
                str(preparation_path),
                "--intent",
                str(tmp_path / "intent.json"),
                "--output",
                str(tmp_path / "publish.json"),
            ]
        )
    assert ambiguous.value.code == 2

    incomplete_output = tmp_path / "incomplete.json"
    code = _run(
        [
            "airflow",
            "desired-state",
            "publish",
            "--identity-mode",
            "workload_identity",
            "--promotion-evidence",
            str(promotion),
            "--intent",
            str(tmp_path / "intent.json"),
            "--output",
            str(incomplete_output),
        ]
    )
    assert code == 2


def test_prepare_rejects_promotion_output_path_collision(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    promotion = tmp_path / "promotion.json"
    _promotion(promotion)
    original = promotion.read_bytes()
    _patch_authority(monkeypatch)
    monkeypatch.setenv("CI_PIPELINE_ID", "10")
    monkeypatch.setenv("CI_JOB_ID", "11")

    code = _run(
        [
            "airflow",
            "desired-state",
            "prepare",
            "--promotion-evidence",
            str(promotion),
            "--expected-revision",
            "absent",
            "--output",
            str(promotion),
        ]
    )

    assert code == 2
    assert promotion.read_bytes() == original
