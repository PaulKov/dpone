from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment, DesiredStateRevision
from dpone.contracts.airflow_desired_state_reconcile import (
    DesiredStateReconcileEvidence,
)
from dpone.ports.airflow_desired_state import DesiredStateReadResult, DesiredStateWriteResult
from dpone.readiness.airflow_desired_state_authority import (
    AirflowDesiredStateAuthority,
)
from dpone.services.airflow_desired_state_reconcile import DesiredStateReconcileError

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")


class _Store:
    def __init__(self, read_result: DesiredStateReadResult) -> None:
        self.read_result = read_result
        self.written: bytes | None = None

    def read(self, **_: object) -> DesiredStateReadResult:
        return self.read_result

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


def _desired_body() -> bytes:
    return AirflowDesiredDeployment.from_mapping(
        {
            "schema": "dpone.airflow-desired-deployment.v1",
            "environment": "dev",
            "source": {
                "project": "group/repository",
                "ref": "master",
                "pipeline_id": "1",
                "job_id": "2",
                "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
                "git_sha": "1" * 40,
            },
            "promotion": {
                "registry_scope_id": _DIGESTS[4],
                "release_id": _DIGESTS[0],
                "deployment_id": _DIGESTS[1],
                "airflow_index_sha256": _DIGESTS[2],
                "runtime_image_digest": _DIGESTS[3],
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "publication_evidence_sha256": _DIGESTS[4],
            },
            "previous": {"revision": None, "deployment_id": None},
            "promoted_at": "2026-07-28T10:00:00Z",
        }
    ).to_json_bytes()


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


def _run(args: list[str]) -> int:
    with pytest.raises(SystemExit) as exited:
        cli_main.main(args)
    return int(exited.value.code or 0)


def test_fetch_command_commits_snapshot_and_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "desired.json"
    status = tmp_path / "status.json"
    store = _Store(
        DesiredStateReadResult.present(
            _desired_body(),
            DesiredStateRevision('"etag"'),
        )
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.DesiredStateStoreOptions.build",
        lambda _: store,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.load_airflow_desired_state_authority",
        _authority,
    )

    code = _run(
        [
            "airflow",
            "desired-state",
            "fetch",
            "--identity-mode",
            "workload_identity",
            "--output",
            str(snapshot),
            "--status-output",
            str(status),
        ]
    )

    payload = json.loads(status.read_text(encoding="utf-8"))
    assert code == 0
    assert snapshot.read_bytes() == _desired_body()
    assert payload["schema"] == "dpone.airflow-desired-state-fetch.v1"
    assert payload["status"] == "fetched"
    assert payload["deployment_id"] == _DIGESTS[1]


def _reconcile_args(tmp_path: Path) -> list[str]:
    return [
        "airflow",
        "desired-state",
        "reconcile",
        "--connection-type",
        "airflow",
        "--connection-id",
        "s3_dpone_artifacts_reader",
        "--artifact-connection-type",
        "airflow",
        "--artifact-connection-id",
        "s3_dpone_artifacts_reader",
        "--cache-root",
        str(tmp_path / "cache"),
    ]


def test_reconcile_command_writes_typed_success_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def reconcile(**kwargs: object) -> DesiredStateReconcileEvidence:
        captured.update(kwargs)
        evidence = DesiredStateReconcileEvidence(
            status="activated",
            environment="dev",
            observed_revision='"etag"',
            desired_state_sha256=_DIGESTS[0],
            registry_scope_id=_authority().registry_scope_id,
            source_project="group/repository",
            source_ref="master",
            release_id=_DIGESTS[1],
            deployment_id=_DIGESTS[2],
            occurrence_id="123e4567-e89b-42d3-a456-426614174000",
            source_git_sha="1" * 40,
            airflow_index_sha256=_DIGESTS[3],
            runtime_image_digest=_DIGESTS[3],
            expected_dag_ids=("DAG__platform__smoke__run",),
            activation_id="123e4567-e89b-42d3-a456-426614174001",
            previous_deployment_id=None,
            predecessor_status="bootstrap",
            materialized=True,
            activated=True,
        )
        status = Path(str(kwargs["cache_root"])) / "status/last-reconcile-status.json"
        status.parent.mkdir(parents=True)
        status.write_bytes(evidence.to_json_bytes())
        captured["evidence"] = evidence
        return evidence

    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.reconcile_desired_state",
        reconcile,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.load_airflow_desired_state_authority",
        _authority,
    )

    code = _run(_reconcile_args(tmp_path))

    status_path = tmp_path / "cache/status/last-reconcile-status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["schema"] == "dpone.airflow-desired-state-reconcile.v1"
    assert payload["status"] == "activated"
    assert captured["authority"] == _authority()
    expected = captured["evidence"]
    assert isinstance(expected, DesiredStateReconcileEvidence)
    assert status_path.read_bytes() == expected.to_json_bytes()


def test_reconcile_command_exposes_uncertain_state_without_backend_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail(**_: object) -> DesiredStateReconcileEvidence:
        status = tmp_path / "cache/status/last-reconcile-status.json"
        status.parent.mkdir(parents=True)
        status.write_text(
            json.dumps(
                {
                    "schema": "dpone.error.v1",
                    "passed": False,
                    "errors": [
                        {
                            "code": "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_FAILED",
                            "message": "Airflow desired-state operation failed.",
                        }
                    ],
                    "state_may_have_changed": True,
                }
            ),
            encoding="utf-8",
        )
        raise DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_FAILED",
            "secret backend detail",
            state_may_have_changed=True,
        )

    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.reconcile_desired_state",
        fail,
    )
    monkeypatch.setattr(
        "dpone.commands.airflow_desired_state_cmd.load_airflow_desired_state_authority",
        _authority,
    )

    code = _run(_reconcile_args(tmp_path))

    rendered = (tmp_path / "cache/status/last-reconcile-status.json").read_text(encoding="utf-8")
    payload = json.loads(rendered)
    assert code == 4
    assert payload["state_may_have_changed"] is True
    assert "secret backend detail" not in rendered
