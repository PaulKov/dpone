from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.fs_local import LocalFileSystem
from dpone.cli import main as cli_main
from dpone.contracts.deployment_cache_retention_state import retention_operation_id


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_gitops_schema_cli_help_is_registered(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=Path.cwd())

    for args in (
        ["gitops", "schema", "--help"],
        ["gitops", "schema", "list", "--help"],
        ["gitops", "schema", "show", "--help"],
        ["gitops", "schema", "validate", "--help"],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main(args)
        assert exc.value.code == 0

    assert "GitOps schema catalog" in capsys.readouterr().out


def test_gitops_schema_cli_lists_and_shows_safe_sample_contracts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)

    list_output = Path("safe-sample-schemas.json")
    with pytest.raises(SystemExit) as list_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "list",
                "--prefix",
                "dpone.safe-sample",
                "--format",
                "json",
                "--output",
                list_output.as_posix(),
            ]
        )
    assert list_exit.value.code == 0
    listed = json.loads((tmp_path / list_output).read_text(encoding="utf-8"))
    names = {item["name"] for item in listed["contracts"]}
    assert listed["kind"] == "gitops.schema_catalog"
    assert listed["count"] == 12
    assert "safe-sample-runtime-handoff" in names
    assert "safe-sample-runtime-run" in names
    assert "safe-sample-data-copy" in names
    capsys.readouterr()

    with pytest.raises(SystemExit) as show_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "show",
                "dpone.safe-sample-runtime-run.v1",
                "--format",
                "json",
            ]
        )
    assert show_exit.value.code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["kind"] == "gitops.schema_contract"
    assert shown["contract"]["name"] == "safe-sample-runtime-run"
    assert shown["contract"]["schema"]["properties"]["schema"]["const"] == "dpone.safe-sample-runtime-run.v1"


def test_gitops_schema_cli_validates_payload_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    payload_path = tmp_path / "runtime-run.json"
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.safe-sample-runtime-run.v1",
                "release_id": None,
                "deployment_id": None,
                "execution_status": "failed",
                "data_outcome": "unknown",
                "runtime_execution": {
                    "schema": "dpone.safe-sample-runtime-execution.v1",
                    "execution_status": "failed",
                    "data_outcome": "unknown",
                    "release_id": None,
                    "deployment_id": None,
                    "errors": [],
                },
                "evidence_write": None,
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    output = Path("validation.json")

    with pytest.raises(SystemExit) as valid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.safe-sample-runtime-run.v1",
                "--payload",
                str(payload_path),
                "--format",
                "json",
                "--output",
                output.as_posix(),
            ]
        )

    assert valid_exit.value.code == 0
    report = json.loads((tmp_path / output).read_text(encoding="utf-8"))
    assert report == {
        "kind": "gitops.schema_validation",
        "contract": {
            "name": "safe-sample-runtime-run",
            "kind": "dpone.safe-sample-runtime-run.v1",
            "schema_id": "https://paulkov.github.io/dpone/schemas/gitops/safe-sample-runtime-run.schema.json",
        },
        "payload": str(payload_path),
        "passed": True,
        "issues": [],
    }


def test_gitops_schema_cli_rejects_semantically_contradictory_retention_projection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    digest = "sha256:" + "d" * 64
    payload_path = tmp_path / "retention-apply.json"
    reviewed_plan = "sha256:" + "d" * 64
    review_id = "00000000-0000-4000-8000-000000000001"
    operation_id = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=reviewed_plan,
        review_id=review_id,
    )
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-cache-retention-apply.v3",
                "environment": "dev",
                "promoted_by": "ci://retention",
                "current_deployment_id": None,
                "items": [
                    {
                        "deployment_id": digest,
                        "action": "deleted",
                        "reason": "unreferenced",
                        "path": "/cache/generations/stale",
                    }
                ],
                "deleted_deployment_ids": [],
                "skipped_deployment_ids": [],
                "reviewed_plan_sha256": reviewed_plan,
                "activation_history_revision": digest,
                "operation_id": operation_id,
                "review_id": review_id,
                "receipt_revision": digest,
                "transaction_status": "committed",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as invalid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.deployment-cache-retention-apply.v3",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    report = json.loads(capsys.readouterr().out)
    assert invalid_exit.value.code == 1
    assert report["passed"] is False
    assert report["issues"] == [
        {
            "code": "schema_derived_projection_mismatch",
            "message": "deleted_deployment_ids must be derived exactly from authoritative items",
            "path": "deleted_deployment_ids",
            "source": "dpone.deployment-cache-retention-apply.v3",
        }
    ]

    inverse = json.loads(payload_path.read_text(encoding="utf-8"))
    inverse["items"] = [
        {
            "deployment_id": digest,
            "action": "skipped",
            "reason": "current",
            "path": "/cache/generations/stale",
        }
    ]
    inverse["deleted_deployment_ids"] = [digest]
    inverse["skipped_deployment_ids"] = []
    payload_path.write_text(json.dumps(inverse), encoding="utf-8")

    with pytest.raises(SystemExit) as inverse_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.deployment-cache-retention-apply.v3",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    inverse_report = json.loads(capsys.readouterr().out)
    assert inverse_exit.value.code == 1
    assert inverse_report["passed"] is False
    assert {issue["path"] for issue in inverse_report["issues"]} == {
        "deleted_deployment_ids",
        "skipped_deployment_ids",
    }


def test_gitops_schema_cli_rejects_wrong_retention_operation_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    digest = "sha256:" + "a" * 64
    payload_path = tmp_path / "retention-apply.json"
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-cache-retention-apply.v3",
                "environment": "dev",
                "promoted_by": "ci://retention",
                "current_deployment_id": None,
                "items": [
                    {
                        "deployment_id": digest,
                        "action": "deleted",
                        "reason": "unreferenced",
                        "path": "/cache/generations/stale",
                    }
                ],
                "deleted_deployment_ids": [digest],
                "skipped_deployment_ids": [],
                "reviewed_plan_sha256": "sha256:" + "b" * 64,
                "activation_history_revision": "sha256:" + "c" * 64,
                "operation_id": "sha256:" + "d" * 64,
                "review_id": "00000000-0000-4000-8000-000000000001",
                "receipt_revision": "sha256:" + "e" * 64,
                "transaction_status": "committed",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as invalid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.deployment-cache-retention-apply.v3",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    report = json.loads(capsys.readouterr().out)
    assert invalid_exit.value.code == 1
    assert report["passed"] is False
    assert report["issues"][0]["code"] == "schema_state_semantics_invalid"
    assert report["issues"][0]["path"] == "operation_id"


def test_gitops_schema_cli_rejects_duplicate_retention_plan_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    deployment_id = "sha256:" + "a" * 64
    item = {"deployment_id": deployment_id, "action": "delete", "reason": "unreferenced", "path": "/old"}
    payload_path = tmp_path / "retention-plan.json"
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-cache-retention-plan.v1",
                "environment": "dev",
                "current_deployment_id": None,
                "protected_deployment_ids": [],
                "items": [item, item],
                "delete_candidates": [deployment_id, deployment_id],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as invalid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.deployment-cache-retention-plan.v1",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    report = json.loads(capsys.readouterr().out)
    assert invalid_exit.value.code == 1
    assert report["passed"] is False
    assert {issue["path"] for issue in report["issues"]} == {"items", "delete_candidates"}


def test_gitops_schema_cli_reports_v3_operation_identity_bounds_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    digest = "sha256:" + "a" * 64
    payload_path = tmp_path / "retention-apply.json"
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-cache-retention-apply.v3",
                "environment": "x" * 129,
                "promoted_by": "ci://retention",
                "current_deployment_id": None,
                "items": [{"deployment_id": digest, "action": "deleted", "reason": "unreferenced", "path": "/old"}],
                "deleted_deployment_ids": [digest],
                "skipped_deployment_ids": [],
                "reviewed_plan_sha256": "sha256:" + "b" * 64,
                "activation_history_revision": "sha256:" + "c" * 64,
                "operation_id": "sha256:" + "d" * 64,
                "review_id": "00000000-0000-4000-8000-000000000001",
                "receipt_revision": "sha256:" + "e" * 64,
                "transaction_status": "committed",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as invalid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.deployment-cache-retention-apply.v3",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    report = json.loads(capsys.readouterr().out)
    assert invalid_exit.value.code == 1
    assert report["passed"] is False
    assert report["issues"][0]["path"] == "operation_id"


def test_gitops_schema_cli_reports_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, repo_root=tmp_path)
    payload_path = tmp_path / "runtime-run-bad.json"
    payload_path.write_text(
        json.dumps(
            {
                "schema": "dpone.safe-sample-runtime-run.v1",
                "release_id": None,
                "deployment_id": None,
                "execution_status": "done",
                "data_outcome": "unknown",
                "runtime_execution": {
                    "schema": "dpone.safe-sample-runtime-execution.v1",
                    "execution_status": "failed",
                    "data_outcome": "unknown",
                    "release_id": None,
                    "deployment_id": None,
                    "errors": [],
                },
                "evidence_write": None,
                "errors": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as invalid_exit:
        cli_main.main(
            [
                "gitops",
                "schema",
                "validate",
                "--kind",
                "dpone.safe-sample-runtime-run.v1",
                "--payload",
                str(payload_path),
                "--format",
                "json",
            ]
        )

    assert invalid_exit.value.code == 1


def _patch_cli(monkeypatch: pytest.MonkeyPatch, *, repo_root: Path) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(
            lambda logger: SimpleNamespace(
                logger=logger,
                settings=SimpleNamespace(repo_root=repo_root),
                fs=LocalFileSystem(),
            )
        ),
    )
