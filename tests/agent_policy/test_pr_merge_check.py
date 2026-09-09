from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_SHA = "d" * 40
REVIEWED_HEAD_SHA = "a" * 40
REPOSITORY = "PaulKov/dpone"


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


merge_receipt = _load("dpone_agent_pr_merge_receipt_check_test", "tools/agent_policy/pr_merge_receipt.py")
merge_check = _load("dpone_agent_pr_merge_check_test", "tools/agent_policy/pr_merge_check.py")


def _event(*, integration_sha: str = INTEGRATION_SHA) -> dict[str, Any]:
    return {
        "action": "closed",
        "pull_request": {
            "merged": True,
            "merge_commit_sha": integration_sha,
            "base": {
                "ref": "master",
                "repo": {"full_name": REPOSITORY},
            },
        },
        "repository": {"full_name": REPOSITORY},
    }


def _receipt(*, status: str = "PASS", integration_sha: str = INTEGRATION_SHA) -> dict[str, Any]:
    if status == "FAIL":
        return merge_receipt.failure_payload(
            repository=REPOSITORY,
            integration_commit_sha=integration_sha,
            producer=merge_receipt.Producer("Agent PR receipt", "101", "1"),
            error="source evidence unavailable",
        )
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "binding_id": f"sha256:{'0' * 64}",
        "repository": REPOSITORY,
        "protected_base_ref": "master",
        "pr_number": 455,
        "merged_at": "2026-07-29T00:00:00Z",
        "integration_method": "merge",
        "reviewed_head_sha": REVIEWED_HEAD_SHA,
        "reviewed_head_tree": "b" * 40,
        "base_parent_sha": "c" * 40,
        "integration_commit_sha": integration_sha,
        "integration_tree": "b" * 40,
        "changed_paths": [],
        "pr_body_sha256": f"sha256:{'1' * 64}",
        "source_receipt": {
            "check_run_id": 1,
            "workflow_run_id": 2,
            "workflow_run_attempt": 1,
            "artifact_id": 3,
            "artifact_digest": f"sha256:{'2' * 64}",
            "artifact_size_bytes": 1,
            "archive_sha256": f"sha256:{'3' * 64}",
            "archive_size_bytes": 1,
            "created_at": "2026-07-29T00:00:00Z",
            "completed_at": "2026-07-29T00:00:00Z",
            "receipt_sha256": f"sha256:{'4' * 64}",
            "audit_manifest_sha256": f"sha256:{'5' * 64}",
            "body_sha256": f"sha256:{'1' * 64}",
            "status": "PASS",
        },
        "producer": {
            "workflow": "Agent PR receipt",
            "run_id": "101",
            "run_attempt": "1",
        },
        "errors": [],
        "warnings": [],
    }
    payload["binding_id"] = merge_receipt.compute_binding_id(payload)
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _exit_code(tmp_path: Path, value: str = "0") -> Path:
    path = tmp_path / "producer-exit-code.txt"
    path.write_text(f"{value}\n", encoding="utf-8")
    return path


def test_pass_receipt_projects_required_check_to_exact_integration_commit(tmp_path: Path) -> None:
    receipt_path = _write_json(tmp_path / "receipt.json", _receipt())

    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=receipt_path,
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )

    assert projection.conclusion == "success"
    assert projection.integration_commit_sha == INTEGRATION_SHA
    assert projection.blocker_codes == ()
    assert projection.request["name"] == "Agent PR receipt"
    assert projection.request["head_sha"] == INTEGRATION_SHA
    assert projection.request["external_id"] == "agent-pr-merge-closure:101:1"
    assert projection.request["details_url"].endswith("/actions/runs/101")


@pytest.mark.parametrize(
    ("receipt_payload", "expected_code"),
    [
        (_receipt(status="FAIL"), "MERGE_RECEIPT_FAILED"),
        (_receipt(integration_sha="e" * 40), "INTEGRATION_SHA_MISMATCH"),
        ({"status": "PASS"}, "MERGE_RECEIPT_INVALID"),
    ],
)
def test_non_authoritative_receipt_can_only_publish_failure(
    tmp_path: Path,
    receipt_payload: dict[str, Any],
    expected_code: str,
) -> None:
    receipt_path = _write_json(tmp_path / "receipt.json", receipt_payload)

    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=receipt_path,
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )

    assert projection.conclusion == "failure"
    assert expected_code in projection.blocker_codes
    assert projection.request["head_sha"] == INTEGRATION_SHA


def test_missing_receipt_projects_failure_without_reconstructing_pass(tmp_path: Path) -> None:
    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=tmp_path / "missing.json",
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )

    assert projection.conclusion == "failure"
    assert projection.blocker_codes == ("MERGE_RECEIPT_UNAVAILABLE",)


@pytest.mark.parametrize(
    ("receipt_override", "run_attempt", "expected_code"),
    [
        ({"binding_id": f"sha256:{'f' * 64}"}, "1", "BINDING_ID_MISMATCH"),
        ({}, "2", "PRODUCER_IDENTITY_MISMATCH"),
    ],
)
def test_stale_or_tampered_receipt_can_only_project_failure(
    tmp_path: Path,
    receipt_override: dict[str, Any],
    run_attempt: str,
    expected_code: str,
) -> None:
    receipt = {**_receipt(), **receipt_override}
    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", receipt),
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt=run_attempt,
    )

    assert projection.conclusion == "failure"
    assert expected_code in projection.blocker_codes


@pytest.mark.parametrize(
    ("exit_code_path", "expected_code"),
    [
        ("missing", "PRODUCER_EXIT_CODE_UNAVAILABLE"),
        ("nonzero", "PRODUCER_EXIT_CODE_MISMATCH"),
    ],
)
def test_success_receipt_cannot_hide_unsuccessful_producer_exit(
    tmp_path: Path,
    exit_code_path: str,
    expected_code: str,
) -> None:
    producer_exit = tmp_path / "missing-exit-code.txt"
    if exit_code_path == "nonzero":
        producer_exit = _exit_code(tmp_path, "1")

    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", _receipt()),
        producer_exit_code_path=producer_exit,
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )

    assert projection.conclusion == "failure"
    assert projection.blocker_codes == (expected_code,)


@pytest.mark.parametrize(
    "event",
    [
        {"action": "edited", "pull_request": {"merged": True}},
        _event(integration_sha="short"),
        {
            **_event(),
            "pull_request": {
                **_event()["pull_request"],
                "base": {"ref": "master", "repo": {"full_name": "other/repo"}},
            },
        },
    ],
)
def test_event_is_the_only_integration_commit_authority(event: dict[str, Any], tmp_path: Path) -> None:
    receipt_path = _write_json(tmp_path / "receipt.json", _receipt())

    with pytest.raises(ValueError, match="event"):
        merge_check.derive_projection(
            event=event,
            receipt_path=receipt_path,
            producer_exit_code_path=_exit_code(tmp_path),
            repository=REPOSITORY,
            run_id="101",
            run_attempt="1",
        )


def test_publish_validates_exact_github_response(tmp_path: Path) -> None:
    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", _receipt()),
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )
    poster = Mock(
        return_value={
            "id": 42,
            "name": "Agent PR receipt",
            "head_sha": INTEGRATION_SHA,
            "status": "completed",
            "conclusion": "failure",
            "details_url": f"https://github.com/{REPOSITORY}/runs/42",
            "external_id": "agent-pr-merge-closure:101:1",
            "app": {"id": 15368, "slug": "github-actions"},
        }
    )
    updater = Mock(
        return_value={
            "id": 42,
            "name": "Agent PR receipt",
            "head_sha": INTEGRATION_SHA,
            "status": "completed",
            "conclusion": "success",
            "details_url": f"https://github.com/{REPOSITORY}/runs/42",
            "external_id": "agent-pr-merge-closure:101:1",
            "app": {"id": 15368, "slug": "github-actions"},
        }
    )

    check_run_id, app_id = merge_check.publish_projection(
        projection,
        repository=REPOSITORY,
        token="approved-token",
        poster=poster,
        updater=updater,
    )

    initial_request = poster.call_args.kwargs["payload"]
    assert initial_request["head_sha"] == INTEGRATION_SHA
    assert initial_request["conclusion"] == "failure"
    update_request = updater.call_args.kwargs["payload"]
    assert "head_sha" not in update_request
    assert update_request["conclusion"] == "success"
    assert (check_run_id, app_id) == (42, 15368)


def test_publish_rejects_provider_response_for_another_commit(tmp_path: Path) -> None:
    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", _receipt()),
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )
    poster = Mock(
        return_value={
            "id": 42,
            "name": "Agent PR receipt",
            "head_sha": "e" * 40,
            "status": "completed",
            "conclusion": "failure",
            "details_url": f"https://github.com/{REPOSITORY}/actions/runs/101",
            "external_id": "agent-pr-merge-closure:101:1",
            "app": {"id": 15368, "slug": "github-actions"},
        }
    )
    updater = Mock()

    with pytest.raises(RuntimeError, match="response"):
        merge_check.publish_projection(
            projection,
            repository=REPOSITORY,
            token="approved-token",
            poster=poster,
            updater=updater,
        )
    updater.assert_not_called()


def test_failure_projection_never_attempts_success_update(tmp_path: Path) -> None:
    projection = merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", _receipt(status="FAIL")),
        producer_exit_code_path=_exit_code(tmp_path, "1"),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )
    poster = Mock(
        return_value={
            "id": 42,
            "name": "Agent PR receipt",
            "head_sha": INTEGRATION_SHA,
            "status": "completed",
            "conclusion": "failure",
            "details_url": f"https://github.com/{REPOSITORY}/runs/42",
            "external_id": "agent-pr-merge-closure:101:1",
            "app": {"id": 15368, "slug": "github-actions"},
        }
    )
    updater = Mock()

    merge_check.publish_projection(
        projection,
        repository=REPOSITORY,
        token="approved-token",
        poster=poster,
        updater=updater,
    )

    updater.assert_not_called()
