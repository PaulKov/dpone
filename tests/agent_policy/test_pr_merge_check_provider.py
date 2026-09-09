from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from tests.agent_policy.test_pr_merge_check import (
    INTEGRATION_SHA,
    REPOSITORY,
    _event,
    _exit_code,
    _receipt,
    _write_json,
    merge_check,
)


def _projection(tmp_path: Path) -> Any:
    return merge_check.derive_projection(
        event=_event(),
        receipt_path=_write_json(tmp_path / "receipt.json", _receipt()),
        producer_exit_code_path=_exit_code(tmp_path),
        repository=REPOSITORY,
        run_id="101",
        run_attempt="1",
    )


def _initial_response(**override: Any) -> dict[str, Any]:
    return {
        "id": 42,
        "name": "Agent PR receipt",
        "head_sha": INTEGRATION_SHA,
        "status": "completed",
        "conclusion": "failure",
        "details_url": f"https://github.com/{REPOSITORY}/runs/42",
        "external_id": "agent-pr-merge-closure:101:1",
        "app": {"id": 15368, "slug": "github-actions"},
        **override,
    }


@pytest.mark.parametrize(
    "override",
    [
        {"details_url": "https://github.com/PaulKov/dpone/actions/runs/999"},
        {"details_url": "https://github.com/PaulKov/dpone/runs/999"},
        {"details_url": "https://github.com/PaulKov/dpone/runs/42?attempt=1"},
        {"details_url": "https://github.com/other/dpone/runs/42"},
        {"external_id": "agent-pr-merge-closure:999:1"},
        {"app": {"id": 15368, "slug": "untrusted"}},
    ],
)
def test_publish_rejects_unbound_provider_identity(tmp_path: Path, override: dict[str, Any]) -> None:
    with pytest.raises(RuntimeError, match="response|identity"):
        merge_check.publish_projection(
            _projection(tmp_path),
            repository=REPOSITORY,
            token="approved-token",
            poster=Mock(return_value=_initial_response(**override)),
            updater=Mock(),
        )


def test_publish_accepts_exact_requested_actions_url(tmp_path: Path) -> None:
    requested_url = f"https://github.com/{REPOSITORY}/actions/runs/101"
    initial = _initial_response(details_url=requested_url)
    success = {**initial, "conclusion": "success"}

    assert merge_check.publish_projection(
        _projection(tmp_path),
        repository=REPOSITORY,
        token="approved-token",
        poster=Mock(return_value=initial),
        updater=Mock(return_value=success),
    ) == (42, 15368)


@pytest.mark.parametrize("drift", ["external_id", "check_run_id", "app_id"])
def test_invalid_success_update_is_rolled_back_to_failure(tmp_path: Path, drift: str) -> None:
    initial = _initial_response()
    invalid_success = {**initial, "conclusion": "success"}
    if drift == "external_id":
        invalid_success["external_id"] = "wrong"
    elif drift == "check_run_id":
        invalid_success.update(
            id=43,
            details_url=f"https://github.com/{REPOSITORY}/runs/43",
        )
    else:
        invalid_success["app"] = {"id": 999, "slug": "github-actions"}
    updater = Mock(side_effect=[invalid_success, initial])

    with pytest.raises(RuntimeError, match="response|identity"):
        merge_check.publish_projection(
            _projection(tmp_path),
            repository=REPOSITORY,
            token="approved-token",
            poster=Mock(return_value=initial),
            updater=updater,
        )

    assert updater.call_count == 2
    assert updater.call_args_list[1].kwargs["payload"]["conclusion"] == "failure"


def test_failure_projection_exits_nonzero_after_publishing_failure(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    event = _write_json(tmp_path / "event.json", _event())
    receipt = _write_json(tmp_path / "receipt.json", _receipt())
    monkeypatch.setenv("TEST_TOKEN", "approved-token")
    monkeypatch.setattr(merge_check, "publish_projection", Mock(return_value=(42, 15368)))

    exit_code = merge_check.main(
        [
            "--event",
            str(event),
            "--receipt",
            str(receipt),
            "--producer-exit-code",
            str(_exit_code(tmp_path)),
            "--repository",
            REPOSITORY,
            "--run-id",
            "101",
            "--run-attempt",
            "2",
            "--github-token-env",
            "TEST_TOKEN",
        ]
    )

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAIL"
