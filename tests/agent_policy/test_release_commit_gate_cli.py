from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from tests.agent_policy._release_commit_gate_helpers import (
    CONTEXT,
    POLICY_DIGEST,
    RULESET_ID,
    check_payload,
    cli_args,
    commit_policy_repo,
    evaluate,
    frozen_policy,
    policy_text,
    release_gate,
    ruleset,
    run_cli,
    snapshot,
    stub_frozen_policy,
)


def test_cli_without_approved_token_emits_json_failure(monkeypatch: Any, capsys: Any) -> None:
    token_env = "SECRET_VALUE_MUST_NOT_APPEAR"
    monkeypatch.delenv(token_env, raising=False)
    stub_frozen_policy(monkeypatch, CONTEXT)
    code, captured, payload = run_cli(cli_args(token_env), capsys)
    assert code == 1
    assert payload["status"] == "FAIL"
    assert payload["decision"] == "NO-GO"
    assert payload["policy_sha256"] == POLICY_DIGEST
    assert [item["code"] for item in payload["blockers"]] == ["GITHUB_TOKEN_UNAVAILABLE"]
    assert token_env not in captured.out


def test_cli_returns_zero_for_a_complete_live_success(monkeypatch: Any, capsys: Any) -> None:
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    monkeypatch.setenv(token_env, "approved-token")
    stub_frozen_policy(monkeypatch, CONTEXT)

    def _poll_ok(*_args: Any, **kwargs: Any) -> Any:
        report = evaluate(snapshot(), policy_contexts=(CONTEXT,))
        return report._replace(policy_sha256=str(kwargs.get("policy_sha256") or POLICY_DIGEST))

    monkeypatch.setattr(release_gate, "poll_release_gate", _poll_ok)
    code, _, payload = run_cli(cli_args(token_env), capsys)
    assert code == 0
    assert payload["status"] == "PASS"
    assert payload["decision"] == "GO"
    assert payload["policy_sha256"] == POLICY_DIGEST
    assert "approved-token" not in json.dumps(payload)


def test_reduced_live_ruleset_cannot_bypass_checked_in_context_policy(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    stub_frozen_policy(monkeypatch, CONTEXT, "Build")
    monkeypatch.setenv(token_env, "approved-token")
    live = ruleset(CONTEXT)
    monkeypatch.setattr(release_gate.github_api, "github_json", Mock(return_value=live))
    monkeypatch.setattr(
        release_gate.github_api,
        "github_paginated_items",
        Mock(side_effect=[[check_payload()], []]),
    )
    code, _, payload = run_cli(cli_args(token_env, ruleset_id=None), capsys)
    assert code == 1
    assert payload["policy_sha256"] == POLICY_DIGEST
    assert [item["code"] for item in payload["blockers"]] == ["RULESET_POLICY_DRIFT"]


def test_explicit_ruleset_id_cannot_bypass_checked_in_policy_id(monkeypatch: Any, capsys: Any) -> None:
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    stub_frozen_policy(monkeypatch, CONTEXT)
    poll = Mock(return_value=evaluate(snapshot()))
    monkeypatch.setenv(token_env, "approved-token")
    monkeypatch.setattr(release_gate, "poll_release_gate", poll)
    code, _, payload = run_cli(cli_args(token_env, ruleset_id=RULESET_ID + 1), capsys)
    assert code == 1
    assert [item["code"] for item in payload["blockers"]] == ["INVALID_CONFIGURATION"]
    poll.assert_not_called()


def test_frozen_commit_policy_digest_ignores_worktree_mutation(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    root, commit, digest = commit_policy_repo(tmp_path, CONTEXT, "Build")
    (root / frozen_policy.DEFAULT_POLICY).write_text(policy_text(CONTEXT), encoding="utf-8")
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    monkeypatch.setenv(token_env, "approved-token")
    live = ruleset(CONTEXT)
    monkeypatch.setattr(release_gate.github_api, "github_json", Mock(return_value=live))
    monkeypatch.setattr(
        release_gate.github_api,
        "github_paginated_items",
        Mock(side_effect=[[check_payload(sha=commit)], []]),
    )
    code, _, payload = run_cli(
        cli_args(token_env, ruleset_id=None, repo_root=root, commit_sha=commit),
        capsys,
    )
    assert code == 1
    assert payload["policy_sha256"] == digest
    assert [item["code"] for item in payload["blockers"]] == ["RULESET_POLICY_DRIFT"]


def test_caller_selected_policy_path_is_rejected(monkeypatch: Any, capsys: Any, tmp_path: Path) -> None:
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    monkeypatch.setenv(token_env, "approved-token")
    poll = Mock()
    monkeypatch.setattr(release_gate, "poll_release_gate", poll)
    code, _, payload = run_cli(
        cli_args(token_env, policy=tmp_path / "alternate-policy.yml"),
        capsys,
    )
    assert code == 1
    assert [item["code"] for item in payload["blockers"]] == ["INVALID_CONFIGURATION"]
    assert "frozen commit policy" in payload["blockers"][0]["message"]
    poll.assert_not_called()


@pytest.mark.parametrize(
    ("extra_args", "message", "with_base"),
    [
        (["--commit-sha", "abc1234"], "40 hexadecimal", True),
        (["--timeout-seconds", "-1"], "non-negative", True),
        (["--timeout-seconds", "nan"], "finite", True),
        (["--poll-interval-seconds", "0"], "greater than zero", True),
        (["--ruleset-id", "0"], "positive integer", True),
        (["--ruleset-id", "not-an-id"], "", False),
    ],
)
def test_cli_invalid_configuration_fails_as_json(
    extra_args: list[str],
    message: str,
    with_base: bool,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    token_env = "DPONE_TEST_RELEASE_GATE_TOKEN"
    monkeypatch.setenv(token_env, "approved-token")
    stub_frozen_policy(monkeypatch, CONTEXT)
    args = [*cli_args(token_env), *extra_args] if with_base else extra_args
    code, captured, payload = run_cli(args, capsys)
    assert code == 1
    assert captured.err == ""
    assert [item["code"] for item in payload["blockers"]] == ["INVALID_CONFIGURATION"]
    assert message in payload["blockers"][0]["message"]
    assert "approved-token" not in json.dumps(payload)
