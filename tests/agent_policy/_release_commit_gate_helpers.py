from __future__ import annotations

import hashlib
import importlib
import json
import subprocess
from pathlib import Path
from typing import Any

COMMIT_SHA = "a" * 40
GITHUB_ACTIONS_APP_ID = 15368
CONTEXT = "Quality checks (3.12)"
REPO = "PaulKov/dpone"
RULESET_ID = 18806829
POLICY_DIGEST = "b" * 64
release_gate = importlib.import_module("tools.agent_policy.release_commit_gate")
frozen_policy = importlib.import_module("tools.agent_policy.release_frozen_policy")


def requirement(name: str = CONTEXT, app_id: int | None = GITHUB_ACTIONS_APP_ID) -> Any:
    return release_gate.RequiredContext(name, app_id)


def observation(
    *,
    context: str = CONTEXT,
    source: str = "check_run",
    state: str = "success",
    evidence_id: int = 1,
    integration_id: int | None = GITHUB_ACTIONS_APP_ID,
) -> Any:
    producer = integration_id if source == "check_run" else None
    return release_gate.Observation(context, source, evidence_id, state, producer, COMMIT_SHA)


def check_payload(
    *,
    name: str = CONTEXT,
    sha: str = COMMIT_SHA,
    evidence_id: int = 1,
    suite_id: int = 100,
    status: str = "completed",
    conclusion: str | None = "success",
) -> dict[str, Any]:
    return {
        "id": evidence_id,
        "name": name,
        "head_sha": sha,
        "status": status,
        "conclusion": conclusion,
        "app": {"id": GITHUB_ACTIONS_APP_ID},
        "check_suite": {"id": suite_id},
    }


def check(**kwargs: Any) -> Any:
    return release_gate._check_run(check_payload(**kwargs), COMMIT_SHA)


def snapshot(
    *,
    required_contexts: tuple[Any, ...] | None = None,
    observations: tuple[Any, ...] | None = None,
    enforcement: str = "active",
) -> Any:
    evidence = observations if observations is not None else (observation(),)
    required = required_contexts if required_contexts is not None else (requirement(),)
    return release_gate.LiveSnapshot(enforcement, required, evidence)


def evaluate(snapshot: Any, *, policy_contexts: tuple[str, ...] | None = None) -> Any:
    expected = policy_contexts
    if expected is None:
        expected = tuple(sorted({item.name for item in snapshot.required_contexts}))
    return release_gate.evaluate_snapshot(snapshot, REPO, COMMIT_SHA, RULESET_ID, 1, expected)


def codes(report: Any) -> list[str]:
    return [blocker.code for blocker in report.blockers]


def assert_codes(snapshot: Any, *expected: str) -> None:
    assert codes(evaluate(snapshot)) == list(expected)


def cli_args(
    token_env: str,
    *,
    ruleset_id: int | None = RULESET_ID,
    repo_root: Path | None = None,
    commit_sha: str = COMMIT_SHA,
    policy: Path | None = None,
) -> list[str]:
    args = [*f"--repo {REPO} --commit-sha {commit_sha}".split(), "--github-token-env", token_env]
    if ruleset_id is not None:
        args.extend(("--ruleset-id", str(ruleset_id)))
    if repo_root is not None:
        args.extend(("--repo-root", str(repo_root)))
    if policy is not None:
        args.extend(("--policy", str(policy)))
    return args


def policy_text(*contexts: str, ruleset_id: int = RULESET_ID) -> str:
    checks = "".join(f"      - {json.dumps(context)}\n" for context in contexts)
    return f"ruleset:\n  id: {ruleset_id}\n  branches:\n    - master\n  required_status_checks:\n    checks:\n{checks}"


def stub_frozen_policy(
    monkeypatch: Any,
    *contexts: str,
    ruleset_id: int = RULESET_ID,
    digest: str = POLICY_DIGEST,
) -> None:
    requirements = frozen_policy.PolicyRequirements(ruleset_id, tuple(sorted(contexts)), "master")
    monkeypatch.setattr(
        release_gate,
        "load_frozen_policy",
        lambda **_kwargs: (requirements, digest),
    )


def commit_policy_repo(tmp_path: Path, *contexts: str, ruleset_id: int = RULESET_ID) -> tuple[Path, str, str]:
    policy_path = tmp_path / frozen_policy.DEFAULT_POLICY
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    text = policy_text(*contexts, ruleset_id=ruleset_id)
    raw = text.encode("utf-8")
    policy_path.write_bytes(raw)
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "policy"], cwd=tmp_path, check=True, capture_output=True, text=True)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return tmp_path, commit, hashlib.sha256(raw).hexdigest()


def poll(fetcher: Any, sleeper: Any, clock: Any, *, timeout: float) -> Any:
    return release_gate.poll_release_gate(
        REPO,
        COMMIT_SHA,
        RULESET_ID,
        (CONTEXT,),
        "approved-token",
        timeout,
        2.0,
        fetcher,
        sleeper,
        clock,
        policy_sha256=POLICY_DIGEST,
    )


def run_cli(args: list[str], capsys: Any) -> tuple[int, Any, dict[str, Any]]:
    code = release_gate.main(args)
    captured = capsys.readouterr()
    return code, captured, json.loads(captured.out)


def ruleset(*contexts: str, integration_id: Any = GITHUB_ACTIONS_APP_ID) -> dict[str, Any]:
    checks = [{"context": context, "integration_id": integration_id} for context in contexts]
    rule = {"type": "required_status_checks", "parameters": {"required_status_checks": checks}}
    return {"enforcement": "active", "rules": [rule]}
