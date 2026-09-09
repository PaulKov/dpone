"""Fail closed unless live GitHub required checks pass for one release commit."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


github_api = _load_sibling("dpone_agent_pr_receipt_github_api", "pr_receipt_github_api.py")
models = _load_sibling("dpone_agent_release_commit_models", "release_commit_models.py")
evaluate = _load_sibling("dpone_agent_release_commit_evaluate", "release_commit_evaluate.py")
live_ruleset = _load_sibling("dpone_agent_governance_live_ruleset", "governance_live_ruleset.py")
DEFAULT_POLICY = Path(models.DEFAULT_POLICY_PATH)
RequiredContext = models.RequiredContext
Observation = models.Observation
LiveSnapshot = models.LiveSnapshot
Blocker = models.Blocker
GateReport = models.GateReport
evaluate_snapshot = evaluate.evaluate_snapshot
TERMINAL_BLOCKERS = models.TERMINAL_BLOCKERS
FULL_SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40}$")
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_POLL_ATTEMPTS = 10_000
TOKEN_MESSAGE = "An approved GitHub token is required for live release evidence."


def _frozen_policy() -> Any:
    return _load_sibling("dpone_agent_release_frozen_policy", "release_frozen_policy.py")


def _sealed_authority() -> Any:
    return _load_sibling("dpone_agent_release_authority_baseline", "release_authority_baseline.py")


def accepted_policy_path(path: Path) -> Path:
    """Load checkout-dependent policy parsing only for the CLI boundary."""

    return _frozen_policy().accepted_policy_path(path)


def load_frozen_policy(*, root: Path, commit_sha: str) -> tuple[Any, str]:
    """Load checkout-dependent policy parsing only for the CLI boundary."""

    return _frozen_policy().load_frozen_policy(root=root, commit_sha=commit_sha)


def load_sealed_authority() -> tuple[Any, str]:
    """Load the fixed stdlib-only authority pair sealed beside this entrypoint."""

    authority = _sealed_authority().load_sibling_release_authority(Path(__file__))
    return authority, authority.policy_sha256


def fetch_live_snapshot(*, repo: str, commit_sha: str, ruleset_id: int, token: str) -> Any:
    """Fetch the live ruleset plus both exact-commit evidence APIs."""
    ruleset = github_api.github_json(
        f"repos/{repo}/rulesets/{ruleset_id}",
        token=token,
        fresh=True,
    )
    endpoint = f"repos/{repo}/commits/{commit_sha}"
    check_payloads = github_api.github_paginated_items(
        f"{endpoint}/check-runs?filter=latest", token=token, array_key="check_runs"
    )
    status_payloads = github_api.github_paginated_items(f"{endpoint}/statuses", token=token)
    checks = tuple(_check_run(item, commit_sha) for item in check_payloads if item.get("name"))
    statuses = tuple(_commit_status(item, commit_sha) for item in status_payloads if item.get("context"))
    projection = live_ruleset.live_ruleset_projection(ruleset, ruleset_id=ruleset_id)
    return LiveSnapshot(
        str(ruleset.get("enforcement") or ""),
        _required_contexts(ruleset),
        (*checks, *statuses),
        projection,
    )


def _check_run(item: dict[str, Any], commit_sha: str) -> Any:
    app = item.get("app")
    status = str(item.get("status") or "unknown").lower()
    conclusion = item.get("conclusion")
    state = str(conclusion or "unknown").lower() if status == "completed" else status
    return Observation(
        context=str(item.get("name") or ""),
        source="check_run",
        evidence_id=_positive_id(item.get("id"), "check-run id"),
        state=str(state).lower(),
        integration_id=_positive_id(app.get("id"), "integration id", required=False) if isinstance(app, dict) else None,
        commit_sha=_matching_commit_sha(item.get("head_sha"), commit_sha),
    )


def _commit_status(item: dict[str, Any], commit_sha: str) -> Any:
    return Observation(
        context=str(item.get("context") or ""),
        source="status",
        evidence_id=_positive_id(item.get("id"), "commit-status id"),
        state=str(item.get("state") or "unknown").lower(),
        commit_sha=_matching_commit_sha(item.get("sha"), commit_sha),
    )


def _positive_id(value: Any, label: str, *, required: bool = True) -> int | None:
    parsed = None if isinstance(value, bool) or value is None else _integer_or_none(value)
    if (required and parsed is None) or (parsed is not None and parsed <= 0):
        raise ValueError(f"GitHub {label} must be a positive integer")
    return parsed


def _integer_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _matching_commit_sha(value: Any, expected: str) -> str:
    if not isinstance(value, str) or not FULL_SHA_PATTERN.fullmatch(value) or value.lower() != expected.lower():
        raise ValueError("live GitHub evidence commit SHA does not match the requested commit")
    return value.lower()


def _required_contexts(ruleset: dict[str, Any]) -> tuple[Any, ...]:
    rules = ruleset.get("rules")
    if not isinstance(rules, list):
        raise ValueError("live ruleset rules must be a list")
    contexts: set[Any] = set()
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters")
        checks = parameters.get("required_status_checks") if isinstance(parameters, dict) else None
        if not isinstance(checks, list):
            raise ValueError("live ruleset required_status_checks must be a list")
        for check in checks:
            if not isinstance(check, dict) or not isinstance(check.get("context"), str):
                raise ValueError("live required status check must have a context")
            context = check["context"].strip()
            if not context:
                raise ValueError("live required status check context must not be empty")
            app_id = _integer_or_none(check.get("integration_id"))
            app_id = app_id if app_id is not None and app_id > 0 else None
            contexts.add(RequiredContext(context, app_id))
    return tuple(sorted(contexts, key=lambda item: (item.name, item.integration_id or -1)))


def _poll_attempt_count(timeout_seconds: float, poll_interval_seconds: float) -> int | None:
    if not (
        math.isfinite(timeout_seconds)
        and timeout_seconds >= 0
        and math.isfinite(poll_interval_seconds)
        and poll_interval_seconds > 0
    ):
        return None
    quotient = timeout_seconds // poll_interval_seconds
    return MAX_POLL_ATTEMPTS if not math.isfinite(quotient) else min(int(quotient) + 1, MAX_POLL_ATTEMPTS)


def poll_release_gate(
    repo: str,
    commit_sha: str,
    ruleset_id: int,
    policy_contexts: tuple[str, ...],
    token: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
    fetcher: Callable[..., Any] = fetch_live_snapshot,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    *,
    policy_sha256: str = "",
    policy_projection: dict[str, Any] | None = None,
    sealed_authority: Any | None = None,
) -> Any:
    max_attempts = _poll_attempt_count(timeout_seconds, poll_interval_seconds)
    if max_attempts is None:
        return _failure_report(
            repo,
            commit_sha,
            ruleset_id,
            0,
            "INVALID_CONFIGURATION",
            "Polling configuration is invalid.",
            policy_sha256=policy_sha256,
        )
    deadline = clock() + timeout_seconds
    for attempt in range(1, max_attempts + 1):
        try:
            snapshot = fetcher(repo=repo, commit_sha=commit_sha, ruleset_id=ruleset_id, token=token)
            report = evaluate_snapshot(
                snapshot,
                repo,
                commit_sha,
                ruleset_id,
                attempt,
                policy_contexts,
                policy_sha256=policy_sha256,
                policy_projection=policy_projection,
            )
            if sealed_authority is not None:
                authority_blockers = _sealed_authority_blockers(snapshot, sealed_authority)
                if authority_blockers:
                    report = report._replace(
                        status="FAIL",
                        blockers=(*report.blockers, *authority_blockers),
                    )
        except (AttributeError, KeyError, OSError, OverflowError, RuntimeError, TypeError, ValueError) as exc:
            api_failure = isinstance(exc, (OSError, RuntimeError))
            code = "GITHUB_API_UNAVAILABLE" if api_failure else "LIVE_EVIDENCE_INVALID"
            message = "Live GitHub evidence unavailable." if api_failure else "Live GitHub evidence is invalid."
            report = _failure_report(repo, commit_sha, ruleset_id, attempt, code, message, policy_sha256=policy_sha256)
        terminal = any(blocker.code in TERMINAL_BLOCKERS for blocker in report.blockers)
        if report.status == "PASS" or terminal or attempt == max_attempts:
            return report
        remaining = deadline - clock()
        if remaining <= 0:
            return report
        sleeper(min(poll_interval_seconds, remaining))
    raise AssertionError("bounded polling must execute at least once")


def _sealed_authority_blockers(snapshot: Any, authority: Any) -> tuple[Any, ...]:
    """Compare every observable live ruleset field to the sealed baseline."""

    projection = getattr(snapshot, "ruleset_projection", None)
    if not isinstance(projection, dict):
        return (Blocker("SEALED_AUTHORITY_UNAVAILABLE", "Live ruleset authority projection is unavailable."),)
    observed = live_ruleset.observable_ruleset_projection(projection)
    blockers: list[Any] = []
    if observed != authority.projection:
        blockers.append(
            Blocker(
                "SEALED_AUTHORITY_DRIFT",
                "Live ruleset authority differs from the sealed canonical baseline.",
            )
        )
    for field in ("bypass_actors", "version_id"):
        value = projection.get(field)
        if value is not None and value != authority.privileged[field]:
            blockers.append(
                Blocker(
                    "SEALED_PRIVILEGED_AUTHORITY_DRIFT",
                    f"Live ruleset {field} differs from the sealed canonical baseline.",
                )
            )
    return tuple(blockers)


def _failure_report(
    repo: str,
    commit_sha: str,
    ruleset_id: int,
    attempts: int,
    code: str,
    message: str,
    *,
    policy_sha256: str = "",
) -> Any:
    return GateReport("FAIL", repo, commit_sha, ruleset_id, attempts, (), (Blocker(code, message),), policy_sha256)


def _config_errors(args: argparse.Namespace, ruleset_id: int | None, policy: Any) -> list[str]:
    bounded_attempts = _poll_attempt_count(args.timeout_seconds, args.poll_interval_seconds)
    policy_id = policy.ruleset_id if policy else None
    valid_sha = args.commit_sha and FULL_SHA_PATTERN.fullmatch(args.commit_sha)
    checks = (
        (not (args.repo and REPOSITORY_PATTERN.fullmatch(args.repo)), "repository must use the owner/name form"),
        (not valid_sha, "commit SHA must contain exactly 40 hexadecimal characters"),
        (policy is None, "frozen commit policy requirements are unavailable or invalid"),
        (ruleset_id is None or ruleset_id <= 0, "ruleset id must be a positive integer"),
        (
            args.ruleset_id is not None and args.ruleset_id != policy_id,
            "ruleset id must match the checked-in policy id",
        ),
        (
            bounded_attempts is None,
            "polling values must be finite and bounded; timeout must be non-negative "
            "and interval must be greater than zero",
        ),
    )
    return [message for invalid, message in checks if invalid]


def main(argv: list[str] | None = None) -> int:
    """Run the exact-commit gate and emit one credential-free JSON report."""
    parser = argparse.ArgumentParser(description=__doc__, exit_on_error=False)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--ruleset-id", type=int)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--sealed-authority", action="store_true")
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--timeout-seconds", type=float, default=0.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=10.0)
    try:
        args, unknown = parser.parse_known_args(argv)
        if unknown:
            raise ValueError("unsupported command-line argument")
    except (argparse.ArgumentError, ValueError):
        report = _failure_report("", "", 0, 0, "INVALID_CONFIGURATION", "Command-line arguments are invalid.")
        print(json.dumps(report.to_payload(), indent=2, sort_keys=True))
        return 1

    policy = None
    policy_sha256 = ""
    repo, commit_sha = args.repo or "", args.commit_sha or ""
    if commit_sha and FULL_SHA_PATTERN.fullmatch(commit_sha):
        try:
            if args.sealed_authority:
                if args.policy is not None:
                    raise ValueError("sealed authority rejects caller-selected policy paths")
                policy, policy_sha256 = load_sealed_authority()
            else:
                accepted_policy_path(args.policy or DEFAULT_POLICY)
                policy, policy_sha256 = load_frozen_policy(root=args.repo_root, commit_sha=commit_sha.lower())
        except (OSError, RuntimeError, TypeError, UnicodeError, ValueError):
            policy = None
            policy_sha256 = ""
    ruleset_id = args.ruleset_id if args.ruleset_id is not None else policy.ruleset_id if policy else None
    errors = _config_errors(args, ruleset_id, policy)
    if errors:
        report = _failure_report(
            repo,
            commit_sha,
            ruleset_id or 0,
            0,
            "INVALID_CONFIGURATION",
            "; ".join(errors),
            policy_sha256=policy_sha256,
        )
    elif not (token := os.environ.get(args.github_token_env)):
        assert policy is not None and ruleset_id is not None
        report = _failure_report(
            repo,
            commit_sha.lower(),
            ruleset_id,
            0,
            "GITHUB_TOKEN_UNAVAILABLE",
            TOKEN_MESSAGE,
            policy_sha256=policy_sha256,
        )
    else:
        assert policy is not None and ruleset_id is not None
        report = poll_release_gate(
            repo,
            commit_sha.lower(),
            ruleset_id,
            policy.context_names,
            token,
            args.timeout_seconds,
            args.poll_interval_seconds,
            policy_sha256=policy_sha256,
            policy_projection=getattr(policy, "ruleset_projection", None),
            sealed_authority=policy if args.sealed_authority else None,
        )
    print(json.dumps(report.to_payload(), indent=2, sort_keys=True))
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
