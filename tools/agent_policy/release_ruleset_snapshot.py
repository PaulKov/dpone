"""Capture a canonical live ruleset projection bound to exact release checks."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

MAX_REPORT_BYTES = 4 * 1024 * 1024
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


github_api = _load_sibling("dpone_release_ruleset_github_api", "pr_receipt_github_api.py")
live_ruleset = _load_sibling("dpone_release_ruleset_projection", "governance_live_ruleset.py")


def _sealed_authority() -> Any:
    return _load_sibling("dpone_release_ruleset_authority", "release_authority_baseline.py")


def _yaml_policy() -> Any:
    return _load_sibling("dpone_release_ruleset_yaml_policy", "release_ruleset_yaml_policy.py")


def _bounded_report(path: Path) -> tuple[dict[str, Any], bytes]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("required-check report must be one regular file")
    size = path.stat().st_size
    if size <= 0 or size > MAX_REPORT_BYTES:
        raise ValueError("required-check report size is outside its closed limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_REPORT_BYTES + 1)
    if len(raw) != size or len(raw) > MAX_REPORT_BYTES:
        raise ValueError("required-check report changed while read or exceeded its limit")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("required-check report must be one UTF-8 JSON object") from exc
    if not isinstance(payload, dict):
        raise ValueError("required-check report must be one JSON object")
    return payload, raw


def _policy_projection(
    path: Path,
    *,
    ruleset_id: int,
    expected_checks: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    """Build the legacy checked-out YAML policy projection on demand."""

    return _yaml_policy().policy_projection(path, ruleset_id=ruleset_id, expected_checks=expected_checks)


def _sealed_policy_projection(
    *,
    ruleset_id: int,
    expected_checks: list[dict[str, Any]],
) -> tuple[dict[str, Any], str, str]:
    """Load the canonical stdlib-only authority sealed beside this entrypoint."""

    authority = _sealed_authority().load_sibling_release_authority(Path(__file__))
    expected_producers = {str(item["context"]): item["integration_id"] for item in expected_checks}
    if authority.ruleset_id != ruleset_id:
        raise ValueError("sealed authority ruleset id differs from release evidence")
    if authority.integration_ids != expected_producers:
        raise ValueError("required-check receipt producers differ from sealed authority")
    return authority.full_projection, authority.policy_sha256, authority.policy_projection_sha256


def capture(
    *,
    repository: str,
    commit_sha: str,
    required_check_report: Path,
    policy: Path | None,
    token: str,
    sealed_authority: bool = False,
) -> dict[str, Any]:
    """Return one canonical ruleset projection cross-bound to required checks."""

    if REPOSITORY.fullmatch(repository) is None or FULL_SHA.fullmatch(commit_sha) is None:
        raise ValueError("release identity is invalid")
    if sealed_authority and policy is not None:
        raise ValueError("sealed authority rejects caller-selected policy paths")
    if not sealed_authority and policy is None:
        raise ValueError("branch-protection policy is required")
    report, report_raw = _bounded_report(required_check_report)
    if (
        report.get("schema_version") != 1
        or report.get("status") != "PASS"
        or report.get("decision") != "GO"
        or report.get("repository") != repository
        or report.get("commit_sha") != commit_sha
        or report.get("blockers") != []
    ):
        raise ValueError("required-check report does not authorize the release commit")
    ruleset_id = report.get("ruleset_id")
    if isinstance(ruleset_id, bool) or not isinstance(ruleset_id, int) or ruleset_id <= 0:
        raise ValueError("required-check report ruleset id is invalid")
    contexts = report.get("contexts")
    expected_checks: list[dict[str, Any]] = []
    if not isinstance(contexts, list) or not contexts:
        raise ValueError("required-check report contexts are unavailable")
    for context in contexts:
        if not isinstance(context, dict) or context.get("status") != "PASS":
            raise ValueError("required-check report context is not successful")
        name = context.get("context")
        integration_id = context.get("integration_id")
        if (
            not isinstance(name, str)
            or not name
            or isinstance(integration_id, bool)
            or not isinstance(integration_id, int)
            or integration_id <= 0
        ):
            raise ValueError("required-check report producer identity is invalid")
        expected_checks.append({"context": name, "integration_id": integration_id})
    expected_checks.sort(key=lambda item: str(item["context"]))
    if len({str(item["context"]) for item in expected_checks}) != len(expected_checks):
        raise ValueError("required-check report contexts are ambiguous")
    if sealed_authority:
        expected_projection, policy_sha256, policy_projection_sha256 = _sealed_policy_projection(
            ruleset_id=ruleset_id,
            expected_checks=expected_checks,
        )
        if report.get("policy_path") != ".agents/policy/github-branch-protection.yml":
            raise ValueError("required-check report policy path differs from sealed authority")
    else:
        assert policy is not None
        expected_projection, policy_sha256 = _policy_projection(
            policy,
            ruleset_id=ruleset_id,
            expected_checks=expected_checks,
        )
        policy_projection_sha256 = hashlib.sha256(
            json.dumps(expected_projection, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
    if report.get("policy_sha256") != policy_sha256:
        raise ValueError("required-check report does not bind the frozen branch-protection policy")
    if not token:
        raise ValueError("approved GitHub token is required")
    live_payload = github_api.github_json(
        f"repos/{repository}/rulesets/{ruleset_id}",
        token=token,
        fresh=True,
    )
    live_projection = live_ruleset.live_ruleset_projection(live_payload, ruleset_id=ruleset_id)
    projection = live_ruleset.observable_ruleset_projection(live_projection)
    expected_observable = live_ruleset.observable_ruleset_projection(expected_projection)
    if projection != expected_observable:
        raise ValueError("live ruleset projection does not match frozen release authority")
    for privileged_field in ("bypass_actors", "version_id"):
        observed = live_projection.get(privileged_field)
        if observed is not None and observed != expected_projection[privileged_field]:
            raise ValueError(f"live ruleset {privileged_field} differs from frozen release authority")
    return {
        "schema": "dpone.release_ruleset_snapshot.v1",
        "status": "PASS",
        "decision": "GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "ruleset_id": ruleset_id,
        "policy_sha256": policy_sha256,
        "policy_projection_sha256": policy_projection_sha256,
        "privileged_baseline": {
            "bypass_actors": expected_projection["bypass_actors"],
            "updated_at": expected_projection["updated_at"],
            "version_id": expected_projection["version_id"],
        },
        "privileged_fields_observed": {
            "bypass_actors": live_projection.get("bypass_actors") is not None,
            "version_id": live_projection.get("version_id") is not None,
        },
        "required_check_report_sha256": hashlib.sha256(report_raw).hexdigest(),
        "projection": projection,
        "blockers": [],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--required-check-report", required=True, type=Path)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--sealed-authority", action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    args = parser.parse_args(argv)
    try:
        payload = capture(
            repository=args.repo,
            commit_sha=args.commit_sha,
            required_check_report=args.required_check_report,
            policy=args.policy,
            token=os.environ.get(args.github_token_env, ""),
            sealed_authority=args.sealed_authority,
        )
    except (OSError, RuntimeError, TypeError, UnicodeError, ValueError) as exc:
        payload = {
            "schema": "dpone.release_ruleset_snapshot.v1",
            "status": "FAIL",
            "decision": "NO-GO",
            "repository": args.repo,
            "commit_sha": args.commit_sha,
            "blockers": [{"code": "RULESET_SNAPSHOT_INVALID", "message": str(exc)}],
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["capture"]
